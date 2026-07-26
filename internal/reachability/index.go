// Package reachability maintains bounded, derived closures for recursive
// tuple-to-userset relations.
package reachability

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/RoaringBitmap/roaring/v2"
	"go.uber.org/zap"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/tuple"
)

type indexKey struct {
	storeID    string
	objectType string
	relation   string
}

type snapshot struct {
	ids       map[string]uint32
	ancestors []*roaring.Bitmap
}

type state uint8

const (
	stateEmpty state = iota
	stateBuilding
	stateReady
	stateDisabled
)

type entry struct {
	mu          sync.Mutex
	generation  uint64
	buildID     uint64
	state       state
	snapshot    *snapshot
	cancel      context.CancelFunc
	publishedAt time.Time // set when a ready snapshot or disabled sentinel is published
}

// pollState throttles changelog polls for one store. Its fields are guarded
// by its own mutex; the inflight flag guarantees at most one poll per store
// at a time.
type pollState struct {
	mu          sync.Mutex
	inflight    bool
	lastPolled  time.Time
	initialized bool
	newestSeen  time.Time
}

// defaultSnapshotTTL aligns with the check query cache controller TTL: both
// implement the same bounded-staleness contract for multi-instance
// deployments.
const defaultSnapshotTTL = 10 * time.Second

// Index is an in-memory, rebuildable reachability index. Its contents are
// derived from direct tuples and are never used when construction exceeds a
// configured bound.
//
// Freshness is maintained by three cooperating mechanisms:
//   - local mutation guards (BeginMutation / BeginStoreMutation) invalidate
//     precisely and immediately for writes handled by this process;
//   - an optional changelog poll (WithChangelog) invalidates keys touched by
//     writes from other processes, best effort;
//   - a snapshot TTL (WithSnapshotTTL) is the correctness bound: a snapshot
//     older than the TTL is never served and is rebuilt from live data.
type Index struct {
	mu                sync.Mutex // protects entries, storeGenerations, storeLocks, and pollStates
	entries           map[indexKey]*entry
	effectiveEntries  map[effectiveIndexKey]*effectiveEntry
	effectivePlans    map[string]*effectiveModelPlan
	storeGenerations  map[string]uint64
	storeLocks        map[string]*sync.Mutex
	pollStates        map[string]*pollState
	maxNodes          int
	maxClosureEntries uint64
	buildTimeout      time.Duration
	snapshotTTL       time.Duration
	changelog         storage.ChangelogBackend
	pollInterval      time.Duration
	logger            logger.Logger
}

// Option configures an Index.
type Option func(*Index)

// WithMaxNodes bounds the number of objects in one indexed graph.
func WithMaxNodes(maxNodes int) Option {
	return func(index *Index) {
		index.maxNodes = maxNodes
	}
}

// WithMaxClosureEntries bounds the sum of all ancestor bitmap cardinalities.
func WithMaxClosureEntries(maxEntries uint64) Option {
	return func(index *Index) {
		index.maxClosureEntries = maxEntries
	}
}

// WithBuildTimeout bounds one detached index build.
func WithBuildTimeout(timeout time.Duration) Option {
	return func(index *Index) {
		index.buildTimeout = timeout
	}
}

// WithSnapshotTTL bounds how long a ready snapshot or a disabled sentinel may
// be served after it was published. An expired entry is treated like an
// absent one: checks fall back to the normal resolver while a detached
// rebuild runs. A zero or negative TTL disables expiry; that is only safe
// when every tuple mutation is guaranteed to pass through this process's
// mutation guard.
func WithSnapshotTTL(ttl time.Duration) Option {
	return func(index *Index) {
		index.snapshotTTL = ttl
	}
}

// WithChangelog enables best-effort invalidation from the store changelog.
// Before serving index results for a store, the index polls ReadChanges at
// most once per poll interval and invalidates the keys touched by changes it
// has not seen yet. This is an optimization only: ULIDs are not
// commit-ordered and reads may hit lagging replicas, so a poll can miss a
// recent change. The snapshot TTL remains the correctness bound.
func WithChangelog(changelog storage.ChangelogBackend) Option {
	return func(index *Index) {
		index.changelog = changelog
	}
}

// WithChangelogPollInterval overrides the minimum interval between two
// changelog polls for the same store. It defaults to half the snapshot TTL.
func WithChangelogPollInterval(interval time.Duration) Option {
	return func(index *Index) {
		index.pollInterval = interval
	}
}

// WithLogger sets the logger used for best-effort paths such as changelog
// polling.
func WithLogger(l logger.Logger) Option {
	return func(index *Index) {
		index.logger = l
	}
}

// New returns a bounded, empty reachability index.
func New(options ...Option) *Index {
	index := &Index{
		entries:           make(map[indexKey]*entry),
		effectiveEntries:  make(map[effectiveIndexKey]*effectiveEntry),
		effectivePlans:    make(map[string]*effectiveModelPlan),
		storeGenerations:  make(map[string]uint64),
		storeLocks:        make(map[string]*sync.Mutex),
		pollStates:        make(map[string]*pollState),
		maxNodes:          100_000,
		maxClosureEntries: 1_000_000,
		buildTimeout:      30 * time.Second,
		snapshotTTL:       defaultSnapshotTTL,
		logger:            logger.NewNoopLogger(),
	}

	for _, option := range options {
		option(index)
	}

	if index.pollInterval <= 0 {
		if index.snapshotTTL > 0 {
			index.pollInterval = index.snapshotTTL / 2
		} else {
			index.pollInterval = defaultSnapshotTTL / 2
		}
	}

	return index
}

// Mutation guards the index keys affected by a tuple mutation.
type Mutation struct {
	locks     []*sync.Mutex
	storeLock *sync.Mutex
	once      sync.Once
}

// End releases the mutation guard. It is safe to call multiple times.
func (m *Mutation) End() {
	m.once.Do(func() {
		for i := len(m.locks) - 1; i >= 0; i-- {
			m.locks[i].Unlock()
		}
		if m.storeLock != nil {
			m.storeLock.Unlock()
		}
	})
}

// BeginMutation invalidates only the index keys that could be affected by the
// supplied writes and deletes. It must remain held until the datastore mutation
// has completed, including a failed mutation, so a concurrent build cannot
// publish a pre-write snapshot after the write has committed.
func (i *Index) BeginMutation(
	storeID string,
	writes []*openfgav1.TupleKey,
	deletes []*openfgav1.TupleKeyWithoutCondition,
) *Mutation {
	storeLock := i.getStoreLock(storeID)
	storeLock.Lock()

	keys := affectedKeys(storeID, writes, deletes)
	entries := make([]*entry, 0, len(keys))
	for _, key := range keys {
		entries = append(entries, i.getEntry(key))
	}

	i.mu.Lock()
	effectiveKeys := make([]effectiveIndexKey, 0)
	for key := range i.effectiveEntries {
		if key.storeID == storeID {
			effectiveKeys = append(effectiveKeys, key)
		}
	}
	i.mu.Unlock()
	sort.Slice(effectiveKeys, func(a, b int) bool {
		return effectiveKeys[a].modelID < effectiveKeys[b].modelID
	})

	locks := make([]*sync.Mutex, 0, len(entries)+len(effectiveKeys))
	for _, entry := range entries {
		entry.mu.Lock()
		resetEntry(entry)
		locks = append(locks, &entry.mu)
	}
	for _, key := range effectiveKeys {
		effectiveEntry := i.getEffectiveEntry(key)
		effectiveEntry.mu.Lock()
		resetEffectiveEntry(effectiveEntry)
		locks = append(locks, &effectiveEntry.mu)
	}

	return &Mutation{locks: locks, storeLock: storeLock}
}

// Invalidate removes all derived closures for a store. It is intended for
// store-wide mutations such as deleting a store.
func (i *Index) Invalidate(storeID string) {
	i.BeginStoreMutation(storeID).End()
}

// BeginStoreMutation invalidates every key in a store and prevents new keys
// from being created until End. It is used for store-wide mutations.
func (i *Index) BeginStoreMutation(storeID string) *Mutation {
	storeLock := i.getStoreLock(storeID)
	storeLock.Lock()

	i.mu.Lock()
	i.storeGenerations[storeID]++
	entries := make([]*entry, 0)
	for key, entry := range i.entries {
		if key.storeID == storeID {
			entries = append(entries, entry)
		}
	}
	effectiveEntries := make([]*effectiveEntry, 0)
	for key, entry := range i.effectiveEntries {
		if key.storeID == storeID {
			effectiveEntries = append(effectiveEntries, entry)
		}
	}
	i.mu.Unlock()

	for _, entry := range entries {
		entry.mu.Lock()
		resetEntry(entry)
		entry.mu.Unlock()
	}
	for _, entry := range effectiveEntries {
		entry.mu.Lock()
		resetEffectiveEntry(entry)
		entry.mu.Unlock()
	}

	return &Mutation{storeLock: storeLock}
}

// resetEntry invalidates an entry while its mutex is held.
func resetEntry(entry *entry) {
	if entry.cancel != nil {
		entry.cancel()
		entry.cancel = nil
	}
	entry.generation++
	entry.state = stateEmpty
	entry.snapshot = nil
	entry.publishedAt = time.Time{}
}

func (i *Index) getStoreLock(storeID string) *sync.Mutex {
	i.mu.Lock()
	defer i.mu.Unlock()

	storeLock := i.storeLocks[storeID]
	if storeLock == nil {
		storeLock = &sync.Mutex{}
		i.storeLocks[storeID] = storeLock
	}
	return storeLock
}

func (i *Index) getEntry(key indexKey) *entry {
	i.mu.Lock()
	defer i.mu.Unlock()

	indexEntry := i.entries[key]
	if indexEntry == nil {
		indexEntry = &entry{}
		i.entries[key] = indexEntry
	}
	return indexEntry
}

func (i *Index) storeGeneration(storeID string) uint64 {
	i.mu.Lock()
	defer i.mu.Unlock()
	return i.storeGenerations[storeID]
}

func affectedKeys(storeID string, writes []*openfgav1.TupleKey, deletes []*openfgav1.TupleKeyWithoutCondition) []indexKey {
	keys := make(map[indexKey]struct{}, len(writes)+len(deletes))
	add := func(object, relation string) {
		objectType, _ := tuple.SplitObject(object)
		if objectType != "" && relation != "" {
			keys[indexKey{storeID: storeID, objectType: objectType, relation: relation}] = struct{}{}
		}
	}
	for _, write := range writes {
		add(write.GetObject(), write.GetRelation())
	}
	for _, delete := range deletes {
		add(delete.GetObject(), delete.GetRelation())
	}

	result := make([]indexKey, 0, len(keys))
	for key := range keys {
		result = append(result, key)
	}
	sort.Slice(result, func(a, b int) bool {
		if result[a].objectType == result[b].objectType {
			return result[a].relation < result[b].relation
		}
		return result[a].objectType < result[b].objectType
	})
	return result
}

// MatchesAnyAncestor reports whether any seed is equal to, or reaches, one of
// targets through the direct relation. used is false when the graph cannot be
// indexed within its configured bounds; callers must use their normal resolver
// in that case.
func (i *Index) MatchesAnyAncestor(
	_ context.Context,
	datastore storage.RelationshipTupleReader,
	storeID, objectType, relation string,
	seeds []string,
	targets map[string]struct{},
) (matches bool, used bool, err error) {
	if len(seeds) == 0 || len(targets) == 0 {
		return false, false, nil
	}

	key := indexKey{storeID: storeID, objectType: objectType, relation: relation}
	i.maybePollChangelog(storeID)
	storeLock := i.getStoreLock(storeID)
	storeLock.Lock()
	entry := i.getEntry(key)
	entry.mu.Lock()
	storeLock.Unlock()

	// A snapshot or disabled sentinel past the TTL cannot be trusted: a write
	// on another process sharing the datastore never reaches this process's
	// mutation guard. Treat the entry as absent so the caller falls back to
	// its normal resolver while a detached rebuild runs.
	if i.snapshotTTL > 0 &&
		(entry.state == stateReady || entry.state == stateDisabled) &&
		time.Since(entry.publishedAt) > i.snapshotTTL {
		resetEntry(entry)
	}

	if entry.state == stateReady {
		snapshot := entry.snapshot
		entry.mu.Unlock()
		return matchesSnapshot(snapshot, seeds, targets), true, nil
	}
	if entry.state == stateEmpty {
		buildCtx, cancel := context.WithTimeout(context.Background(), i.buildTimeout)
		entry.state = stateBuilding
		entryGeneration := entry.generation
		entry.buildID++
		buildID := entry.buildID
		entry.cancel = cancel
		storeGeneration := i.storeGeneration(storeID)
		go i.buildAndPublish(buildCtx, cancel, datastore, key, entry, entryGeneration, storeGeneration, buildID)
	}
	entry.mu.Unlock()
	return false, false, nil
}

func matchesSnapshot(snapshot *snapshot, seeds []string, targets map[string]struct{}) bool {
	targetIDs := roaring.NewBitmap()
	for target := range targets {
		if id, ok := snapshot.ids[target]; ok {
			targetIDs.Add(id)
		}
	}
	if targetIDs.IsEmpty() {
		return false
	}
	for _, seed := range seeds {
		id, ok := snapshot.ids[seed]
		if ok && (targetIDs.Contains(id) || snapshot.ancestors[id].Intersects(targetIDs)) {
			return true
		}
	}
	return false
}

func (i *Index) buildAndPublish(
	ctx context.Context,
	cancel context.CancelFunc,
	datastore storage.RelationshipTupleReader,
	key indexKey,
	entry *entry,
	entryGeneration, storeGeneration, buildID uint64,
) {
	defer cancel()

	snapshot, result := i.build(ctx, datastore, key)
	entry.mu.Lock()
	defer entry.mu.Unlock()
	if entry.generation != entryGeneration ||
		entry.buildID != buildID ||
		i.storeGeneration(key.storeID) != storeGeneration ||
		entry.state != stateBuilding {
		return
	}
	entry.cancel = nil
	switch result {
	case buildReady:
		entry.snapshot = snapshot
		entry.state = stateReady
		entry.publishedAt = time.Now()
	case buildDisabled:
		entry.state = stateDisabled
		entry.publishedAt = time.Now()
	default:
		entry.state = stateEmpty
	}
}

func (i *Index) getPollState(storeID string) *pollState {
	i.mu.Lock()
	defer i.mu.Unlock()

	ps := i.pollStates[storeID]
	if ps == nil {
		ps = &pollState{}
		i.pollStates[storeID] = ps
	}
	return ps
}

// maybePollChangelog starts one detached changelog poll for the store when
// the previous poll finished more than a poll interval ago. It never blocks
// the check path.
func (i *Index) maybePollChangelog(storeID string) {
	if i.changelog == nil {
		return
	}

	ps := i.getPollState(storeID)
	ps.mu.Lock()
	if ps.inflight || time.Since(ps.lastPolled) < i.pollInterval {
		ps.mu.Unlock()
		return
	}
	ps.inflight = true
	ps.mu.Unlock()

	go i.pollChangelog(storeID, ps)
}

// pollChangelog reads the most recent changes for a store and invalidates the
// index keys touched by changes it has not seen before. This is a best-effort
// fast path for writes performed by other processes: ULIDs are not
// commit-ordered and reads may hit lagging replicas, so a change can be
// missed here. The snapshot TTL is the correctness bound.
func (i *Index) pollChangelog(storeID string, ps *pollState) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	changes, _, err := i.changelog.ReadChanges(ctx, storeID, storage.ReadChangesFilter{}, storage.ReadChangesOptions{
		SortDesc: true,
		Pagination: storage.PaginationOptions{
			PageSize: storage.DefaultPageSize,
		},
	})

	ps.mu.Lock()
	ps.lastPolled = time.Now()
	ps.inflight = false
	if err != nil {
		ps.mu.Unlock()
		// ErrNotFound means the store has no changelog entries yet. Either
		// way there is nothing to invalidate from; keep serving until the
		// snapshot TTL expires.
		i.logger.Debug("reachability index changelog poll failed",
			zap.String("store_id", storeID),
			zap.Error(err),
		)
		return
	}
	newest := changes[0].GetTimestamp().AsTime()
	baseline := ps.newestSeen
	initialized := ps.initialized
	ps.newestSeen = newest
	ps.initialized = true
	ps.mu.Unlock()

	if !initialized {
		// The first poll only records a baseline. Builds read live data, so
		// changes older than the baseline are already reflected in any
		// snapshot published after this point; a change racing this first
		// poll is covered by the snapshot TTL.
		return
	}
	if !newest.After(baseline) {
		return
	}

	// A full page of unseen changes may hide older unseen ones beyond the
	// page limit; invalidate the whole store rather than risk missing a key.
	oldest := changes[len(changes)-1].GetTimestamp().AsTime()
	if oldest.After(baseline) && len(changes) == storage.DefaultPageSize {
		i.Invalidate(storeID)
		return
	}

	tupleKeys := make([]*openfgav1.TupleKey, 0, len(changes))
	for _, change := range changes {
		if change.GetTimestamp().AsTime().After(baseline) {
			tupleKeys = append(tupleKeys, change.GetTupleKey())
		}
	}
	for _, key := range affectedKeys(storeID, tupleKeys, nil) {
		i.invalidateKey(key)
	}
}

// invalidateKey resets an existing entry, cancelling any in-flight build. It
// never creates entries for keys that were not indexed.
func (i *Index) invalidateKey(key indexKey) {
	i.mu.Lock()
	indexEntry := i.entries[key]
	i.mu.Unlock()
	if indexEntry == nil {
		return
	}

	indexEntry.mu.Lock()
	resetEntry(indexEntry)
	indexEntry.mu.Unlock()
}

type buildResult uint8

const (
	buildRetry buildResult = iota
	buildReady
	buildDisabled
)

func (i *Index) build(ctx context.Context, datastore storage.RelationshipTupleReader, key indexKey) (*snapshot, buildResult) {
	iter, err := datastore.Read(ctx, key.storeID, storage.ReadFilter{
		Object:   key.objectType + ":",
		Relation: key.relation,
	}, storage.ReadOptions{
		Consistency: storage.ConsistencyOptions{Preference: openfgav1.ConsistencyPreference_HIGHER_CONSISTENCY},
	})
	if err != nil {
		return nil, buildRetry
	}
	defer iter.Stop()

	ids := make(map[string]uint32)
	parents := make(map[uint32][]uint32)
	addID := func(object string) (uint32, bool) {
		if id, ok := ids[object]; ok {
			return id, true
		}
		if len(ids) >= i.maxNodes {
			return 0, false
		}
		id := uint32(len(ids))
		ids[object] = id
		return id, true
	}

	for {
		item, err := iter.Next(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil, buildRetry
			}
			if errors.Is(err, storage.ErrIteratorDone) {
				break
			}
			return nil, buildRetry
		}

		tupleKey := item.GetKey()
		if tupleKey.GetRelation() != key.relation {
			continue
		}
		// The index stores only reachability, not condition expressions or their
		// evaluation context. A conditional tuple on the indexed relation must
		// therefore use the normal resolver, even if its user type is not part
		// of the same-type reachability graph.
		if tupleKey.GetCondition() != nil {
			return nil, buildDisabled
		}
		objectType, _ := tuple.SplitObject(tupleKey.GetObject())
		userType, _ := tuple.SplitObject(tupleKey.GetUser())
		if objectType != key.objectType || userType != key.objectType {
			continue
		}

		objectID, ok := addID(tupleKey.GetObject())
		if !ok {
			return nil, buildDisabled
		}
		parentID, ok := addID(tupleKey.GetUser())
		if !ok {
			return nil, buildDisabled
		}
		parents[objectID] = append(parents[objectID], parentID)
	}
	if ctx.Err() != nil {
		return nil, buildRetry
	}

	ancestors := make([]*roaring.Bitmap, len(ids))
	visiting := make([]bool, len(ids))
	complete := make([]bool, len(ids))
	var entries uint64

	var visit func(uint32) (*roaring.Bitmap, error)
	visit = func(id uint32) (*roaring.Bitmap, error) {
		if complete[id] {
			return ancestors[id], nil
		}
		if visiting[id] {
			return nil, errCycle
		}
		visiting[id] = true
		bitmap := roaring.NewBitmap()
		for _, parentID := range parents[id] {
			bitmap.Add(parentID)
			parentAncestors, err := visit(parentID)
			if err != nil {
				return nil, err
			}
			bitmap.Or(parentAncestors)
		}
		visiting[id] = false
		complete[id] = true
		ancestors[id] = bitmap
		entries += uint64(bitmap.GetCardinality())
		if entries > i.maxClosureEntries {
			return nil, errClosureTooLarge
		}
		return bitmap, nil
	}

	for id := range ancestors {
		if _, err := visit(uint32(id)); err != nil {
			if errors.Is(err, errClosureTooLarge) || errors.Is(err, errCycle) {
				return nil, buildDisabled
			}
			return nil, buildRetry
		}
	}

	return &snapshot{ids: ids, ancestors: ancestors}, buildReady
}

var (
	errClosureTooLarge = errors.New("reachability closure exceeds configured limit")
	errCycle           = errors.New("cycle in reachability graph")
)
