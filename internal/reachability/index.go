package reachability

import (
	"errors"
	"sort"
	"sync"
	"time"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
)

var errCycle = errors.New("cycle detected")

type state uint8

const (
	stateEmpty state = iota
	stateBuilding
	stateReady
	stateDisabled
)

type buildResult uint8

const (
	buildRetry buildResult = iota
	buildReady
	buildDisabled
)

const defaultSnapshotTTL = 10 * time.Second

// Index is a bounded, in-memory effective-subject index. Snapshots are derived
// from authorization models and relationship tuples and are published only
// after a complete build.
type Index struct {
	mu                sync.Mutex
	effectiveEntries  map[effectiveIndexKey]*effectiveEntry
	effectivePlans    map[string]*effectiveModelPlan
	storeGenerations  map[string]uint64
	storeLocks        map[string]*sync.Mutex
	maxNodes          int
	maxClosureEntries uint64
	buildTimeout      time.Duration
	snapshotTTL       time.Duration
}

// Option configures an Index.
type Option func(*Index)

// WithMaxNodes bounds indexed objects and subject identities.
func WithMaxNodes(maxNodes int) Option {
	return func(index *Index) {
		index.maxNodes = maxNodes
	}
}

// WithMaxClosureEntries bounds the total effective-subject and membership
// bitmap cardinality in a snapshot.
func WithMaxClosureEntries(maxEntries uint64) Option {
	return func(index *Index) {
		index.maxClosureEntries = maxEntries
	}
}

// WithBuildTimeout bounds one detached snapshot build.
func WithBuildTimeout(timeout time.Duration) Option {
	return func(index *Index) {
		index.buildTimeout = timeout
	}
}

// WithSnapshotTTL bounds how long a ready snapshot or disabled sentinel may be
// served. A zero or negative TTL disables expiry and is safe only when every
// tuple mutation passes through this process's mutation guard.
func WithSnapshotTTL(ttl time.Duration) Option {
	return func(index *Index) {
		index.snapshotTTL = ttl
	}
}

// New returns an empty effective-subject index.
func New(options ...Option) *Index {
	index := &Index{
		effectiveEntries:  make(map[effectiveIndexKey]*effectiveEntry),
		effectivePlans:    make(map[string]*effectiveModelPlan),
		storeGenerations:  make(map[string]uint64),
		storeLocks:        make(map[string]*sync.Mutex),
		maxNodes:          100_000,
		maxClosureEntries: 1_000_000,
		buildTimeout:      30 * time.Second,
		snapshotTTL:       defaultSnapshotTTL,
	}
	for _, option := range options {
		option(index)
	}
	return index
}

// Mutation prevents a snapshot from being served or published while a
// datastore mutation is in progress.
type Mutation struct {
	locks     []*sync.Mutex
	storeLock *sync.Mutex
	once      sync.Once
}

// End releases the mutation guard. It is safe to call more than once.
func (m *Mutation) End() {
	if m == nil {
		return
	}
	m.once.Do(func() {
		for idx := len(m.locks) - 1; idx >= 0; idx-- {
			m.locks[idx].Unlock()
		}
		if m.storeLock != nil {
			m.storeLock.Unlock()
		}
	})
}

// BeginMutation invalidates every effective snapshot for the store before a
// tuple mutation and holds their locks until the mutation completes.
func (i *Index) BeginMutation(
	storeID string,
	_ []*openfgav1.TupleKey,
	_ []*openfgav1.TupleKeyWithoutCondition,
) *Mutation {
	storeLock := i.getStoreLock(storeID)
	storeLock.Lock()

	i.mu.Lock()
	keys := make([]effectiveIndexKey, 0)
	for key := range i.effectiveEntries {
		if key.storeID == storeID {
			keys = append(keys, key)
		}
	}
	i.mu.Unlock()
	sort.Slice(keys, func(a, b int) bool {
		return keys[a].modelID < keys[b].modelID
	})

	locks := make([]*sync.Mutex, 0, len(keys))
	for _, key := range keys {
		entry := i.getEffectiveEntry(key)
		entry.mu.Lock()
		resetEffectiveEntry(entry)
		locks = append(locks, &entry.mu)
	}
	return &Mutation{locks: locks, storeLock: storeLock}
}

// Invalidate removes every effective snapshot for a store.
func (i *Index) Invalidate(storeID string) {
	i.BeginStoreMutation(storeID).End()
}

// BeginStoreMutation invalidates a store and prevents new snapshots from being
// created until End.
func (i *Index) BeginStoreMutation(storeID string) *Mutation {
	storeLock := i.getStoreLock(storeID)
	storeLock.Lock()

	i.mu.Lock()
	i.storeGenerations[storeID]++
	entries := make([]*effectiveEntry, 0)
	for key, entry := range i.effectiveEntries {
		if key.storeID == storeID {
			entries = append(entries, entry)
		}
	}
	i.mu.Unlock()

	for _, entry := range entries {
		entry.mu.Lock()
		resetEffectiveEntry(entry)
		entry.mu.Unlock()
	}
	return &Mutation{storeLock: storeLock}
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

func (i *Index) storeGeneration(storeID string) uint64 {
	i.mu.Lock()
	defer i.mu.Unlock()
	return i.storeGenerations[storeID]
}
