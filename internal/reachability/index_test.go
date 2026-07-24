package reachability

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/storage/memory"
)

func requireMatchEventually(
	t *testing.T,
	index *Index,
	datastore storage.RelationshipTupleReader,
	storeID, objectType, relation string,
	seeds []string,
	targets map[string]struct{},
	want bool,
) {
	t.Helper()
	require.Eventually(t, func() bool {
		matches, used, err := index.MatchesAnyAncestor(
			context.Background(), datastore, storeID, objectType, relation, seeds, targets,
		)
		return err == nil && used && matches == want
	}, time.Second, 10*time.Millisecond)
}

func requireStateEventually(t *testing.T, index *Index, key indexKey, want state) {
	t.Helper()
	require.Eventually(t, func() bool {
		entry := index.getEntry(key)
		entry.mu.Lock()
		defer entry.mu.Unlock()
		return entry.state == want
	}, time.Second, 10*time.Millisecond)
}

type countingReader struct {
	storage.RelationshipTupleReader
	reads atomic.Int32
}

func (r *countingReader) Read(
	ctx context.Context,
	store string,
	filter storage.ReadFilter,
	options storage.ReadOptions,
) (storage.TupleIterator, error) {
	r.reads.Add(1)
	return r.RelationshipTupleReader.Read(ctx, store, filter, options)
}

type blockingReader struct {
	storage.RelationshipTupleReader
	block      atomic.Bool
	started    chan struct{}
	cancelled  chan struct{}
	startOnce  sync.Once
	cancelOnce sync.Once
}

func (r *blockingReader) Read(
	ctx context.Context,
	store string,
	filter storage.ReadFilter,
	options storage.ReadOptions,
) (storage.TupleIterator, error) {
	if !r.block.Load() {
		return r.RelationshipTupleReader.Read(ctx, store, filter, options)
	}

	r.startOnce.Do(func() { close(r.started) })
	return &blockingIterator{cancelled: r.cancelled, once: &r.cancelOnce}, nil
}

type blockingIterator struct {
	cancelled chan struct{}
	once      *sync.Once
}

func (i *blockingIterator) Next(ctx context.Context) (*openfgav1.Tuple, error) {
	<-ctx.Done()
	i.once.Do(func() { close(i.cancelled) })
	return nil, ctx.Err()
}

func (i *blockingIterator) Head(ctx context.Context) (*openfgav1.Tuple, error) {
	return i.Next(ctx)
}

func (i *blockingIterator) Stop() {}

func (i *blockingIterator) IsOrdered() bool { return false }

func TestIndexMatchesAncestors(t *testing.T) {
	t.Parallel()

	ds := memory.New()
	storeID := "store"
	require.NoError(t, ds.Write(context.Background(), storeID, nil, []*openfgav1.TupleKey{
		{Object: "folder:engineering", Relation: "parent", User: "folder:root"},
		{Object: "folder:platform", Relation: "parent", User: "folder:engineering"},
		{Object: "folder:api", Relation: "parent", User: "folder:platform"},
	}))

	index := New(WithMaxNodes(10), WithMaxClosureEntries(100))

	t.Run("includes the object and all of its ancestors", func(t *testing.T) {
		requireMatchEventually(t, index, ds, storeID, "folder", "parent",
			[]string{"folder:api"},
			map[string]struct{}{"folder:api": {}, "folder:engineering": {}, "folder:root": {}}, true)
	})

	t.Run("does not match unrelated folders", func(t *testing.T) {
		requireMatchEventually(t, index, ds, storeID, "folder", "parent",
			[]string{"folder:api"}, map[string]struct{}{"folder:other": {}}, false)
	})
}

func TestIndexFallsBackWhenClosureExceedsBudget(t *testing.T) {
	t.Parallel()

	ds := memory.New()
	storeID := "store"
	require.NoError(t, ds.Write(context.Background(), storeID, nil, []*openfgav1.TupleKey{
		{Object: "folder:child", Relation: "parent", User: "folder:parent"},
		{Object: "folder:parent", Relation: "parent", User: "folder:root"},
	}))

	index := New(WithMaxNodes(10), WithMaxClosureEntries(2))
	matches, used, err := index.MatchesAnyAncestor(
		context.Background(),
		ds,
		storeID,
		"folder",
		"parent",
		[]string{"folder:child"},
		map[string]struct{}{"folder:root": {}},
	)

	require.NoError(t, err)
	require.False(t, used)
	require.False(t, matches)
	requireStateEventually(t, index, indexKey{storeID: storeID, objectType: "folder", relation: "parent"}, stateDisabled)
}

func TestIndexFallsBackWhenRelationHasConditionalTuple(t *testing.T) {
	t.Parallel()

	ds := memory.New()
	storeID := "store"
	require.NoError(t, ds.Write(context.Background(), storeID, nil, []*openfgav1.TupleKey{
		{
			Object:   "folder:child",
			Relation: "parent",
			User:     "folder:root",
			Condition: &openfgav1.RelationshipCondition{
				Name: "requires_context",
			},
		},
	}))

	index := New()
	matches, used, err := index.MatchesAnyAncestor(
		context.Background(),
		ds,
		storeID,
		"folder",
		"parent",
		[]string{"folder:child"},
		map[string]struct{}{"folder:root": {}},
	)

	require.NoError(t, err)
	require.False(t, used)
	require.False(t, matches)
	requireStateEventually(t, index, indexKey{storeID: storeID, objectType: "folder", relation: "parent"}, stateDisabled)
}

func TestIndexInvalidationPreventsUsingStaleClosure(t *testing.T) {
	t.Parallel()

	ds := memory.New()
	storeID := "store"
	require.NoError(t, ds.Write(context.Background(), storeID, nil, []*openfgav1.TupleKey{
		{Object: "folder:child", Relation: "parent", User: "folder:old-root"},
	}))

	index := New()
	targets := map[string]struct{}{"folder:old-root": {}}

	requireMatchEventually(t, index, ds, storeID, "folder", "parent", []string{"folder:child"}, targets, true)

	index.Invalidate(storeID)
	require.NoError(t, ds.Write(context.Background(), storeID,
		[]*openfgav1.TupleKeyWithoutCondition{{Object: "folder:child", Relation: "parent", User: "folder:old-root"}},
		[]*openfgav1.TupleKey{{Object: "folder:child", Relation: "parent", User: "folder:new-root"}},
	))

	requireMatchEventually(t, index, ds, storeID, "folder", "parent", []string{"folder:child"}, targets, false)
}

func TestIndexMutationInvalidatesAfterFailedWrite(t *testing.T) {
	t.Parallel()

	ds := memory.New()
	storeID := "store"
	require.NoError(t, ds.Write(context.Background(), storeID, nil, []*openfgav1.TupleKey{
		{Object: "folder:child", Relation: "parent", User: "folder:root"},
	}))

	index := New()
	targets := map[string]struct{}{"folder:root": {}}
	requireMatchEventually(t, index, ds, storeID, "folder", "parent", []string{"folder:child"}, targets, true)

	mutation := index.BeginMutation(storeID, []*openfgav1.TupleKey{
		{Object: "folder:child", Relation: "parent", User: "folder:root"},
	}, nil)
	var wg sync.WaitGroup
	wg.Add(1)
	result := make(chan struct {
		matches bool
		used    bool
		err     error
	}, 1)
	go func() {
		defer wg.Done()
		matches, used, err := index.MatchesAnyAncestor(context.Background(), ds, storeID, "folder", "parent", []string{"folder:child"}, targets)
		result <- struct {
			matches bool
			used    bool
			err     error
		}{matches, used, err}
	}()

	select {
	case <-result:
		t.Fatal("check completed while mutation was in progress")
	case <-time.After(50 * time.Millisecond):
	}

	mutation.End()
	mutation.End()
	wg.Wait()
	res := <-result
	require.NoError(t, res.err)
	requireMatchEventually(t, index, ds, storeID, "folder", "parent", []string{"folder:child"}, targets, true)
}

func TestIndexMutationDoesNotBlockOtherStores(t *testing.T) {
	t.Parallel()

	ds := memory.New()
	require.NoError(t, ds.Write(context.Background(), "store-a", nil, []*openfgav1.TupleKey{
		{Object: "folder:child", Relation: "parent", User: "folder:root"},
	}))
	require.NoError(t, ds.Write(context.Background(), "store-b", nil, []*openfgav1.TupleKey{
		{Object: "folder:child", Relation: "parent", User: "folder:root"},
	}))

	index := New()
	mutation := index.BeginMutation("store-a", []*openfgav1.TupleKey{
		{Object: "folder:child", Relation: "parent", User: "folder:root"},
	}, nil)
	defer mutation.End()

	result := make(chan struct {
		matches bool
		used    bool
		err     error
	}, 1)
	go func() {
		matches, used, err := index.MatchesAnyAncestor(
			context.Background(),
			ds,
			"store-b",
			"folder",
			"parent",
			[]string{"folder:child"},
			map[string]struct{}{"folder:root": {}},
		)
		result <- struct {
			matches bool
			used    bool
			err     error
		}{matches, used, err}
	}()

	select {
	case res := <-result:
		require.NoError(t, res.err)
	case <-time.After(time.Second):
		t.Fatal("store-b check blocked on store-a mutation")
	}
}

func TestIndexRejectsCycles(t *testing.T) {
	t.Parallel()

	ds := memory.New()
	storeID := "store"
	require.NoError(t, ds.Write(context.Background(), storeID, nil, []*openfgav1.TupleKey{
		{Object: "folder:one", Relation: "parent", User: "folder:two"},
		{Object: "folder:two", Relation: "parent", User: "folder:one"},
	}))

	index := New()
	_, used, err := index.MatchesAnyAncestor(
		context.Background(),
		ds,
		storeID,
		"folder",
		"parent",
		[]string{"folder:one"},
		map[string]struct{}{"folder:two": {}},
	)

	require.NoError(t, err)
	require.False(t, used)
	requireStateEventually(t, index, indexKey{storeID: storeID, objectType: "folder", relation: "parent"}, stateDisabled)
}

func TestIndexDoesNotInvalidateUnrelatedRelation(t *testing.T) {
	t.Parallel()

	ds := memory.New()
	storeID := "store"
	require.NoError(t, ds.Write(context.Background(), storeID, nil, []*openfgav1.TupleKey{
		{Object: "folder:child", Relation: "parent", User: "folder:root"},
	}))
	index := New()
	targets := map[string]struct{}{"folder:root": {}}
	requireMatchEventually(t, index, ds, storeID, "folder", "parent", []string{"folder:child"}, targets, true)

	mutation := index.BeginMutation(storeID, []*openfgav1.TupleKey{
		{Object: "folder:child", Relation: "editor", User: "user:anne"},
	}, nil)
	mutation.End()

	matches, used, err := index.MatchesAnyAncestor(context.Background(), ds, storeID, "folder", "parent", []string{"folder:child"}, targets)
	require.NoError(t, err)
	require.True(t, used)
	require.True(t, matches)
}

func TestIndexInvalidatesRelevantWritesAndDeletes(t *testing.T) {
	t.Parallel()

	ds := memory.New()
	storeID := "store"
	require.NoError(t, ds.Write(context.Background(), storeID, nil, []*openfgav1.TupleKey{
		{Object: "folder:child", Relation: "parent", User: "folder:root"},
	}))
	index := New()
	targets := map[string]struct{}{"folder:root": {}}
	requireMatchEventually(t, index, ds, storeID, "folder", "parent", []string{"folder:child"}, targets, true)

	deleteMutation := index.BeginMutation(storeID, nil, []*openfgav1.TupleKeyWithoutCondition{
		{Object: "folder:child", Relation: "parent", User: "folder:root"},
	})
	require.NoError(t, ds.Write(context.Background(), storeID, []*openfgav1.TupleKeyWithoutCondition{
		{Object: "folder:child", Relation: "parent", User: "folder:root"},
	}, nil))
	deleteMutation.End()
	requireMatchEventually(t, index, ds, storeID, "folder", "parent", []string{"folder:child"}, targets, false)

	writeMutation := index.BeginMutation(storeID, []*openfgav1.TupleKey{
		{Object: "folder:child", Relation: "parent", User: "folder:root"},
	}, nil)
	require.NoError(t, ds.Write(context.Background(), storeID, nil, []*openfgav1.TupleKey{
		{Object: "folder:child", Relation: "parent", User: "folder:root"},
	}))
	writeMutation.End()
	requireMatchEventually(t, index, ds, storeID, "folder", "parent", []string{"folder:child"}, targets, true)
}

func TestIndexReenablesAfterRelevantMutation(t *testing.T) {
	t.Parallel()

	ds := memory.New()
	storeID := "store"
	conditional := &openfgav1.TupleKey{
		Object: "folder:child", Relation: "parent", User: "folder:root",
		Condition: &openfgav1.RelationshipCondition{Name: "requires_context"},
	}
	require.NoError(t, ds.Write(context.Background(), storeID, nil, []*openfgav1.TupleKey{conditional}))
	index := New()
	key := indexKey{storeID: storeID, objectType: "folder", relation: "parent"}
	_, _, err := index.MatchesAnyAncestor(context.Background(), ds, storeID, "folder", "parent", []string{"folder:child"}, map[string]struct{}{"folder:root": {}})
	require.NoError(t, err)
	requireStateEventually(t, index, key, stateDisabled)

	mutation := index.BeginMutation(storeID, nil, []*openfgav1.TupleKeyWithoutCondition{
		{Object: "folder:child", Relation: "parent", User: "folder:root"},
	})
	require.NoError(t, ds.Write(context.Background(), storeID, []*openfgav1.TupleKeyWithoutCondition{
		{Object: "folder:child", Relation: "parent", User: "folder:root"},
	}, nil))
	mutation.End()
	requireMatchEventually(t, index, ds, storeID, "folder", "parent", []string{"folder:child"}, map[string]struct{}{"folder:root": {}}, false)
	requireStateEventually(t, index, key, stateReady)
}

func TestIndexBuildSingleflightFallsBackImmediately(t *testing.T) {
	t.Parallel()

	ds := memory.New()
	storeID := "store"
	require.NoError(t, ds.Write(context.Background(), storeID, nil, []*openfgav1.TupleKey{
		{Object: "folder:child", Relation: "parent", User: "folder:root"},
	}))
	reader := &countingReader{RelationshipTupleReader: ds}
	index := New()
	targets := map[string]struct{}{"folder:root": {}}

	var wg sync.WaitGroup
	results := make(chan struct {
		matches bool
		used    bool
		err     error
	}, 20)
	for range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			matches, used, err := index.MatchesAnyAncestor(context.Background(), reader, storeID, "folder", "parent", []string{"folder:child"}, targets)
			results <- struct {
				matches bool
				used    bool
				err     error
			}{matches, used, err}
		}()
	}
	wg.Wait()
	close(results)
	for result := range results {
		require.NoError(t, result.err)
		if result.used {
			require.True(t, result.matches)
		}
	}

	requireStateEventually(t, index, indexKey{storeID: storeID, objectType: "folder", relation: "parent"}, stateReady)
	require.Equal(t, int32(1), reader.reads.Load())
}

func TestIndexBuildDeadlineResetsStateAndAllowsRetry(t *testing.T) {
	t.Parallel()

	ds := memory.New()
	storeID := "store"
	require.NoError(t, ds.Write(context.Background(), storeID, nil, []*openfgav1.TupleKey{
		{Object: "folder:child", Relation: "parent", User: "folder:root"},
	}))
	reader := &blockingReader{
		RelationshipTupleReader: ds,
		started:                 make(chan struct{}),
		cancelled:               make(chan struct{}),
	}
	reader.block.Store(true)
	index := New(WithBuildTimeout(25 * time.Millisecond))
	key := indexKey{storeID: storeID, objectType: "folder", relation: "parent"}
	targets := map[string]struct{}{"folder:root": {}}

	matches, used, err := index.MatchesAnyAncestor(
		context.Background(), reader, storeID, "folder", "parent", []string{"folder:child"}, targets,
	)
	require.NoError(t, err)
	require.False(t, used)
	require.False(t, matches)
	<-reader.started
	<-reader.cancelled
	requireStateEventually(t, index, key, stateEmpty)

	reader.block.Store(false)
	requireMatchEventually(t, index, reader, storeID, "folder", "parent", []string{"folder:child"}, targets, true)
}

func TestIndexRelevantMutationCancelsStaleBuild(t *testing.T) {
	t.Parallel()

	ds := memory.New()
	storeID := "store"
	require.NoError(t, ds.Write(context.Background(), storeID, nil, []*openfgav1.TupleKey{
		{Object: "folder:child", Relation: "parent", User: "folder:root"},
	}))
	reader := &blockingReader{
		RelationshipTupleReader: ds,
		started:                 make(chan struct{}),
		cancelled:               make(chan struct{}),
	}
	reader.block.Store(true)
	index := New(WithBuildTimeout(time.Second))
	targets := map[string]struct{}{"folder:root": {}}

	_, used, err := index.MatchesAnyAncestor(
		context.Background(), reader, storeID, "folder", "parent", []string{"folder:child"}, targets,
	)
	require.NoError(t, err)
	require.False(t, used)
	<-reader.started

	mutation := index.BeginMutation(storeID, []*openfgav1.TupleKey{
		{Object: "folder:child", Relation: "parent", User: "folder:root"},
	}, nil)
	<-reader.cancelled
	mutation.End()

	reader.block.Store(false)
	requireMatchEventually(t, index, reader, storeID, "folder", "parent", []string{"folder:child"}, targets, true)
}
