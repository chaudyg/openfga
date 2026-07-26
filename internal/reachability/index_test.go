package reachability

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/storage/memory"
	"github.com/openfga/openfga/pkg/testutils"
)

func requireEffectiveMatchEventually(
	t *testing.T,
	index *Index,
	datastore storage.RelationshipTupleReader,
	model *openfgav1.AuthorizationModel,
	object, relation, user string,
	requestContext *structpb.Struct,
	want bool,
) {
	t.Helper()
	require.Eventually(t, func() bool {
		matches, used, err := index.MatchesEffectiveSubjectForModel(
			context.Background(),
			datastore, "store", model,
			object,
			relation,
			user,
			requestContext,
		)
		return err == nil && used && matches == want
	}, time.Second, 10*time.Millisecond)
}

func genericEffectiveModel() *openfgav1.AuthorizationModel {
	return testutils.MustTransformDSLToProtoWithID(`
		model
			schema 1.1
		type user
		type group
			relations
				define member: [user, group#member]
		type node
			relations
				define parent: [node]
				define reader: [user, group#member] or reader from parent
				define writer: [user, group#member] or writer from parent
				define scoped_reader: [user with scope_allowed, group#member with scope_allowed] or scoped_reader from parent
				define tenant_reader: [user with tenant_match, group#member with tenant_match] or tenant_reader from parent
				define can_access: reader or writer
				define can_scoped_access: scoped_reader
				define can_tenant_access: tenant_reader
		condition scope_allowed(scope: string, scopes: list<string>) {
			scope in scopes
		}
		condition tenant_match(requested_tenant: string, assigned_tenant: string) {
			requested_tenant == assigned_tenant
		}
	`)
}

func TestIndexMatchesGenericEffectiveSubjects(t *testing.T) {
	t.Parallel()

	model := genericEffectiveModel()
	ds := memory.New()
	storeID := "store"
	require.NoError(t, ds.Write(context.Background(), storeID, nil, []*openfgav1.TupleKey{
		{Object: "group:engineering", Relation: "member", User: "user:anne"},
		{Object: "group:platform", Relation: "member", User: "group:engineering#member"},
		{Object: "node:root", Relation: "reader", User: "group:platform#member"},
		{Object: "node:services", Relation: "parent", User: "node:root"},
		{Object: "node:api", Relation: "parent", User: "node:services"},
	}))

	index := New(WithMaxNodes(100), WithMaxClosureEntries(1_000))
	requireEffectiveMatchEventually(
		t, index, ds, model,
		"node:api", "can_access", "user:anne", nil, true,
	)
	requireEffectiveMatchEventually(
		t, index, ds, model,
		"node:api", "can_access", "user:bob", nil, false,
	)
}

func TestIndexMatchesGenericConditionDimensions(t *testing.T) {
	t.Parallel()

	model := genericEffectiveModel()
	ds := memory.New()
	storeID := "store"
	scopes := structpb.NewListValue(&structpb.ListValue{Values: []*structpb.Value{
		structpb.NewStringValue("production"),
	}})
	require.NoError(t, ds.Write(context.Background(), storeID, nil, []*openfgav1.TupleKey{
		{Object: "group:engineering", Relation: "member", User: "user:anne"},
		{
			Object:   "node:root",
			Relation: "scoped_reader",
			User:     "group:engineering#member",
			Condition: &openfgav1.RelationshipCondition{
				Name: "scope_allowed",
				Context: &structpb.Struct{Fields: map[string]*structpb.Value{
					"scopes": scopes,
				}},
			},
		},
		{
			Object:   "node:root",
			Relation: "tenant_reader",
			User:     "group:engineering#member",
			Condition: &openfgav1.RelationshipCondition{
				Name: "tenant_match",
				Context: &structpb.Struct{Fields: map[string]*structpb.Value{
					"assigned_tenant": structpb.NewStringValue("tenant-a"),
				}},
			},
		},
		{Object: "node:child", Relation: "parent", User: "node:root"},
	}))

	index := New(WithMaxNodes(100), WithMaxClosureEntries(1_000))
	allowedCtx := &structpb.Struct{Fields: map[string]*structpb.Value{
		"scope":            structpb.NewStringValue("production"),
		"requested_tenant": structpb.NewStringValue("tenant-a"),
	}}
	deniedCtx := &structpb.Struct{Fields: map[string]*structpb.Value{
		"scope":            structpb.NewStringValue("staging"),
		"requested_tenant": structpb.NewStringValue("tenant-b"),
	}}

	requireEffectiveMatchEventually(
		t, index, ds, model,
		"node:child", "can_scoped_access", "user:anne", allowedCtx, true,
	)
	requireEffectiveMatchEventually(
		t, index, ds, model,
		"node:child", "can_scoped_access", "user:anne", deniedCtx, false,
	)
	requireEffectiveMatchEventually(
		t, index, ds, model,
		"node:child", "can_tenant_access", "user:anne", allowedCtx, true,
	)
	requireEffectiveMatchEventually(
		t, index, ds, model,
		"node:child", "can_tenant_access", "user:anne", deniedCtx, false,
	)
}

func TestEffectiveIndexRevocationInvalidatesBeforeCommit(t *testing.T) {
	t.Parallel()

	model := genericEffectiveModel()
	ds := memory.New()
	storeID := "store"
	memberTuple := &openfgav1.TupleKey{
		Object: "group:engineering", Relation: "member", User: "user:anne",
	}
	require.NoError(t, ds.Write(context.Background(), storeID, nil, []*openfgav1.TupleKey{
		memberTuple,
		{Object: "node:root", Relation: "reader", User: "group:engineering#member"},
	}))

	index := New(WithMaxNodes(100), WithMaxClosureEntries(1_000))
	requireEffectiveMatchEventually(
		t, index, ds, model,
		"node:root", "can_access", "user:anne", nil, true,
	)

	mutation := index.BeginMutation(storeID, nil, []*openfgav1.TupleKeyWithoutCondition{{
		Object: memberTuple.GetObject(), Relation: memberTuple.GetRelation(), User: memberTuple.GetUser(),
	}})
	require.NoError(t, ds.Write(context.Background(), storeID, []*openfgav1.TupleKeyWithoutCondition{{
		Object: memberTuple.GetObject(), Relation: memberTuple.GetRelation(), User: memberTuple.GetUser(),
	}}, nil))
	mutation.End()

	matches, used, err := index.MatchesEffectiveSubjectForModel(
		context.Background(), ds, storeID, model,
		"node:root", "can_access", "user:anne", nil,
	)
	require.NoError(t, err)
	require.False(t, used)
	require.False(t, matches)

	requireEffectiveMatchEventually(
		t, index, ds, model,
		"node:root", "can_access", "user:anne", nil, false,
	)
}

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
