package check

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/internal/modelgraph"
	"github.com/openfga/openfga/internal/planner"
	"github.com/openfga/openfga/internal/reachability"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/storage/memory"
	"github.com/openfga/openfga/pkg/testutils"
	"github.com/openfga/openfga/pkg/tuple"
)

type countingRelationshipReader struct {
	storage.RelationshipTupleReader
	queries atomic.Uint32
}

func (r *countingRelationshipReader) Read(
	ctx context.Context,
	store string,
	filter storage.ReadFilter,
	options storage.ReadOptions,
) (storage.TupleIterator, error) {
	r.queries.Add(1)
	return r.RelationshipTupleReader.Read(ctx, store, filter, options)
}

func (r *countingRelationshipReader) ReadPage(
	ctx context.Context,
	store string,
	filter storage.ReadFilter,
	options storage.ReadPageOptions,
) ([]*openfgav1.Tuple, string, error) {
	r.queries.Add(1)
	return r.RelationshipTupleReader.ReadPage(ctx, store, filter, options)
}

func (r *countingRelationshipReader) ReadUserTuple(
	ctx context.Context,
	store string,
	filter storage.ReadUserTupleFilter,
	options storage.ReadUserTupleOptions,
) (*openfgav1.Tuple, error) {
	r.queries.Add(1)
	return r.RelationshipTupleReader.ReadUserTuple(ctx, store, filter, options)
}

func (r *countingRelationshipReader) ReadUsersetTuples(
	ctx context.Context,
	store string,
	filter storage.ReadUsersetTuplesFilter,
	options storage.ReadUsersetTuplesOptions,
) (storage.TupleIterator, error) {
	r.queries.Add(1)
	return r.RelationshipTupleReader.ReadUsersetTuples(ctx, store, filter, options)
}

func (r *countingRelationshipReader) ReadStartingWithUser(
	ctx context.Context,
	store string,
	filter storage.ReadStartingWithUserFilter,
	options storage.ReadStartingWithUserOptions,
) (storage.TupleIterator, error) {
	r.queries.Add(1)
	return r.RelationshipTupleReader.ReadStartingWithUser(ctx, store, filter, options)
}

func TestEffectiveIndexWarmCheckUsesNoTupleReads(t *testing.T) {
	t.Parallel()

	model := testutils.MustTransformDSLToProtoWithID(`
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
				define can_access: reader or writer
				define can_scoped_access: scoped_reader
		condition scope_allowed(scope: string, scopes: list<string>) {
			scope in scopes
		}
	`)
	modelGraph, err := modelgraph.New(model)
	require.NoError(t, err)

	ds := memory.New()
	storeID := "store"
	require.NoError(t, ds.Write(context.Background(), storeID, nil, []*openfgav1.TupleKey{
		tuple.NewTupleKey("group:engineering", "member", "user:anne"),
		tuple.NewTupleKey("group:platform", "member", "group:engineering#member"),
		tuple.NewTupleKey("node:root", "reader", "group:platform#member"),
		tuple.NewTupleKey("node:services", "parent", "node:root"),
		tuple.NewTupleKey("node:api", "parent", "node:services"),
		{
			Object:   "node:root",
			Relation: "scoped_reader",
			User:     "group:engineering#member",
			Condition: &openfgav1.RelationshipCondition{
				Name: "scope_allowed",
				Context: &structpb.Struct{Fields: map[string]*structpb.Value{
					"scopes": structpb.NewListValue(&structpb.ListValue{Values: []*structpb.Value{
						structpb.NewStringValue("production"),
					}}),
				}},
			},
		},
	}))

	reader := &countingRelationshipReader{RelationshipTupleReader: ds}
	index := reachability.New(
		reachability.WithMaxNodes(100),
		reachability.WithMaxClosureEntries(1_000),
	)
	require.Eventually(t, func() bool {
		_, used, matchErr := index.MatchesEffectiveSubjectForModel(
			context.Background(),
			reader,
			storeID,
			model,
			"node:api",
			"can_access",
			"user:anne",
			nil,
		)
		return matchErr == nil && used
	}, time.Second, 10*time.Millisecond)

	resolver := New(Config{
		Model:                     modelGraph,
		Datastore:                 reader,
		Planner:                   planner.New(&planner.Config{}),
		ConcurrencyLimit:          10,
		LastCacheInvalidationTime: time.Now().Add(-time.Hour),
		ReachabilityIndex:         index,
	})
	request, err := NewRequest(RequestParams{
		StoreID:  storeID,
		Model:    modelGraph,
		TupleKey: tuple.NewTupleKey("node:api", "can_access", "user:anne"),
	})
	require.NoError(t, err)

	reader.queries.Store(0)
	response, err := resolver.ResolveCheck(context.Background(), request)
	require.NoError(t, err)
	require.True(t, response.GetAllowed())
	require.Zero(t, reader.queries.Load())

	negativeRequest, err := NewRequest(RequestParams{
		StoreID:  storeID,
		Model:    modelGraph,
		TupleKey: tuple.NewTupleKey("node:api", "can_access", "user:bob"),
	})
	require.NoError(t, err)

	response, err = resolver.ResolveCheck(context.Background(), negativeRequest)
	require.NoError(t, err)
	require.False(t, response.GetAllowed())
	require.Zero(t, reader.queries.Load())

	dimensionContext := &structpb.Struct{Fields: map[string]*structpb.Value{
		"scope": structpb.NewStringValue("production"),
	}}
	require.Eventually(t, func() bool {
		_, used, matchErr := index.MatchesEffectiveSubjectForModel(
			context.Background(),
			reader,
			storeID,
			model,
			"node:api",
			"can_scoped_access",
			"user:anne",
			dimensionContext,
		)
		return matchErr == nil && used
	}, time.Second, 10*time.Millisecond)

	conditionalRequest, err := NewRequest(RequestParams{
		StoreID:  storeID,
		Model:    modelGraph,
		TupleKey: tuple.NewTupleKey("node:api", "can_scoped_access", "user:anne"),
		Context:  dimensionContext,
	})
	require.NoError(t, err)

	reader.queries.Store(0)
	response, err = resolver.ResolveCheck(context.Background(), conditionalRequest)
	require.NoError(t, err)
	require.True(t, response.GetAllowed())
	require.Zero(t, reader.queries.Load())
}
