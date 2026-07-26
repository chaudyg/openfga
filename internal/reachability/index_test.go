package reachability

import (
	"context"
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
	require.NoError(t, ds.Write(context.Background(), "store", nil, []*openfgav1.TupleKey{
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
	scopes := structpb.NewListValue(&structpb.ListValue{Values: []*structpb.Value{
		structpb.NewStringValue("production"),
	}})
	require.NoError(t, ds.Write(context.Background(), "store", nil, []*openfgav1.TupleKey{
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
	allowedContext := &structpb.Struct{Fields: map[string]*structpb.Value{
		"scope":            structpb.NewStringValue("production"),
		"requested_tenant": structpb.NewStringValue("tenant-a"),
	}}
	deniedContext := &structpb.Struct{Fields: map[string]*structpb.Value{
		"scope":            structpb.NewStringValue("staging"),
		"requested_tenant": structpb.NewStringValue("tenant-b"),
	}}

	requireEffectiveMatchEventually(
		t, index, ds, model,
		"node:child", "can_scoped_access", "user:anne", allowedContext, true,
	)
	requireEffectiveMatchEventually(
		t, index, ds, model,
		"node:child", "can_scoped_access", "user:anne", deniedContext, false,
	)
	requireEffectiveMatchEventually(
		t, index, ds, model,
		"node:child", "can_tenant_access", "user:anne", allowedContext, true,
	)
	requireEffectiveMatchEventually(
		t, index, ds, model,
		"node:child", "can_tenant_access", "user:anne", deniedContext, false,
	)
}

func TestEffectiveIndexRevocationInvalidatesBeforeCommit(t *testing.T) {
	t.Parallel()

	model := genericEffectiveModel()
	ds := memory.New()
	memberTuple := &openfgav1.TupleKey{
		Object: "group:engineering", Relation: "member", User: "user:anne",
	}
	require.NoError(t, ds.Write(context.Background(), "store", nil, []*openfgav1.TupleKey{
		memberTuple,
		{Object: "node:root", Relation: "reader", User: "group:engineering#member"},
	}))

	index := New(WithMaxNodes(100), WithMaxClosureEntries(1_000))
	requireEffectiveMatchEventually(
		t, index, ds, model,
		"node:root", "can_access", "user:anne", nil, true,
	)

	mutation := index.BeginMutation("store", nil, []*openfgav1.TupleKeyWithoutCondition{{
		Object: memberTuple.GetObject(), Relation: memberTuple.GetRelation(), User: memberTuple.GetUser(),
	}})
	require.NoError(t, ds.Write(context.Background(), "store", []*openfgav1.TupleKeyWithoutCondition{{
		Object: memberTuple.GetObject(), Relation: memberTuple.GetRelation(), User: memberTuple.GetUser(),
	}}, nil))
	mutation.End()

	matches, used, err := index.MatchesEffectiveSubjectForModel(
		context.Background(), ds, "store", model,
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

func TestEffectiveIndexDisablesSnapshotAtNodeLimit(t *testing.T) {
	t.Parallel()

	model := genericEffectiveModel()
	ds := memory.New()
	require.NoError(t, ds.Write(context.Background(), "store", nil, []*openfgav1.TupleKey{
		{Object: "node:root", Relation: "reader", User: "user:anne"},
		{Object: "node:child", Relation: "parent", User: "node:root"},
	}))

	index := New(WithMaxNodes(1))
	_, used, err := index.MatchesEffectiveSubjectForModel(
		context.Background(), ds, "store", model,
		"node:child", "can_access", "user:anne", nil,
	)
	require.NoError(t, err)
	require.False(t, used)
	require.Eventually(t, func() bool {
		entry := index.getEffectiveEntry(effectiveIndexKey{storeID: "store", modelID: model.GetId()})
		entry.mu.Lock()
		defer entry.mu.Unlock()
		return entry.state == stateDisabled
	}, time.Second, 10*time.Millisecond)
}
