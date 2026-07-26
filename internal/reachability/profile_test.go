package reachability

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/openfga/openfga/pkg/testutils"
)

func TestBuildEffectiveModelPlan(t *testing.T) {
	t.Parallel()

	t.Run("derives monotonic same-type inheritance", func(t *testing.T) {
		model := testutils.MustTransformDSLToProtoWithID(`
			model
				schema 1.1
			type user
			type collection
				relations
					define parent: [collection]
					define reader: [user] or reader from parent
					define writer: [user] or writer from parent
					define can_read: reader or writer
		`)
		plan := buildEffectiveModelPlan(model)
		require.NotNil(t, plan)
		require.Equal(t, effectiveRelationPlan{sources: []effectiveSource{
			{relation: "reader", inheritanceRelation: "parent"},
			{relation: "writer", inheritanceRelation: "parent"},
		}}, plan.indexedRelations[modelRelationKey{
			objectType: "collection", relation: "can_read",
		}])
	})

	t.Run("rejects intersection and exclusion rewrites", func(t *testing.T) {
		model := testutils.MustTransformDSLToProtoWithID(`
			model
				schema 1.1
			type user
			type collection
				relations
					define parent: [collection]
					define reader: [user] or reader from parent
					define writer: [user]
					define blocked: [user]
					define strict_reader: reader and writer
					define unblocked_reader: reader but not blocked
		`)
		plan := buildEffectiveModelPlan(model)
		require.NotNil(t, plan)
		_, supportsIntersection := plan.indexedRelations[modelRelationKey{
			objectType: "collection", relation: "strict_reader",
		}]
		_, supportsExclusion := plan.indexedRelations[modelRelationKey{
			objectType: "collection", relation: "unblocked_reader",
		}]
		require.False(t, supportsIntersection)
		require.False(t, supportsExclusion)
	})

	t.Run("rejects unsupported subject-set membership rewrites", func(t *testing.T) {
		model := testutils.MustTransformDSLToProtoWithID(`
			model
				schema 1.1
			type user
			type group
				relations
					define direct: [user]
					define enabled: [user]
					define member: direct and enabled
			type collection
				relations
					define parent: [collection]
					define reader: [group#member] or reader from parent
		`)
		require.Nil(t, buildEffectiveModelPlan(model))
	})
}
