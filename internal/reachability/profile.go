package reachability

import (
	"sort"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
)

type modelRelationKey struct {
	objectType string
	relation   string
}

type effectiveSource struct {
	relation            string
	inheritanceRelation string
}

type effectiveRelationPlan struct {
	sources []effectiveSource
}

func (p effectiveRelationPlan) hasInheritance() bool {
	for _, source := range p.sources {
		if source.inheritanceRelation != "" {
			return true
		}
	}
	return false
}

type effectiveModelPlan struct {
	indexedRelations    map[modelRelationKey]effectiveRelationPlan
	membershipRelations map[modelRelationKey]effectiveRelationPlan
	sourceRelations     map[modelRelationKey]struct{}
	hierarchyRelations  map[modelRelationKey]struct{}
}

func buildEffectiveModelPlan(model *openfgav1.AuthorizationModel) *effectiveModelPlan {
	if model == nil {
		return nil
	}
	typeDefinitions := make(map[string]*openfgav1.TypeDefinition, len(model.GetTypeDefinitions()))
	for _, typeDefinition := range model.GetTypeDefinitions() {
		typeDefinitions[typeDefinition.GetType()] = typeDefinition
	}

	relationPlans := make(map[modelRelationKey]effectiveRelationPlan)
	getPlan := func(key modelRelationKey) (effectiveRelationPlan, bool) {
		if plan, ok := relationPlans[key]; ok {
			return plan, true
		}
		typeDefinition := typeDefinitions[key.objectType]
		if typeDefinition == nil {
			return effectiveRelationPlan{}, false
		}
		sources, ok := flattenEffectiveSources(
			typeDefinitions, typeDefinition, key.relation, make(map[modelRelationKey]bool),
		)
		if !ok {
			return effectiveRelationPlan{}, false
		}
		plan := effectiveRelationPlan{sources: sources}
		relationPlans[key] = plan
		return plan, true
	}

	indexedRelations := make(map[modelRelationKey]effectiveRelationPlan)
	for _, typeDefinition := range model.GetTypeDefinitions() {
		for relation := range typeDefinition.GetRelations() {
			key := modelRelationKey{objectType: typeDefinition.GetType(), relation: relation}
			if plan, ok := getPlan(key); ok && plan.hasInheritance() {
				indexedRelations[key] = plan
			}
		}
	}
	if len(indexedRelations) == 0 {
		return nil
	}

	membershipQueue := make([]modelRelationKey, 0)
	enqueueMemberships := func(relationKey modelRelationKey) {
		typeDefinition := typeDefinitions[relationKey.objectType]
		if typeDefinition == nil {
			return
		}
		metadata := typeDefinition.GetMetadata().GetRelations()[relationKey.relation]
		for _, reference := range metadata.GetDirectlyRelatedUserTypes() {
			if relation := reference.GetRelation(); relation != "" {
				membershipQueue = append(membershipQueue, modelRelationKey{
					objectType: reference.GetType(),
					relation:   relation,
				})
			}
		}
	}
	for key, plan := range indexedRelations {
		for _, source := range plan.sources {
			enqueueMemberships(modelRelationKey{
				objectType: key.objectType,
				relation:   source.relation,
			})
		}
	}

	membershipRelations := make(map[modelRelationKey]effectiveRelationPlan)
	visitedMemberships := make(map[modelRelationKey]struct{})
	for len(membershipQueue) > 0 {
		key := membershipQueue[0]
		membershipQueue = membershipQueue[1:]
		if _, visited := visitedMemberships[key]; visited {
			continue
		}
		visitedMemberships[key] = struct{}{}
		plan, ok := getPlan(key)
		if !ok || plan.hasInheritance() {
			return nil
		}
		membershipRelations[key] = plan
		for _, source := range plan.sources {
			enqueueMemberships(modelRelationKey{
				objectType: key.objectType,
				relation:   source.relation,
			})
		}
	}

	sourceRelations := make(map[modelRelationKey]struct{})
	hierarchyRelations := make(map[modelRelationKey]struct{})
	for key, plan := range indexedRelations {
		for _, source := range plan.sources {
			sourceRelations[modelRelationKey{objectType: key.objectType, relation: source.relation}] = struct{}{}
			if source.inheritanceRelation != "" {
				hierarchyRelations[modelRelationKey{
					objectType: key.objectType,
					relation:   source.inheritanceRelation,
				}] = struct{}{}
			}
		}
	}
	for key, plan := range membershipRelations {
		for _, source := range plan.sources {
			sourceRelations[modelRelationKey{objectType: key.objectType, relation: source.relation}] = struct{}{}
		}
	}

	return &effectiveModelPlan{
		indexedRelations:    indexedRelations,
		membershipRelations: membershipRelations,
		sourceRelations:     sourceRelations,
		hierarchyRelations:  hierarchyRelations,
	}
}

func flattenEffectiveSources(
	typeDefinitions map[string]*openfgav1.TypeDefinition,
	typeDefinition *openfgav1.TypeDefinition,
	relation string,
	visiting map[modelRelationKey]bool,
) ([]effectiveSource, bool) {
	key := modelRelationKey{objectType: typeDefinition.GetType(), relation: relation}
	if visiting[key] {
		return nil, false
	}
	rewrite := typeDefinition.GetRelations()[relation]
	if rewrite == nil {
		return nil, false
	}
	visiting[key] = true
	defer delete(visiting, key)

	sources, ok := flattenEffectiveUserset(typeDefinitions, typeDefinition, relation, rewrite, visiting)
	if !ok {
		return nil, false
	}
	return mergeEffectiveSources(sources)
}

func flattenEffectiveUserset(
	typeDefinitions map[string]*openfgav1.TypeDefinition,
	typeDefinition *openfgav1.TypeDefinition,
	currentRelation string,
	rewrite *openfgav1.Userset,
	visiting map[modelRelationKey]bool,
) ([]effectiveSource, bool) {
	switch userset := rewrite.GetUserset().(type) {
	case *openfgav1.Userset_This:
		return []effectiveSource{{relation: currentRelation}}, true
	case *openfgav1.Userset_ComputedUserset:
		return flattenEffectiveSources(
			typeDefinitions, typeDefinition, userset.ComputedUserset.GetRelation(), visiting,
		)
	case *openfgav1.Userset_TupleToUserset:
		ttu := userset.TupleToUserset
		tuplesetRelation := ttu.GetTupleset().GetRelation()
		if !isDirectSameTypeTupleset(typeDefinition, tuplesetRelation) {
			return nil, false
		}
		computedRelation := ttu.GetComputedUserset().GetRelation()
		var sources []effectiveSource
		if computedRelation == currentRelation {
			sources = []effectiveSource{{relation: currentRelation}}
		} else {
			var ok bool
			sources, ok = flattenEffectiveSources(
				typeDefinitions, typeDefinition, computedRelation, visiting,
			)
			if !ok {
				return nil, false
			}
		}
		for idx := range sources {
			if sources[idx].inheritanceRelation != "" &&
				sources[idx].inheritanceRelation != tuplesetRelation {
				return nil, false
			}
			sources[idx].inheritanceRelation = tuplesetRelation
		}
		return sources, true
	case *openfgav1.Userset_Union:
		var result []effectiveSource
		for _, child := range userset.Union.GetChild() {
			childSources, ok := flattenEffectiveUserset(
				typeDefinitions, typeDefinition, currentRelation, child, visiting,
			)
			if !ok {
				return nil, false
			}
			result = append(result, childSources...)
		}
		return result, true
	default:
		return nil, false
	}
}

func isDirectSameTypeTupleset(typeDefinition *openfgav1.TypeDefinition, relation string) bool {
	metadata := typeDefinition.GetMetadata().GetRelations()[relation]
	if metadata == nil {
		return false
	}
	for _, reference := range metadata.GetDirectlyRelatedUserTypes() {
		if reference.GetType() == typeDefinition.GetType() &&
			reference.GetRelation() == "" &&
			reference.GetWildcard() == nil {
			return true
		}
	}
	return false
}

func mergeEffectiveSources(sources []effectiveSource) ([]effectiveSource, bool) {
	merged := make(map[string]effectiveSource, len(sources))
	for _, source := range sources {
		existing, ok := merged[source.relation]
		if ok && existing.inheritanceRelation != "" &&
			source.inheritanceRelation != "" &&
			existing.inheritanceRelation != source.inheritanceRelation {
			return nil, false
		}
		if existing.inheritanceRelation == "" {
			existing.inheritanceRelation = source.inheritanceRelation
		}
		existing.relation = source.relation
		merged[source.relation] = existing
	}
	result := make([]effectiveSource, 0, len(merged))
	for _, source := range merged {
		result = append(result, source)
	}
	sort.Slice(result, func(a, b int) bool {
		return result[a].relation < result[b].relation
	})
	return result, true
}
