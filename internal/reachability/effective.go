package reachability

import (
	"context"
	"errors"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/RoaringBitmap/roaring/v2"
	"google.golang.org/protobuf/types/known/structpb"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/internal/validation"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
)

type effectiveIndexKey struct {
	storeID string
	modelID string
}

type effectiveResourceKey struct {
	object    string
	relation  string
	dimension string
}

type effectiveObjectRelation struct {
	object   string
	relation string
}

type effectiveSnapshot struct {
	subjectIDs               map[string]uint32
	memberships              map[string]*roaring.Bitmap
	effectiveSubjects        map[effectiveResourceKey]*roaring.Bitmap
	conditionRequestFields   map[string]string
	objectRelationConditions map[effectiveObjectRelation]map[string]struct{}
}

type effectiveEntry struct {
	mu          sync.Mutex
	generation  uint64
	buildID     uint64
	state       state
	snapshot    *effectiveSnapshot
	cancel      context.CancelFunc
	publishedAt time.Time
}

func resetEffectiveEntry(entry *effectiveEntry) {
	if entry.cancel != nil {
		entry.cancel()
		entry.cancel = nil
	}
	entry.generation++
	entry.state = stateEmpty
	entry.snapshot = nil
	entry.publishedAt = time.Time{}
}

func (i *Index) getEffectiveEntry(key effectiveIndexKey) *effectiveEntry {
	i.mu.Lock()
	defer i.mu.Unlock()
	indexEntry := i.effectiveEntries[key]
	if indexEntry == nil {
		indexEntry = &effectiveEntry{}
		i.effectiveEntries[key] = indexEntry
	}
	return indexEntry
}

func (i *Index) getEffectiveModelPlan(model *openfgav1.AuthorizationModel) *effectiveModelPlan {
	if model == nil || model.GetId() == "" {
		return nil
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	if plan, ok := i.effectivePlans[model.GetId()]; ok {
		return plan
	}
	plan := buildEffectiveModelPlan(model)
	i.effectivePlans[model.GetId()] = plan
	return plan
}

// MatchesEffectiveSubjectForModel evaluates eligible monotonic, same-type
// recursive TTU relations from a model-derived immutable snapshot.
func (i *Index) MatchesEffectiveSubjectForModel(
	_ context.Context,
	datastore storage.RelationshipTupleReader,
	storeID string,
	model *openfgav1.AuthorizationModel,
	object, relation, user string,
	requestContext *structpb.Struct,
) (matches bool, used bool, err error) {
	objectType, _ := tuple.SplitObject(object)
	plan := i.getEffectiveModelPlan(model)
	if plan == nil {
		return false, false, nil
	}
	if _, ok := plan.indexedRelations[modelRelationKey{objectType: objectType, relation: relation}]; !ok {
		return false, false, nil
	}

	key := effectiveIndexKey{storeID: storeID, modelID: model.GetId()}
	storeLock := i.getStoreLock(storeID)
	// A write-held store lock means a mutation is invalidating this store's
	// snapshots; delegate to the normal resolver instead of parking the
	// check on the guard for the duration of the datastore write. Concurrent
	// checks share the read lock and do not contend with each other.
	if !storeLock.TryRLock() {
		return false, false, nil
	}
	entry := i.getEffectiveEntry(key)
	entry.mu.Lock()
	storeLock.RUnlock()

	if i.snapshotTTL > 0 &&
		(entry.state == stateReady || entry.state == stateDisabled) &&
		time.Since(entry.publishedAt) > i.snapshotTTL {
		resetEffectiveEntry(entry)
	}
	if entry.state == stateReady {
		snapshot := entry.snapshot
		entry.mu.Unlock()
		dimensions, applicable := snapshot.requestDimensions(object, relation, requestContext)
		if !applicable {
			return false, false, nil
		}
		return matchesEffectiveSnapshot(snapshot, object, relation, user, dimensions), true, nil
	}
	if entry.state == stateEmpty {
		buildCtx, cancel := context.WithTimeout(context.Background(), i.buildTimeout)
		entry.state = stateBuilding
		entryGeneration := entry.generation
		entry.buildID++
		buildID := entry.buildID
		entry.cancel = cancel
		storeGeneration := i.storeGeneration(storeID)
		go i.buildAndPublishEffective(
			buildCtx, cancel, datastore, model, plan, key, entry,
			entryGeneration, storeGeneration, buildID,
		)
	}
	entry.mu.Unlock()
	return false, false, nil
}

func (s *effectiveSnapshot) requestDimensions(
	object, relation string,
	requestContext *structpb.Struct,
) ([]string, bool) {
	conditionNames := s.objectRelationConditions[effectiveObjectRelation{
		object: object, relation: relation,
	}]
	if len(conditionNames) == 0 {
		return nil, true
	}
	if requestContext == nil {
		return nil, false
	}
	dimensions := make([]string, 0, len(conditionNames))
	for conditionName := range conditionNames {
		requestField := s.conditionRequestFields[conditionName]
		value, ok := requestContext.GetFields()[requestField]
		if !ok || value.GetStringValue() == "" {
			return nil, false
		}
		dimensions = append(dimensions, conditionDimension(conditionName, value.GetStringValue()))
	}
	sort.Strings(dimensions)
	return dimensions, true
}

func matchesEffectiveSnapshot(
	snapshot *effectiveSnapshot,
	object, relation, user string,
	dimensions []string,
) bool {
	subjectID, ok := snapshot.subjectIDs[user]
	if !ok {
		return false
	}
	memberships := snapshot.memberships[user]
	if memberships == nil {
		memberships = roaring.BitmapOf(subjectID)
	}
	baseKey := effectiveResourceKey{object: object, relation: relation}
	effective := snapshot.effectiveSubjects[baseKey]
	if len(dimensions) == 1 {
		if scoped := snapshot.effectiveSubjects[effectiveResourceKey{
			object: object, relation: relation, dimension: dimensions[0],
		}]; scoped != nil {
			effective = scoped
		}
	} else if len(dimensions) > 1 {
		if effective == nil {
			effective = roaring.New()
		} else {
			effective = effective.Clone()
		}
		for _, dimension := range dimensions {
			if scoped := snapshot.effectiveSubjects[effectiveResourceKey{
				object: object, relation: relation, dimension: dimension,
			}]; scoped != nil {
				effective.Or(scoped)
			}
		}
	}
	return effective != nil && effective.Intersects(memberships)
}

func (i *Index) buildAndPublishEffective(
	ctx context.Context,
	cancel context.CancelFunc,
	datastore storage.RelationshipTupleReader,
	model *openfgav1.AuthorizationModel,
	plan *effectiveModelPlan,
	key effectiveIndexKey,
	entry *effectiveEntry,
	entryGeneration, storeGeneration, buildID uint64,
) {
	defer cancel()
	snapshot, result := i.buildEffective(ctx, datastore, model, plan, key)
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

type effectiveResolvedSource struct {
	objectType          string
	object              string
	relation            string
	inheritanceRelation string
	dimension           string
}

func (i *Index) buildEffective(
	ctx context.Context,
	datastore storage.RelationshipTupleReader,
	model *openfgav1.AuthorizationModel,
	plan *effectiveModelPlan,
	key effectiveIndexKey,
) (*effectiveSnapshot, buildResult) {
	typeSystem, err := typesystem.New(model)
	if err != nil {
		return nil, buildDisabled
	}
	iter, err := datastore.Read(ctx, key.storeID, storage.ReadFilter{}, storage.ReadOptions{
		Consistency: storage.ConsistencyOptions{Preference: openfgav1.ConsistencyPreference_HIGHER_CONSISTENCY},
	})
	if err != nil {
		return nil, buildRetry
	}
	defer iter.Stop()

	subjectIDs := make(map[string]uint32)
	objectsByType := make(map[string]map[string]struct{})
	direct := make(map[effectiveResourceKey]*roaring.Bitmap)
	directSubjects := make(map[modelRelationKey]map[string][]string)
	hierarchies := make(map[modelRelationKey]map[string][]string)
	sourceDimensions := make(map[modelRelationKey]map[string]struct{})
	conditionRequestFields := make(map[string]string)
	objectCount := 0
	indexedObjectTypes := effectiveIndexedObjectTypes(plan)

	addSubject := func(subject string) bool {
		if _, ok := subjectIDs[subject]; ok {
			return true
		}
		if len(subjectIDs) >= i.maxNodes {
			return false
		}
		subjectIDs[subject] = uint32(len(subjectIDs))
		return true
	}
	addObject := func(objectType, object string) bool {
		if objectsByType[objectType] == nil {
			objectsByType[objectType] = make(map[string]struct{})
		}
		if _, ok := objectsByType[objectType][object]; ok {
			return true
		}
		if _, indexed := indexedObjectTypes[objectType]; indexed {
			if objectCount >= i.maxNodes {
				return false
			}
			objectCount++
		}
		objectsByType[objectType][object] = struct{}{}
		return true
	}

	membershipSources := effectiveMembershipSources(plan)

	for {
		item, nextErr := iter.Next(ctx)
		if nextErr != nil {
			if ctx.Err() != nil {
				return nil, buildRetry
			}
			if errors.Is(nextErr, storage.ErrIteratorDone) {
				break
			}
			return nil, buildRetry
		}
		tupleKey := item.GetKey()
		if validation.ValidateTupleForRead(typeSystem, tupleKey) != nil {
			continue
		}
		objectType, _ := tuple.SplitObject(tupleKey.GetObject())
		relationKey := modelRelationKey{objectType: objectType, relation: tupleKey.GetRelation()}

		if _, ok := plan.hierarchyRelations[relationKey]; ok {
			userObject, userRelation := tuple.SplitObjectRelation(tupleKey.GetUser())
			if tupleKey.GetCondition() != nil ||
				userRelation != "" ||
				tuple.GetType(userObject) != objectType ||
				!addObject(objectType, tupleKey.GetObject()) ||
				!addObject(objectType, userObject) {
				return nil, buildDisabled
			}
			if hierarchies[relationKey] == nil {
				hierarchies[relationKey] = make(map[string][]string)
			}
			hierarchies[relationKey][tupleKey.GetObject()] = append(
				hierarchies[relationKey][tupleKey.GetObject()], userObject,
			)
			continue
		}
		if _, ok := plan.sourceRelations[relationKey]; !ok {
			continue
		}
		if tuple.IsTypedWildcard(tupleKey.GetUser()) || !addObject(objectType, tupleKey.GetObject()) ||
			!addSubject(tupleKey.GetUser()) {
			return nil, buildDisabled
		}
		if _, membershipSource := membershipSources[relationKey]; membershipSource &&
			tupleKey.GetCondition() != nil {
			return nil, buildDisabled
		}
		dimensions, requestField, ok := indexedConditionDimensions(
			model.GetConditions(), tupleKey.GetCondition(),
		)
		if !ok {
			return nil, buildDisabled
		}
		if tupleKey.GetCondition() != nil {
			conditionName := tupleKey.GetCondition().GetName()
			if existing := conditionRequestFields[conditionName]; existing != "" && existing != requestField {
				return nil, buildDisabled
			}
			conditionRequestFields[conditionName] = requestField
		}
		for _, dimension := range dimensions {
			resourceKey := effectiveResourceKey{
				object: tupleKey.GetObject(), relation: tupleKey.GetRelation(), dimension: dimension,
			}
			bitmap := direct[resourceKey]
			if bitmap == nil {
				bitmap = roaring.New()
				direct[resourceKey] = bitmap
			}
			bitmap.Add(subjectIDs[tupleKey.GetUser()])
			if dimension != "" {
				if sourceDimensions[relationKey] == nil {
					sourceDimensions[relationKey] = make(map[string]struct{})
				}
				sourceDimensions[relationKey][dimension] = struct{}{}
			}
		}
		if tupleKey.GetCondition() == nil {
			if directSubjects[relationKey] == nil {
				directSubjects[relationKey] = make(map[string][]string)
			}
			directSubjects[relationKey][tupleKey.GetObject()] = append(
				directSubjects[relationKey][tupleKey.GetObject()], tupleKey.GetUser(),
			)
		}
	}

	membershipEdges, ok := buildEffectiveMembershipEdges(
		plan, objectsByType, directSubjects, addSubject,
	)
	if !ok {
		return nil, buildDisabled
	}

	resolvedMemo := make(map[effectiveResolvedSource]*roaring.Bitmap)
	visiting := make(map[effectiveResolvedSource]bool)
	var resolveSource func(effectiveResolvedSource) (*roaring.Bitmap, error)
	resolveSource = func(source effectiveResolvedSource) (*roaring.Bitmap, error) {
		if bitmap, ok := resolvedMemo[source]; ok {
			return bitmap, nil
		}
		if visiting[source] {
			return nil, errCycle
		}
		visiting[source] = true
		bitmap := roaring.New()
		if directBitmap := direct[effectiveResourceKey{
			object: source.object, relation: source.relation, dimension: source.dimension,
		}]; directBitmap != nil {
			bitmap.Or(directBitmap)
		}
		if source.inheritanceRelation != "" {
			hierarchyKey := modelRelationKey{
				objectType: source.objectType, relation: source.inheritanceRelation,
			}
			for _, parent := range hierarchies[hierarchyKey][source.object] {
				parentBitmap, resolveErr := resolveSource(effectiveResolvedSource{
					objectType:          source.objectType,
					object:              parent,
					relation:            source.relation,
					inheritanceRelation: source.inheritanceRelation,
					dimension:           source.dimension,
				})
				if resolveErr != nil {
					return nil, resolveErr
				}
				bitmap.Or(parentBitmap)
			}
		}
		visiting[source] = false
		resolvedMemo[source] = bitmap
		return bitmap, nil
	}

	effectiveSubjects := make(map[effectiveResourceKey]*roaring.Bitmap)
	objectRelationConditions := make(map[effectiveObjectRelation]map[string]struct{})
	for target, relationPlan := range plan.indexedRelations {
		for object := range objectsByType[target.objectType] {
			base := roaring.New()
			dimensions := make(map[string]struct{})
			for _, source := range relationPlan.sources {
				sourceKey := modelRelationKey{objectType: target.objectType, relation: source.relation}
				for dimension := range sourceDimensions[sourceKey] {
					dimensions[dimension] = struct{}{}
				}
				sourceBitmap, resolveErr := resolveSource(effectiveResolvedSource{
					objectType:          target.objectType,
					object:              object,
					relation:            source.relation,
					inheritanceRelation: source.inheritanceRelation,
				})
				if resolveErr != nil {
					return nil, buildDisabled
				}
				base.Or(sourceBitmap)
			}
			if !base.IsEmpty() {
				effectiveSubjects[effectiveResourceKey{
					object: object, relation: target.relation,
				}] = base.Clone()
			}
			for dimension := range dimensions {
				scoped := base.Clone()
				hasScopedSubjects := false
				for _, source := range relationPlan.sources {
					sourceBitmap, resolveErr := resolveSource(effectiveResolvedSource{
						objectType:          target.objectType,
						object:              object,
						relation:            source.relation,
						inheritanceRelation: source.inheritanceRelation,
						dimension:           dimension,
					})
					if resolveErr != nil {
						return nil, buildDisabled
					}
					if !sourceBitmap.IsEmpty() {
						hasScopedSubjects = true
						scoped.Or(sourceBitmap)
					}
				}
				if !hasScopedSubjects {
					continue
				}
				effectiveSubjects[effectiveResourceKey{
					object: object, relation: target.relation, dimension: dimension,
				}] = scoped
				conditionName, _, _ := strings.Cut(dimension, "\x00")
				objectRelation := effectiveObjectRelation{object: object, relation: target.relation}
				if objectRelationConditions[objectRelation] == nil {
					objectRelationConditions[objectRelation] = make(map[string]struct{})
				}
				objectRelationConditions[objectRelation][conditionName] = struct{}{}
			}
		}
	}

	memberships := buildMemberships(subjectIDs, membershipEdges)
	if effectiveCardinality(effectiveSubjects, memberships) > i.maxClosureEntries {
		return nil, buildDisabled
	}
	return &effectiveSnapshot{
		subjectIDs:               subjectIDs,
		memberships:              memberships,
		effectiveSubjects:        effectiveSubjects,
		conditionRequestFields:   conditionRequestFields,
		objectRelationConditions: objectRelationConditions,
	}, buildReady
}

func effectiveIndexedObjectTypes(plan *effectiveModelPlan) map[string]struct{} {
	objectTypes := make(map[string]struct{})
	for relationKey := range plan.indexedRelations {
		objectTypes[relationKey.objectType] = struct{}{}
	}
	return objectTypes
}

func effectiveMembershipSources(plan *effectiveModelPlan) map[modelRelationKey]struct{} {
	sources := make(map[modelRelationKey]struct{})
	for key, membershipPlan := range plan.membershipRelations {
		for _, source := range membershipPlan.sources {
			sources[modelRelationKey{
				objectType: key.objectType, relation: source.relation,
			}] = struct{}{}
		}
	}
	return sources
}

func buildEffectiveMembershipEdges(
	plan *effectiveModelPlan,
	objectsByType map[string]map[string]struct{},
	directSubjects map[modelRelationKey]map[string][]string,
	addSubject func(string) bool,
) (map[string][]string, bool) {
	membershipEdges := make(map[string][]string)
	for target, membershipPlan := range plan.membershipRelations {
		for object := range objectsByType[target.objectType] {
			container := object + "#" + target.relation
			for _, source := range membershipPlan.sources {
				sourceKey := modelRelationKey{objectType: target.objectType, relation: source.relation}
				for _, subject := range directSubjects[sourceKey][object] {
					if !addSubject(container) {
						return nil, false
					}
					membershipEdges[subject] = append(membershipEdges[subject], container)
				}
			}
		}
	}
	return membershipEdges, true
}

func effectiveCardinality(
	effectiveSubjects map[effectiveResourceKey]*roaring.Bitmap,
	memberships map[string]*roaring.Bitmap,
) uint64 {
	var entries uint64
	for _, bitmap := range effectiveSubjects {
		entries += bitmap.GetCardinality()
	}
	for _, bitmap := range memberships {
		entries += bitmap.GetCardinality()
	}
	return entries
}

func buildMemberships(subjectIDs map[string]uint32, edges map[string][]string) map[string]*roaring.Bitmap {
	memberships := make(map[string]*roaring.Bitmap, len(subjectIDs))
	for subject, subjectID := range subjectIDs {
		bitmap := roaring.BitmapOf(subjectID)
		queue := []string{subject}
		visited := map[string]struct{}{subject: {}}
		for len(queue) > 0 {
			current := queue[0]
			queue = queue[1:]
			for _, container := range edges[current] {
				if _, ok := visited[container]; ok {
					continue
				}
				visited[container] = struct{}{}
				bitmap.Add(subjectIDs[container])
				queue = append(queue, container)
			}
		}
		memberships[subject] = bitmap
	}
	return memberships
}

func indexedConditionDimensions(
	conditions map[string]*openfgav1.Condition,
	condition *openfgav1.RelationshipCondition,
) ([]string, string, bool) {
	if condition == nil {
		return []string{""}, "", true
	}
	definition := conditions[condition.GetName()]
	if definition == nil {
		return nil, "", false
	}
	parts := strings.Fields(definition.GetExpression())
	if len(parts) != 3 {
		return nil, "", false
	}
	left, operator, right := parts[0], parts[1], parts[2]
	conditionContext := condition.GetContext().GetFields()
	switch operator {
	case "in":
		values, ok := conditionContext[right]
		if !ok || conditionContext[left] != nil {
			return nil, "", false
		}
		list := values.GetListValue().GetValues()
		if len(list) == 0 {
			return nil, "", false
		}
		result := make([]string, 0, len(list))
		for _, value := range list {
			if value.GetStringValue() == "" {
				return nil, "", false
			}
			result = append(result, conditionDimension(condition.GetName(), value.GetStringValue()))
		}
		return result, left, true
	case "==":
		leftValue, leftBound := conditionContext[left]
		rightValue, rightBound := conditionContext[right]
		if leftBound == rightBound {
			return nil, "", false
		}
		if leftBound {
			if leftValue.GetStringValue() == "" {
				return nil, "", false
			}
			return []string{conditionDimension(condition.GetName(), leftValue.GetStringValue())}, right, true
		}
		if rightValue.GetStringValue() == "" {
			return nil, "", false
		}
		return []string{conditionDimension(condition.GetName(), rightValue.GetStringValue())}, left, true
	default:
		return nil, "", false
	}
}

func conditionDimension(conditionName, value string) string {
	return conditionName + "\x00" + value
}
