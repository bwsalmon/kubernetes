package fort

import "k8s.io/apimachinery/pkg/util/sets"

func fullJoinFactory[LK, RK comparable](name, left, right string) StateUpdateFunc {
	return func(s State, clonedState bool) {
		st := s.(*state)
		j := &fullJoiner[LK, RK]{
			target:      makeOrCloneMap[JoinKey](st, name, clonedState),
			leftSource:  GetMap[LK](st, left),
			left:        makeOrCloneMap[LK](st, "_join_left_"+name, clonedState),
			rightSource: GetMap[RK](st, right),
			right:       makeOrCloneMap[RK](st, "_join_right_"+name, clonedState),
		}

		j.leftSource.addTarget(j)
		j.rightSource.addTarget(j)
	}
}

type fullJoiner[LK, RK comparable] struct {
	target      *CloneMap[JoinKey]
	leftSource  keyValueSource
	left        *CloneMap[LK]
	rightSource keyValueSource
	right       *CloneMap[RK]
}

var _ keyValueTarget = &fullJoiner[string, int]{}

func (j *fullJoiner[LK, RK]) onUpdate(key any, value any, source keyValueSource) {
	if source == j.leftSource {
		kv := KeyValue[LK]{Key: key.(LK), Value: value}
		j.joinUpdate(KeyValueSet[LK]{kv}.All(), j.right.All())
		j.left.Update(key.(LK), value)
	} else {
		kv := KeyValue[RK]{Key: key.(RK), Value: value}
		j.joinUpdate(j.left.All(), KeyValueSet[RK]{kv}.All())
		j.right.Update(key.(RK), value)
	}
}

func (j *fullJoiner[LK, RK]) onDelete(key any, value any, source keyValueSource) {
	//fmt.Printf("Join onDelete %s, %v\n", key, value)
	if source == j.leftSource {
		j.joinDelete(KeyValueSet[LK]{{Key: key.(LK), Value: value}}.All(), j.right.All())
		j.left.Delete(key.(LK))
	} else {
		j.joinDelete(j.left.All(), KeyValueSet[RK]{{Key: key.(RK), Value: value}}.All())
		j.right.Delete(key.(RK))
	}
}

func (j *fullJoiner[LK, RK]) joinUpdate(left KeyValueIterator[LK], right KeyValueIterator[RK]) {
	for lkey, lvalue := range left {
		for rkey, rvalue := range right {
			j.target.Update(
				JoinKey{lkey, rkey},
				JoinValue[LK, RK]{
					Left:  &KeyValue[LK]{Key: lkey, Value: lvalue},
					Right: &KeyValue[RK]{Key: rkey, Value: rvalue},
				},
			)
		}
	}
}

func (j *fullJoiner[LK, RK]) joinDelete(left KeyValueIterator[LK], right KeyValueIterator[RK]) {
	for lkey := range left {
		for rkey := range right {
			//fmt.Printf("Join called delete %s,%s\n", lkey, rkey)
			j.target.Delete(JoinKey{lkey, rkey})
		}
	}
}

func lookupJoinFactory[LK, RK comparable](name, left, right string, lookupFunc LookupFunc[LK, RK]) StateUpdateFunc {
	return func(s State, clonedState bool) {
		st := s.(*state)
		j := &lookupJoiner[LK, RK]{
			target:       makeOrCloneMap[LK](st, name, clonedState),
			leftSource:   GetMap[LK](st, left),
			left:         makeOrCloneMap[LK](st, "_join_left_"+name, clonedState),
			rightSource:  GetMap[RK](st, right),
			right:        makeOrCloneMap[RK](st, "_join_right_"+name, clonedState),
			reverseIndex: makeOrCloneMap[RK](st, "_join_rindex_"+name, clonedState),
			getTargetKey: lookupFunc,
		}

		j.leftSource.addTarget(j)
		j.rightSource.addTarget(j)
	}
}

type lookupJoiner[LK, RK comparable] struct {
	target       *CloneMap[LK]
	leftSource   keyValueSource
	left         *CloneMap[LK]
	rightSource  keyValueSource
	right        *CloneMap[RK]
	reverseIndex *CloneMap[RK]
	getTargetKey func(kv *KeyValue[LK]) RK
}

var _ keyValueTarget = &lookupJoiner[string, int]{}

func (j *lookupJoiner[LK, RK]) onUpdate(inKey any, value any, source keyValueSource) {
	if source == j.leftSource {
		key := inKey.(LK)
		targetKey := j.getTargetKey(&KeyValue[LK]{Key: key, Value: value})

		if existingValue, found := j.left.Get(key); found {
			existingTargetKey := j.getTargetKey(&KeyValue[LK]{Key: key, Value: existingValue})
			if targetKey != existingTargetKey {
				j.removeFromReverseIndex(existingTargetKey, key)
			}
		}

		if targetValue, found := j.right.Get(targetKey); found {
			j.target.Update(key, JoinValue[LK, RK]{
				Left: &KeyValue[LK]{
					Key:   key,
					Value: value,
				},
				Right: &KeyValue[RK]{
					Key:   targetKey,
					Value: targetValue,
				},
			})
		} else {
			j.target.Delete(key)
		}

		j.addToReverseIndex(targetKey, key)
		j.left.Update(key, value)
	} else {
		key := inKey.(RK)
		entry, found := j.reverseIndex.Get(key)
		if found {
			for targetKey := range *(entry.(*sets.Set[LK])) {
				if targetValue, found := j.left.Get(targetKey); found {
					j.target.Update(targetKey, JoinValue[LK, RK]{
						Left: &KeyValue[LK]{
							Key:   targetKey,
							Value: targetValue,
						},
						Right: &KeyValue[RK]{
							Key:   key,
							Value: value,
						},
					})
				}
			}
		}
		j.right.Update(key, value)
	}
}

func (j *lookupJoiner[LK, RK]) onDelete(inKey any, value any, source keyValueSource) {
	if source == j.leftSource {
		key := inKey.(LK)
		j.target.Delete(key)
		targetKey := j.getTargetKey(&KeyValue[LK]{Key: key, Value: value})
		j.removeFromReverseIndex(targetKey, key)
		j.left.Delete(key)
	} else {
		key := inKey.(RK)
		if entry, found := j.reverseIndex.Get(key); found {
			for targetKey := range *(entry.(*sets.Set[LK])) {
				j.target.Delete(targetKey)
			}
		}
		j.right.Delete(key)
	}
}

func (m *lookupJoiner[LK, RK]) addToReverseIndex(sourceKey RK, targetKey LK) {
	var sourceSet sets.Set[LK]
	if currSourceSet, found := m.reverseIndex.Get(sourceKey); found {
		sourceSet = currSourceSet.(*sets.Set[LK]).Clone()
		sourceSet.Insert(targetKey)
	} else {
		sourceSet = sets.New(targetKey)
	}
	m.reverseIndex.Update(sourceKey, &sourceSet)
}

func (m *lookupJoiner[LK, RK]) removeFromReverseIndex(sourceKey RK, targetKey LK) {
	if currSourceSet, found := m.reverseIndex.Get(sourceKey); found {
		sourceSet := currSourceSet.(*sets.Set[LK]).Clone()
		sourceSet.Delete(targetKey)
		if len(sourceSet) == 0 {
			m.reverseIndex.Delete(sourceKey)
		} else {
			m.reverseIndex.Update(sourceKey, &sourceSet)
		}
	}
}
