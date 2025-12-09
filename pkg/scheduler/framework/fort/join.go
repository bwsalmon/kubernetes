package fort

import "k8s.io/apimachinery/pkg/util/sets"

func fullJoinFactory[LK, RK comparable](left, right string) SourceSpec {
	return func(s State, name string, clonedState bool) any {
		st := s.(*state)
		j := &fullJoiner[LK, RK]{
			target:      newKeyValueConnector[JoinKey[LK, RK]](),
			leftSource:  GetSource(st, left),
			left:        makeOrCloneMap[LK](st, "@join_left_"+name, clonedState),
			rightSource: GetSource(st, right),
			right:       makeOrCloneMap[RK](st, "@join_right_"+name, clonedState),
		}

		j.leftSource.addTarget(j)
		j.rightSource.addTarget(j)

		return j.target
	}
}

type fullJoiner[LK, RK comparable] struct {
	target      *keyValueConnector[JoinKey[LK, RK]]
	leftSource  KeyValueSource
	left        *CloneMap[LK]
	rightSource KeyValueSource
	right       *CloneMap[RK]
}

var _ KeyValueTarget = &fullJoiner[string, int]{}

func (j *fullJoiner[LK, RK]) onUpdate(key any, value any, source KeyValueSource) {
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

func (j *fullJoiner[LK, RK]) onDelete(key any, value any, source KeyValueSource) {
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
				JoinKey[LK, RK]{lkey, rkey},
				JoinValue{
					Left:  lvalue,
					Right: rvalue,
				},
			)
		}
	}
}

func (j *fullJoiner[LK, RK]) joinDelete(left KeyValueIterator[LK], right KeyValueIterator[RK]) {
	for lkey, lval := range left {
		for rkey, rval := range right {
			//fmt.Printf("Join called delete %s,%s\n", lkey, rkey)
			j.target.Delete(
				JoinKey[LK, RK]{lkey, rkey},
				JoinValue{
					Left:  lval,
					Right: rval,
				},
			)
		}
	}
}

func lookupJoinFactory[LK, RK comparable](left, right string, lookupFunc LookupFunc[LK, RK]) SourceSpec {
	return func(s State, name string, clonedState bool) any {
		st := s.(*state)
		j := &lookupJoiner[LK, RK]{
			target:       makeOrCloneMap[LK](st, name, clonedState),
			leftSource:   GetMap[LK](st, left),
			left:         makeOrCloneMap[LK](st, "@join_left_"+name, clonedState),
			rightSource:  GetMap[RK](st, right),
			right:        makeOrCloneMap[RK](st, "@join_right_"+name, clonedState),
			reverseIndex: makeOrCloneMap[RK](st, "@join_rindex_"+name, clonedState),
			getTargetKey: lookupFunc,
		}

		j.leftSource.addTarget(j)
		j.rightSource.addTarget(j)

		return j.target
	}
}

type lookupJoiner[LK, RK comparable] struct {
	target       *CloneMap[LK]
	leftSource   KeyValueSource
	left         *CloneMap[LK]
	rightSource  KeyValueSource
	right        *CloneMap[RK]
	reverseIndex *CloneMap[RK]
	getTargetKey func(kv *KeyValue[LK]) RK
}

var _ KeyValueTarget = &lookupJoiner[string, int]{}

func (j *lookupJoiner[LK, RK]) onUpdate(inKey any, value any, source KeyValueSource) {
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
			j.target.Update(key, JoinValue{
				Left:  value,
				Right: targetValue,
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
					j.target.Update(targetKey, JoinValue{
						Left:  targetValue,
						Right: value,
					})
				}
			}
		}
		j.right.Update(key, value)
	}
}

func (j *lookupJoiner[LK, RK]) onDelete(inKey any, value any, source KeyValueSource) {
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
