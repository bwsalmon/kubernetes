package fort

func newJoinFactory[LK, RK comparable](name, left, right string, leftLookup LookupFunc[RK, LK], rightLookup LookupFunc[LK, RK]) StateUpdateFunc {
	return func(s State, clonedState bool) {
		st := s.(*state)
		j := &joiner[LK, RK]{
			target:      makeOrCloneMap[JoinKey](st, name, clonedState),
			leftSource:  GetMap[LK](st, left),
			left:        makeOrCloneMap[LK](st, "_join_left_"+name, clonedState),
			leftLookup:  leftLookup,
			rightSource: GetMap[RK](st, right),
			right:       makeOrCloneMap[RK](st, "_join_right_"+name, clonedState),
			rightLookup: rightLookup,
		}

		j.leftSource.addTarget(j)
		j.rightSource.addTarget(j)
	}
}

type joiner[LK, RK comparable] struct {
	target      *CloneMap[JoinKey]
	leftSource  keyValueSource
	left        *CloneMap[LK]
	leftLookup  LookupFunc[RK, LK]
	rightSource keyValueSource
	right       *CloneMap[RK]
	rightLookup LookupFunc[LK, RK]
}

var _ keyValueTarget = &joiner[string, int]{}

func (j *joiner[LK, RK]) onUpdate(key any, value any, source keyValueSource) {
	if source == j.leftSource {
		kv := KeyValue[LK]{Key: key.(LK), Value: value}
		rightItems := j.rightLookup(&kv, j.right)
		j.joinUpdate(KeyValueSet[LK]{kv}.All(), rightItems)
		j.left.Update(key.(LK), value)
	} else {
		kv := KeyValue[RK]{Key: key.(RK), Value: value}
		leftItems := j.leftLookup(&kv, j.left)
		j.joinUpdate(leftItems, KeyValueSet[RK]{kv}.All())
		j.right.Update(key.(RK), value)
	}
}

func (j *joiner[LK, RK]) onDelete(key any, value any, source keyValueSource) {
	//fmt.Printf("Join onDelete %s, %v\n", key, value)
	if source == j.leftSource {
		kv := KeyValue[LK]{Key: key.(LK), Value: value}
		rightItems := j.rightLookup(&kv, j.right)
		j.joinDelete(KeyValueSet[LK]{{Key: key.(LK), Value: value}}.All(), rightItems)
		j.left.Delete(key.(LK))
	} else {
		kv := KeyValue[RK]{Key: key.(RK), Value: value}
		leftItems := j.leftLookup(&kv, j.left)
		j.joinDelete(leftItems, KeyValueSet[RK]{{Key: key.(RK), Value: value}}.All())
		j.right.Delete(key.(RK))
	}
}

func (j *joiner[LK, RK]) joinUpdate(left KeyValueIterator[LK], right KeyValueIterator[RK]) {
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

func (j *joiner[LK, RK]) joinDelete(left KeyValueIterator[LK], right KeyValueIterator[RK]) {
	for lkey := range left {
		for rkey := range right {
			//fmt.Printf("Join called delete %s,%s\n", lkey, rkey)
			j.target.Delete(JoinKey{lkey, rkey})
		}
	}
}

// Get all the items in the targetItems set, expressed as a lookup function for a join.
func getAllItems[S, T comparable](sourceItem *KeyValue[S], targetItems *CloneMap[T]) KeyValueIterator[T] {
	return targetItems.All()
}

// Lookup the entry in the target map based on the source key. Is a lookupFunc for joins.
func lookupByKey[K comparable](sourceItem *KeyValue[K], targetItems *CloneMap[K]) KeyValueIterator[K] {
	kvSet := KeyValueSet[K]{}
	existing, found := targetItems.Get(sourceItem.Key)
	if found {
		kvSet = KeyValueSet[K]{{Key: sourceItem.Key, Value: existing}}
	}
	return kvSet.All()
}
