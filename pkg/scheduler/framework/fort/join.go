package fort

func newJoinFactory[LK, RK comparable](name, left, right string) StateUpdateFunc {
	return func(s State, clonedState bool) {
		st := s.(*state)
		j := &joiner[LK, RK]{
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

type joiner[LK, RK comparable] struct {
	target      *CloneMap[JoinKey]
	leftSource  keyValueSource
	left        *CloneMap[LK]
	rightSource keyValueSource
	right       *CloneMap[RK]
}

var _ keyValueTarget = &joiner[string, int]{}

func (j *joiner[LK, RK]) onUpdate(key any, value any, source keyValueSource) {
	if source == j.leftSource {
		j.joinUpdate(KeyValueSet[LK]{{Key: key.(LK), Value: value}}.All(), j.right.All())
		j.left.Update(key.(LK), value)
	} else {
		j.joinUpdate(j.left.All(), KeyValueSet[RK]{{Key: key.(RK), Value: value}}.All())
		j.right.Update(key.(RK), value)
	}
}

func (j *joiner[LK, RK]) onDelete(key any, value any, source keyValueSource) {
	//fmt.Printf("Join onDelete %s, %v\n", key, value)
	if source == j.leftSource {
		j.joinDelete(KeyValueSet[LK]{{Key: key.(LK), Value: value}}.All(), j.right.All())
		j.left.Delete(key.(LK))
	} else {
		j.joinDelete(j.left.All(), KeyValueSet[RK]{{Key: key.(RK), Value: value}}.All())
		j.right.Delete(key.(RK))
	}
}

func (j *joiner[LK, RK]) joinUpdate(left KeyValueIterator[LK], right KeyValueIterator[RK]) {
	for lkey, lvalue := range left {
		for rkey, rvalue := range right {
			j.target.Update(
				JoinKey{lkey, rkey},
				JoinValue[LK, RK]{
					Left:  KeyValue[LK]{Key: lkey, Value: lvalue},
					Right: KeyValue[RK]{Key: rkey, Value: rvalue},
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
