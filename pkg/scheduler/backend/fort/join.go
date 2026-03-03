package fort

type fullJoiner[LK comparable, LV any, RK comparable, RV any] struct {
	target      *keyValueConnector[JoinKey[LK, RK], JoinValue[LV, RV]]
	leftSource  leftSide[LK, LV, RK, RV]
	left        *CloneMap[LK, LV]
	rightSource rightSide[LK, LV, RK, RV]
	right       *CloneMap[RK, RV]
}

func newFullJoiner[LK comparable, LV any, RK comparable, RV any](
	left Source[LK, LV],
	right Source[RK, RV],
	cloneFrom Source[JoinKey[LK, RK], JoinValue[LV, RV]],
) *fullJoiner[LK, LV, RK, RV] {
	newJoiner := &fullJoiner[LK, LV, RK, RV]{
		target:      newKeyValueConnector[JoinKey[LK, RK], JoinValue[LV, RV]](),
		leftSource:  leftSide[LK, LV, RK, RV]{},
		rightSource: rightSide[LK, LV, RK, RV]{},
	}

	newJoiner.leftSource.parent = newJoiner
	newJoiner.rightSource.parent = newJoiner

	if cloneFrom == nil {
		newJoiner.left = makeOrCloneMap[LK, LV](nil)
		newJoiner.right = makeOrCloneMap[RK, RV](nil)
	} else {
		c := cloneFrom.(*fullJoiner[LK, LV, RK, RV])
		newJoiner.left = c.left.Clone()
		newJoiner.right = c.right.Clone()
	}

	left.addTarget(&newJoiner.leftSource)
	right.addTarget(&newJoiner.rightSource)

	return newJoiner
}

type leftSide[LK comparable, LV any, RK comparable, RV any] struct {
	parent *fullJoiner[LK, LV, RK, RV]
}

func (s *leftSide[LK, LV, RK, RV]) OnUpdate(key LK, value LV, source Source[LK, LV]) error {
	j := s.parent
	kv := KeyValue[LK, LV]{Key: key, Value: value}
	j.joinUpdate(KeyValueSet[LK, LV]{kv}.All(), j.right.All())
	j.left.Update(key, value)
	return nil
}

func (s *leftSide[LK, LV, RK, RV]) OnDelete(key LK, value LV, source Source[LK, LV]) error {
	j := s.parent
	j.joinDelete(KeyValueSet[LK, LV]{{Key: key, Value: value}}.All(), j.right.All())
	j.left.Delete(key)
	return nil
}

type rightSide[LK comparable, LV any, RK comparable, RV any] struct {
	parent *fullJoiner[LK, LV, RK, RV]
}

func (s *rightSide[LK, LV, RK, RV]) OnUpdate(key RK, value RV, source Source[RK, RV]) error {
	j := s.parent
	kv := KeyValue[RK, RV]{Key: key, Value: value}
	j.joinUpdate(j.left.All(), KeyValueSet[RK, RV]{kv}.All())
	j.right.Update(key, value)
	return nil
}

func (s *rightSide[LK, LV, RK, RV]) OnDelete(key RK, value RV, source Source[RK, RV]) error {
	j := s.parent
	j.joinDelete(j.left.All(), KeyValueSet[RK, RV]{{Key: key, Value: value}}.All())
	j.right.Delete(key)
	return nil
}

func (j *fullJoiner[LK, LV, RK, RV]) joinUpdate(left KeyValueIterator[LK, LV], right KeyValueIterator[RK, RV]) {
	for lkey, lvalue := range left {
		for rkey, rvalue := range right {
			j.target.Update(
				JoinKey[LK, RK]{lkey, rkey},
				JoinValue[LV, RV]{
					Left:  lvalue,
					Right: rvalue,
				},
			)
		}
	}
}

func (j *fullJoiner[LK, LV, RK, RV]) joinDelete(left KeyValueIterator[LK, LV], right KeyValueIterator[RK, RV]) {
	for lkey, lval := range left {
		for rkey, rval := range right {
			//fmt.Printf("Join called delete %s,%s\n", lkey, rkey)
			j.target.Delete(
				JoinKey[LK, RK]{lkey, rkey},
				JoinValue[LV, RV]{
					Left:  lval,
					Right: rval,
				},
			)
		}
	}
}

func (j *fullJoiner[LK, LV, RK, RV]) addTarget(t Target[JoinKey[LK, RK], JoinValue[LV, RV]]) {
	j.target.addTarget(t)
}

func (j *fullJoiner[LK, LV, RK, RV]) Print() {
	j.left.Print()
	j.right.Print()
}
