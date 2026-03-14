package fort

import "k8s.io/client-go/tools/cache"

func getLock(lock LockGroup, source cache.SharedInformer) LockGroup {
	if lock != nil {
		return lock
	}
	if q, ok := source.(CloneableSharedInformerQuery); ok {
		return q.GetLockGroup()
	}
	return NewLockGroup()
}

func (q *Select[Out, In]) Build() CloneableSharedInformerQuery {
	lock := getLock(q.Lock, q.From)
	m := &FlatMap[Out, In]{
		Lock: lock,
		Map: func(value In) ([]Out, error) {
			if q.Where == nil || q.Where(value) {
				out, err := q.Select(value)
				if err != nil {
					return nil, err
				}
				return []Out{out}, nil
			}
			return nil, nil
		},
		Over: q.From,
	}
	inf := m.Build()
	inf.SetName("select-query")
	return inf
}

func (q *Join[Out, Left, Right]) Build() CloneableSharedInformerQuery {
	lock := getLock(q.Lock, q.From)
	sq := &Select[Out, JoinValue[Left, Right]]{
		Lock: lock,
		Select: func(joined JoinValue[Left, Right]) (Out, error) {
			return q.Select(joined.Left, joined.Right)
		},
		From: newJoiner[Left, Right](lock, q.From, q.Join, q.On),
		Where: func(joined JoinValue[Left, Right]) bool {
			if q.Where == nil {
				return true
			}
			return q.Where(joined.Left, joined.Right)
		},
	}
	inf := sq.Build()
	inf.SetName("join-query")
	return inf
}

func (q *GroupBy[Out, In]) Build() CloneableSharedInformerQuery {
	lock := getLock(q.Lock, q.From)
	inf := newGrouper[Out, In](lock, q.Select, q.GroupBy, q.From, q.Where)
	inf.SetName("groupBy-query")
	return inf
}

func (q *GroupByJoin[Out, Left, Right]) Build() CloneableSharedInformerQuery {
	lock := getLock(q.Lock, q.From)
	g := &GroupBy[Out, JoinValue[Left, Right]]{
		Lock:   lock,
		Select: q.Select,
		From:   newJoiner[Left, Right](lock, q.From, q.Join, q.On),
		GroupBy: func(joined JoinValue[Left, Right]) (any, []GroupField) {
			return q.GroupBy(joined.Left, joined.Right)
		},
	}
	inf := g.Build()
	inf.SetName("groupByJoin-query")
	return inf
}

func (q *FlatMap[Out, In]) Build() CloneableSharedInformerQuery {
	lock := getLock(q.Lock, q.Over)
	inf := newFlatMapper[Out, In](lock, q.Map, q.Over)
	inf.SetName("flatMap-query")
	return inf
}
