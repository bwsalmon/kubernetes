package fort

func (q *Select[Out, In]) Build() CloneableSharedInformerQuery {
	m := &FlatMap[Out, In]{
		Map: func(value In) ([]Out, error) {
			if q.Where == nil || q.Where(value) {
				out, err := q.Select(value)
				return []Out{out}, err
			}
			return []Out{}, nil
		},
		Over: q.From,
	}
	return m.Build()
}

type JoinValue[L, R any] struct {
	Left  L
	Right R
}

func (q *Join[Out, Left, Right]) Build() CloneableSharedInformerQuery {
	sq := &Select[Out, JoinValue[Left, Right]]{
		Select: func(joined JoinValue[Left, Right]) (Out, error) {
			return q.Select(joined.Left, joined.Right)
		},
		From: newJoiner[Left, Right](q.From, q.Join, q.On),
		Where: func(joined JoinValue[Left, Right]) bool {
			return q.Where == nil || q.Where(joined.Left, joined.Right)
		},
	}

	return sq.Build()
}

func (q *GroupBy[Out, In]) Build() CloneableSharedInformerQuery {
	return newGrouper[Out, In](q.Select, q.GroupBy, q.From, q.Where)
}

func (q *GroupByJoin[Out, Left, Right]) Build() CloneableSharedInformerQuery {
	g := &GroupBy[Out, JoinValue[Left, Right]]{
		Select: q.Select,
		From:   newJoiner[Left, Right](q.From, q.Join, q.On),
		GroupBy: func(joined JoinValue[Left, Right]) ([]string, []GroupField) {
			return q.GroupBy(joined.Left, joined.Right)
		},
	}
	return g.Build()
}

func (q *FlatMap[Out, In]) Build() CloneableSharedInformerQuery {
	return newFlatMapper[Out, In](q.Map, q.Over)
}
