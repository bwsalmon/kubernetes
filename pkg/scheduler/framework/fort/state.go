package fort

import (
	"fmt"
	"log"
)

type StateUpdateFunc func(s State, clonedState bool)

type keyValueTarget interface {
	onUpdate(key any, value any, source keyValueSource)
	onDelete(key any, value any, source keyValueSource)
}

type keyValueSource interface {
	addTarget(target keyValueTarget)
}

func newState(spec StateSpec) State {
	s := &state{
		spec: spec,
		root: newCloneMap(map[string]any{}, nil, nil, 0),
	}
	spec.(*stateSpec).Update(s, false)
	return s
}

type state struct {
	spec StateSpec
	root *CloneMap[string]
}

func (s *state) Clone() State {
	newState := &state{
		spec: s.spec,
		root: newCloneMap(map[string]any{}, nil, nil, 0),
	}
	newState.root.root = newState.root

	for key, value := range s.root.data {
		cloned := value.(Cloneable).Clone(newState.root)
		newState.root.Update(key, cloned)
	}

	spec := newState.spec.(*stateSpec)
	spec.Update(newState, true)

	return newState
}

func makeOrCloneMap[K comparable](s *state, name string, isClone bool) *CloneMap[K] {
	if isClone {
		if v, found := s.root.Get(name); found {
			return v.(*CloneMap[K])
		}
		log.Fatalf("Couldn't find map %s", name)
	}

	nm := newCloneMap(map[K]any{}, nil, s.root, 0)
	s.root.Update(name, nm)
	return nm
}

func (s *state) Print() {
	fmt.Printf("State{\n")
	for name, cmap := range s.root.All() {
		fmt.Printf("  %s:\n", name)
		cmap.(*CloneMap[any]).Print()
	}
	fmt.Printf("}\n")
}

type stateSpec struct {
	updateFuncs []StateUpdateFunc
}

func newSpec() *stateSpec {
	return &stateSpec{
		updateFuncs: []StateUpdateFunc{},
	}
}

func (s *stateSpec) Register(f StateUpdateFunc) {
	s.updateFuncs = append(s.updateFuncs, f)
}

func (s *stateSpec) Update(st *state, clonedState bool) {
	for _, f := range s.updateFuncs {
		f(st, clonedState)
	}
}
