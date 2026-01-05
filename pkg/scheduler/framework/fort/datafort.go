package fort

import (
	"fmt"
	"log"
)

type KeyValueTarget interface {
	onUpdate(key any, value any, source KeyValueSource)
	onDelete(key any, value any, source KeyValueSource)
}

type KeyValueSource interface {
	addTarget(target KeyValueTarget)
}

type SourceSpec struct {
	Create       SourceFactory
	Dependencies []string
}

type SourceFactory func(s DataFort, name string, clonedState bool) (any, error)

func newDataFort(spec Spec) DataFort {
	s := &dataFort{
		spec: spec,
		root: newCloneMap(map[string]any{}, nil, nil, 0),
	}
	spec.(*stateSpec).Update(s, false)
	return s
}

type dataFort struct {
	spec Spec
	root *CloneMap[string]
}

type Cloneable interface {
	CloneIfNotOwned(owner any) any
}

func (s *dataFort) Clone() DataFort {
	newState := &dataFort{
		spec: s.spec,
		root: newCloneMap(map[string]any{}, nil, nil, 0),
	}
	newState.root.root = newState.root

	for key, value := range s.root.data {
		cloned := value.(Cloneable).CloneIfNotOwned(newState.root)
		newState.root.Update(key, cloned)
	}

	spec := newState.spec.(*stateSpec)
	spec.Update(newState, true)

	return newState
}

func (s *dataFort) Get(name string) (any, bool) {
	return s.root.Get(name)
}

func getSource(d *dataFort, name string) KeyValueSource {
	s, found := d.Get(name)
	if !found {
		log.Fatalf("Couldn't find source %s", name)
	}
	return s.(KeyValueSource)
}

func makeOrCloneMap[K comparable](s *dataFort, name string, isClone bool) *CloneMap[K] {
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

func (s *dataFort) Print() {
	fmt.Printf("State{\n")
	for name, cmap := range s.root.All() {
		fmt.Printf("  %s:\n", name)
		cmap.(*CloneMap[any]).Print()
	}
	fmt.Printf("}\n")
}

type specName struct {
	name string
	spec *SourceSpec
}

type stateSpec struct {
	specMap map[string]bool
	specs   []specName
}

func newSpec() *stateSpec {
	return &stateSpec{
		specMap: map[string]bool{},
		specs:   []specName{},
	}
}

func (s *stateSpec) New(name string, spec *SourceSpec) error {
	if name[0] == '@' {
		return fmt.Errorf("Names beginning with @ are reserved for internal use")
	}
	if _, exists := s.specMap[name]; exists {
		log.Fatalf("A source named %s already exists", name)
		return fmt.Errorf("A source named %s already exists", name)
	}

	for _, dep := range spec.Dependencies {
		if _, found := s.specMap[dep]; !found {
			log.Fatalf("Missing dependency %s for new source %s", dep, name)
			return fmt.Errorf("Missing dependency %s for new source %s", dep, name)
		}
	}

	s.specMap[name] = true
	s.specs = append(s.specs, specName{name: name, spec: spec})
	return nil
}

func (s *stateSpec) Update(st *dataFort, clonedState bool) error {
	for _, spec := range s.specs {
		newSource, err := spec.spec.Create(st, spec.name, clonedState)
		if err != nil {
			return err
		}
		st.root.Update(spec.name, newSource)
	}
	return nil
}

func (s KeyValueSet[K]) All() KeyValueIterator[K] {
	return func(yield func(key K, value any) bool) {
		for _, kv := range s {
			if !yield(kv.Key, kv.Value) {
				return
			}
		}
	}
}

func GetItem[T any](d DataFort, name string) T {
	m, found := d.Get(name)
	if !found {
		log.Fatalf("Couldn't find item %s", name)
	}
	return m.(T)
}
