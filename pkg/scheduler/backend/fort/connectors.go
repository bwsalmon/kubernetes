package fort

// A target to store the results from a view.
// Only relevant externally for creating new operators.
type Target[K comparable, V any] interface {
	OnUpdate(key K, value V, source Source[K, V]) error
	OnDelete(key K, value V, source Source[K, V]) error
}

// Sources should be opaque externally. They are only used
// to link sources to one another using the operators.
type Source[K comparable, V any] interface {
	addTarget(t Target[K, V])
	Print()
}

func (s KeyValueSet[K, V]) All() KeyValueIterator[K, V] {
	return func(yield func(key K, value V) bool) {
		for _, kv := range s {
			if !yield(kv.Key, kv.Value) {
				return
			}
		}
	}
}

type keyValueConnector[K comparable, V any] struct {
	targets []Target[K, V]
}

var _ Source[string, int] = &keyValueConnector[string, int]{}

func newKeyValueConnector[K comparable, V any]() *keyValueConnector[K, V] {
	return &keyValueConnector[K, V]{
		targets: []Target[K, V]{},
	}
}

func (m *keyValueConnector[K, V]) addTarget(target Target[K, V]) {
	m.targets = append(m.targets, target)
}

func (m *keyValueConnector[K, V]) Update(key K, value V) {
	for _, target := range m.targets {
		target.OnUpdate(key, value, m)
	}
}

func (m *keyValueConnector[K, V]) Delete(key K, value V) {
	for _, target := range m.targets {
		target.OnDelete(key, value, m)
	}
}

func (c *keyValueConnector[K, V]) Clone() any {
	return &keyValueConnector[K, V]{
		targets: append([]Target[K, V]{}, c.targets...),
	}
}

func (c *keyValueConnector[K, V]) Release() {}

func (c *keyValueConnector[K, V]) Print() {}

type writeMap[K comparable, V any] struct {
	targets []Target[K, V]
}

var _ WriteMap[string, int] = &writeMap[string, int]{}

func newWriteMap[K comparable, V any]() *writeMap[K, V] {
	return &writeMap[K, V]{targets: []Target[K, V]{}}
}

func (w *writeMap[K, V]) addTarget(t Target[K, V]) {
	w.targets = append(w.targets, t)
}

func (w *writeMap[K, V]) Update(key K, value V) error {
	for _, t := range w.targets {
		if err := t.OnUpdate(key, value, w); err != nil {
			return err
		}
	}
	return nil
}

func (w *writeMap[K, V]) Delete(key K, value V) error {
	for _, t := range w.targets {
		if err := t.OnDelete(key, value, w); err != nil {
			return err
		}
	}
	return nil
}

func (w *writeMap[K, V]) Print() {}

type readMap[K comparable, V any] struct {
	data *CloneMap[K, V]
}

var _ ReadMap[string, int] = &readMap[string, int]{}

func newReadMap[K comparable, V any]() *readMap[K, V] {
	return &readMap[K, V]{
		data: newCloneMap[K, V](map[K]any{}, nil, 0),
	}
}

func (r *readMap[K, V]) Get(key K) (V, bool) {
	return r.data.Get(key)
}

func (r *readMap[K, V]) All() KeyValueIterator[K, V] {
	return r.data.All()
}

func (r *readMap[K, V]) OnUpdate(key K, value V, source Source[K, V]) error {
	r.data.Update(key, value)
	return nil
}

func (r *readMap[K, V]) OnDelete(key K, value V, source Source[K, V]) error {
	r.data.Delete(key)
	return nil
}

func (r *readMap[K, V]) Clone() *readMap[K, V] {
	return &readMap[K, V]{
		data: r.data.Clone(),
	}
}

func (r *readMap[K, V]) Print() {
	r.data.Print()
}
