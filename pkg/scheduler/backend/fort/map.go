package fort

import (
	"fmt"
	"sync"
)

type CloneMap[K comparable, V any] struct {
	lock       sync.RWMutex
	references int64
	data       map[K]any
	base       *CloneMap[K, V]
}

func makeOrCloneMap[K comparable, V any](from *CloneMap[K, V]) *CloneMap[K, V] {
	if from == nil {
		return newCloneMap[K, V](map[K]any{}, nil, 0)
	}
	return from.Clone()
}

func newCloneMap[K comparable, V any](data map[K]any, base *CloneMap[K, V], references int64) *CloneMap[K, V] {
	return &CloneMap[K, V]{
		references: references,
		data:       data,
		base:       base,
	}
}

func (m *CloneMap[K, V]) Get(key K) (V, bool) {
	m.lock.RLock()
	defer m.lock.RUnlock()
	v, found, _ := m.getLocked(key)
	return v, found
}

func (m *CloneMap[K, V]) GetMutability(key K) (any, bool, bool) {
	m.lock.RLock()
	defer m.lock.RUnlock()
	return m.getLocked(key)
}

func (m *CloneMap[K, V]) Has(key K) bool {
	_, found := m.Get(key)
	return found
}

func (m *CloneMap[K, V]) getLocked(key K) (V, bool, bool) {
	v, foundHere := m.data[key]

	found := foundHere
	if !found && m.base != nil {
		v, found = m.base.Get(key)
	}

	var empty V
	if !found || v == tombstone {
		return empty, false, false
	}

	return v.(V), found, foundHere
}

func (m *CloneMap[K, V]) All() KeyValueIterator[K, V] {
	return func(yield func(key K, value V) bool) {
		m.lock.RLock()
		defer m.lock.RUnlock()

		for key, value := range m.data {
			if value != tombstone {
				if !yield(key, value.(V)) {
					return
				}
			}
		}
		if m.base != nil {
			for key, value := range m.base.All() {
				if _, found := m.data[key]; !found {
					if !yield(key, value) {
						return
					}
				}
			}
		}
	}
}

func (m *CloneMap[K, V]) Update(key K, value V) {
	m.lock.Lock()
	defer m.lock.Unlock()
	m.data[key] = value
}

var tombstone = &struct{}{}

func (m *CloneMap[K, V]) Delete(key K) V {
	m.lock.Lock()
	defer m.lock.Unlock()

	v, found, _ := m.getLocked(key)
	if found {
		if m.base == nil {
			delete(m.data, key)
		} else {
			m.data[key] = tombstone
		}
		return v
	}

	var empty V
	return empty
}

func (m *CloneMap[K, V]) Clone() *CloneMap[K, V] {
	m.lock.Lock()
	defer m.lock.Unlock()

	newBase := newCloneMap(m.data, m.base, 2)

	m.data = map[K]any{}
	m.base = newBase

	return newCloneMap(map[K]any{}, newBase, 0)
}

func (m *CloneMap[K, V]) Release() {
	m.lock.RLock()
	defer m.lock.RUnlock()

	if m.base != nil {
		m.base.removeRef()
	}
}

// Down the ref count on the map
// Use this as an opportunity to check for
// merging opportunites.
func (m *CloneMap[K, V]) removeRef() {
	m.lock.Lock()
	defer m.lock.Unlock()

	m.references--

	// Check if we can merge into our base.
	// Note that this is a property of our
	// base, not ourself, but the removal of a
	// reference is a good time to check the previous
	// tree.
	m.mergeBaseIfPossible()
}

// Assumes that the map lock is held.
func (m *CloneMap[K, V]) mergeBaseIfPossible() {
	if m.base != nil {
		m.base.lock.Lock()
		defer m.base.lock.Unlock()

		m.base.mergeBaseIfPossible()
		if m.base.references == 1 {
			// Update our base's map with our data.
			// Do this rather than the inverse because we
			// assume the base is larger.
			updateLockedMaps(m.base.data, m.data)

			// Move the base's map into our object
			// and drop our reference to the base to
			// allow it to be garbage collected.
			m.data = m.base.data
			m.base.references = 0
			m.base = nil
		}
	}
}

func updateLockedMaps[K comparable](dest map[K]any, src map[K]any) {
	for key, value := range src {
		if value == tombstone {
			delete(dest, key)
		} else {
			dest[key] = value
		}
	}
}

func (m *CloneMap[K, V]) Print() {
	m.lock.RLock()
	defer m.lock.RUnlock()
	fmt.Print("  CloneMap{\n")
	for key, value := range m.All() {
		fmt.Printf("    %v: %v\n", key, value)
	}
	fmt.Print("  }\n")
}
