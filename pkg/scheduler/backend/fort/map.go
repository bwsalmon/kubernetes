package fort

import (
	"fmt"
	"sync"
)

type CloneMap[K comparable] struct {
	lock       sync.RWMutex
	references int64
	data       map[K]any
	base       *CloneMap[K]
	root       any
	targets    []KeyValueTarget
}

var _ KeyValueMap[string] = &CloneMap[string]{}
var _ KeyValueSource = &CloneMap[string]{}
var _ InputView[string] = &CloneMap[string]{}
var _ Cloneable = &CloneMap[string]{}

func newCloneMap[K comparable](data map[K]any, base *CloneMap[K], root any, references int64) *CloneMap[K] {
	return &CloneMap[K]{
		references: references,
		data:       data,
		base:       base,
		root:       root,
	}
}

func Get[K comparable, T any](m *CloneMap[K], key K) T {
	v, found := m.Get(key)
	if found && v != nil {
		return v.(T)
	}
	var empty T
	return empty
}

func (m *CloneMap[K]) Get(key K) (any, bool) {
	m.lock.RLock()
	defer m.lock.RUnlock()
	return m.getLocked(key)
}

func (m *CloneMap[K]) Has(key K) bool {
	_, found := m.Get(key)
	return found
}

func (m *CloneMap[K]) getLocked(key K) (any, bool) {
	v, found := m.data[key]

	if !found && m.base != nil {
		v, found = m.base.Get(key)
	}

	if v == tombstone {
		return nil, false
	}

	return v, found
}

func (m *CloneMap[K]) All() KeyValueIterator[K] {
	return func(yield func(key K, value any) bool) {
		m.lock.RLock()
		defer m.lock.RUnlock()

		for key, value := range m.data {
			if value != tombstone {
				if !yield(key, value) {
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

func (m *CloneMap[K]) Update(key K, value any) {
	if m.updateNoCallback(key, value) {
		m.callOnUpdate(&KeyValue[K]{Key: key, Value: value})
	}
}

func (m *CloneMap[K]) updateNoCallback(key K, value any) bool {
	m.lock.Lock()
	defer m.lock.Unlock()

	if value != m.data[key] {
		m.data[key] = value
		return true
	}

	return false
}

var tombstone = &struct{}{}

func (m *CloneMap[K]) Delete(key K) {
	kv := KeyValue[K]{}
	if m.deleteNoCallback(key, &kv) {
		//fmt.Printf("Deleted %s, %v\n", kv.Key, kv.Value)
		m.callOnDelete(&kv)
	}
}

func (m *CloneMap[K]) deleteNoCallback(key K, callback *KeyValue[K]) bool {
	m.lock.Lock()
	defer m.lock.Unlock()

	val, found := m.getLocked(key)
	if found {
		if m.base == nil {
			delete(m.data, key)
		} else {
			// Note that if the key doesn't exist in our base map
			// we are adding an unnecessary entry, but we will remove it
			// when we do the merge later, so we avoid doing a
			// lookup in the base map here.
			m.data[key] = tombstone
		}

		*callback = KeyValue[K]{Key: key, Value: val}
		return true
	}

	return false
}

func (m *CloneMap[K]) CloneIfNotOwned(root any) any {
	m.lock.Lock()
	defer m.lock.Unlock()

	if m.root != root {
		newBase := newCloneMap(m.data, m.base, m.root, 2)

		m.data = map[K]any{}
		m.base = newBase
		m.root = root

		return newCloneMap(map[K]any{}, newBase, root, 0)
	}

	return m
}

func (m *CloneMap[K]) Release() {
	m.lock.RLock()
	defer m.lock.RUnlock()

	if m.base != nil {
		m.base.removeRef()
	}
}

// Down the ref count on the map
// Use this as an opportunity to check for
// merging opportunites.
func (m *CloneMap[K]) removeRef() {
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
func (m *CloneMap[K]) mergeBaseIfPossible() {
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

func (m *CloneMap[K]) addTarget(target KeyValueTarget) {
	m.targets = append(m.targets, target)
}

func (m *CloneMap[K]) callOnUpdate(kv *KeyValue[K]) {
	for _, target := range m.targets {
		target.OnUpdate(kv.Key, kv.Value, m)
	}
}

func (m *CloneMap[K]) callOnDelete(kv *KeyValue[K]) {
	for _, target := range m.targets {
		target.OnDelete(kv.Key, kv.Value, m)
	}
}

func (m *CloneMap[K]) OnUpdate(key any, value any, _ KeyValueSource) {
	m.Update(key.(K), value)
}

func (m *CloneMap[K]) OnDelete(key any, value any, _ KeyValueSource) {
	m.Delete(key.(K))
}

func (m *CloneMap[K]) Print() {
	m.lock.RLock()
	defer m.lock.RUnlock()
	fmt.Print("  CloneMap{\n")
	for key, value := range m.All() {
		fmt.Printf("    %v: %v\n", key, value)
	}
	fmt.Print("  }\n")
}
