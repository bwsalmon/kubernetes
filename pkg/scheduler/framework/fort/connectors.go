package fort

import (
	"sync"

	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/cache"
)

type keyValueConnector[K comparable] struct {
	targets []KeyValueTarget
}

var _ KeyValueSource = &keyValueConnector[string]{}
var _ Cloneable = &keyValueConnector[string]{}

func newKeyValueConnector[K comparable]() *keyValueConnector[K] {
	return &keyValueConnector[K]{
		targets: []KeyValueTarget{},
	}
}

func (m *keyValueConnector[K]) addTarget(target KeyValueTarget) {
	m.targets = append(m.targets, target)
}

func (m *keyValueConnector[K]) Update(key K, value any) {
	for _, target := range m.targets {
		target.onUpdate(key, value, m)
	}
}

func (m *keyValueConnector[K]) Delete(key K, value any) {
	for _, target := range m.targets {
		target.onDelete(key, value, m)
	}
}

func (c *keyValueConnector[K]) Clone(owner any) Cloneable {
	return &keyValueConnector[K]{
		targets: append([]KeyValueTarget{}, c.targets...),
	}
}

func newMaterializer[K comparable](source string) *SourceSpec {
	return &SourceSpec{
		Create: func(s State, name string, isClone bool) (any, error) {
			st := s.(*state)
			v, _ := st.root.Get(source)
			if mapValue, isMap := v.(*CloneMap[K]); isMap {
				return mapValue, nil
			}

			sourceValue := v.(KeyValueSource)
			newMap := makeOrCloneMap[K](st, name, isClone)
			sourceValue.addTarget(newMap)
			return newMap, nil
		},
		Dependencies: []string{source},
	}
}

type wrappedInformer struct {
	lock sync.Mutex
	keyValueConnector[string]
	informer     cache.SharedInformer
	registration cache.ResourceEventHandlerRegistration
}

var _ cache.ResourceEventHandler = &wrappedInformer{}
var _ KeyValueSource = &wrappedInformer{}
var _ Cloneable = &wrappedInformer{}

type hasUID interface {
	GetUID() types.UID
}

type hasName interface {
	GetName() string
}

func (w *wrappedInformer) OnAdd(obj interface{}, isInInitialList bool) {
	w.lock.Lock()
	defer w.lock.Unlock()
	key := string(obj.(hasName).GetName())
	w.keyValueConnector.Update(key, obj)
}

func (w *wrappedInformer) OnUpdate(oldObj, newObj interface{}) {
	w.lock.Lock()
	defer w.lock.Unlock()
	key := string(newObj.(hasName).GetName())
	w.keyValueConnector.Update(key, newObj)
}

func (w *wrappedInformer) OnDelete(obj interface{}) {
	w.lock.Lock()
	defer w.lock.Unlock()
	key := string(obj.(hasName).GetName())
	w.keyValueConnector.Delete(key, obj)
}

func (w *wrappedInformer) addTarget(target KeyValueTarget) {
	w.lock.Lock()
	defer w.lock.Unlock()
	w.keyValueConnector.addTarget(target)
}

func (w *wrappedInformer) Clone(owner any) Cloneable {
	return newKeyValueConnector[string]()
}

func wrapInformer(informer cache.SharedInformer) (*wrappedInformer, error) {
	w := &wrappedInformer{
		keyValueConnector: keyValueConnector[string]{
			targets: []KeyValueTarget{},
		},
		informer: informer,
	}

	var err error
	w.registration, err = informer.AddEventHandler(w)
	if err != nil {
		return nil, err
	}

	return w, nil
}
