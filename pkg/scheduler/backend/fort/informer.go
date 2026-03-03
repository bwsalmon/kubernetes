package fort

import (
	"sync"

	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/cache"
)

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
