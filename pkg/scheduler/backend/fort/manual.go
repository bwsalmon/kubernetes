package fort

import (
	"context"
	"fmt"
	"time"

	"k8s.io/client-go/tools/cache"
)

// manualInformer implements ManualSharedInformer using a shared LockGroup.
// It is designed for testing and high-performance simulations where clones
// can inherit populated state from a parent via B-Tree structural cloning.
type manualInformer struct {
	name string

	lock LockGroup

	handlers      map[int]cache.ResourceEventHandler
	nextHandlerId int

	synced chan struct{}

	hasSynced bool
	isStopped bool

	lastSyncResourceVersion string

	transform cache.TransformFunc

	keyFunc cache.KeyFunc
	indexer CloneableIndexer

	watchErrorHandler            cache.WatchErrorHandler
	watchErrorHandlerWithContext cache.WatchErrorHandlerWithContext
}

var _ ManualSharedInformer = &manualInformer{}

type manualInformerRegistration struct {
	informer *manualInformer
	id       int
}

var _ cache.ResourceEventHandlerRegistration = &manualInformerRegistration{}

func (r *manualInformerRegistration) HasSynced() bool {
	return r.informer.HasSynced()
}

func (r *manualInformerRegistration) HasSyncedChecker() cache.DoneChecker {
	return r.informer.HasSyncedChecker()
}

type manualDoneChecker struct {
	synced <-chan struct{}
}

func (c *manualDoneChecker) Name() string { return "" }
func (c *manualDoneChecker) Done() <-chan struct{} { return c.synced }

func (p *manualInformer) GetLockGroup() LockGroup {
	return p.lock
}

func (p *manualInformer) AddEventHandler(h cache.ResourceEventHandler) (cache.ResourceEventHandlerRegistration, error) {
	return p.AddEventHandlerWithOptions(h, cache.HandlerOptions{})
}

func (p *manualInformer) AddEventHandlerWithResyncPeriod(handler cache.ResourceEventHandler, resyncPeriod time.Duration) (cache.ResourceEventHandlerRegistration, error) {
	return p.AddEventHandler(handler)
}

func (p *manualInformer) AddEventHandlerWithOptions(handler cache.ResourceEventHandler, options cache.HandlerOptions) (cache.ResourceEventHandlerRegistration, error) {
	return p.addEventHandler(handler, true)
}

func (p *manualInformer) AddEventHandlerNoReplay(handler cache.ResourceEventHandler) (cache.ResourceEventHandlerRegistration, error) {
	return p.addEventHandler(handler, false)
}

// addEventHandler registers a handler, optionally replaying the entire current state.
func (p *manualInformer) addEventHandler(handler cache.ResourceEventHandler, replay bool) (cache.ResourceEventHandlerRegistration, error) {
	p.lock.Lock()
	defer p.lock.Unlock()

	p.nextHandlerId++
	r := &manualInformerRegistration{
		informer: p,
		id:       p.nextHandlerId,
	}
	p.handlers[p.nextHandlerId] = handler

	if replay {
		// Atomic replay of current state to ensure the new handler is fully hydrated.
		list := p.indexer.List()
		for _, obj := range list {
			// Use dispatchEvent to handle Locked handlers correctly.
			p.dispatchEvent(handler, 
				func(h cache.ResourceEventHandler) { h.OnAdd(obj, true) }, 
				func(l LockedResourceEventHandler) { l.OnAddLocked(obj, true) })
		}
	}

	return r, nil
}

// dispatchEvent selects the appropriate handler method based on whether the handler
// supports the Locked interface (to avoid re-entrant deadlocks on the shared LockGroup).
func (p *manualInformer) dispatchEvent(h cache.ResourceEventHandler, std func(cache.ResourceEventHandler), locked func(LockedResourceEventHandler)) {
	if m, ok := h.(LockedResourceEventHandler); ok {
		locked(m)
	} else {
		std(h)
	}
}

func (p *manualInformer) RemoveEventHandler(r cache.ResourceEventHandlerRegistration) error {
	p.lock.Lock()
	defer p.lock.Unlock()

	pr := r.(*manualInformerRegistration)
	delete(p.handlers, pr.id)
	return nil
}

func (p *manualInformer) GetStore() cache.Store {
	return p.indexer
}

func (p *manualInformer) GetController() cache.Controller {
	return nil
}

func (p *manualInformer) Run(stopCh <-chan struct{}) {
	<-stopCh
}

func (p *manualInformer) RunWithContext(ctx context.Context) {
	<-ctx.Done()
}

func (p *manualInformer) LastSyncResourceVersion() string {
	return p.lastSyncResourceVersion
}

func (p *manualInformer) SetWatchErrorHandler(handler cache.WatchErrorHandler) error {
	p.lock.Lock()
	defer p.lock.Unlock()
	p.watchErrorHandler = handler
	return nil
}

func (p *manualInformer) SetWatchErrorHandlerWithContext(handler cache.WatchErrorHandlerWithContext) error {
	p.lock.Lock()
	defer p.lock.Unlock()
	p.watchErrorHandlerWithContext = handler
	return nil
}

func (p *manualInformer) TriggerWatchError(err error) {
	p.lock.RLock()
	h := p.watchErrorHandler
	hc := p.watchErrorHandlerWithContext
	p.lock.RUnlock()

	if h != nil {
		h(nil, err)
	}
	if hc != nil {
		hc(context.TODO(), nil, err)
	}
}

func (p *manualInformer) SetTransform(handler cache.TransformFunc) error {
	p.lock.Lock()
	defer p.lock.Unlock()

	if p.transform != nil {
		return fmt.Errorf("Setting transform when it is already set.")
	}
	p.transform = handler
	return nil
}

func (p *manualInformer) HasSynced() bool {
	p.lock.RLock()
	defer p.lock.RUnlock()
	return p.hasSynced
}

func (p *manualInformer) IsStopped() bool {
	p.lock.RLock()
	defer p.lock.RUnlock()
	return p.isStopped
}

func (p *manualInformer) SetHasSynced() {
	p.lock.Lock()
	defer p.lock.Unlock()

	if p.hasSynced {
		return
	}

	p.hasSynced = true
	close(p.synced)
}

func (p *manualInformer) SetIsStopped() {
	p.lock.Lock()
	defer p.lock.Unlock()
	p.isStopped = true
}

func (p *manualInformer) GetKeyFunc() cache.KeyFunc {
	return p.keyFunc
}

func (i *manualInformer) HasSyncedChecker() cache.DoneChecker {
	return &manualDoneChecker{synced: i.synced}
}

func (p *manualInformer) OnAdd(obj any, isInInitialList bool) {
	p.lock.Lock()
	defer p.lock.Unlock()
	p.OnAddLocked(obj, isInInitialList)
}

func (p *manualInformer) OnAddLocked(obj any, isInInitialList bool) {
	transformed := obj
	if p.transform != nil {
		transformed, _ = p.transform(obj)
	}

	p.indexer.Add(transformed)

	for _, h := range p.handlers {
		p.dispatchEvent(h, 
			func(h cache.ResourceEventHandler) { h.OnAdd(transformed, isInInitialList) }, 
			func(l LockedResourceEventHandler) { l.OnAddLocked(transformed, isInInitialList) })
	}
}

func (p *manualInformer) OnUpdate(oldObj, newObj any) {
	p.lock.Lock()
	defer p.lock.Unlock()
	p.OnUpdateLocked(oldObj, newObj)
}

func (p *manualInformer) OnUpdateLocked(oldObj, newObj any) {
	oldTransformed := oldObj
	newTransformed := newObj
	if p.transform != nil {
		oldTransformed, _ = p.transform(oldObj)
		newTransformed, _ = p.transform(newObj)
	}

	p.indexer.Update(newTransformed)

	for _, h := range p.handlers {
		p.dispatchEvent(h, 
			func(h cache.ResourceEventHandler) { h.OnUpdate(oldTransformed, newTransformed) }, 
			func(l LockedResourceEventHandler) { l.OnUpdateLocked(oldTransformed, newTransformed) })
	}
}

func (p *manualInformer) OnDelete(oldObj any) {
	p.lock.Lock()
	defer p.lock.Unlock()
	p.OnDeleteLocked(oldObj)
}

func (p *manualInformer) OnDeleteLocked(oldObj any) {
	transformed := oldObj
	if p.transform != nil {
		transformed, _ = p.transform(oldObj)
	}

	p.indexer.Delete(transformed)

	for _, h := range p.handlers {
		p.dispatchEvent(h, 
			func(h cache.ResourceEventHandler) { h.OnDelete(transformed) }, 
			func(l LockedResourceEventHandler) { l.OnDeleteLocked(transformed) })
	}
}

// Clone creates a new instance.
// REQUIRES: Caller must hold the shared LockGroup (RLock or Lock).
func (p *manualInformer) Clone(_ []cache.SharedInformer) CloneableSharedInformerQuery {
	newInformer := &manualInformer{
		name:                    p.name,
		handlers:                map[int]cache.ResourceEventHandler{},
		transform:               p.transform,
		isStopped:               p.isStopped,
		hasSynced:               p.hasSynced,
		lastSyncResourceVersion: p.lastSyncResourceVersion,
		keyFunc:                 p.keyFunc,
		indexer:                 p.indexer.Clone(), // Fast O(1) B-Tree clone
		lock:                    NewLockGroup(),
		synced:                  make(chan struct{}),
	}
	if p.hasSynced {
		close(newInformer.synced)
	}

	return newInformer
}

func (p *manualInformer) SetName(name string) {
	p.name = name
}

func (p *manualInformer) GetSources() []cache.SharedInformer {
	return nil
}
