package fort

import (
	"context"
	"fmt"
	"time"

	"k8s.io/client-go/tools/cache"
)

// manualInformer implements ManualSharedInformer using a shared LockGroup.
type manualInformer struct {
	name string

	lock LockGroup

	handlers      map[int]cache.ResourceEventHandler
	nextHandlerId int

	doneCheckers []*manualInformerDoneChecker

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

type manualInformerDoneChecker struct {
	informer *manualInformer
	synced   chan struct{}
}

var _ cache.ResourceEventHandlerRegistration = &manualInformerRegistration{}

func (r *manualInformerRegistration) HasSynced() bool {
	return r.informer.HasSynced()
}

func (r *manualInformerRegistration) HasSyncedChecker() cache.DoneChecker {
	return r.informer.HasSyncedChecker()
}

func (c *manualInformerDoneChecker) Name() string {
	return ""
}

func (c *manualInformerDoneChecker) Done() <-chan struct{} {
	return c.synced
}

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
	p.lock.Lock()
	defer p.lock.Unlock()

	p.nextHandlerId++
	r := &manualInformerRegistration{
		informer: p,
		id:       p.nextHandlerId,
	}
	p.handlers[p.nextHandlerId] = handler

	// Atomic replay of current state
	list := p.indexer.List()
	for _, obj := range list {
		// If handler supports Locked events, use them to avoid deadlock.
		if m, ok := handler.(LockedResourceEventHandler); ok {
			m.OnAddLocked(obj, true)
		} else {
			handler.OnAdd(obj, true)
		}
	}

	return r, nil
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
	for _, d := range p.doneCheckers {
		close(d.synced)
	}
	p.doneCheckers = nil
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
	i.lock.Lock()
	defer i.lock.Unlock()

	ch := make(chan struct{})
	if i.hasSynced {
		close(ch)
	}
	checker := &manualInformerDoneChecker{
		informer: i,
		synced:   ch,
	}
	if !i.hasSynced {
		i.doneCheckers = append(i.doneCheckers, checker)
	}
	return checker
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
		if h != nil {
			if m, ok := h.(LockedResourceEventHandler); ok {
				m.OnAddLocked(transformed, isInInitialList)
			} else {
				h.OnAdd(transformed, isInInitialList)
			}
		}
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
		if h != nil {
			if m, ok := h.(LockedResourceEventHandler); ok {
				m.OnUpdateLocked(oldTransformed, newTransformed)
			} else {
				h.OnUpdate(oldTransformed, newTransformed)
			}
		}
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
		if h != nil {
			if m, ok := h.(LockedResourceEventHandler); ok {
				m.OnDeleteLocked(transformed)
			} else {
				h.OnDelete(transformed)
			}
		}
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
	}

	return newInformer
}

func (p *manualInformer) SetName(name string) {
	p.name = name
}
