package fort

import (
	"context"
	"fmt"
	"sync"
	"time"

	"k8s.io/client-go/tools/cache"
)

// manualInformer implements ManualSharedInformer and provides a mechanism to manually
// trigger informer events. It acts as the "leaf" source in many simulation or test chains.
type manualInformer struct {
	name string

	lock sync.Mutex

	handlers      map[int]cache.ResourceEventHandler
	nextHandlerId int

	doneCheckers []*manualInformerDoneChecker

	hasSynced bool
	isStopped bool

	lastSyncResourceVersion string

	transform cache.TransformFunc

	keyFunc cache.KeyFunc

	watchErrorHandler            cache.WatchErrorHandler
	watchErrorHandlerWithContext cache.WatchErrorHandlerWithContext
}

var _ ManualSharedInformer = &manualInformer{}

type manualInformerRegistration struct {
	informer *manualInformer
	id       int
}

// manualInformerDoneChecker implementation for DoneChecker interface.
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

// Lock returns the underlying mutex. Used by LockInformerSet for consistent snapshoting.
func (p *manualInformer) Lock() *sync.Mutex {
	return &p.lock
}

func (p *manualInformer) AddEventHandler(h cache.ResourceEventHandler) (cache.ResourceEventHandlerRegistration, error) {
	p.lock.Lock()
	defer p.lock.Unlock()

	p.nextHandlerId++
	r := &manualInformerRegistration{
		informer: p,
		id:       p.nextHandlerId,
	}
	p.handlers[p.nextHandlerId] = h
	return r, nil
}

func (p *manualInformer) AddEventHandlerWithResyncPeriod(handler cache.ResourceEventHandler, resyncPeriod time.Duration) (cache.ResourceEventHandlerRegistration, error) {
	return p.AddEventHandler(handler)
}

func (p *manualInformer) AddEventHandlerWithOptions(handler cache.ResourceEventHandler, options cache.HandlerOptions) (cache.ResourceEventHandlerRegistration, error) {
	return p.AddEventHandler(handler)
}

func (p *manualInformer) RemoveEventHandler(r cache.ResourceEventHandlerRegistration) error {
	p.lock.Lock()
	defer p.lock.Unlock()

	pr := r.(*manualInformerRegistration)
	delete(p.handlers, pr.id)
	return nil
}

func (p *manualInformer) GetStore() cache.Store {
	return nil
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
	p.lock.Lock()
	defer p.lock.Unlock()

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

// TriggerWatchError manually triggers the registered watch error handlers.
func (p *manualInformer) TriggerWatchError(err error) {
	p.lock.Lock()
	h := p.watchErrorHandler
	hc := p.watchErrorHandlerWithContext
	p.lock.Unlock()

	if h != nil {
		h(nil, err) // We don't have a Reflector here
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
	p.lock.Lock()
	defer p.lock.Unlock()

	return p.hasSynced
}

func (p *manualInformer) IsStopped() bool {
	p.lock.Lock()
	defer p.lock.Unlock()

	return p.isStopped
}

// SetHasSynced marks the informer as synced and notifies all waiting checkers.
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

// HasSyncedChecker returns a checker that completes when SetHasSynced is called.
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

// OnAdd triggers the Add event for all registered handlers.
func (p *manualInformer) OnAdd(obj any, isInInitialList bool) {
	p.lock.Lock()
	defer p.lock.Unlock()

	// XXX set last version

	transformed := obj
	if p.transform != nil {
		transformed, _ = p.transform(obj)
	}

	for _, h := range p.handlers {
		if h != nil {
			h.OnAdd(transformed, isInInitialList)
		}
	}
}

// OnUpdate triggers the Update event for all registered handlers.
func (p *manualInformer) OnUpdate(oldObj, newObj any) {
	p.lock.Lock()
	defer p.lock.Unlock()

	// XXX set last version

	oldTransformed := oldObj
	newTransformed := newObj
	if p.transform != nil {
		oldTransformed, _ = p.transform(oldObj)
		newTransformed, _ = p.transform(newObj)
	}

	for _, h := range p.handlers {
		if h != nil {
			h.OnUpdate(oldTransformed, newTransformed)
		}
	}
}

// OnDelete triggers the Delete event for all registered handlers.
func (p *manualInformer) OnDelete(oldObj any) {
	p.lock.Lock()
	defer p.lock.Unlock()

	// XXX set last version

	transformed := oldObj
	if p.transform != nil {
		transformed, _ = p.transform(oldObj)
	}

	for _, h := range p.handlers {
		if h != nil {
			h.OnDelete(transformed)
		}
	}
}

func (p *manualInformer) Clone(_ []cache.SharedInformer) CloneableSharedInformerQuery {
	p.lock.Lock()
	defer p.lock.Unlock()

	newInformer := &manualInformer{
		name:                    p.name,
		handlers:                map[int]cache.ResourceEventHandler{},
		transform:               p.transform,
		isStopped:               p.isStopped,
		hasSynced:               p.hasSynced,
		lastSyncResourceVersion: p.lastSyncResourceVersion,
		keyFunc:                 p.keyFunc,
	}

	return newInformer
}
