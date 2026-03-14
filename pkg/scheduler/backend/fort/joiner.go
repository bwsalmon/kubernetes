package fort

import (
	"context"
	"fmt"
	"time"

	"k8s.io/client-go/tools/cache"
)

// joiner implements a many-to-many join between two source informers.
type joiner[L, R any] struct {
	handler ManualSharedInformer
	on      JoinOnFunc[L, R]

	leftSource  cache.SharedInformer
	rightSource cache.SharedInformer

	leftRegistration  cache.ResourceEventHandlerRegistration
	rightRegistration cache.ResourceEventHandlerRegistration

	// Indexed by the join key. Supports multiple objects per key (many-to-many).
	left  map[any][]L
	right map[any][]R
}

var _ ManualSharedInformer = &joiner[int, int]{}

func newJoiner[L, R any](lock LockGroup, leftSource, rightSource cache.SharedInformer, on JoinOnFunc[L, R]) *joiner[L, R] {
	handler := NewManualSharedInformerWithOptions(lock, DefaultKeyFunc)
	handler.SetName("joiner-handler")
	return newJoinerWithHandler(leftSource, rightSource, on, handler)
}

func newJoinerWithHandler[L, R any](leftSource, rightSource cache.SharedInformer, on JoinOnFunc[L, R], handler ManualSharedInformer) *joiner[L, R] {
	j := &joiner[L, R]{
		handler:     handler,
		on:          on,
		leftSource:  leftSource,
		rightSource: rightSource,
		left:        make(map[any][]L),
		right:       make(map[any][]R),
	}

	j.leftRegistration, _ = leftSource.AddEventHandler(leftHandler[L, R]{j})
	j.rightRegistration, _ = rightSource.AddEventHandler(rightHandler[L, R]{j})

	go func() {
		lCheck := j.leftRegistration.HasSyncedChecker()
		rCheck := j.rightRegistration.HasSyncedChecker()
		<-lCheck.Done()
		<-rCheck.Done()
		j.handler.SetHasSynced()
	}()

	return j
}

func (j *joiner[L, R]) OnAdd(obj any, isInitial bool) {
	j.handler.GetLockGroup().Lock()
	defer j.handler.GetLockGroup().Unlock()
	j.OnAddLocked(obj, isInitial)
}

func (j *joiner[L, R]) OnAddLocked(obj any, isInitial bool) {
}

func (j *joiner[L, R]) OnUpdate(oldObj, newObj any) {
	j.handler.GetLockGroup().Lock()
	defer j.handler.GetLockGroup().Unlock()
	j.OnUpdateLocked(oldObj, newObj)
}

func (j *joiner[L, R]) OnUpdateLocked(oldObj, newObj any) {
}

func (j *joiner[L, R]) OnDelete(obj any) {
	j.handler.GetLockGroup().Lock()
	defer j.handler.GetLockGroup().Unlock()
	j.OnDeleteLocked(obj)
}

func (j *joiner[L, R]) OnDeleteLocked(obj any) {
}

// leftHandler handles events from the 'From' (left) informer.
type leftHandler[L, R any] struct {
	j *joiner[L, R]
}

var _ LockedResourceEventHandler = &leftHandler[int, int]{}

func (h leftHandler[L, R]) OnAdd(obj any, isInitial bool) {
	h.j.handler.GetLockGroup().Lock()
	defer h.j.handler.GetLockGroup().Unlock()
	h.OnAddLocked(obj, isInitial)
}

func (h leftHandler[L, R]) OnAddLocked(obj any, isInitial bool) {
	left := obj.(L)
	key := h.j.on(left, *new(R))
	h.j.left[key] = append(h.j.left[key], left)
	if rights, ok := h.j.right[key]; ok {
		for _, right := range rights {
			h.j.handler.OnAddLocked(JoinValue[L, R]{Left: left, Right: right}, isInitial)
		}
	}
}

func (h leftHandler[L, R]) OnUpdate(oldObj, newObj any) {
	h.j.handler.GetLockGroup().Lock()
	defer h.j.handler.GetLockGroup().Unlock()
	h.OnUpdateLocked(oldObj, newObj)
}

func (h leftHandler[L, R]) OnUpdateLocked(oldObj, newObj any) {
	oldLeft := oldObj.(L)
	newLeft := newObj.(L)
	oldKey := h.j.on(oldLeft, *new(R))
	newKey := h.j.on(newLeft, *new(R))

	if oldKey == newKey {
		if rights, ok := h.j.right[oldKey]; ok {
			for _, right := range rights {
				h.j.handler.OnUpdateLocked(JoinValue[L, R]{Left: oldLeft, Right: right}, JoinValue[L, R]{Left: newLeft, Right: right})
			}
		}
		slice := h.j.left[oldKey]
		for i, v := range slice {
			if any(v) == any(oldLeft) {
				slice[i] = newLeft
				break
			}
		}
	} else {
		if rights, ok := h.j.right[oldKey]; ok {
			for _, right := range rights {
				h.j.handler.OnDeleteLocked(JoinValue[L, R]{Left: oldLeft, Right: right})
			}
		}
		slice := h.j.left[oldKey]
		for i, v := range slice {
			if any(v) == any(oldLeft) {
				h.j.left[oldKey] = append(slice[:i], slice[i+1:]...)
				break
			}
		}

		h.j.left[newKey] = append(h.j.left[newKey], newLeft)
		if rights, ok := h.j.right[newKey]; ok {
			for _, right := range rights {
				h.j.handler.OnAddLocked(JoinValue[L, R]{Left: newLeft, Right: right}, false)
			}
		}
	}
}

func (h leftHandler[L, R]) OnDelete(obj any) {
	h.j.handler.GetLockGroup().Lock()
	defer h.j.handler.GetLockGroup().Unlock()
	h.OnDeleteLocked(obj)
}

func (h leftHandler[L, R]) OnDeleteLocked(obj any) {
	left := obj.(L)
	key := h.j.on(left, *new(R))
	if rights, ok := h.j.right[key]; ok {
		for _, right := range rights {
			h.j.handler.OnDeleteLocked(JoinValue[L, R]{Left: left, Right: right})
		}
	}
	slice := h.j.left[key]
	for i, v := range slice {
		if any(v) == any(left) {
			h.j.left[key] = append(slice[:i], slice[i+1:]...)
			break
		}
	}
}

// rightHandler handles events from the 'Join' (right) informer.
type rightHandler[L, R any] struct {
	j *joiner[L, R]
}

var _ LockedResourceEventHandler = &rightHandler[int, int]{}

func (h rightHandler[L, R]) OnAdd(obj any, isInitial bool) {
	h.j.handler.GetLockGroup().Lock()
	defer h.j.handler.GetLockGroup().Unlock()
	h.OnAddLocked(obj, isInitial)
}

func (h rightHandler[L, R]) OnAddLocked(obj any, isInitial bool) {
	right := obj.(R)
	key := h.j.on(*new(L), right)
	h.j.right[key] = append(h.j.right[key], right)
	if lefts, ok := h.j.left[key]; ok {
		for _, left := range lefts {
			h.j.handler.OnAddLocked(JoinValue[L, R]{Left: left, Right: right}, isInitial)
		}
	}
}

func (h rightHandler[L, R]) OnUpdate(oldObj, newObj any) {
	h.j.handler.GetLockGroup().Lock()
	defer h.j.handler.GetLockGroup().Unlock()
	h.OnUpdateLocked(oldObj, newObj)
}

func (h rightHandler[L, R]) OnUpdateLocked(oldObj, newObj any) {
	oldRight := oldObj.(R)
	newRight := newObj.(R)
	oldKey := h.j.on(*new(L), oldRight)
	newKey := h.j.on(*new(L), newRight)

	if oldKey == newKey {
		if lefts, ok := h.j.left[oldKey]; ok {
			for _, left := range lefts {
				h.j.handler.OnUpdateLocked(JoinValue[L, R]{Left: left, Right: oldRight}, JoinValue[L, R]{Left: left, Right: newRight})
			}
		}
		slice := h.j.right[oldKey]
		for i, v := range slice {
			if any(v) == any(oldRight) {
				slice[i] = newRight
				break
			}
		}
	} else {
		if lefts, ok := h.j.left[oldKey]; ok {
			for _, left := range lefts {
				h.j.handler.OnDeleteLocked(JoinValue[L, R]{Left: left, Right: oldRight})
			}
		}
		slice := h.j.right[oldKey]
		for i, v := range slice {
			if any(v) == any(oldRight) {
				h.j.right[oldKey] = append(slice[:i], slice[i+1:]...)
				break
			}
		}

		h.j.right[newKey] = append(h.j.right[newKey], newRight)
		if lefts, ok := h.j.left[newKey]; ok {
			for _, left := range lefts {
				h.j.handler.OnAddLocked(JoinValue[L, R]{Left: left, Right: newRight}, false)
			}
		}
	}
}

func (h rightHandler[L, R]) OnDelete(obj any) {
	h.j.handler.GetLockGroup().Lock()
	defer h.j.handler.GetLockGroup().Unlock()
	h.OnDeleteLocked(obj)
}

func (h rightHandler[L, R]) OnDeleteLocked(obj any) {
	right := obj.(R)
	key := h.j.on(*new(L), right)
	if lefts, ok := h.j.left[key]; ok {
		for _, left := range lefts {
			h.j.handler.OnDeleteLocked(JoinValue[L, R]{Left: left, Right: right})
		}
	}
	slice := h.j.right[key]
	for i, v := range slice {
		if any(v) == any(right) {
			h.j.right[key] = append(slice[:i], slice[i+1:]...)
			break
		}
	}
}

func (j *joiner[L, R]) GetLockGroup() LockGroup {
	return j.handler.GetLockGroup()
}

func (j *joiner[L, R]) AddEventHandler(h cache.ResourceEventHandler) (cache.ResourceEventHandlerRegistration, error) {
	return j.handler.AddEventHandler(h)
}

func (j *joiner[L, R]) AddEventHandlerWithResyncPeriod(handler cache.ResourceEventHandler, resyncPeriod time.Duration) (cache.ResourceEventHandlerRegistration, error) {
	return j.handler.AddEventHandlerWithResyncPeriod(handler, resyncPeriod)
}

func (j *joiner[L, R]) AddEventHandlerWithOptions(handler cache.ResourceEventHandler, options cache.HandlerOptions) (cache.ResourceEventHandlerRegistration, error) {
	return j.handler.AddEventHandlerWithOptions(handler, options)
}

func (j *joiner[L, R]) RemoveEventHandler(r cache.ResourceEventHandlerRegistration) error {
	return j.handler.RemoveEventHandler(r)
}

func (j *joiner[L, R]) HasSyncedChecker() cache.DoneChecker {
	return j.handler.HasSyncedChecker()
}

// Clone creates a new instance.
// REQUIRES: Caller must hold the shared LockGroup (RLock or Lock).
func (j *joiner[L, R]) Clone(newSources []cache.SharedInformer) CloneableSharedInformerQuery {
	nl := newSources[0]
	nr := newSources[1]
	
	newLock := nl.(CloneableSharedInformerQuery).GetLockGroup()
	
	p := j.handler.(*manualInformer)

	// Clone the handler (snapshot results)
	newIndexer := cache.NewIndexer(p.keyFunc, cache.Indexers{})
	for _, obj := range p.indexer.List() {
		newIndexer.Add(obj)
	}
	newHandler := &manualInformer{
		name:      p.name,
		handlers:  map[int]cache.ResourceEventHandler{},
		transform: p.transform,
		hasSynced: p.hasSynced,
		keyFunc:   p.keyFunc,
		indexer:   newIndexer,
		lock:      newLock,
	}

	nj := &joiner[L, R]{
		handler:     newHandler,
		on:          j.on,
		leftSource:  nl,
		rightSource: nr,
		left:        make(map[any][]L),
		right:       make(map[any][]R),
	}

	// Deep copy internal maps
	for k, v := range j.left {
		nj.left[k] = append([]L(nil), v...)
	}
	for k, v := range j.right {
		nj.right[k] = append([]R(nil), v...)
	}

	nj.leftRegistration, _ = nl.AddEventHandler(leftHandler[L, R]{nj})
	nj.rightRegistration, _ = nr.AddEventHandler(rightHandler[L, R]{nj})

	return nj
}

func (j *joiner[L, R]) GetStore() cache.Store {
	return j.handler.GetStore()
}

func (j *joiner[L, R]) GetController() cache.Controller {
	return nil
}

func (j *joiner[L, R]) Run(stopCh <-chan struct{}) {
	<-stopCh
}

func (j *joiner[L, R]) RunWithContext(ctx context.Context) {
	<-ctx.Done()
}

func (j *joiner[L, R]) LastSyncResourceVersion() string {
	return j.handler.LastSyncResourceVersion()
}

func (j *joiner[L, R]) SetWatchErrorHandler(handler cache.WatchErrorHandler) error {
	_ = j.leftSource.SetWatchErrorHandler(handler)
	_ = j.rightSource.SetWatchErrorHandler(handler)
	return nil
}

func (j *joiner[L, R]) SetWatchErrorHandlerWithContext(handler cache.WatchErrorHandlerWithContext) error {
	_ = j.leftSource.SetWatchErrorHandlerWithContext(handler)
	_ = j.rightSource.SetWatchErrorHandlerWithContext(handler)
	return nil
}

func (j *joiner[L, R]) SetTransform(handler cache.TransformFunc) error {
	return fmt.Errorf("Join queries don't support transform")
}

func (j *joiner[L, R]) HasSynced() bool {
	return j.handler.HasSynced()
}

func (j *joiner[L, R]) IsStopped() bool {
	return j.handler.IsStopped()
}

func (j *joiner[L, R]) SetIsStopped() {
	j.handler.SetIsStopped()
}

func (j *joiner[L, R]) SetHasSynced() {
	j.handler.SetHasSynced()
}

func (j *joiner[L, R]) GetKeyFunc() cache.KeyFunc {
	return j.handler.GetKeyFunc()
}

func (j *joiner[L, R]) TriggerWatchError(err error) {
	j.handler.TriggerWatchError(err)
}

func (j *joiner[L, R]) SetName(name string) {
	j.handler.SetName(name)
}
