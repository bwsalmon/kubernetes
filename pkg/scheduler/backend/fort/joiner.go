package fort

import (
	"context"
	"fmt"
	"time"

	"k8s.io/client-go/tools/cache"
)

// joiner implements a many-to-many join between two source informers.
// It maintains internal B-Trees of join sets (slices of objects) to allow
// O(1) structural cloning and fast multi-match lookups.
type joiner[L, R any] struct {
	handler ManualSharedInformer
	on      JoinOnFunc[L, R]

	leftSource  cache.SharedInformer
	rightSource cache.SharedInformer

	leftRegistration  cache.ResourceEventHandlerRegistration
	rightRegistration cache.ResourceEventHandlerRegistration

	// Indexed by the join key. Supports multiple objects per key (many-to-many).
	// Mutations on these slices MUST use COW to ensure snapshot isolation.
	left  BTreeMap[[]L]
	right BTreeMap[[]R]
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
		left:        NewBTreeMap[[]L](),
		right:       NewBTreeMap[[]R](),
	}

	j.leftRegistration, _ = leftSource.AddEventHandler(leftHandler[L, R]{j})
	j.rightRegistration, _ = rightSource.AddEventHandler(rightHandler[L, R]{j})

	// Cascade sync state from sources.
	go func() {
		lCheck := j.leftRegistration.HasSyncedChecker()
		rCheck := j.rightRegistration.HasSyncedChecker()
		
		select {
		case <-lCheck.Done():
			select {
			case <-rCheck.Done():
				j.handler.SetHasSynced()
			case <-j.handler.IsStoppedChan():
			}
		case <-j.handler.IsStoppedChan():
		}
	}()

	return j
}

// joiner does not handle events itself; it uses specialized side handlers
// to process updates from its two upstream sources.
func (j *joiner[L, R]) OnAdd(obj any, isInitial bool)    {}
func (j *joiner[L, R]) OnUpdate(old, new any)           {}
func (j *joiner[L, R]) OnDelete(obj any)                {}
func (j *joiner[L, R]) OnAddLocked(obj any, init bool)  {}
func (j *joiner[L, R]) OnUpdateLocked(old, new any)     {}
func (j *joiner[L, R]) OnDeleteLocked(obj any)          {}

// sideHandlers for left and right sources.

type leftHandler[L, R any] struct{ j *joiner[L, R] }

func (h leftHandler[L, R]) OnAdd(obj any, init bool) {
	h.j.handler.GetLockGroup().Lock()
	defer h.j.handler.GetLockGroup().Unlock()
	h.OnAddLocked(obj, init)
}
func (h leftHandler[L, R]) OnAddLocked(obj any, init bool) { h.j.onLeftAdd(obj.(L), init) }
func (h leftHandler[L, R]) OnUpdate(old, new any) {
	h.j.handler.GetLockGroup().Lock()
	defer h.j.handler.GetLockGroup().Unlock()
	h.OnUpdateLocked(old, new)
}
func (h leftHandler[L, R]) OnUpdateLocked(old, new any) { h.j.onLeftUpdate(old.(L), new.(L)) }
func (h leftHandler[L, R]) OnDelete(obj any) {
	h.j.handler.GetLockGroup().Lock()
	defer h.j.handler.GetLockGroup().Unlock()
	h.OnDeleteLocked(obj)
}
func (h leftHandler[L, R]) OnDeleteLocked(obj any) { h.j.onLeftDelete(obj.(L)) }

type rightHandler[L, R any] struct{ j *joiner[L, R] }

func (h rightHandler[L, R]) OnAdd(obj any, init bool) {
	h.j.handler.GetLockGroup().Lock()
	defer h.j.handler.GetLockGroup().Unlock()
	h.OnAddLocked(obj, init)
}
func (h rightHandler[L, R]) OnAddLocked(obj any, init bool) { h.j.onRightAdd(obj.(R), init) }
func (h rightHandler[L, R]) OnUpdate(old, new any) {
	h.j.handler.GetLockGroup().Lock()
	defer h.j.handler.GetLockGroup().Unlock()
	h.OnUpdateLocked(old, new)
}
func (h rightHandler[L, R]) OnUpdateLocked(old, new any) { h.j.onRightUpdate(old.(R), new.(R)) }
func (h rightHandler[L, R]) OnDelete(obj any) {
	h.j.handler.GetLockGroup().Lock()
	defer h.j.handler.GetLockGroup().Unlock()
	h.OnDeleteLocked(obj)
}
func (h rightHandler[L, R]) OnDeleteLocked(obj any) { h.j.onRightDelete(obj.(R)) }

// objectsEqual robustly compares two objects using DefaultKeyFunc to avoid panics on non-comparable types.
func objectsEqual(a, b any) bool {
	ka, _ := DefaultKeyFunc(a)
	kb, _ := DefaultKeyFunc(b)
	return ka == kb
}

// Core join logic.

func (j *joiner[L, R]) onLeftAdd(left L, isInitial bool) {
	key := j.on(left, *new(R))
	keyStr, _ := DefaultKeyFunc(key)

	// COW: Shallow-clone the existing slice before updating to protect snapshots.
	items, _ := j.left.Get(keyStr)
	newItems := append(append([]L(nil), items...), left)
	j.left.Set(keyStr, newItems)

	// Emit join results for all matching objects on the right side.
	if rights, ok := j.right.Get(keyStr); ok {
		for _, right := range rights {
			j.handler.OnAddLocked(JoinValue[L, R]{Left: left, Right: right}, isInitial)
		}
	}
}

func (j *joiner[L, R]) onLeftUpdate(oldLeft, newLeft L) {
	oldKey := j.on(oldLeft, *new(R))
	newKey := j.on(newLeft, *new(R))

	if oldKey == newKey {
		keyStr, _ := DefaultKeyFunc(oldKey)
		if rights, ok := j.right.Get(keyStr); ok {
			for _, right := range rights {
				j.handler.OnUpdateLocked(JoinValue[L, R]{Left: oldLeft, Right: right}, JoinValue[L, R]{Left: newLeft, Right: right})
			}
		}
		// COW: Update the stored object in the left map by cloning the slice.
		slice, _ := j.left.Get(keyStr)
		newSlice := append([]L(nil), slice...)
		for i, v := range newSlice {
			if objectsEqual(v, oldLeft) {
				newSlice[i] = newLeft
				break
			}
		}
		j.left.Set(keyStr, newSlice)
	} else {
		// Key changed: perform atomic Delete + Add transition.
		j.onLeftDelete(oldLeft)
		j.onLeftAdd(newLeft, false)
	}
}

func (j *joiner[L, R]) onLeftDelete(left L) {
	key := j.on(left, *new(R))
	keyStr, _ := DefaultKeyFunc(key)

	if rights, ok := j.right.Get(keyStr); ok {
		for _, right := range rights {
			j.handler.OnDeleteLocked(JoinValue[L, R]{Left: left, Right: right})
		}
	}
	// COW: Filter out the deleted object into a new slice.
	slice, _ := j.left.Get(keyStr)
	newSlice := make([]L, 0, len(slice))
	for _, v := range slice {
		if !objectsEqual(v, left) {
			newSlice = append(newSlice, v)
		}
	}
	if len(newSlice) == 0 {
		j.left.Delete(keyStr)
	} else {
		j.left.Set(keyStr, newSlice)
	}
}

func (j *joiner[L, R]) onRightAdd(right R, isInitial bool) {
	key := j.on(*new(L), right)
	keyStr, _ := DefaultKeyFunc(key)

	// COW: Shallow-clone the existing slice.
	items, _ := j.right.Get(keyStr)
	newItems := append(append([]R(nil), items...), right)
	j.right.Set(keyStr, newItems)

	if lefts, ok := j.left.Get(keyStr); ok {
		for _, left := range lefts {
			j.handler.OnAddLocked(JoinValue[L, R]{Left: left, Right: right}, isInitial)
		}
	}
}

func (j *joiner[L, R]) onRightUpdate(oldRight, newRight R) {
	oldKey := j.on(*new(L), oldRight)
	newKey := j.on(*new(L), newRight)

	if oldKey == newKey {
		keyStr, _ := DefaultKeyFunc(oldKey)
		if lefts, ok := j.left.Get(keyStr); ok {
			for _, left := range lefts {
				j.handler.OnUpdateLocked(JoinValue[L, R]{Left: left, Right: oldRight}, JoinValue[L, R]{Left: left, Right: newRight})
			}
		}
		// COW: Update the stored object.
		slice, _ := j.right.Get(keyStr)
		newSlice := append([]R(nil), slice...)
		for i, v := range newSlice {
			if objectsEqual(v, oldRight) {
				newSlice[i] = newRight
				break
			}
		}
		j.right.Set(keyStr, newSlice)
	} else {
		j.onRightDelete(oldRight)
		j.onRightAdd(newRight, false)
	}
}

func (j *joiner[L, R]) onRightDelete(right R) {
	key := j.on(*new(L), right)
	keyStr, _ := DefaultKeyFunc(key)

	if lefts, ok := j.left.Get(keyStr); ok {
		for _, left := range lefts {
			j.handler.OnDeleteLocked(JoinValue[L, R]{Left: left, Right: right})
		}
	}
	// COW: Filter into new slice.
	slice, _ := j.right.Get(keyStr)
	newSlice := make([]R, 0, len(slice))
	for _, v := range slice {
		if !objectsEqual(v, right) {
			newSlice = append(newSlice, v)
		}
	}
	if len(newSlice) == 0 {
		j.right.Delete(keyStr)
	} else {
		j.right.Set(keyStr, newSlice)
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

func (j *joiner[L, R]) AddEventHandlerNoReplay(h cache.ResourceEventHandler) (cache.ResourceEventHandlerRegistration, error) {
	return j.handler.AddEventHandlerNoReplay(h)
}

func (j *joiner[L, R]) RemoveEventHandler(r cache.ResourceEventHandlerRegistration) error {
	return j.handler.RemoveEventHandler(r)
}

func (j *joiner[L, R]) HasSyncedChecker() cache.DoneChecker {
	return j.handler.HasSyncedChecker()
}

// Clone creates a new instance of the joiner.
// REQUIRES: Caller must hold the shared LockGroup (RLock or Lock) of the parent.
func (j *joiner[L, R]) Clone(newSources []cache.SharedInformer) CloneableSharedInformerQuery {
	nl := newSources[0]
	nr := newSources[1]
	
	newLock := nl.(CloneableSharedInformerQuery).GetLockGroup()
	
	// Structural COW clone of the result storage.
	newHandler := j.handler.Clone(nil).(ManualSharedInformer)
	newHandler.(*manualInformer).lock = newLock

	nj := &joiner[L, R]{
		handler:     newHandler,
		on:          j.on,
		leftSource:  nl,
		rightSource: nr,
		left:        j.left.Clone(),  // O(1) B-Tree clone.
		right:       j.right.Clone(), // O(1) B-Tree clone.
	}

	// Optimize: Use NoReplay during cloning to maintain the "born hydrated" state
	// inherited from the COW structural copies. This avoids redundant O(N) hydration.
	if ms, ok := nl.(ManualSharedInformer); ok {
		nj.leftRegistration, _ = ms.AddEventHandlerNoReplay(leftHandler[L, R]{nj})
	} else {
		nj.leftRegistration, _ = nl.AddEventHandler(leftHandler[L, R]{nj})
	}
	if ms, ok := nr.(ManualSharedInformer); ok {
		nj.rightRegistration, _ = ms.AddEventHandlerNoReplay(rightHandler[L, R]{nj})
	} else {
		nj.rightRegistration, _ = nr.AddEventHandler(rightHandler[L, R]{nj})
	}

	return nj
}

func (j *joiner[L, R]) GetStore() cache.Store {
	return j.handler.GetStore()
}

func (j *joiner[L, R]) GetController() cache.Controller {
	return nil
}

// Run starts the informer and unregisters from sources when stopCh is closed.
func (j *joiner[L, R]) Run(stopCh <-chan struct{}) {
	defer j.SetIsStopped()
	defer func() {
		if j.leftSource != nil && j.leftRegistration != nil {
			_ = j.leftSource.RemoveEventHandler(j.leftRegistration)
		}
		if j.rightSource != nil && j.rightRegistration != nil {
			_ = j.rightSource.RemoveEventHandler(j.rightRegistration)
		}
	}()
	<-stopCh
}

func (j *joiner[L, R]) RunWithContext(ctx context.Context) {
	j.Run(ctx.Done())
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

func (j *joiner[L, R]) IsStoppedChan() <-chan struct{} {
	return j.handler.(interface{ IsStoppedChan() <-chan struct{} }).IsStoppedChan()
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

func (j *joiner[L, R]) GetSources() []cache.SharedInformer {
	return []cache.SharedInformer{j.leftSource, j.rightSource}
}
