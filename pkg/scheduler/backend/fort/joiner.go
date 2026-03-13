package fort

import (
	"context"
	"fmt"
	"sync"
	"time"

	"k8s.io/client-go/tools/cache"
)

type joiner[L, R any] struct {
	handler ManualSharedInformer
	on      JoinOnFunc[L, R]

	leftSource  cache.SharedInformer
	rightSource cache.SharedInformer

	leftRegistration  cache.ResourceEventHandlerRegistration
	rightRegistration cache.ResourceEventHandlerRegistration

	lock sync.Mutex
	left  map[any][]L
	right map[any][]R
}

func newJoiner[L, R any](leftSource, rightSource cache.SharedInformer, on JoinOnFunc[L, R]) *joiner[L, R] {
	j := &joiner[L, R]{
		handler:     NewManualSharedInformer(),
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

type leftHandler[L, R any] struct {
	j *joiner[L, R]
}

func (h leftHandler[L, R]) OnAdd(obj any, isInitial bool) {
	h.j.lock.Lock()
	defer h.j.lock.Unlock()

	left := obj.(L)
	key := h.j.on(left, *new(R))
	h.j.left[key] = append(h.j.left[key], left)
	if rights, ok := h.j.right[key]; ok {
		for _, right := range rights {
			h.j.handler.OnAdd(JoinValue[L, R]{Left: left, Right: right}, isInitial)
		}
	}
}

func (h leftHandler[L, R]) OnUpdate(oldObj, newObj any) {
	h.j.lock.Lock()
	defer h.j.lock.Unlock()

	oldLeft := oldObj.(L)
	newLeft := newObj.(L)
	oldKey := h.j.on(oldLeft, *new(R))
	newKey := h.j.on(newLeft, *new(R))

	if oldKey == newKey {
		if rights, ok := h.j.right[oldKey]; ok {
			for _, right := range rights {
				h.j.handler.OnUpdate(JoinValue[L, R]{Left: oldLeft, Right: right}, JoinValue[L, R]{Left: newLeft, Right: right})
			}
		}
		// Update oldLeft to newLeft in h.j.left[oldKey]
		slice := h.j.left[oldKey]
		for i, v := range slice {
			if any(v) == any(oldLeft) {
				slice[i] = newLeft
				break
			}
		}
	} else {
		// Old key
		if rights, ok := h.j.right[oldKey]; ok {
			for _, right := range rights {
				h.j.handler.OnDelete(JoinValue[L, R]{Left: oldLeft, Right: right})
			}
		}
		slice := h.j.left[oldKey]
		for i, v := range slice {
			if any(v) == any(oldLeft) {
				h.j.left[oldKey] = append(slice[:i], slice[i+1:]...)
				break
			}
		}

		// New key
		h.j.left[newKey] = append(h.j.left[newKey], newLeft)
		if rights, ok := h.j.right[newKey]; ok {
			for _, right := range rights {
				h.j.handler.OnAdd(JoinValue[L, R]{Left: newLeft, Right: right}, false)
			}
		}
	}
}

func (h leftHandler[L, R]) OnDelete(obj any) {
	h.j.lock.Lock()
	defer h.j.lock.Unlock()

	left := obj.(L)
	key := h.j.on(left, *new(R))
	if rights, ok := h.j.right[key]; ok {
		for _, right := range rights {
			h.j.handler.OnDelete(JoinValue[L, R]{Left: left, Right: right})
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

type rightHandler[L, R any] struct {
	j *joiner[L, R]
}

func (h rightHandler[L, R]) OnAdd(obj any, isInitial bool) {
	h.j.lock.Lock()
	defer h.j.lock.Unlock()

	right := obj.(R)
	key := h.j.on(*new(L), right)
	h.j.right[key] = append(h.j.right[key], right)
	if lefts, ok := h.j.left[key]; ok {
		for _, left := range lefts {
			h.j.handler.OnAdd(JoinValue[L, R]{Left: left, Right: right}, isInitial)
		}
	}
}

func (h rightHandler[L, R]) OnUpdate(oldObj, newObj any) {
	h.j.lock.Lock()
	defer h.j.lock.Unlock()

	oldRight := oldObj.(R)
	newRight := newObj.(R)
	oldKey := h.j.on(*new(L), oldRight)
	newKey := h.j.on(*new(L), newRight)

	if oldKey == newKey {
		if lefts, ok := h.j.left[oldKey]; ok {
			for _, left := range lefts {
				h.j.handler.OnUpdate(JoinValue[L, R]{Left: left, Right: oldRight}, JoinValue[L, R]{Left: left, Right: newRight})
			}
		}
		// Update oldRight to newRight in h.j.right[oldKey]
		slice := h.j.right[oldKey]
		for i, v := range slice {
			if any(v) == any(oldRight) {
				slice[i] = newRight
				break
			}
		}
	} else {
		// Old key
		if lefts, ok := h.j.left[oldKey]; ok {
			for _, left := range lefts {
				h.j.handler.OnDelete(JoinValue[L, R]{Left: left, Right: oldRight})
			}
		}
		slice := h.j.right[oldKey]
		for i, v := range slice {
			if any(v) == any(oldRight) {
				h.j.right[oldKey] = append(slice[:i], slice[i+1:]...)
				break
			}
		}

		// New key
		h.j.right[newKey] = append(h.j.right[newKey], newRight)
		if lefts, ok := h.j.left[newKey]; ok {
			for _, left := range lefts {
				h.j.handler.OnAdd(JoinValue[L, R]{Left: left, Right: newRight}, false)
			}
		}
	}
}

func (h rightHandler[L, R]) OnDelete(obj any) {
	h.j.lock.Lock()
	defer h.j.lock.Unlock()

	right := obj.(R)
	key := h.j.on(*new(L), right)
	if lefts, ok := h.j.left[key]; ok {
		for _, left := range lefts {
			h.j.handler.OnDelete(JoinValue[L, R]{Left: left, Right: right})
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

func (j *joiner[L, R]) Lock() *sync.Mutex {
	return j.handler.(*manualInformer).Lock()
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

func (j *joiner[L, R]) Clone(sources []cache.SharedInformer) CloneableSharedInformerQuery {
	return newJoiner[L, R](sources[0], sources[1], j.on)
}

func (j *joiner[L, R]) GetStore() cache.Store {
	return nil
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
	return nil
}

func (j *joiner[L, R]) SetWatchErrorHandlerWithContext(handler cache.WatchErrorHandlerWithContext) error {
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
