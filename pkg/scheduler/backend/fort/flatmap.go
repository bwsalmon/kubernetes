package fort

import (
	"context"
	"fmt"
	"sync"
	"time"

	"k8s.io/client-go/tools/cache"
)

type flatMapper[Out, In any] struct {
	handler      ManualSharedInformer
	mapper       FlatMapFunc[Out, In]
	registration cache.ResourceEventHandlerRegistration
	doneChecker  cache.DoneChecker
	source       cache.SharedInformer
}

var _ CloneableSharedInformerQuery = &flatMapper[int, int]{}

func newFlatMapper[Out, In any](mapper FlatMapFunc[Out, In], from cache.SharedInformer) *flatMapper[Out, In] {
	return newFlatMapperWithHandler(mapper, from, NewManualSharedInformer())
}

func newFlatMapperWithHandler[Out, In any](mapper FlatMapFunc[Out, In], from cache.SharedInformer, handler ManualSharedInformer) *flatMapper[Out, In] {
	m := &flatMapper[Out, In]{
		handler: handler,
		mapper:  mapper,
		source:  from,
	}

	m.registration, _ = from.AddEventHandler(m)

	// Register a sync checker with the source. When it tells us
	// we are synced update the handler to reflect this.
	go func() {
		check := m.registration.HasSyncedChecker()
		syncedChan := check.Done()
		<-syncedChan
		m.handler.SetHasSynced()
	}()

	return m
}

func (m *flatMapper[Out, In]) Lock() *sync.Mutex {
	return m.handler.(*manualInformer).Lock()
}

func (m *flatMapper[O, I]) OnAdd(obj any, isInitial bool) {
	input := obj.(I)
	results, _ := m.mapper(input)
	for _, r := range results {
		m.handler.OnAdd(r, isInitial)
	}
}

func (m *flatMapper[O, I]) OnUpdate(oldObj, newObj any) {
	oldInput := oldObj.(I)
	newInput := newObj.(I)
	oldResults, _ := m.mapper(oldInput)
	newResults, _ := m.mapper(newInput)

	keyFunc := m.handler.GetKeyFunc()
	oldKeys := make(map[string]O)
	for _, r := range oldResults {
		key, _ := keyFunc(r)
		oldKeys[key] = r
	}

	newKeys := make(map[string]O)
	for _, r := range newResults {
		key, _ := keyFunc(r)
		newKeys[key] = r
	}

	for key, oldR := range oldKeys {
		if newR, ok := newKeys[key]; ok {
			m.handler.OnUpdate(oldR, newR)
			delete(newKeys, key)
		} else {
			m.handler.OnDelete(oldR)
		}
	}
	for _, newR := range newKeys {
		m.handler.OnAdd(newR, false)
	}
}

func (m *flatMapper[O, I]) OnDelete(oldObj any) {
	input := oldObj.(I)
	results, _ := m.mapper(input)
	for _, r := range results {
		m.handler.OnDelete(r)
	}
}

func (m *flatMapper[O, I]) AddEventHandler(h cache.ResourceEventHandler) (cache.ResourceEventHandlerRegistration, error) {
	return m.handler.AddEventHandler(h)
}

func (m *flatMapper[Out, In]) AddEventHandlerWithResyncPeriod(handler cache.ResourceEventHandler, resyncPeriod time.Duration) (cache.ResourceEventHandlerRegistration, error) {
	return m.handler.AddEventHandlerWithResyncPeriod(handler, resyncPeriod)
}

func (m *flatMapper[Out, In]) AddEventHandlerWithOptions(handler cache.ResourceEventHandler, options cache.HandlerOptions) (cache.ResourceEventHandlerRegistration, error) {
	return m.handler.AddEventHandlerWithResyncPeriod(handler, 0) // Options not used in manual
}

func (m *flatMapper[Out, In]) RemoveEventHandler(r cache.ResourceEventHandlerRegistration) error {
	return m.handler.RemoveEventHandler(r)
}

func (m *flatMapper[Out, In]) HasSyncedChecker() cache.DoneChecker {
	return m.handler.HasSyncedChecker()
}

func (m *flatMapper[Out, In]) Clone(source []cache.SharedInformer) CloneableSharedInformerQuery {
	return newFlatMapper(m.mapper, source[0])
}

func (m *flatMapper[Out, In]) GetStore() cache.Store {
	return nil
}

func (m *flatMapper[Out, In]) GetController() cache.Controller {
	return nil
}

func (m *flatMapper[Out, In]) Run(stopCh <-chan struct{}) {
	<-stopCh
}

func (m *flatMapper[Out, In]) RunWithContext(ctx context.Context) {
	<-ctx.Done()
}

func (m *flatMapper[Out, In]) LastSyncResourceVersion() string {
	return m.handler.LastSyncResourceVersion()
}

func (m *flatMapper[Out, In]) SetWatchErrorHandler(handler cache.WatchErrorHandler) error {
	return m.source.SetWatchErrorHandler(handler)
}

func (m *flatMapper[Out, In]) SetWatchErrorHandlerWithContext(handler cache.WatchErrorHandlerWithContext) error {
	return m.source.SetWatchErrorHandlerWithContext(handler)
}

func (m *flatMapper[Out, In]) SetTransform(handler cache.TransformFunc) error {
	return fmt.Errorf("Flat map queries don't support transform")
}

func (m *flatMapper[Out, In]) HasSynced() bool {
	return m.handler.HasSynced()
}

func (m *flatMapper[Out, In]) IsStopped() bool {
	return m.handler.IsStopped()
}
