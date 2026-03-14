package fort

import (
	"context"
	"fmt"
	"time"

	"k8s.io/client-go/tools/cache"
)

// flatMapper implements FlatMap query by applying a mapping function to each object
// from a source informer. It uses a shared LockGroup for transactional consistency.
type flatMapper[Out, In any] struct {
	handler      ManualSharedInformer
	mapper       FlatMapFunc[Out, In]
	registration cache.ResourceEventHandlerRegistration
	source       cache.SharedInformer
}

var _ ManualSharedInformer = &flatMapper[int, int]{}

func newFlatMapper[Out, In any](lock LockGroup, mapper FlatMapFunc[Out, In], from cache.SharedInformer) *flatMapper[Out, In] {
	handler := NewManualSharedInformerWithOptions(lock, DefaultKeyFunc)
	handler.SetName("flatMap-handler")
	return newFlatMapperWithHandler(mapper, from, handler)
}

func newFlatMapperWithHandler[Out, In any](mapper FlatMapFunc[Out, In], from cache.SharedInformer, handler ManualSharedInformer) *flatMapper[Out, In] {
	m := &flatMapper[Out, In]{
		handler: handler,
		mapper:  mapper,
		source:  from,
	}

	// Initial registration from constructor still replays
	m.registration, _ = from.AddEventHandler(m)

	go func() {
		check := m.registration.HasSyncedChecker()
		syncedChan := check.Done()
		<-syncedChan
		m.handler.SetHasSynced()
	}()

	return m
}

func (m *flatMapper[Out, In]) GetLockGroup() LockGroup {
	return m.handler.GetLockGroup()
}

func (m *flatMapper[O, I]) OnAdd(obj any, isInitial bool) {
	m.handler.GetLockGroup().Lock()
	defer m.handler.GetLockGroup().Unlock()
	m.OnAddLocked(obj, isInitial)
}

func (m *flatMapper[O, I]) OnAddLocked(obj any, isInitial bool) {
	input := obj.(I)
	results, _ := m.mapper(input)
	for _, r := range results {
		m.handler.OnAddLocked(r, isInitial)
	}
}

func (m *flatMapper[O, I]) OnUpdate(oldObj, newObj any) {
	m.handler.GetLockGroup().Lock()
	defer m.handler.GetLockGroup().Unlock()
	m.OnUpdateLocked(oldObj, newObj)
}

func (m *flatMapper[O, I]) OnUpdateLocked(oldObj, newObj any) {
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
			m.handler.OnUpdateLocked(oldR, newR)
			delete(newKeys, key)
		} else {
			m.handler.OnDeleteLocked(oldR)
		}
	}
	for _, newR := range newKeys {
		m.handler.OnAddLocked(newR, false)
	}
}

func (m *flatMapper[O, I]) OnDelete(oldObj any) {
	m.handler.GetLockGroup().Lock()
	defer m.handler.GetLockGroup().Unlock()
	m.OnDeleteLocked(oldObj)
}

func (m *flatMapper[O, I]) OnDeleteLocked(oldObj any) {
	input := oldObj.(I)
	results, _ := m.mapper(input)
	for _, r := range results {
		m.handler.OnDeleteLocked(r)
	}
}

func (m *flatMapper[Out, In]) AddEventHandler(h cache.ResourceEventHandler) (cache.ResourceEventHandlerRegistration, error) {
	return m.handler.AddEventHandler(h)
}

func (m *flatMapper[Out, In]) AddEventHandlerWithResyncPeriod(handler cache.ResourceEventHandler, resyncPeriod time.Duration) (cache.ResourceEventHandlerRegistration, error) {
	return m.handler.AddEventHandlerWithResyncPeriod(handler, resyncPeriod)
}

func (m *flatMapper[Out, In]) AddEventHandlerWithOptions(handler cache.ResourceEventHandler, options cache.HandlerOptions) (cache.ResourceEventHandlerRegistration, error) {
	return m.handler.AddEventHandlerWithOptions(handler, options)
}

func (m *flatMapper[Out, In]) AddEventHandlerNoReplay(h cache.ResourceEventHandler) (cache.ResourceEventHandlerRegistration, error) {
	return m.handler.AddEventHandlerNoReplay(h)
}

func (m *flatMapper[Out, In]) RemoveEventHandler(r cache.ResourceEventHandlerRegistration) error {
	return m.handler.RemoveEventHandler(r)
}

func (m *flatMapper[Out, In]) HasSyncedChecker() cache.DoneChecker {
	return m.handler.HasSyncedChecker()
}

// Clone creates a new instance of the flatMapper.
// REQUIRES: Caller must hold the shared LockGroup (RLock or Lock) of the parent.
func (m *flatMapper[Out, In]) Clone(newSources []cache.SharedInformer) CloneableSharedInformerQuery {
	newSource := newSources[0]
	var newLock LockGroup
	if q, ok := newSource.(CloneableSharedInformerQuery); ok {
		newLock = q.GetLockGroup()
	} else {
		newLock = NewLockGroup()
	}

	// Structural clone of the result handler (O(1) B-Tree copy).
	newHandler := m.handler.Clone(nil).(ManualSharedInformer)
	newHandler.(*manualInformer).lock = newLock

	// Optimize: Use NoReplay during cloning to maintain the "born hydrated" state
	// inherited from the COW structural copy. This avoids redundant O(N) hydration.
	cloned := &flatMapper[Out, In]{
		handler: newHandler,
		mapper:  m.mapper,
		source:  newSource,
	}

	if ms, ok := newSource.(ManualSharedInformer); ok {
		cloned.registration, _ = ms.AddEventHandlerNoReplay(cloned)
	} else {
		cloned.registration, _ = newSource.AddEventHandler(cloned)
	}

	return cloned
}

func (m *flatMapper[Out, In]) GetStore() cache.Store {
	return m.handler.GetStore()
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

func (m *flatMapper[Out, In]) SetIsStopped() {
	m.handler.SetIsStopped()
}

func (m *flatMapper[Out, In]) SetHasSynced() {
	m.handler.SetHasSynced()
}

func (m *flatMapper[Out, In]) GetKeyFunc() cache.KeyFunc {
	return m.handler.GetKeyFunc()
}

func (m *flatMapper[Out, In]) TriggerWatchError(err error) {
	m.handler.TriggerWatchError(err)
}

func (m *flatMapper[Out, In]) SetName(name string) {
	m.handler.SetName(name)
}

func (m *flatMapper[Out, In]) GetSources() []cache.SharedInformer {
	return []cache.SharedInformer{m.source}
}
