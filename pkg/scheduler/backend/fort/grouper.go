package fort

import (
	"context"
	"fmt"
	"time"

	"k8s.io/client-go/tools/cache"
)

// grouper implements GroupBy query. It aggregates objects from a source informer
// into groups identified by a comparable key and evaluates aggregate fields.
type grouper[Out, In any] struct {
	handler ManualSharedInformer
	sel     GroupSelectFunc[Out]
	groupBy SingleGroupByFunc[In]
	where   SingleFilterFunc[In]
	source  cache.SharedInformer

	registration cache.ResourceEventHandlerRegistration

	groups BTreeMap[*groupState[Out]]
}

var _ ManualSharedInformer = &grouper[int, int]{}
var _ LockedResourceEventHandler = &grouper[int, int]{}

// groupState maintains the current aggregation state for a single group.
type groupState[Out any] struct {
	count   int
	fields  []any
	lastOut Out // The last object emitted for this group, used for OnUpdate.
}

func newGrouper[Out, In any](lock LockGroup, sel GroupSelectFunc[Out], groupBy SingleGroupByFunc[In], from cache.SharedInformer, where SingleFilterFunc[In]) *grouper[Out, In] {
	handler := NewManualSharedInformerWithOptions(lock, DefaultKeyFunc)
	handler.SetName("grouper-handler")
	return newGrouperWithHandler(sel, groupBy, from, where, handler)
}

func newGrouperWithHandler[Out, In any](sel GroupSelectFunc[Out], groupBy SingleGroupByFunc[In], from cache.SharedInformer, where SingleFilterFunc[In], handler ManualSharedInformer) *grouper[Out, In] {
	g := &grouper[Out, In]{
		handler: handler,
		sel:     sel,
		groupBy: groupBy,
		where:   where,
		source:  from,
		groups:  NewBTreeMap[*groupState[Out]](),
	}

	g.registration, _ = from.AddEventHandler(g)

	go func() {
		check := g.registration.HasSyncedChecker()
		<-check.Done()
		g.handler.SetHasSynced()
	}()

	return g
}

func (g *grouper[Out, In]) OnAdd(obj any, isInitial bool) {
	g.handler.GetLockGroup().Lock()
	defer g.handler.GetLockGroup().Unlock()
	g.OnAddLocked(obj, isInitial)
}

func (g *grouper[Out, In]) OnAddLocked(obj any, isInitial bool) {
	input := obj.(In)
	if g.where != nil && !g.where(input) {
		return
	}

	key, fields := g.groupBy(input)
	keyStr, _ := DefaultKeyFunc(key)

	state, ok := g.groups.Get(keyStr)
	var oldOut Out
	if ok {
		oldOut = state.lastOut
	}

	// COW: Create a new state if modifying
	var newState groupState[Out]
	if ok {
		newState = *state
		newState.fields = append([]any(nil), state.fields...)
	}

	newFields := g.evaluateFields(fields, &newState, input, true)
	newOut, _ := g.sel(newFields)
	newState.lastOut = newOut
	g.groups.Set(keyStr, &newState)

	if ok {
		g.handler.OnUpdateLocked(oldOut, newOut)
	} else {
		g.handler.OnAddLocked(newOut, isInitial)
	}
}

func (g *grouper[Out, In]) OnUpdate(oldObj, newObj any) {
	g.handler.GetLockGroup().Lock()
	defer g.handler.GetLockGroup().Unlock()
	g.OnUpdateLocked(oldObj, newObj)
}

func (g *grouper[Out, In]) OnUpdateLocked(oldObj, newObj any) {
	oldInput := oldObj.(In)
	newInput := newObj.(In)

	oldKey, oldFields := g.groupBy(oldInput)
	newKey, newFields := g.groupBy(newInput)

	if oldKey == newKey {
		keyStr, _ := DefaultKeyFunc(oldKey)
		state, ok := g.groups.Get(keyStr)
		if !ok {
			g.OnAddLocked(newObj, false)
			return
		}

		oldOut := state.lastOut
		// COW: Create a new state
		newState := *state
		newState.fields = append([]any(nil), state.fields...)

		g.evaluateFields(oldFields, &newState, oldInput, false)
		resFields := g.evaluateFields(newFields, &newState, newInput, true)
		
		newOut, _ := g.sel(resFields)
		newState.lastOut = newOut
		g.groups.Set(keyStr, &newState)
		g.handler.OnUpdateLocked(oldOut, newOut)
	} else {
		g.OnDeleteLocked(oldObj)
		g.OnAddLocked(newObj, false)
	}
}

func (g *grouper[Out, In]) OnDelete(obj any) {
	g.handler.GetLockGroup().Lock()
	defer g.handler.GetLockGroup().Unlock()
	g.OnDeleteLocked(obj)
}

func (g *grouper[Out, In]) OnDeleteLocked(obj any) {
	input := obj.(In)
	if g.where != nil && !g.where(input) {
		return
	}

	key, fields := g.groupBy(input)
	keyStr, _ := DefaultKeyFunc(key)

	state, ok := g.groups.Get(keyStr)
	if !ok {
		return
	}

	oldOut := state.lastOut
	// COW: Create a new state
	newState := *state
	newState.fields = append([]any(nil), state.fields...)

	newFields := g.evaluateFields(fields, &newState, input, false)

	if newState.count == 0 {
		g.groups.Delete(keyStr)
		g.handler.OnDeleteLocked(oldOut)
	} else {
		newOut, _ := g.sel(newFields)
		newState.lastOut = newOut
		g.groups.Set(keyStr, &newState)
		g.handler.OnUpdateLocked(oldOut, newOut)
	}
}

// evaluateFields calculates or updates the values of aggregate fields for a group.
func (g *grouper[Out, In]) evaluateFields(fields []GroupField, state *groupState[Out], input In, adding bool) []GroupField {
	if state.fields == nil {
		state.fields = make([]any, len(fields))
	}

	if adding {
		state.count++
	} else {
		state.count--
	}

	res := make([]GroupField, len(fields))
	for i, f := range fields {
		gf := f.(*groupField)
		if gf.count {
			res[i] = int64(state.count)
		} else if gf.sum != nil {
			if state.fields[i] == nil {
				state.fields[i] = int64(0)
			}
			val := *gf.sum
			if adding {
				state.fields[i] = state.fields[i].(int64) + val
			} else {
				state.fields[i] = state.fields[i].(int64) - val
			}
			res[i] = state.fields[i]
		} else if gf.distinct != nil {
			if state.fields[i] == nil {
				state.fields[i] = make(map[any]int)
			}
			// COW for the distinct map
			m := state.fields[i].(map[any]int)
			newM := make(map[any]int, len(m))
			for k, v := range m {
				newM[k] = v
			}
			
			if adding {
				newM[gf.distinct]++
			} else {
				newM[gf.distinct]--
				if newM[gf.distinct] == 0 {
					delete(newM, gf.distinct)
				}
			}
			state.fields[i] = newM
			
			var distincts []any
			for k := range newM {
				distincts = append(distincts, k)
			}
			res[i] = distincts
		} else if gf.anyValue != nil {
			state.fields[i] = gf.anyValue
			res[i] = gf.anyValue
		} else if gf.key != nil {
			res[i] = gf.key
		}
	}
	return res
}

func (g *grouper[Out, In]) GetLockGroup() LockGroup {
	return g.handler.GetLockGroup()
}

func (g *grouper[Out, In]) AddEventHandler(h cache.ResourceEventHandler) (cache.ResourceEventHandlerRegistration, error) {
	return g.handler.AddEventHandler(h)
}

func (g *grouper[Out, In]) AddEventHandlerWithResyncPeriod(handler cache.ResourceEventHandler, resyncPeriod time.Duration) (cache.ResourceEventHandlerRegistration, error) {
	return g.handler.AddEventHandlerWithResyncPeriod(handler, resyncPeriod)
}

func (g *grouper[Out, In]) AddEventHandlerWithOptions(handler cache.ResourceEventHandler, options cache.HandlerOptions) (cache.ResourceEventHandlerRegistration, error) {
	return g.handler.AddEventHandlerWithOptions(handler, options)
}

func (g *grouper[Out, In]) RemoveEventHandler(r cache.ResourceEventHandlerRegistration) error {
	return g.handler.RemoveEventHandler(r)
}

func (g *grouper[Out, In]) HasSyncedChecker() cache.DoneChecker {
	return g.handler.HasSyncedChecker()
}

// Clone creates a new instance.
// REQUIRES: Caller must hold the shared LockGroup (RLock or Lock).
func (g *grouper[Out, In]) Clone(newSources []cache.SharedInformer) CloneableSharedInformerQuery {
	ns := newSources[0]
	newLock := ns.(CloneableSharedInformerQuery).GetLockGroup()
	
	newHandler := g.handler.Clone(nil).(ManualSharedInformer)
	newHandler.(*manualInformer).lock = newLock

	ng := &grouper[Out, In]{
		handler: newHandler,
		sel:     g.sel,
		groupBy: g.groupBy,
		where:   g.where,
		source:  ns,
		groups:  g.groups.Clone(), // Fast O(1) B-Tree clone
	}

	ng.registration, _ = ns.AddEventHandler(ng)

	return ng
}

func (g *grouper[Out, In]) GetStore() cache.Store {
	return g.handler.GetStore()
}

func (g *grouper[Out, In]) GetController() cache.Controller {
	return nil
}

func (g *grouper[Out, In]) Run(stopCh <-chan struct{}) {
	<-stopCh
}

func (g *grouper[Out, In]) RunWithContext(ctx context.Context) {
	<-ctx.Done()
}

func (g *grouper[Out, In]) LastSyncResourceVersion() string {
	return g.handler.LastSyncResourceVersion()
}

func (g *grouper[Out, In]) SetWatchErrorHandler(handler cache.WatchErrorHandler) error {
	return g.source.SetWatchErrorHandler(handler)
}

func (g *grouper[Out, In]) SetWatchErrorHandlerWithContext(handler cache.WatchErrorHandlerWithContext) error {
	return g.source.SetWatchErrorHandlerWithContext(handler)
}

func (g *grouper[Out, In]) SetTransform(handler cache.TransformFunc) error {
	return fmt.Errorf("Group by queries don't support transform")
}

func (g *grouper[Out, In]) HasSynced() bool {
	return g.handler.HasSynced()
}

func (g *grouper[Out, In]) IsStopped() bool {
	return g.handler.IsStopped()
}

func (g *grouper[Out, In]) SetIsStopped() {
	g.handler.SetIsStopped()
}

func (g *grouper[Out, In]) SetHasSynced() {
	g.handler.SetHasSynced()
}

func (g *grouper[Out, In]) GetKeyFunc() cache.KeyFunc {
	return g.handler.GetKeyFunc()
}

func (g *grouper[Out, In]) TriggerWatchError(err error) {
	g.handler.TriggerWatchError(err)
}

func (g *grouper[Out, In]) SetName(name string) {
	g.handler.SetName(name)
}
