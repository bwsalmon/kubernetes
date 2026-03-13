package fort

import (
	"context"
	"fmt"
	"sync"
	"time"

	"k8s.io/client-go/tools/cache"
)

type grouper[Out, In any] struct {
	handler ManualSharedInformer
	sel     GroupSelectFunc[Out]
	groupBy SingleGroupByFunc[In]
	where   SingleFilterFunc[In]
	source  cache.SharedInformer

	registration cache.ResourceEventHandlerRegistration

	lock   sync.Mutex
	groups map[any]*groupState[Out]
}

type groupState[Out any] struct {
	count   int
	fields  []any
	lastOut Out
}

func newGrouper[Out, In any](sel GroupSelectFunc[Out], groupBy SingleGroupByFunc[In], from cache.SharedInformer, where SingleFilterFunc[In]) *grouper[Out, In] {
	g := &grouper[Out, In]{
		handler: NewManualSharedInformer(),
		sel:     sel,
		groupBy: groupBy,
		where:   where,
		source:  from,
		groups:  make(map[any]*groupState[Out]),
	}

	g.registration, _ = from.AddEventHandler(g)

	go func() {
		check := g.registration.HasSyncedChecker()
		<-check.Done()
		g.handler.SetHasSynced()
	}()

	return g
}

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
			m := state.fields[i].(map[any]int)
			if adding {
				m[gf.distinct]++
			} else {
				m[gf.distinct]--
				if m[gf.distinct] == 0 {
					delete(m, gf.distinct)
				}
			}
			var distincts []any
			for k := range m {
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

func (g *grouper[Out, In]) OnAdd(obj any, isInitial bool) {
	g.lock.Lock()
	defer g.lock.Unlock()

	input := obj.(In)
	if g.where != nil && !g.where(input) {
		return
	}

	key, fields := g.groupBy(input)

	state, ok := g.groups[key]
	var oldOut Out
	if ok {
		oldOut = state.lastOut
	} else {
		state = &groupState[Out]{}
		g.groups[key] = state
	}

	newFields := g.evaluateFields(fields, state, input, true)
	newOut, _ := g.sel(newFields)
	state.lastOut = newOut

	if ok {
		g.handler.OnUpdate(oldOut, newOut)
	} else {
		g.handler.OnAdd(newOut, isInitial)
	}
}

func (g *grouper[Out, In]) OnUpdate(oldObj, newObj any) {
	g.lock.Lock()
	defer g.lock.Unlock()

	oldInput := oldObj.(In)
	newInput := newObj.(In)

	oldKey, oldFields := g.groupBy(oldInput)
	newKey, newFields := g.groupBy(newInput)

	if oldKey == newKey {
		state, ok := g.groups[oldKey]
		if !ok {
			// This shouldn't happen if everything is consistent, but let's be safe.
			g.lock.Unlock()
			g.OnAdd(newObj, false)
			g.lock.Lock()
			return
		}

		oldOut := state.lastOut
		// To update correctly, we should subtract oldFields and add newFields.
		// Our evaluateFields only takes one input. Let's subtract then add.
		g.evaluateFields(oldFields, state, oldInput, false)
		resFields := g.evaluateFields(newFields, state, newInput, true)
		
		newOut, _ := g.sel(resFields)
		state.lastOut = newOut
		g.handler.OnUpdate(oldOut, newOut)
	} else {
		g.lock.Unlock()
		g.OnDelete(oldObj)
		g.OnAdd(newObj, false)
		g.lock.Lock()
	}
}

func (g *grouper[Out, In]) OnDelete(obj any) {
	g.lock.Lock()
	defer g.lock.Unlock()

	input := obj.(In)
	if g.where != nil && !g.where(input) {
		return
	}

	key, fields := g.groupBy(input)

	state, ok := g.groups[key]
	if !ok {
		return
	}

	oldOut := state.lastOut
	newFields := g.evaluateFields(fields, state, input, false)

	if state.count == 0 {
		delete(g.groups, key)
		g.handler.OnDelete(oldOut)
	} else {
		newOut, _ := g.sel(newFields)
		state.lastOut = newOut
		g.handler.OnUpdate(oldOut, newOut)
	}
}

func (g *grouper[Out, In]) Lock() *sync.Mutex {
	return g.handler.(*manualInformer).Lock()
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

func (g *grouper[Out, In]) Clone(sources []cache.SharedInformer) CloneableSharedInformerQuery {
	return newGrouper[Out, In](g.sel, g.groupBy, sources[0], g.where)
}

func (g *grouper[Out, In]) GetStore() cache.Store {
	return nil
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
	return nil
}

func (g *grouper[Out, In]) SetWatchErrorHandlerWithContext(handler cache.WatchErrorHandlerWithContext) error {
	return nil
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
