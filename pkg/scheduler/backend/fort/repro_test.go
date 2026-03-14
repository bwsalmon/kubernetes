package fort

import (
	"testing"

	"k8s.io/client-go/tools/cache"
)

func TestManualInformer_UpdateKeyChangeBug(t *testing.T) {
	lock := NewLockGroup()
	// Use a keyfunc that depends on the value
	keyFunc := func(obj any) (string, error) {
		return obj.(string), nil
	}
	inf := NewManualSharedInformerWithOptions(lock, keyFunc)

	inf.OnAdd("A", true)
	if len(inf.GetStore().List()) != 1 {
		t.Errorf("Expected 1 item")
	}

	// Update "A" to "B". Key changes from "A" to "B".
	inf.OnUpdate("A", "B")

	items := inf.GetStore().List()
	if len(items) != 1 {
		t.Errorf("Expected 1 item after update, but got %d (leak of old key!)", len(items))
		for _, item := range items {
			t.Logf("Item: %v", item)
		}
	}
}

func TestClonePipeline_CyclePanic(t *testing.T) {
	lock := NewLockGroup()
	source := NewManualSharedInformerWithOptions(lock, DefaultKeyFunc)
	
	q1 := &Select[int, int]{Lock: lock, From: source, Select: func(i int) (int, error) { return i, nil }}
	i1 := q1.Build()
	
	// Create a cycle by forcing sources
	i1.(*flatMapper[int, int]).source = i1 

	defer func() {
		if r := recover(); r == nil {
			t.Errorf("Expected panic on recursive clone cycle")
		}
	}()

	memo := make(map[cache.SharedInformer]cache.SharedInformer)
	ClonePipeline(i1, memo)
}

func TestFlatMap_DuplicateKeys(t *testing.T) {
	lock := NewLockGroup()
	source := NewManualSharedInformerWithOptions(lock, cache.MetaNamespaceKeyFunc)
	
	// Map one input to two outputs with SAME key
	fm := QueryInformer(&FlatMap[string, string]{
		Lock: lock,
		Map: func(s string) ([]string, error) {
			return []string{"result", "result"}, nil
		},
		Over: source,
	})

	var adds, updates, deletes int
	fm.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    func(obj any) { adds++ },
		UpdateFunc: func(old, new any) { updates++ },
		DeleteFunc: func(obj any) { deletes++ },
	})

	source.OnAdd("input", true)
	
	// Since handler is a manualInformer with BTreeIndexer, 
	// the second "result" should overwrite the first.
	if adds != 2 {
		t.Logf("Note: Adds is %d (expected 2 if duplicate results are emitted)", adds)
	}
}
