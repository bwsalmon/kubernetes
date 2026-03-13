package fort

import (
	"sync"
	"testing"

	"k8s.io/client-go/tools/cache"
)

func TestStress_ComplexPipeline(t *testing.T) {
	// Root sources
	sourceA := NewManualSharedInformer() // Emits RawA
	sourceB := NewManualSharedInformer() // Emits RawB

	type RawA struct {
		ID   int
		Vals []int // Will be expanded by FlatMap
	}
	type RawB struct {
		ID    int
		Value int
	}

	type ExpandedA struct {
		ID  int
		Val int
	}

	type Joined struct {
		ID    int
		AVal  int
		BVal  int
		Total int
	}

	type Aggregated struct {
		ID    int
		Sum   int64
		Count int64
	}

	// 1. FlatMap: RawA -> []ExpandedA
	// Each RawA generates multiple ExpandedA objects.
	flatMapA := QueryInformer(&FlatMap[ExpandedA, RawA]{
		Map: func(a RawA) ([]ExpandedA, error) {
			res := make([]ExpandedA, len(a.Vals))
			for i, v := range a.Vals {
				res[i] = ExpandedA{ID: a.ID, Val: v}
			}
			return res, nil
		},
		Over: sourceA,
	})

	// 2. Join: ExpandedA + RawB -> Joined
	// Joins on ID.
	joinedInformer := QueryInformer(&Join[Joined, ExpandedA, RawB]{
		Select: func(a ExpandedA, b RawB) (Joined, error) {
			return Joined{
				ID:    a.ID,
				AVal:  a.Val,
				BVal:  b.Value,
				Total: a.Val + b.Value,
			}, nil
		},
		From: flatMapA,
		Join: sourceB,
		On: func(a ExpandedA, b RawB) any {
			if a.ID != 0 {
				return [1]int{a.ID}
			}
			return [1]int{b.ID}
		},
	})

	// 3. GroupBy: Joined -> Aggregated
	// Groups by ID, calculates Sum of Total and Count of items.
	finalAggregator := QueryInformer(&GroupBy[Aggregated, Joined]{
		Select: func(fields []GroupField) (Aggregated, error) {
			return Aggregated{
				ID:    fields[0].(int),
				Sum:   fields[1].(int64),
				Count: fields[2].(int64),
			}, nil
		},
		From: joinedInformer,
		GroupBy: func(j Joined) (any, []GroupField) {
			return j.ID, []GroupField{
				AnyValue(j.ID),
				Sum(int64(j.Total)),
				Count(),
			}
		},
	})

	// Track final results
	var lock sync.Mutex
	finalResults := make(map[int]Aggregated)
	finalAggregator.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj any) {
			agg := obj.(Aggregated)
			lock.Lock()
			finalResults[agg.ID] = agg
			lock.Unlock()
		},
		UpdateFunc: func(old, new any) {
			agg := new.(Aggregated)
			lock.Lock()
			finalResults[agg.ID] = agg
			lock.Unlock()
		},
		DeleteFunc: func(obj any) {
			agg := obj.(Aggregated)
			lock.Lock()
			delete(finalResults, agg.ID)
			lock.Unlock()
		},
	})

	// --- Stress Phase ---
	numIDs := 100
	valsPerID := 10
	
	// Pre-populate source B
	for i := 1; i <= numIDs; i++ {
		sourceB.OnAdd(RawB{ID: i, Value: 100}, true)
	}

	// Push RawA data
	// Each ID i will have Sum = (1+2...+10) + (10 * 100) = 55 + 1000 = 1055
	// Total across all IDs = 100 * 1055 = 105,500
	for i := 1; i <= numIDs; i++ {
		vals := make([]int, valsPerID)
		for j := 0; j < valsPerID; j++ {
			vals[j] = j + 1
		}
		sourceA.OnAdd(RawA{ID: i, Vals: vals}, false)
	}

	// Validate results
	lock.Lock()
	if len(finalResults) != numIDs {
		t.Errorf("Expected %d groups, got %d", numIDs, len(finalResults))
	}
	for id, agg := range finalResults {
		expectedSum := int64(1055)
		if agg.Sum != expectedSum {
			t.Errorf("ID %d: expected sum %d, got %d", id, expectedSum, agg.Sum)
		}
		if agg.Count != int64(valsPerID) {
			t.Errorf("ID %d: expected count %d, got %d", id, valsPerID, agg.Count)
		}
	}
	lock.Unlock()

	// --- Update Phase (Churn) ---
	// Update all RawB values to 200
	// New Sum = 55 + (10 * 200) = 2055
	for i := 1; i <= numIDs; i++ {
		sourceB.OnUpdate(RawB{ID: i, Value: 100}, RawB{ID: i, Value: 200})
	}

	lock.Lock()
	for id, agg := range finalResults {
		expectedSum := int64(2055)
		if agg.Sum != expectedSum {
			t.Errorf("AFTER UPDATE ID %d: expected sum %d, got %d", id, expectedSum, agg.Sum)
		}
	}
	lock.Unlock()

	// --- Delete Phase ---
	// Delete half of sourceA
	for i := 1; i <= numIDs/2; i++ {
		vals := make([]int, valsPerID)
		for j := 0; j < valsPerID; j++ {
			vals[j] = j + 1
		}
		sourceA.OnDelete(RawA{ID: i, Vals: vals})
	}

	lock.Lock()
	if len(finalResults) != numIDs/2 {
		t.Errorf("AFTER DELETE: Expected %d groups, got %d", numIDs/2, len(finalResults))
	}
	lock.Unlock()
}

func BenchmarkPipeline(b *testing.B) {
	sourceA := NewManualSharedInformer()
	sourceB := NewManualSharedInformer()

	type RawA struct { ID int; Vals []int }
	type RawB struct { ID int; Value int }
	type ExpandedA struct { ID int; Val int }
	type Joined struct { ID, AVal, BVal, Total int }
	type Aggregated struct { ID int; Sum int64 }

	flatMapA := QueryInformer(&FlatMap[ExpandedA, RawA]{
		Map: func(a RawA) ([]ExpandedA, error) {
			res := make([]ExpandedA, len(a.Vals))
			for i, v := range a.Vals { res[i] = ExpandedA{ID: a.ID, Val: v} }
			return res, nil
		},
		Over: sourceA,
	})

	joinedInformer := QueryInformer(&Join[Joined, ExpandedA, RawB]{
		Select: func(a ExpandedA, b RawB) (Joined, error) {
			return Joined{ID: a.ID, AVal: a.Val, BVal: b.Value, Total: a.Val + b.Value}, nil
		},
		From: flatMapA,
		Join: sourceB,
		On: func(a ExpandedA, b RawB) any {
			if a.ID != 0 { return [1]int{a.ID} }
			return [1]int{b.ID}
		},
	})

	finalAggregator := QueryInformer(&GroupBy[Aggregated, Joined]{
		Select: func(fields []GroupField) (Aggregated, error) {
			return Aggregated{ID: fields[0].(int), Sum: fields[1].(int64)}, nil
		},
		From: joinedInformer,
		GroupBy: func(j Joined) (any, []GroupField) {
			return j.ID, []GroupField{AnyValue(j.ID), Sum(int64(j.Total))}
		},
	})

	finalAggregator.AddEventHandler(cache.ResourceEventHandlerFuncs{})

	sourceB.OnAdd(RawB{ID: 1, Value: 100}, true)
	vals := []int{1, 2, 3, 4, 5}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sourceA.OnAdd(RawA{ID: 1, Vals: vals}, false)
		sourceA.OnDelete(RawA{ID: 1, Vals: vals})
	}
}
