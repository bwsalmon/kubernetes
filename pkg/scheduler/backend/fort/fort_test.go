package fort

import (
	"fmt"
	"testing"

	"k8s.io/client-go/tools/cache"
)

type User struct {
	ID   int
	Name string
}

type Order struct {
	ID     int
	UserID int
	Amount int
}

type UserOrder struct {
	UserName string
	Amount   int
}

func TestSelectJoinGroupBy(t *testing.T) {
	lock := NewLockGroup()
	users := NewManualSharedInformerWithOptions(lock, cache.MetaNamespaceKeyFunc)
	orders := NewManualSharedInformerWithOptions(lock, cache.MetaNamespaceKeyFunc)

	// Query pipeline setup
	userOrders := QueryInformer(&Join[UserOrder, User, Order]{
		Lock: lock,
		Select: func(u User, o Order) (UserOrder, error) {
			return UserOrder{UserName: u.Name, Amount: o.Amount}, nil
		},
		From: users,
		Join: orders,
		On: func(u User, o Order) any {
			if u.ID != 0 {
				return [1]int{u.ID}
			}
			return [1]int{o.UserID}
		},
		Where: func(u User, o Order) bool {
			return u.ID == o.UserID
		},
	})

	type UserTotal struct {
		UserName string
		Total    int64
	}
	userTotals := QueryInformer(&GroupBy[UserTotal, UserOrder]{
		Lock: lock,
		Select: func(fields []GroupField) (UserTotal, error) {
			return UserTotal{
				UserName: fields[0].(string),
				Total:    fields[1].(int64),
			}, nil
		},
		From: userOrders,
		GroupBy: func(uo UserOrder) (any, []GroupField) {
			return [1]string{uo.UserName},
				[]GroupField{
					AnyValue(uo.UserName),
					Sum(int64(uo.Amount)),
				}
		},
	})

	var totalResults []UserTotal
	userTotals.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj any) {
			totalResults = append(totalResults, obj.(UserTotal))
		},
		UpdateFunc: func(oldObj, newObj any) {
			for i, r := range totalResults {
				if r.UserName == newObj.(UserTotal).UserName {
					totalResults[i] = newObj.(UserTotal)
					return
				}
			}
			totalResults = append(totalResults, newObj.(UserTotal))
		},
	})

	// Pushing data
	users.OnAdd(User{ID: 1, Name: "Alice"}, true)
	orders.OnAdd(Order{ID: 101, UserID: 1, Amount: 50}, true)
	orders.OnAdd(Order{ID: 102, UserID: 1, Amount: 30}, false)

	if len(totalResults) != 1 {
		t.Errorf("Expected 1 total result, got %d", len(totalResults) )
	} else if totalResults[0].Total != 80 {
		t.Errorf("Expected total 80, got %d", totalResults[0].Total)
	}
}

func TestFlatMap(t *testing.T) {
	type TaggedItem struct {
		ID  int
		Tag string
	}
	keyFunc := func(obj any) (string, error) {
		ti := obj.(*TaggedItem)
		return fmt.Sprintf("%d/%s", ti.ID, ti.Tag), nil
	}
	lock := NewLockGroup()
	source := NewManualSharedInformerWithOptions(lock, cache.MetaNamespaceKeyFunc)
	handler := NewManualSharedInformerWithOptions(lock, keyFunc)
	
	type Item struct {
		ID   int
		Tags []string
	}

	m := &FlatMap[*TaggedItem, *Item]{
		Lock: lock,
		Map: func(item *Item) ([]*TaggedItem, error) {
			var res []*TaggedItem
			for _, tag := range item.Tags {
				res = append(res, &TaggedItem{ID: item.ID, Tag: tag})
			}
			return res, nil
		},
		Over: source,
	}
	// Manual build to use custom handler
	flatMap := newFlatMapperWithHandler[*TaggedItem, *Item](m.Map, m.Over, handler)

	var results []*TaggedItem
	flatMap.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj any) {
			results = append(results, obj.(*TaggedItem))
		},
	})

	source.OnAdd(&Item{ID: 1, Tags: []string{"a", "b", "c"}}, true)

	if len(results) != 3 {
		t.Errorf("Expected 3 results, got %d", len(results))
	}
}

func TestJoinUpdatesAndDeletes(t *testing.T) {
	lock := NewLockGroup()
	left := NewManualSharedInformerWithOptions(lock, cache.MetaNamespaceKeyFunc)
	right := NewManualSharedInformerWithOptions(lock, cache.MetaNamespaceKeyFunc)

	type L struct{ ID int; Val string }
	type R struct{ ID int; Val string }
	type Joined struct{ LVal, RVal string }

	joinedInformer := QueryInformer(&Join[Joined, L, R]{
		Lock: lock,
		Select: func(l L, r R) (Joined, error) {
			return Joined{LVal: l.Val, RVal: r.Val}, nil
		},
		From: left,
		Join: right,
		On: func(l L, r R) any {
			if l.ID != 0 { return [1]int{l.ID} }
			return [1]int{r.ID}
		},
	})

	var results []Joined
	joinedInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj any) {
			results = append(results, obj.(Joined))
		},
		DeleteFunc: func(obj any) {
			target := obj.(Joined)
			for i, r := range results {
				if r == target {
					results = append(results[:i], results[i+1:]...)
					break
				}
			}
		},
	})

	l1 := L{ID: 1, Val: "L1"}
	r1 := R{ID: 1, Val: "R1"}
	r2 := R{ID: 1, Val: "R2"}

	left.OnAdd(l1, true)
	right.OnAdd(r1, true)
	right.OnAdd(r2, true)

	if len(results) != 2 {
		t.Errorf("Expected 2 results, got %d", len(results))
	}

	right.OnDelete(r1)
	if len(results) != 1 {
		t.Errorf("Expected 1 result after delete, got %d", len(results))
	}

	left.OnDelete(l1)
	if len(results) != 0 {
		t.Errorf("Expected 0 results after left delete, got %d", len(results))
	}
}

func TestGroupByAggregations(t *testing.T) {
	lock := NewLockGroup()
	source := NewManualSharedInformerWithOptions(lock, cache.MetaNamespaceKeyFunc)

	type Data struct {
		Category string
		Value    int64
		Tag      string
	}
	type Aggregated struct {
		Category string
		Count    int64
		Sum      int64
		Distinct []string
		Any      string
	}

	grouped := QueryInformer(&GroupBy[Aggregated, Data]{
		Lock: lock,
		Select: func(fields []GroupField) (Aggregated, error) {
			// fields: [Category, Count, Sum, Distinct, Any]
			distinctAny := fields[3].([]any)
			distinct := make([]string, len(distinctAny))
			for i, v := range distinctAny {
				distinct[i] = v.(string)
			}
			return Aggregated{
				Category: fields[0].(string),
				Count:    fields[1].(int64),
				Sum:      fields[2].(int64),
				Distinct: distinct,
				Any:      fields[4].(string),
			}, nil
		},
		From: source,
		GroupBy: func(d Data) (any, []GroupField) {
			return [1]string{d.Category},
				[]GroupField{
					AnyValue(d.Category),
					Count(),
					Sum(d.Value),
					Distinct(d.Tag),
					AnyValue(d.Tag),
				}
		},
	})

	var last Aggregated
	grouped.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    func(obj any) { last = obj.(Aggregated) },
		UpdateFunc: func(oldObj, newObj any) { last = newObj.(Aggregated) },
	})

	source.OnAdd(Data{Category: "A", Value: 10, Tag: "T1"}, true)
	source.OnAdd(Data{Category: "A", Value: 20, Tag: "T2"}, false)
	source.OnAdd(Data{Category: "A", Value: 30, Tag: "T1"}, false)

	if last.Count != 3 {
		t.Errorf("Expected count 3, got %d", last.Count)
	}
	if last.Sum != 60 {
		t.Errorf("Expected sum 60, got %d", last.Sum)
	}
	if len(last.Distinct) != 2 {
		t.Errorf("Expected 2 distinct tags, got %d", len(last.Distinct))
	}
}

func TestClone(t *testing.T) {
	lock := NewLockGroup()
	source := NewManualSharedInformerWithOptions(lock, cache.MetaNamespaceKeyFunc)
	query := QueryInformer(&Select[int, int]{
		Lock:   lock,
		Select: func(i int) (int, error) { return i * 2, nil },
		From:   source,
	})

	newLock := NewLockGroup()
	newSource := NewManualSharedInformerWithOptions(newLock, cache.MetaNamespaceKeyFunc)
	cloned := query.Clone([]cache.SharedInformer{newSource})

	var results []int
	cloned.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj any) { results = append(results, obj.(int)) },
	})

	newSource.OnAdd(21, true)
	if len(results) != 1 || results[0] != 42 {
		t.Errorf("Expected 42, got %v", results)
	}
}
