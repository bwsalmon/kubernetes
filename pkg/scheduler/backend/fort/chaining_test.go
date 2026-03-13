package fort

import (
	"fmt"
	"testing"

	"k8s.io/client-go/tools/cache"
)

func TestChaining_FlatMapToJoin(t *testing.T) {
	source1 := NewManualSharedInformer()
	source2 := NewManualSharedInformer()

	type Item struct {
		ID   int
		Tags []string
	}
	type Tagged struct {
		ID  int
		Tag string
	}
	type Meta struct {
		Tag   string
		Value string
	}
	type Result struct {
		ID    int
		Tag   string
		Value string
	}

	taggedKeyFunc := func(obj any) (string, error) {
		t := obj.(Tagged)
		return fmt.Sprintf("%d/%s", t.ID, t.Tag), nil
	}
	taggedHandler := NewManualSharedInformerWithKeyFunc(taggedKeyFunc)

	// FlatMap source1
	m := &FlatMap[Tagged, Item]{
		Map: func(i Item) ([]Tagged, error) {
			var res []Tagged
			for _, tag := range i.Tags {
				res = append(res, Tagged{ID: i.ID, Tag: tag})
			}
			return res, nil
		},
		Over: source1,
	}
	taggedInformer := newFlatMapperWithHandler[Tagged, Item](m.Map, m.Over, taggedHandler)

	// Join tagged with source2 (Meta)
	finalInformer := QueryInformer(&Join[Result, Tagged, Meta]{
		Select: func(t Tagged, m Meta) (Result, error) {
			return Result{ID: t.ID, Tag: t.Tag, Value: m.Value}, nil
		},
		From: taggedInformer,
		Join: source2,
		On: func(t Tagged, m Meta) any {
			if t.Tag != "" { return [1]string{t.Tag} }
			return [1]string{m.Tag}
		},
	})

	var results []Result
	finalInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj any) {
			results = append(results, obj.(Result))
		},
		DeleteFunc: func(obj any) {
			target := obj.(Result)
			for i, r := range results {
				if r == target {
					results = append(results[:i], results[i+1:]...)
					break
				}
			}
		},
	})

	source2.OnAdd(Meta{Tag: "gold", Value: "High"}, true)
	source1.OnAdd(Item{ID: 101, Tags: []string{"gold", "silver"}}, true)

	if len(results) != 1 || results[0].Tag != "gold" {
		t.Errorf("Expected 1 gold result, got %v", results)
	}

	// Add another meta
	source2.OnAdd(Meta{Tag: "silver", Value: "Medium"}, false)
	if len(results) != 2 {
		t.Errorf("Expected 2 results after adding silver meta, got %d", len(results))
	}

	// Remove one tag from source1
	source1.OnUpdate(Item{ID: 101, Tags: []string{"gold", "silver"}}, Item{ID: 101, Tags: []string{"gold"}})
	if len(results) != 1 || results[0].Tag != "gold" {
		t.Errorf("Expected only gold result after update, got %v", results)
	}
}

func TestChaining_JoinToGroupByToFlatMap(t *testing.T) {
	users := NewManualSharedInformer()
	orders := NewManualSharedInformer()

	type User struct{ ID int; Name string }
	type Order struct{ ID, UserID int; Amount int }
	type UserOrder struct{ Name string; Amount int }

	// 1. Join
	userOrders := QueryInformer(&Join[UserOrder, User, Order]{
		Select: func(u User, o Order) (UserOrder, error) {
			return UserOrder{Name: u.Name, Amount: o.Amount}, nil
		},
		From: users,
		Join: orders,
		On: func(u User, o Order) any {
			if u.ID != 0 { return [1]int{u.ID} }
			return [1]int{o.UserID}
		},
	})

	type UserTotal struct {
		Name  string
		Total int64
	}

	// 2. GroupBy
	userTotals := QueryInformer(&GroupBy[UserTotal, UserOrder]{
		Select: func(fields []GroupField) (UserTotal, error) {
			return UserTotal{Name: fields[0].(string), Total: fields[1].(int64)}, nil
		},
		From: userOrders,
		GroupBy: func(uo UserOrder) (any, []GroupField) {
			return [1]string{uo.Name}, []GroupField{AnyValue(uo.Name), Sum(int64(uo.Amount))}
		},
	})

	type Alert struct {
		Msg string
	}

	// 3. FlatMap (Alert if total > 100)
	alerts := QueryInformer(&FlatMap[Alert, UserTotal]{
		Map: func(ut UserTotal) ([]Alert, error) {
			if ut.Total > 100 {
				return []Alert{{Msg: ut.Name + " is a big spender"}}, nil
			}
			return nil, nil
		},
		Over: userTotals,
	})

	var activeAlerts []Alert
	alerts.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj any) { activeAlerts = append(activeAlerts, obj.(Alert)) },
		DeleteFunc: func(obj any) {
			target := obj.(Alert)
			for i, a := range activeAlerts {
				if a == target {
					activeAlerts = append(activeAlerts[:i], activeAlerts[i+1:]...)
					break
				}
			}
		},
	})

	users.OnAdd(User{ID: 1, Name: "Bob"}, true)
	orders.OnAdd(Order{ID: 1, UserID: 1, Amount: 60}, true)
	if len(activeAlerts) != 0 {
		t.Errorf("Expected no alerts yet, got %d", len(activeAlerts))
	}

	orders.OnAdd(Order{ID: 2, UserID: 1, Amount: 50}, false) // Total 110
	if len(activeAlerts) != 1 || activeAlerts[0].Msg != "Bob is a big spender" {
		t.Errorf("Expected 1 alert for Bob, got %v", activeAlerts)
	}

	// Refund one order
	orders.OnDelete(Order{ID: 2, UserID: 1, Amount: 50}) // Total 60
	if len(activeAlerts) != 0 {
		t.Errorf("Expected alert to be removed after refund, got %d", len(activeAlerts))
	}
}
