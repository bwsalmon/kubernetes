package fort

import (
	"runtime"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestSource(t *testing.T) {
	spec := NewSpec()
	NewSource[string](spec, "src")
	state := New(spec)
	src := Source[string](state, "src")
	src.Update("foo", "bar")

	m := GetMap[string](state, "src")
	v, found := m.Get("foo")
	if !found {
		t.Fatal("not found")
	}
	if v != "bar" {
		t.Fatalf("Expected bar got %s", v)
	}

	src.Delete("foo")
	v, found = m.Get("foo")
	if found {
		t.Fatal("Found when unexepcted")
	}
}

func setupSources() StateSpec {
	spec := NewSpec()
	NewSource[string](spec, "a")
	NewSource[string](spec, "b")
	return spec
}

func expectMap[K comparable, V any](t *testing.T, m KeyValueMap[K], expected map[K]V) {
	actual := map[K]V{}
	for k, v := range m.All() {
		actual[k] = v.(V)
	}
	if d := cmp.Diff(expected, actual); d != "" {
		_, filename, line, _ := runtime.Caller(1)
		t.Fatalf("Diff from expected (-expect, +actual) at %s:%d: %s", filename, line, d)
	}
}

func TestFullJoin(t *testing.T) {
	spec := setupSources()
	FullJoin[string, string](spec, "c", "a", "b")

	state := New(spec)
	a := Source[string](state, "a")
	b := Source[string](state, "b")
	c := GetMap[JoinKey](state, "c")

	a.Update("foo", "fab")

	expectMap(t, c,
		map[JoinKey]JoinValue[string, string]{},
	)

	b.Update("boo", "bab")
	expectMap(t, c,
		map[JoinKey]JoinValue[string, string]{
			{"foo", "boo"}: {
				Left:  &KeyValue[string]{Key: "foo", Value: "fab"},
				Right: &KeyValue[string]{Key: "boo", Value: "bab"},
			},
		},
	)

	b.Update("coo", "cab")
	expectMap(t, c,
		map[JoinKey]JoinValue[string, string]{
			{"foo", "boo"}: {
				Left:  &KeyValue[string]{Key: "foo", Value: "fab"},
				Right: &KeyValue[string]{Key: "boo", Value: "bab"},
			},
			{"foo", "coo"}: {
				Left:  &KeyValue[string]{Key: "foo", Value: "fab"},
				Right: &KeyValue[string]{Key: "coo", Value: "cab"},
			},
		},
	)

	b.Update("coo", "cabb")
	expectMap(t, c,
		map[JoinKey]JoinValue[string, string]{
			{"foo", "boo"}: {
				Left:  &KeyValue[string]{Key: "foo", Value: "fab"},
				Right: &KeyValue[string]{Key: "boo", Value: "bab"},
			},
			{"foo", "coo"}: {
				Left:  &KeyValue[string]{Key: "foo", Value: "fab"},
				Right: &KeyValue[string]{Key: "coo", Value: "cabb"},
			},
		},
	)

	b.Delete("coo")
	expectMap(t, c,
		map[JoinKey]JoinValue[string, string]{
			{"foo", "boo"}: {
				Left:  &KeyValue[string]{Key: "foo", Value: "fab"},
				Right: &KeyValue[string]{Key: "boo", Value: "bab"},
			},
		},
	)

	b.Update("doo", "dab")
	expectMap(t, c,
		map[JoinKey]JoinValue[string, string]{
			{"foo", "boo"}: {
				Left:  &KeyValue[string]{Key: "foo", Value: "fab"},
				Right: &KeyValue[string]{Key: "boo", Value: "bab"},
			},
			{"foo", "doo"}: {
				Left:  &KeyValue[string]{Key: "foo", Value: "fab"},
				Right: &KeyValue[string]{Key: "doo", Value: "dab"},
			},
		},
	)

	a.Update("goo", "gab")
	expectMap(t, c,
		map[JoinKey]JoinValue[string, string]{
			{"foo", "boo"}: {
				Left:  &KeyValue[string]{Key: "foo", Value: "fab"},
				Right: &KeyValue[string]{Key: "boo", Value: "bab"},
			},
			{"foo", "doo"}: {
				Left:  &KeyValue[string]{Key: "foo", Value: "fab"},
				Right: &KeyValue[string]{Key: "doo", Value: "dab"},
			},
			{"goo", "boo"}: {
				Left:  &KeyValue[string]{Key: "goo", Value: "gab"},
				Right: &KeyValue[string]{Key: "boo", Value: "bab"},
			},
			{"goo", "doo"}: {
				Left:  &KeyValue[string]{Key: "goo", Value: "gab"},
				Right: &KeyValue[string]{Key: "doo", Value: "dab"},
			},
		},
	)

	a.Delete("foo")
	expectMap(t, c,
		map[JoinKey]JoinValue[string, string]{
			{"goo", "boo"}: {
				Left:  &KeyValue[string]{Key: "goo", Value: "gab"},
				Right: &KeyValue[string]{Key: "boo", Value: "bab"},
			},
			{"goo", "doo"}: {
				Left:  &KeyValue[string]{Key: "goo", Value: "gab"},
				Right: &KeyValue[string]{Key: "doo", Value: "dab"},
			},
		},
	)
}

func lookupByValue(kv *KeyValue[string]) string {
	return kv.Value.(string)
}

func TestLookupJoin(t *testing.T) {
	spec := setupSources()
	LookupJoin(spec, "c", "a", "b", lookupByValue)

	state := New(spec)
	a := Source[string](state, "a")
	b := Source[string](state, "b")
	c := GetMap[string](state, "c")

	a.Update("foo", "fab")
	expectMap(t, c,
		map[string]JoinValue[string, string]{},
	)

	b.Update("boo", "bab")
	expectMap(t, c,
		map[string]JoinValue[string, string]{},
	)

	b.Update("fab", "gab")
	expectMap(t, c,
		map[string]JoinValue[string, string]{
			"foo": {
				Left:  &KeyValue[string]{Key: "foo", Value: "fab"},
				Right: &KeyValue[string]{Key: "fab", Value: "gab"},
			},
		},
	)

	a.Update("hoo", "fab")
	expectMap(t, c,
		map[string]JoinValue[string, string]{
			"foo": {
				Left:  &KeyValue[string]{Key: "foo", Value: "fab"},
				Right: &KeyValue[string]{Key: "fab", Value: "gab"},
			},
			"hoo": {
				Left:  &KeyValue[string]{Key: "hoo", Value: "fab"},
				Right: &KeyValue[string]{Key: "fab", Value: "gab"},
			},
		},
	)

	a.Delete("foo")
	expectMap(t, c,
		map[string]JoinValue[string, string]{
			"hoo": {
				Left:  &KeyValue[string]{Key: "hoo", Value: "fab"},
				Right: &KeyValue[string]{Key: "fab", Value: "gab"},
			},
		},
	)

	a.Update("hoo", "bab")
	expectMap(t, c, map[string]JoinValue[string, string]{})

	a.Update("hoo", "fab")
	expectMap(t, c,
		map[string]JoinValue[string, string]{
			"hoo": {
				Left:  &KeyValue[string]{Key: "hoo", Value: "fab"},
				Right: &KeyValue[string]{Key: "fab", Value: "gab"},
			},
		},
	)

	a.Update("soo", "fab")
	expectMap(t, c,
		map[string]JoinValue[string, string]{
			"soo": {
				Left:  &KeyValue[string]{Key: "soo", Value: "fab"},
				Right: &KeyValue[string]{Key: "fab", Value: "gab"},
			},
			"hoo": {
				Left:  &KeyValue[string]{Key: "hoo", Value: "fab"},
				Right: &KeyValue[string]{Key: "fab", Value: "gab"},
			},
		},
	)

	b.Delete("fab")
	expectMap(t, c, map[string]JoinValue[string, string]{})

	b.Update("fab", "tab")
	expectMap(t, c,
		map[string]JoinValue[string, string]{
			"soo": {
				Left:  &KeyValue[string]{Key: "soo", Value: "fab"},
				Right: &KeyValue[string]{Key: "fab", Value: "tab"},
			},
			"hoo": {
				Left:  &KeyValue[string]{Key: "hoo", Value: "fab"},
				Right: &KeyValue[string]{Key: "fab", Value: "tab"},
			},
		},
	)
}
