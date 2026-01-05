package fort

import (
	"runtime"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestSource(t *testing.T) {
	spec := NewSpec()
	spec.New("src", NewExternalSource[string]())
	state := New(spec)
	src := GetItem[ExternalView[string]](state, "src")
	src.Update("foo", "bar")

	m := GetItem[KeyValueMap[string]](state, "src")
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

func setupSources() Spec {
	spec := NewSpec()
	spec.New("a", NewExternalSource[string]())
	spec.New("b", NewExternalSource[string]())
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
	spec.New("ci", FullJoin[string, string]("a", "b"))
	spec.New("c", Materialize[JoinKey[string, string]]("ci"))

	state := New(spec)
	a := GetItem[ExternalView[string]](state, "a")
	b := GetItem[ExternalView[string]](state, "b")
	c := GetItem[KeyValueMap[JoinKey[string, string]]](state, "c")

	a.Update("foo", "fab")

	expectMap(t, c,
		map[JoinKey[string, string]]JoinValue{},
	)

	b.Update("boo", "bab")
	expectMap(t, c,
		map[JoinKey[string, string]]JoinValue{
			{"foo", "boo"}: {
				Left:  "fab",
				Right: "bab",
			},
		},
	)

	b.Update("coo", "cab")
	expectMap(t, c,
		map[JoinKey[string, string]]JoinValue{
			{"foo", "boo"}: {
				Left:  "fab",
				Right: "bab",
			},
			{"foo", "coo"}: {
				Left:  "fab",
				Right: "cab",
			},
		},
	)

	b.Update("coo", "cabb")
	expectMap(t, c,
		map[JoinKey[string, string]]JoinValue{
			{"foo", "boo"}: {
				Left:  "fab",
				Right: "bab",
			},
			{"foo", "coo"}: {
				Left:  "fab",
				Right: "cabb",
			},
		},
	)

	b.Delete("coo")
	expectMap(t, c,
		map[JoinKey[string, string]]JoinValue{
			{"foo", "boo"}: {
				Left:  "fab",
				Right: "bab",
			},
		},
	)

	b.Update("doo", "dab")
	expectMap(t, c,
		map[JoinKey[string, string]]JoinValue{
			{"foo", "boo"}: {
				Left:  "fab",
				Right: "bab",
			},
			{"foo", "doo"}: {
				Left:  "fab",
				Right: "dab",
			},
		},
	)

	a.Update("goo", "gab")
	expectMap(t, c,
		map[JoinKey[string, string]]JoinValue{
			{"foo", "boo"}: {
				Left:  "fab",
				Right: "bab",
			},
			{"foo", "doo"}: {
				Left:  "fab",
				Right: "dab",
			},
			{"goo", "boo"}: {
				Left:  "gab",
				Right: "bab",
			},
			{"goo", "doo"}: {
				Left:  "gab",
				Right: "dab",
			},
		},
	)

	a.Delete("foo")
	expectMap(t, c,
		map[JoinKey[string, string]]JoinValue{
			{"goo", "boo"}: {
				Left:  "gab",
				Right: "bab",
			},
			{"goo", "doo"}: {
				Left:  "gab",
				Right: "dab",
			},
		},
	)
}

func lookupByValue(kv *KeyValue[string]) string {
	return kv.Value.(string)
}

func TestLookupJoin(t *testing.T) {
	spec := setupSources()
	spec.New("ci", LookupJoin("a", "b", lookupByValue))
	spec.New("c", Materialize[string]("ci"))

	state := New(spec)
	a := GetItem[ExternalView[string]](state, "a")
	b := GetItem[ExternalView[string]](state, "b")
	c := GetItem[KeyValueMap[string]](state, "c")

	a.Update("foo", "fab")
	expectMap(t, c,
		map[string]JoinValue{},
	)

	b.Update("boo", "bab")
	expectMap(t, c,
		map[string]JoinValue{},
	)

	b.Update("fab", "gab")
	expectMap(t, c,
		map[string]JoinValue{
			"foo": {
				Left:  "fab",
				Right: "gab",
			},
		},
	)

	a.Update("hoo", "fab")
	expectMap(t, c,
		map[string]JoinValue{
			"foo": {
				Left:  "fab",
				Right: "gab",
			},
			"hoo": {
				Left:  "fab",
				Right: "gab",
			},
		},
	)

	a.Delete("foo")
	expectMap(t, c,
		map[string]JoinValue{
			"hoo": {
				Left:  "fab",
				Right: "gab",
			},
		},
	)

	a.Update("hoo", "bab")
	expectMap(t, c, map[string]JoinValue{})

	a.Update("hoo", "fab")
	expectMap(t, c,
		map[string]JoinValue{
			"hoo": {
				Left:  "fab",
				Right: "gab",
			},
		},
	)

	a.Update("soo", "fab")
	expectMap(t, c,
		map[string]JoinValue{
			"soo": {
				Left:  "fab",
				Right: "gab",
			},
			"hoo": {
				Left:  "fab",
				Right: "gab",
			},
		},
	)

	b.Delete("fab")
	expectMap(t, c, map[string]JoinValue{})

	b.Update("fab", "tab")
	expectMap(t, c,
		map[string]JoinValue{
			"soo": {
				Left:  "fab",
				Right: "tab",
			},
			"hoo": {
				Left:  "fab",
				Right: "tab",
			},
		},
	)
}
