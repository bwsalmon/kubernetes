/*
Copyright The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package zork

import (
	"reflect"
	"strings"
	"testing"
)

func TestTokenize(t *testing.T) {
	for _, tc := range []struct {
		input string
		want  []string
	}{
		{"", nil},
		{"   ", nil},
		{"LOOK", []string{"look"}},
		{"Take the brass lantern!", []string{"take", "brass", "lantern"}},
		{"put the egg in the trophy case", []string{"put", "egg", "in", "trophy", "case"}},
		{"  turn   on   lamp  ", []string{"turn", "on", "lamp"}},
		{"attack troll with sword.", []string{"attack", "troll", "with", "sword"}},
		{"please, kill the troll", []string{"kill", "troll"}},
	} {
		if got := tokenize(tc.input); !reflect.DeepEqual(got, tc.want) {
			t.Errorf("tokenize(%q) = %q, want %q", tc.input, got, tc.want)
		}
	}
}

func TestSplitCommands(t *testing.T) {
	for _, tc := range []struct {
		input string
		want  []string
	}{
		{"", nil},
		{"look", []string{"look"}},
		{"open mailbox. take leaflet", []string{"open mailbox", "take leaflet"}},
		{"north; north; up", []string{"north", "north", "up"}},
		{"take lamp then turn on lamp", []string{"take lamp", "turn on lamp"}},
		{"look.\n look .", []string{"look", "look"}},
	} {
		if got := splitCommands(tc.input); !reflect.DeepEqual(got, tc.want) {
			t.Errorf("splitCommands(%q) = %q, want %q", tc.input, got, tc.want)
		}
	}
}

func TestAsDirection(t *testing.T) {
	for word, want := range map[string]direction{
		"n": north, "north": north,
		"sw": southwest, "southwest": southwest,
		"u": up, "upstairs": up,
		"d": down, "downstairs": down,
		"enter": in, "exit": out,
	} {
		got, ok := asDirection(word)
		if !ok || got != want {
			t.Errorf("asDirection(%q) = %q, %v; want %q, true", word, got, ok, want)
		}
	}
	if _, ok := asDirection("sideways"); ok {
		t.Error(`asDirection("sideways") should not resolve to a direction`)
	}
}

func TestSplitAt(t *testing.T) {
	before, after, found := splitAt([]string{"egg", "in", "case"}, "in", "into")
	if !found || !reflect.DeepEqual(before, []string{"egg"}) || !reflect.DeepEqual(after, []string{"case"}) {
		t.Errorf("splitAt around a preposition = %q, %q, %v", before, after, found)
	}
	before, after, found = splitAt([]string{"egg"}, "in")
	if found || !reflect.DeepEqual(before, []string{"egg"}) || after != nil {
		t.Errorf("splitAt without a preposition = %q, %q, %v", before, after, found)
	}
}

func TestWithArticle(t *testing.T) {
	for name, want := range map[string]string{
		"brass lantern":       "a brass lantern",
		"elvish sword":        "an elvish sword",
		"issue of the paper":  "an issue of the paper",
		"quantity of water":   "a quantity of water",
		"a nasty-looking axe": "a nasty-looking axe",
	} {
		if got := withArticle(name); got != want {
			t.Errorf("withArticle(%q) = %q, want %q", name, got, want)
		}
	}
}

func TestJoinList(t *testing.T) {
	for _, tc := range []struct {
		names []string
		want  string
	}{
		{nil, "nothing"},
		{[]string{"a leaflet"}, "a leaflet"},
		{[]string{"a lamp", "a sword"}, "a lamp and a sword"},
		{[]string{"a lamp", "a sword", "an egg"}, "a lamp, a sword and an egg"},
	} {
		if got := joinList(tc.names); got != tc.want {
			t.Errorf("joinList(%q) = %q, want %q", tc.names, got, tc.want)
		}
	}
}

func TestPlural(t *testing.T) {
	for _, tc := range []struct {
		n    int
		want string
	}{{0, "0 moves"}, {1, "1 move"}, {2, "2 moves"}} {
		if got := plural(tc.n, "move"); got != tc.want {
			t.Errorf("plural(%d, \"move\") = %q, want %q", tc.n, got, tc.want)
		}
	}
}

func TestItemMatches(t *testing.T) {
	lamp := newWorld().items["lamp"]
	for _, words := range [][]string{{"lamp"}, {"lantern"}, {"brass"}, {"brass", "lantern"}, {"turn", "on", "lamp"}} {
		if !lamp.matches(words) {
			t.Errorf("the brass lantern should answer to %q", words)
		}
	}
	for _, words := range [][]string{{"sword"}, {"troll"}, {}} {
		if lamp.matches(words) {
			t.Errorf("the brass lantern should not answer to %q", words)
		}
	}
}

// TestResolveScope checks what the player can reach: what they hold, what is in
// the room, and what is inside anything open, but nothing that is a room away
// or shut in a box.
func TestResolveScope(t *testing.T) {
	g := New()
	if it, _ := g.resolve([]string{"mailbox"}); it == nil {
		t.Error("the mailbox is in this room and should resolve")
	}
	if it, msg := g.resolve([]string{"leaflet"}); it != nil {
		t.Errorf("the leaflet is shut in the mailbox and should not resolve, got %q (%q)", it.id, msg)
	}
	run(g, "open mailbox")
	if it, _ := g.resolve([]string{"leaflet"}); it == nil {
		t.Error("the leaflet should resolve once the mailbox is open")
	}
	if it, msg := g.resolve([]string{"sword"}); it != nil || !strings.Contains(msg, "cannot see any sword") {
		t.Errorf("the sword is indoors and should not resolve, got %v (%q)", it, msg)
	}

	// In the dark you can still find what you are holding, and nothing else.
	run(g, "take leaflet", "north", "east", "open window", "west", "take lamp", "up")
	if it, _ := g.resolve([]string{"leaflet"}); it == nil {
		t.Error("a carried leaflet should resolve even in the dark")
	}
	if it, msg := g.resolve([]string{"rope"}); it != nil || !strings.Contains(msg, "too dark") {
		t.Errorf("the rope should be invisible in the dark, got %v (%q)", it, msg)
	}
}

func TestContentsListing(t *testing.T) {
	w := newWorld()
	sack := w.items["sack"]
	if got := contentsListing(w, sack, ""); got != "" {
		t.Errorf("a closed sack should keep its contents to itself, got %q", got)
	}
	sack.open = true
	want := "\nThe brown sack contains:\n  A hot pepper sandwich\n  A clove of garlic"
	if got := contentsListing(w, sack, ""); got != want {
		t.Errorf("contentsListing = %q, want %q", got, want)
	}
	// A transparent container shows what is in it without being opened.
	if got := contentsListing(w, w.items["bottle"], ""); !strings.Contains(got, "A quantity of water") {
		t.Errorf("the glass bottle should show the water through the glass, got %q", got)
	}
	if got := contentsListing(w, w.items["lamp"], ""); got != "" {
		t.Errorf("the lantern holds nothing and should list nothing, got %q", got)
	}
}
