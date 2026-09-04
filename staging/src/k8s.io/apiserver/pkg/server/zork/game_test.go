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
	"strings"
	"testing"
)

// step is one command and what the player should see in the answer.
type step struct {
	cmd string
	// want are substrings the output must contain.
	want []string
	// notWant are substrings the output must not contain.
	notWant []string
}

// play runs a script, checking each step and returning the final output.
func play(t *testing.T, g *Game, steps []step) string {
	t.Helper()
	var out string
	for i, s := range steps {
		out = g.Execute(s.cmd)
		for _, want := range s.want {
			if !strings.Contains(out, want) {
				t.Fatalf("step %d %q: output does not contain %q:\n%s", i, s.cmd, want, out)
			}
		}
		for _, notWant := range s.notWant {
			if strings.Contains(out, notWant) {
				t.Fatalf("step %d %q: output unexpectedly contains %q:\n%s", i, s.cmd, notWant, out)
			}
		}
	}
	return out
}

// run executes commands without checking what they print, for getting the
// player to where a test actually begins.
func run(g *Game, cmds ...string) string {
	var out string
	for _, cmd := range cmds {
		out = g.Execute(cmd)
	}
	return out
}

// toLivingRoom is the shortest way in through the kitchen window.
var toLivingRoom = []string{"north", "east", "open window", "west", "west"}

// toCellar picks up the lantern on the way down through the trap door.
var toCellar = append(append([]string{}, toLivingRoom...),
	"take lamp", "turn on lamp", "take sword", "move rug", "open trap door", "down")

func TestNewGameStartsWestOfHouse(t *testing.T) {
	g := New()
	desc := g.Describe()
	for _, want := range []string{"West of House", "white house", "There is a small mailbox here."} {
		if !strings.Contains(desc, want) {
			t.Errorf("opening description does not contain %q:\n%s", want, desc)
		}
	}
	if g.Score() != 0 || g.Moves() != 0 || g.IsOver() {
		t.Errorf("a new game should be at 0 points and 0 moves and not be over, got score=%d moves=%d over=%v", g.Score(), g.Moves(), g.IsOver())
	}
	if g.MaxScore() != 70 {
		t.Errorf("a perfect game should be worth 70 points, got %d", g.MaxScore())
	}
}

// TestWalkthroughToVictory plays the game from the mailbox to the trophy case.
// It is the test that keeps the world winnable: every treasure has to be
// reachable, every door has to open and the chimney has to let the player back
// out of the cellar once the trap door is barred behind them.
func TestWalkthroughToVictory(t *testing.T) {
	g := New()
	final := play(t, g, []step{
		{cmd: "open mailbox", want: []string{"reveals a leaflet"}},
		{cmd: "take leaflet", want: []string{"Taken."}},
		{cmd: "read leaflet", want: []string{"WELCOME TO ZORK!"}},
		{cmd: "north", want: []string{"North of House"}},
		{cmd: "north", want: []string{"Forest Path"}},
		{cmd: "climb tree", want: []string{"Up a Tree", "birds nest", "jewel-encrusted egg"}},
		{cmd: "take egg", want: []string{"Taken."}},
		{cmd: "down", want: []string{"Forest Path"}},
		{cmd: "south", want: []string{"North of House"}},
		{cmd: "east", want: []string{"Behind House", "window"}},
		{cmd: "west", want: []string{"The window is closed."}},
		{cmd: "open window", want: []string{"Opened."}},
		{cmd: "west", want: []string{"Kitchen", "brown paper sack"}},
		{cmd: "west", want: []string{"Living Room", "brass lantern", "elvish sword"}},
		{cmd: "take lamp", want: []string{"Taken."}},
		{cmd: "turn on lamp", want: []string{"brass lantern is now on"}},
		{cmd: "take sword", want: []string{"Taken."}},
		{cmd: "open case", want: []string{"empty"}},
		{cmd: "put egg in case", want: []string{"Done."}},
		{cmd: "score", want: []string{"Your score is 10 of a possible 70"}},
		{cmd: "down", want: []string{"You cannot go that way."}},
		{cmd: "move rug", want: []string{"trap door"}},
		{cmd: "down", want: []string{"The trap door is closed."}},
		{cmd: "open trap door", want: []string{"Opened."}},
		{cmd: "down", want: []string{"Cellar", "crashes shut", "barring it"}},
		{cmd: "up", want: []string{"barred from the other side"}},
		{cmd: "north", want: []string{"Troll Room", "troll"}},
		{cmd: "east", want: []string{"The troll fends you off"}},
		{cmd: "kill troll with sword", want: []string{"staggers back"}},
		{cmd: "kill troll with sword", want: []string{"folds up like a deprecated API", "axe"}},
		{cmd: "east", want: []string{"East-West Passage", "bar of platinum"}},
		{cmd: "take bar", want: []string{"Taken."}},
		{cmd: "west", want: []string{"Troll Room"}},
		{cmd: "south", want: []string{"Cellar"}},
		{cmd: "south", want: []string{"East of Chasm"}},
		{cmd: "east", want: []string{"Gallery", "painting"}},
		{cmd: "take painting", want: []string{"Taken."}},
		{cmd: "north", want: []string{"Studio", "chimney"}},
		{cmd: "up", want: []string{"chimney is narrow"}},
		{cmd: "south", want: []string{"Gallery"}},
		{cmd: "drop sword", want: []string{"Dropped."}},
		{cmd: "drop bar", want: []string{"Dropped."}},
		{cmd: "drop leaflet", want: []string{"Dropped."}},
		{cmd: "north", want: []string{"Studio"}},
		{cmd: "up", want: []string{"Kitchen", "unbarred"}},
		{cmd: "west", want: []string{"Living Room", "trap door stands open"}},
		{cmd: "put painting in case", want: []string{"Done."}},
		{cmd: "score", want: []string{"Your score is 55 of a possible 70"}},
		{cmd: "down", want: []string{"Cellar"}, notWant: []string{"barring it"}},
		{cmd: "south", want: []string{"East of Chasm"}},
		{cmd: "east", want: []string{"Gallery", "There is a platinum bar here."}},
		{cmd: "take bar", want: []string{"Taken."}},
		{cmd: "west", want: []string{"East of Chasm"}},
		{cmd: "north", want: []string{"Cellar"}},
		{cmd: "up", want: []string{"Living Room"}},
		{cmd: "put bar in case", want: []string{"Done.", "**** You have won ****"}},
	})

	if !g.IsOver() {
		t.Error("the game should be over once every treasure is in the case")
	}
	if g.Score() != g.MaxScore() {
		t.Errorf("a full walkthrough should score %d, got %d", g.MaxScore(), g.Score())
	}
	if !strings.Contains(final, "Master of the Great Underground API Server") {
		t.Errorf("a perfect score should earn the top rank:\n%s", final)
	}
	if got := g.Execute("north"); !strings.Contains(got, "restart") {
		t.Errorf("a finished game should point the player at restart, got %q", got)
	}
}

func TestMovingInTheDarkFeedsTheGrue(t *testing.T) {
	g := New()
	run(g, toLivingRoom...)
	play(t, g, []step{
		{cmd: "east", want: []string{"Kitchen"}},
		{cmd: "up", want: []string{"It is pitch black", "grue"}, notWant: []string{"Attic"}},
		{cmd: "take rope", want: []string{"too dark"}},
		{cmd: "down", want: []string{"**** You have died ****", "eaten by a grue"}},
	})
	if !g.IsOver() {
		t.Fatal("being eaten should end the game")
	}
	play(t, g, []step{
		{cmd: "look", want: []string{"You are dead"}},
		{cmd: "score", want: []string{"Your score is"}},
		{cmd: "restart", want: []string{"West of House", "There is a small mailbox here."}},
	})
	if g.IsOver() || g.Score() != 0 || g.Moves() != 0 {
		t.Errorf("restart should hand back a brand new game, got score=%d moves=%d over=%v", g.Score(), g.Moves(), g.IsOver())
	}
}

func TestLanternLightsTheAttic(t *testing.T) {
	g := New()
	run(g, toLivingRoom...)
	play(t, g, []step{
		{cmd: "take lamp", want: []string{"Taken."}},
		{cmd: "east", want: []string{"Kitchen"}},
		{cmd: "up", want: []string{"It is pitch black"}},
		{cmd: "turn on lamp", want: []string{"now on", "Attic", "coil of rope", "nasty-looking knife"}},
		{cmd: "take all", want: []string{"coil of rope: Taken.", "nasty-looking knife: Taken."}},
		{cmd: "inventory", want: []string{"A brass lantern (providing light)", "A coil of rope", "A nasty-looking knife"}},
		{cmd: "turn off lamp", want: []string{"now off", "It is pitch black"}},
		{cmd: "turn on lamp", want: []string{"now on", "Attic"}},
		{cmd: "down", want: []string{"Kitchen"}},
	})
}

func TestTrollGuardsThePassages(t *testing.T) {
	g := New()
	run(g, toCellar...)
	play(t, g, []step{
		{cmd: "north", want: []string{"Troll Room", "blocks all passages"}},
		{cmd: "drop sword", want: []string{"Dropped."}},
		{cmd: "kill troll", want: []string{"bare hands"}},
		{cmd: "south", want: []string{"The troll fends you off"}},
		{cmd: "take sword", want: []string{"Taken."}},
		{cmd: "kill troll with lamp", want: []string{"poor weapon"}},
		{cmd: "attack troll with sword", want: []string{"staggers back"}},
		{cmd: "attack troll with sword", want: []string{"folds up like a deprecated API"}},
		{cmd: "south", want: []string{"Cellar"}},
		{cmd: "north", want: []string{"Troll Room", "bloody axe"}, notWant: []string{"blocks all passages"}},
		{cmd: "take axe", want: []string{"Taken."}},
	})
}

func TestTreasuresScoreOnceEach(t *testing.T) {
	g := New()
	run(g, "north", "north", "up")
	play(t, g, []step{
		{cmd: "take egg", want: []string{"Taken."}},
	})
	if g.Score() != 5 {
		t.Fatalf("taking the egg should be worth 5 points, got %d", g.Score())
	}
	// Dropping and taking it again must not pay twice.
	run(g, "drop egg", "take egg")
	if g.Score() != 5 {
		t.Fatalf("the egg should only pay out once, got %d", g.Score())
	}

	run(g, "down", "south")
	run(g, toLivingRoom[1:]...)
	play(t, g, []step{
		{cmd: "put egg in case", want: []string{"The trophy case is closed."}},
		{cmd: "open case", want: []string{"empty"}},
		{cmd: "put egg in case", want: []string{"Done."}},
	})
	if g.Score() != 10 {
		t.Fatalf("depositing the egg should bring the score to 10, got %d", g.Score())
	}
	play(t, g, []step{
		{cmd: "look", want: []string{"The trophy case contains:", "A jewel-encrusted egg"}},
		{cmd: "take egg", want: []string{"Taken."}},
		{cmd: "put egg in case", want: []string{"Done."}},
	})
	if g.Score() != 10 {
		t.Fatalf("the case should only pay out once per treasure, got %d", g.Score())
	}
	if g.IsOver() {
		t.Error("one treasure out of three should not end the game")
	}
}

// TestTreasureDeliveredInsideSomethingElse covers the player who never takes
// the egg out of the nest: what is in the case is in the case, however it got
// there.
func TestTreasureDeliveredInsideSomethingElse(t *testing.T) {
	g := New()
	run(g, "north", "north", "up", "take nest", "down", "south")
	run(g, toLivingRoom[1:]...)
	play(t, g, []step{
		{cmd: "open case", want: []string{"empty"}},
		{cmd: "put nest in case", want: []string{"Done."}},
		{cmd: "examine case", want: []string{"The trophy case contains:", "A birds nest"}},
	})
	if g.Score() != 5 {
		t.Errorf("the egg in its nest in the case should be worth its 5 points, got %d", g.Score())
	}
	// Taking it back out and putting it in on its own pays the points for
	// picking a treasure up, but not for the case a second time.
	run(g, "take egg")
	if g.Score() != 10 {
		t.Errorf("picking up the egg should add its 5 points, got %d", g.Score())
	}
	run(g, "put egg in case")
	if g.Score() != 10 {
		t.Errorf("the case should not pay for the egg twice, got %d", g.Score())
	}
}

func TestEnterAndExit(t *testing.T) {
	g := New()
	play(t, g, []step{
		{cmd: "enter", want: []string{"boarded shut"}},
		{cmd: "north", want: []string{"North of House"}},
		{cmd: "east", want: []string{"Behind House"}},
		{cmd: "enter", want: []string{"The window is closed."}},
		{cmd: "open window", want: []string{"Opened."}},
		{cmd: "enter", want: []string{"Kitchen"}},
		{cmd: "out", want: []string{"Behind House"}},
		{cmd: "in", want: []string{"Kitchen"}},
		// The window can only be reached from the side it is on.
		{cmd: "close window", want: []string{"cannot see any window here"}},
		{cmd: "out", want: []string{"Behind House"}},
		{cmd: "close window", want: []string{"Closed."}},
		{cmd: "in", want: []string{"The window is closed."}},
	})
}

func TestTakeAllAndDropAll(t *testing.T) {
	g := New()
	run(g, toLivingRoom...)
	play(t, g, []step{
		{cmd: "take all", want: []string{"brass lantern: Taken.", "elvish sword: Taken."}, notWant: []string{"trophy case", "oriental rug"}},
		{cmd: "drop all", want: []string{"Dropped."}},
		{cmd: "inventory", want: []string{"empty-handed"}},
		{cmd: "look", want: []string{"There is a brass lantern here.", "There is an elvish sword here."}},
		{cmd: "take all", want: []string{"Taken."}},
	})
	// The rug is worth moving, not carrying.
	play(t, g, []step{
		{cmd: "take rug", want: []string{"far too heavy"}},
		{cmd: "look under rug", want: []string{"uncovering a closed"}},
		{cmd: "move rug", want: []string{"refuses to move any further"}},
	})
}

func TestExamineAndRead(t *testing.T) {
	g := New()
	play(t, g, []step{
		{cmd: "examine mailbox", want: []string{"small mailbox"}},
		{cmd: "read mailbox", want: []string{"nothing written"}},
		{cmd: "read leaflet", want: []string{"cannot see any leaflet"}},
		{cmd: "open mailbox", want: []string{"reveals a leaflet"}},
		{cmd: "read leaflet", want: []string{"Beware of grues"}},
		{cmd: "examine me", want: []string{"treasure"}},
		{cmd: "examine grue", want: []string{"never seen"}},
		{cmd: "look at house", want: []string{"cannot see any house here"}},
		{cmd: "close mailbox", want: []string{"Closed."}},
		{cmd: "close mailbox", want: []string{"already closed"}},
		{cmd: "take mailbox", want: []string{"securely anchored"}},
	})
}

func TestChainedCommands(t *testing.T) {
	g := New()
	out := g.Execute("open mailbox. take leaflet; read leaflet")
	for _, want := range []string{"reveals a leaflet", "Taken.", "WELCOME TO ZORK!"} {
		if !strings.Contains(out, want) {
			t.Errorf("chained commands did not produce %q:\n%s", want, out)
		}
	}
	if g.Moves() != 3 {
		t.Errorf("three chained commands should cost three moves, got %d", g.Moves())
	}

	long := strings.Repeat("look.", maxCommandsPerLine+1)
	if got := g.Execute(long); !strings.Contains(got, "more than") {
		t.Errorf("an over-long chain should be refused, got %q", got)
	}
	if got := g.Execute("   "); !strings.Contains(got, "I beg your pardon") {
		t.Errorf("empty input should be answered politely, got %q", got)
	}
}

func TestMetaCommands(t *testing.T) {
	g := New()
	play(t, g, []step{
		{cmd: "help", want: []string{"Moving", "trophy case"}},
		{cmd: "version", want: []string{"ZORK: The Great Underground API Server"}},
		{cmd: "save", want: []string{"no saving here"}},
		{cmd: "diagnose", want: []string{"perfect health"}},
		{cmd: "exits", want: []string{"Ways out: north, south, west"}},
		{cmd: "xyzzy", want: []string{"A hollow voice says \"fool\"."}},
		{cmd: "kubectl apply", want: []string{"inside the API server"}},
		{cmd: "wait", want: []string{"Time passes."}},
		{cmd: "score", want: []string{"in 4 moves", "rank of Beginner"}},
	})
	if g.Moves() != 4 {
		t.Errorf("meta commands should not cost moves, got %d", g.Moves())
	}
	play(t, g, []step{
		{cmd: "quit", want: []string{"Thanks for playing"}},
		{cmd: "north", want: []string{"stopped playing"}},
		{cmd: "restart", want: []string{"West of House"}},
		{cmd: "north", want: []string{"North of House"}},
	})
}

func TestUnknownVerbsAndObjects(t *testing.T) {
	g := New()
	play(t, g, []step{
		{cmd: "frobnicate the mailbox", want: []string{"is not a verb I recognise"}},
		{cmd: "take unicorn", want: []string{"cannot see any unicorn here"}},
		{cmd: "north east", want: []string{"North of House"}},
		{cmd: "go", want: []string{"Go where?"}},
		{cmd: "take", want: []string{"Take what?"}},
		{cmd: "put leaflet", want: []string{"Put what in what?"}},
		{cmd: "up", want: []string{"cannot go that way"}},
	})
}

// TestGamesAreIndependent guards the thing that would break every session at
// once: a world shared between games.
func TestGamesAreIndependent(t *testing.T) {
	first, second := New(), New()
	run(first, toLivingRoom...)
	run(first, "take lamp", "take sword", "move rug")
	if got := first.Execute("look"); strings.Contains(got, "brass lantern") {
		t.Fatalf("the lantern should be in the first player's hands:\n%s", got)
	}

	run(second, toLivingRoom...)
	got := second.Execute("look")
	for _, want := range []string{"brass lantern", "elvish sword", "large oriental rug"} {
		if !strings.Contains(got, want) {
			t.Errorf("the second game should be untouched, but its living room has no %q:\n%s", want, got)
		}
	}
	if strings.Contains(got, "trap door") {
		t.Errorf("the second game's rug should not have been moved:\n%s", got)
	}
}

func TestEatAndDrink(t *testing.T) {
	g := New()
	run(g, toLivingRoom...)
	play(t, g, []step{
		{cmd: "east", want: []string{"Kitchen"}},
		{cmd: "open sack", want: []string{"reveals a hot pepper sandwich and a clove of garlic"}},
		{cmd: "eat sandwich", want: []string{"You are not holding"}},
		{cmd: "take sandwich", want: []string{"Taken."}},
		{cmd: "eat sandwich", want: []string{"hit the spot"}},
		{cmd: "eat sandwich", want: []string{"cannot see any sandwich"}},
		{cmd: "drink water", want: []string{"The glass bottle is closed."}},
		{cmd: "open bottle", want: []string{"reveals a quantity of water"}},
		{cmd: "drink water", want: []string{"hit the spot"}},
		{cmd: "eat bottle", want: []string{"inedible"}},
	})
}

func TestRank(t *testing.T) {
	for _, tc := range []struct {
		score int
		want  string
	}{
		{0, "Beginner"},
		{5, "Amateur Adventurer"},
		{20, "Novice Adventurer"},
		{35, "Junior Adventurer"},
		{50, "Adventurer"},
		{69, "Master Adventurer"},
		{70, "Master of the Great Underground API Server"},
	} {
		if got := rank(tc.score, 70); got != tc.want {
			t.Errorf("rank(%d, 70) = %q, want %q", tc.score, got, tc.want)
		}
	}
}

// TestWorldIsConsistent checks the map itself: every exit has to lead
// somewhere real or explain itself, and every item has to live somewhere real.
func TestWorldIsConsistent(t *testing.T) {
	w := newWorld()
	if _, ok := w.rooms[startRoom]; !ok {
		t.Fatalf("the game starts in room %q, which is not on the map", startRoom)
	}
	for id, r := range w.rooms {
		if r.id != id || r.name == "" || r.desc == "" {
			t.Errorf("room %q is missing an id, a name or a description", id)
		}
		for d, ex := range r.exits {
			switch {
			case ex.to == "" && ex.message == "":
				t.Errorf("room %q has a %s exit that neither leads anywhere nor says why", id, d)
			case ex.to != "":
				if _, ok := w.rooms[ex.to]; !ok {
					t.Errorf("room %q has a %s exit to unknown room %q", id, d, ex.to)
				}
			}
			if ex.via != "" {
				if _, ok := w.items[ex.via]; !ok {
					t.Errorf("room %q has a %s exit through unknown item %q", id, d, ex.via)
				}
				if ex.viaClosed == "" {
					t.Errorf("room %q has a %s exit through %q with nothing to say when it is closed", id, d, ex.via)
				}
			}
		}
	}
	for id, it := range w.items {
		if it.id != id || it.name == "" || len(it.nouns) == 0 {
			t.Errorf("item %q is missing an id, a name or the words to refer to it", id)
		}
		if it.loc == locCarried || it.loc == locNowhere {
			continue
		}
		_, inRoom := w.rooms[it.loc]
		holder, inItem := w.items[it.loc]
		if !inRoom && !inItem {
			t.Errorf("item %q starts in %q, which is neither a room nor an item", id, it.loc)
		}
		if inItem && !holder.container {
			t.Errorf("item %q starts inside %q, which is not a container", id, it.loc)
		}
	}
	if len(w.treasures()) != 3 {
		t.Errorf("the trophy case expects 3 treasures, the world has %d", len(w.treasures()))
	}
}
