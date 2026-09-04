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
	"fmt"
	"strings"
)

// Game is a single playthrough. It is not safe for concurrent use; the caller
// is expected to serialize commands for one game, which is what the HTTP
// handler in this package does.
//
// Nothing in a Game is random: the same commands always produce the same
// story. That keeps the game testable, and means a walkthrough that worked
// yesterday still works today.
type Game struct {
	w    *world
	here string

	score   int
	moves   int
	visited map[string]bool
	// awarded records the one-off points already granted, keyed by tokens
	// such as "take:egg" or "case:egg", so that a treasure taken, dropped
	// and taken again is only worth points once.
	awarded map[string]bool

	// barred is true while somebody unseen has barred the trap door from
	// the living room side.
	barred bool
	// chimney is true once the player has climbed out of the studio. After
	// that nobody bars the trap door again.
	chimney bool

	dead bool
	quit bool
	won  bool
}

// New returns a game at the start of the story.
func New() *Game {
	g := &Game{}
	g.reset()
	return g
}

func (g *Game) reset() {
	g.w = newWorld()
	g.here = startRoom
	g.score = 0
	g.moves = 0
	g.visited = map[string]bool{startRoom: true}
	g.awarded = map[string]bool{}
	g.barred = false
	g.chimney = false
	g.dead = false
	g.quit = false
	g.won = false
}

// Score is the player's current score.
func (g *Game) Score() int { return g.score }

// MaxScore is the score of a perfect game.
func (g *Game) MaxScore() int { return g.w.maxScore() }

// Moves is the number of commands the player has spent so far.
func (g *Game) Moves() int { return g.moves }

// IsOver reports whether the game has ended, by death, victory or surrender.
// A finished game accepts nothing but "restart" and the meta commands.
func (g *Game) IsOver() bool { return g.dead || g.quit || g.won }

// Banner is the title text shown when a game begins.
func (g *Game) Banner() string {
	return `ZORK: The Great Underground API Server
An homage, served by a Kubernetes API server. Release 1 / Serial number 1976.
Type "help" for instructions, "look" to see where you are.`
}

// Describe returns the description of the room the player is standing in,
// without spending a move. It is what a new session opens with.
func (g *Game) Describe() string { return g.describe(true) }

// Execute runs one line of input and returns what the player sees. Several
// commands may be given at once, separated by periods or semicolons, which
// matters more here than it does at a terminal: every command otherwise costs
// a round trip to the API server.
func (g *Game) Execute(input string) string {
	cmds := splitCommands(input)
	if len(cmds) == 0 {
		return "I beg your pardon?"
	}
	if len(cmds) > maxCommandsPerLine {
		return fmt.Sprintf("That is more than %d commands at once. Even adventurers pace themselves.", maxCommandsPerLine)
	}
	out := make([]string, 0, len(cmds))
	for _, cmd := range cmds {
		out = append(out, g.execOne(cmd))
	}
	return strings.Join(out, "\n\n")
}

// maxCommandsPerLine caps how much story a single request can ask for.
const maxCommandsPerLine = 32

func (g *Game) execOne(input string) string {
	words := tokenize(input)
	if len(words) == 0 {
		return "I beg your pardon?"
	}
	verb, rest := words[0], words[1:]

	// Meta commands are answered whether or not the game is still running.
	switch verb {
	case "score":
		return g.scoreLine()
	case "version":
		return g.Banner()
	case "help", "commands", "instructions":
		return helpText
	case "restart", "reset":
		g.reset()
		return "As you wish. The story begins again.\n\n" + g.describe(true)
	case "quit":
		if g.IsOver() {
			return "You have already stopped playing."
		}
		g.quit = true
		return "Thanks for playing.\n\n" + g.scoreLine()
	case "save", "restore", "load":
		return "There is no saving here. This game lives in an API server's memory,\nwhich teaches a certain acceptance of impermanence."
	case "diagnose":
		if g.dead {
			return "You are dead. This is usually terminal."
		}
		return "You have a light wound, which will heal after a couple of moves you\nhave not taken yet. You are otherwise in perfect health."
	}

	if g.IsOver() {
		switch {
		case g.dead:
			return "You are dead, and the dead take no actions. Type \"restart\" to begin again."
		case g.won:
			return "Your adventure is over, and it ended well. Type \"restart\" to play again."
		default:
			return "You have stopped playing. Type \"restart\" to play again."
		}
	}

	g.moves++

	// Bare directions, and "go"/"walk"/"climb" with one.
	if d, ok := asDirection(verb); ok {
		return g.move(d)
	}
	switch verb {
	case "go", "walk", "run", "travel", "head", "climb":
		if len(rest) == 0 {
			if verb == "climb" {
				return "Climb what?"
			}
			return "Go where?"
		}
		if d, ok := asDirection(rest[0]); ok {
			return g.move(d)
		}
		if verb == "climb" {
			// "climb tree" is how everybody tries to get up a tree:
			// anything climbable here is up.
			if _, ok := g.room().exits[up]; ok {
				return g.move(up)
			}
			return "There is nothing here worth climbing."
		}
		return "You cannot go that way."
	}

	switch verb {
	case "look", "l", "stare":
		if len(rest) > 0 {
			// "look at rug", "look in sack", "look under rug".
			switch rest[0] {
			case "at":
				return g.examine(rest[1:])
			case "in", "inside", "into":
				return g.examine(rest[1:])
			case "under", "behind", "beneath":
				return g.moveItem(rest[1:])
			}
			return g.examine(rest)
		}
		return g.describe(true)
	case "examine", "x", "inspect", "describe", "watch":
		return g.examine(rest)
	case "read":
		return g.read(rest)
	case "inventory", "i", "inv":
		return g.inventory()
	case "take", "get", "grab", "carry", "hold", "pick":
		if len(rest) > 0 && rest[0] == "up" {
			rest = rest[1:]
		}
		return g.take(rest)
	case "drop", "release", "discard":
		return g.drop(rest)
	case "put", "place", "insert", "stuff":
		return g.put(rest)
	case "open":
		return g.open(rest)
	case "close", "shut":
		return g.close(rest)
	case "move", "push", "pull", "slide", "drag", "lift":
		return g.moveItem(rest)
	case "turn", "switch":
		return g.turn(rest)
	case "light":
		return g.setLight(rest, true)
	case "extinguish", "douse", "unlight":
		return g.setLight(rest, false)
	case "attack", "kill", "hit", "fight", "slay", "strike", "stab":
		return g.attack(rest)
	case "eat", "taste", "devour":
		return g.eat(rest)
	case "drink", "sip", "swallow":
		return g.drink(rest)
	case "wait", "z":
		return "Time passes."
	case "exits":
		return g.exits()
	case "xyzzy", "plugh", "plover":
		return "A hollow voice says \"fool\"."
	case "pray":
		return "Your prayer is received, queued, and reconciled eventually."
	case "jump", "leap":
		if g.here == "up-a-tree" {
			return "You look down at ten feet of empty air and think better of it."
		}
		return "Wheeeeeee!"
	case "hello", "hi", "greet":
		return "Nothing happens here."
	case "sing", "shout", "yell", "scream":
		return "The walls take no notice. They have heard worse."
	case "sleep":
		return "You are far too busy for that."
	case "kubectl", "kubernetes", "k8s", "apply", "reconcile":
		return "You sketch a manifest in the air. Nothing happens, which is correct:\nyou are inside the API server, and it has already admitted you."
	case "sudo", "root", "escalate":
		return "You are already as privileged as this cluster gets. It has not helped."
	case "etcd", "compact", "defrag":
		return "Somewhere far below, something large turns over in its sleep."
	case "damn", "curses", "drat", "blast":
		return "Such language in a high-class establishment like this!"
	}

	return fmt.Sprintf("%q is not a verb I recognise. Type \"help\" for the ones I do.", verb)
}

// -- movement ---------------------------------------------------------------

func (g *Game) room() *room { return g.w.rooms[g.here] }

// hasLight reports whether the player can see: either the room is lit, or a lit
// light source is carried or lying here.
func (g *Game) hasLight() bool {
	if !g.room().dark {
		return true
	}
	for _, id := range g.w.order {
		it := g.w.items[id]
		if it.lightSource && it.lit && (it.loc == locCarried || it.loc == g.here) {
			return true
		}
	}
	return false
}

// villainHere returns the living villain in this room, if there is one.
func (g *Game) villainHere() *item {
	for _, it := range g.w.itemsIn(g.here) {
		if it.villain && !it.dead {
			return it
		}
	}
	return nil
}

func (g *Game) move(d direction) string {
	r := g.room()
	if r.dark && !g.hasLight() {
		g.dead = true
		return "You stumble off into the dark, and the dark is not empty.\n" +
			"Oh dear, you seem to have been eaten by a grue.\n\n" +
			"    **** You have died ****\n\n" + g.scoreLine() + "\n\nType \"restart\" to begin again."
	}
	if v := g.villainHere(); v != nil {
		return v.guardMsg
	}
	ex, ok := r.exits[d]
	if !ok {
		return "You cannot go that way."
	}
	if ex.via != "" {
		via := g.w.items[ex.via]
		if via.loc == locNowhere {
			return "You cannot go that way."
		}
		if !via.open {
			if ex.via == "trap-door" && g.barred && g.here == "cellar" {
				return "The trap door is closed, and barred from the other side."
			}
			return ex.viaClosed
		}
	}
	if ex.to == "" {
		return ex.message
	}

	// A couple of exits have opinions about what you are carrying, or about
	// what should happen behind you once you are through them.
	var note string
	switch {
	case g.here == "studio" && ex.to == "kitchen":
		if len(g.carried()) > 2 {
			return "The chimney is narrow and you need both hands to climb it. You could\nmanage the lantern and one other thing, but not an armful."
		}
		g.chimney = true
		if g.barred {
			g.barred = false
			g.w.items["trap-door"].open = true
			note = "As you climb, you hear the trap door below being unbarred by whoever\nbarred it. They appear to have lost interest in you."
		}
	case g.here == "living-room" && ex.to == "cellar" && !g.chimney:
		g.barred = true
		g.w.items["trap-door"].open = false
		note = "The trap door crashes shut behind you, and you hear someone barring it."
	}

	out := g.enter(ex.to)
	if note != "" {
		out = out + "\n\n" + note
	}
	return out
}

func (g *Game) enter(id string) string {
	g.here = id
	first := !g.visited[id]
	if first {
		g.visited[id] = true
		g.score += g.room().value
	}
	return g.describe(first)
}

func (g *Game) exits() string {
	if g.room().dark && !g.hasLight() {
		return "You cannot see the walls, let alone the way out of them."
	}
	var open []string
	for _, d := range allDirections {
		if ex, ok := g.room().exits[d]; ok && ex.to != "" {
			open = append(open, string(d))
		}
	}
	if len(open) == 0 {
		return "There is no obvious way out of here."
	}
	return "Ways out: " + strings.Join(open, ", ") + "."
}

// -- looking at things ------------------------------------------------------

func (g *Game) describe(full bool) string {
	r := g.room()
	if r.dark && !g.hasLight() {
		return "It is pitch black. You are likely to be eaten by a grue."
	}
	var b strings.Builder
	b.WriteString(r.name)
	if full {
		b.WriteString("\n" + r.desc)
	}
	for _, it := range g.w.itemsIn(r.id) {
		if it.scenery {
			// Scenery is already part of the room's own description,
			// but what is sitting inside an open one is not.
			b.WriteString(contentsListing(g.w, it, ""))
			continue
		}
		b.WriteString("\n" + it.listing())
		b.WriteString(contentsListing(g.w, it, ""))
	}
	// The trap door is part of the floor, but which way it is lying is
	// worth mentioning every single time.
	if td := g.w.items["trap-door"]; td.loc == r.id {
		if td.open {
			b.WriteString("\nA trap door stands open in the floor, with a staircase leading down into the dark.")
		} else {
			b.WriteString("\nA closed trap door is set into the floor.")
		}
	}
	return b.String()
}

func (g *Game) examine(words []string) string {
	if len(words) == 0 {
		return "Examine what?"
	}
	if words[0] == "me" || words[0] == "myself" || words[0] == "self" {
		return "You look like someone who came here for the treasure and stayed for the grues."
	}
	if words[0] == "grue" {
		return "There is no grue here, and no way to be sure of that for long. Grues are\nnever seen, only inferred, usually far too late."
	}
	it, msg := g.resolve(words)
	if it == nil {
		return msg
	}
	var b strings.Builder
	if it.desc != "" {
		b.WriteString(it.desc)
	} else {
		b.WriteString(fmt.Sprintf("There is nothing special about %s.", withArticle(it.name)))
	}
	if it.lightSource {
		if it.lit {
			b.WriteString(" It is on.")
		} else {
			b.WriteString(" It is off.")
		}
	}
	if it.container {
		switch {
		case it.openable && !it.open && !it.transparent:
			b.WriteString(fmt.Sprintf(" The %s is closed.", it.name))
		case len(g.w.itemsIn(it.id)) == 0:
			b.WriteString(fmt.Sprintf(" The %s is empty.", it.name))
		default:
			b.WriteString("\n" + strings.TrimPrefix(contentsListing(g.w, it, "  "), "\n"))
		}
	}
	return b.String()
}

func (g *Game) read(words []string) string {
	if len(words) == 0 {
		return "Read what?"
	}
	it, msg := g.resolve(words)
	if it == nil {
		return msg
	}
	if g.room().dark && !g.hasLight() {
		return "It is too dark to read."
	}
	if it.text == "" {
		return fmt.Sprintf("There is nothing written on the %s.", it.name)
	}
	return it.text
}

func (g *Game) inventory() string {
	carried := g.carried()
	if len(carried) == 0 {
		return "You are empty-handed."
	}
	var b strings.Builder
	b.WriteString("You are carrying:")
	for _, it := range carried {
		b.WriteString("\n  " + capitalize(withArticle(it.name)))
		if it.lightSource && it.lit {
			b.WriteString(" (providing light)")
		}
		b.WriteString(contentsListing(g.w, it, "  "))
	}
	return b.String()
}

func (g *Game) carried() []*item { return g.w.itemsIn(locCarried) }

// -- handling things --------------------------------------------------------

func (g *Game) take(words []string) string {
	if len(words) == 0 {
		return "Take what?"
	}
	if words[0] == "all" || words[0] == "everything" {
		return g.takeAll()
	}
	it, msg := g.resolve(words)
	if it == nil {
		return msg
	}
	if it.loc == locCarried {
		return "You already have that."
	}
	return g.takeItem(it)
}

func (g *Game) takeAll() string {
	var lines []string
	for _, it := range g.w.itemsIn(g.here) {
		if it.scenery || !it.takeable {
			continue
		}
		lines = append(lines, fmt.Sprintf("%s: %s", it.name, g.takeItem(it)))
	}
	if len(lines) == 0 {
		return "There is nothing here to take."
	}
	return strings.Join(lines, "\n")
}

func (g *Game) takeItem(it *item) string {
	if !it.takeable {
		if it.cantTake != "" {
			return it.cantTake
		}
		return "That is not something you can carry away."
	}
	it.loc = locCarried
	it.moved = true
	if it.treasure && !g.awarded["take:"+it.id] {
		g.awarded["take:"+it.id] = true
		g.score += it.takeValue
	}
	return "Taken."
}

func (g *Game) drop(words []string) string {
	if len(words) == 0 {
		return "Drop what?"
	}
	if words[0] == "all" || words[0] == "everything" {
		carried := g.carried()
		if len(carried) == 0 {
			return "You are empty-handed."
		}
		for _, it := range carried {
			it.loc = g.here
		}
		return "Dropped."
	}
	it, msg := g.resolve(words)
	if it == nil {
		return msg
	}
	if it.loc != locCarried {
		return "You are not carrying that."
	}
	it.loc = g.here
	return "Dropped."
}

func (g *Game) put(words []string) string {
	before, after, ok := splitAt(words, "in", "into", "on", "onto", "inside")
	if !ok {
		if len(words) > 0 && words[0] == "down" {
			return g.drop(words[1:])
		}
		return "Put what in what?"
	}
	if len(before) == 0 {
		return "Put what?"
	}
	if len(after) == 0 {
		return "Put it in what?"
	}
	it, msg := g.resolve(before)
	if it == nil {
		return msg
	}
	target, msg := g.resolve(after)
	if target == nil {
		return msg
	}
	if it == target {
		return fmt.Sprintf("Putting the %s inside itself would be a fine trick.", it.name)
	}
	if !target.container {
		return fmt.Sprintf("The %s cannot hold anything.", target.name)
	}
	if target.openable && !target.open {
		return fmt.Sprintf("The %s is closed.", target.name)
	}
	if it.loc != locCarried {
		if reply := g.takeItem(it); reply != "Taken." {
			return reply
		}
	}
	it.loc = target.id
	out := "Done."
	if target.id == "trophy-case" && it.treasure && !g.awarded["case:"+it.id] {
		g.awarded["case:"+it.id] = true
		g.score += it.caseValue
	}
	if won := g.checkVictory(); won != "" {
		out += "\n\n" + won
	}
	return out
}

// checkVictory returns the ending text once every treasure is in the case.
func (g *Game) checkVictory() string {
	for _, t := range g.w.treasures() {
		if t.loc != "trophy-case" {
			return ""
		}
	}
	g.score += winBonus
	g.won = true
	return "A hush settles over the living room. Inside the case the treasures arrange\n" +
		"themselves, observed state and desired state agree at last, and somewhere\n" +
		"far below a grue sighs and goes looking for other work.\n\n" +
		"    **** You have won ****\n\n" + g.scoreLine()
}

func (g *Game) open(words []string) string {
	if len(words) == 0 {
		return "Open what?"
	}
	it, msg := g.resolve(words)
	if it == nil {
		return msg
	}
	if !it.openable {
		return fmt.Sprintf("The %s cannot be opened.", it.name)
	}
	if it.open {
		return fmt.Sprintf("The %s is already open.", it.name)
	}
	it.open = true
	if it.container {
		if inside := g.w.itemsIn(it.id); len(inside) > 0 {
			names := make([]string, 0, len(inside))
			for _, c := range inside {
				names = append(names, withArticle(c.name))
			}
			return fmt.Sprintf("Opening the %s reveals %s.", it.name, joinList(names))
		}
		return fmt.Sprintf("Opened. The %s is empty.", it.name)
	}
	return "Opened."
}

func (g *Game) close(words []string) string {
	if len(words) == 0 {
		return "Close what?"
	}
	it, msg := g.resolve(words)
	if it == nil {
		return msg
	}
	if !it.openable {
		return fmt.Sprintf("The %s cannot be closed.", it.name)
	}
	if !it.open {
		return fmt.Sprintf("The %s is already closed.", it.name)
	}
	it.open = false
	return "Closed."
}

// moveItem handles pushing, pulling and looking under things. Exactly one thing
// in this world is worth moving, and it is not subtle about it.
func (g *Game) moveItem(words []string) string {
	if len(words) == 0 {
		return "Move what?"
	}
	it, msg := g.resolve(words)
	if it == nil {
		return msg
	}
	if it.id == "rug" {
		td := g.w.items["trap-door"]
		if td.loc == locNowhere {
			td.loc = "living-room"
			return "With a great heave you drag the rug to one side, uncovering a closed\ntrap door set into the floor."
		}
		return "Having done its one job, the rug refuses to move any further."
	}
	if it.loc == locCarried {
		return fmt.Sprintf("You are already holding the %s.", it.name)
	}
	return fmt.Sprintf("Moving the %s reveals nothing.", it.name)
}

func (g *Game) turn(words []string) string {
	switch {
	case len(words) == 0:
		return "Turn what?"
	case words[0] == "on":
		return g.setLight(words[1:], true)
	case words[0] == "off":
		return g.setLight(words[1:], false)
	case len(words) > 1 && words[len(words)-1] == "on":
		return g.setLight(words[:len(words)-1], true)
	case len(words) > 1 && words[len(words)-1] == "off":
		return g.setLight(words[:len(words)-1], false)
	}
	return "Turn it on, or turn it off?"
}

func (g *Game) setLight(words []string, on bool) string {
	if len(words) == 0 {
		return "Which one?"
	}
	it, msg := g.resolve(words)
	if it == nil {
		return msg
	}
	if !it.lightSource {
		if on {
			return fmt.Sprintf("The %s is not something you can turn on.", it.name)
		}
		return fmt.Sprintf("The %s is not something you can turn off.", it.name)
	}
	if it.lit == on {
		if on {
			return fmt.Sprintf("The %s is already on.", it.name)
		}
		return fmt.Sprintf("The %s is already off.", it.name)
	}
	it.lit = on
	if !on {
		out := fmt.Sprintf("The %s is now off.", it.name)
		if g.room().dark && !g.hasLight() {
			out += "\n\nIt is pitch black. You are likely to be eaten by a grue."
		}
		return out
	}
	out := fmt.Sprintf("The %s is now on.", it.name)
	// Light in a dark room is worth a fresh look around.
	if g.room().dark {
		out += "\n\n" + g.describe(true)
	}
	return out
}

func (g *Game) attack(words []string) string {
	target, with, hasWith := splitAt(words, "with", "using", "by")
	if len(target) == 0 {
		return "Attack what?"
	}
	victim, msg := g.resolve(target)
	if victim == nil {
		return msg
	}
	if !victim.villain {
		return fmt.Sprintf("Attacking the %s would achieve nothing, and you would have to live with it.", victim.name)
	}
	var weapon *item
	if hasWith && len(with) > 0 {
		w, msg := g.resolve(with)
		if w == nil {
			return msg
		}
		if w.loc != locCarried {
			return fmt.Sprintf("You are not holding the %s.", w.name)
		}
		if !w.weapon {
			return fmt.Sprintf("The %s makes a poor weapon.", w.name)
		}
		weapon = w
	} else {
		for _, it := range g.carried() {
			if it.weapon {
				weapon = it
				break
			}
		}
	}
	if weapon == nil {
		return fmt.Sprintf("Attacking the %s with your bare hands has one obvious flaw, and he is\nholding it.", victim.name)
	}
	victim.hits++
	if victim.hits < 2 {
		return fmt.Sprintf("You swing the %s. The %s staggers back, bleeding, and reconsiders his\nposition without giving any of it up.", weapon.name, victim.name)
	}
	victim.dead = true
	victim.loc = locNowhere
	if victim.id == "troll" {
		g.w.items["axe"].loc = g.here
	}
	return fmt.Sprintf("The %s comes down squarely, and the %s folds up like a deprecated API.\nA cloud of black fog gathers around him; when it clears, only his axe is\nleft on the floor.", weapon.name, victim.name)
}

func (g *Game) eat(words []string) string {
	if len(words) == 0 {
		return "Eat what?"
	}
	it, msg := g.resolve(words)
	if it == nil {
		return msg
	}
	if !it.edible {
		return fmt.Sprintf("The %s is, on the whole, inedible.", it.name)
	}
	if it.loc != locCarried {
		return fmt.Sprintf("You are not holding the %s.", it.name)
	}
	it.loc = locNowhere
	return "Thank you very much. It really hit the spot."
}

func (g *Game) drink(words []string) string {
	if len(words) == 0 {
		return "Drink what?"
	}
	it, msg := g.resolve(words)
	if it == nil {
		return msg
	}
	if !it.drinkable {
		return fmt.Sprintf("The %s is not something you can drink.", it.name)
	}
	if holder, ok := g.w.items[it.loc]; ok && holder.openable && !holder.open {
		return fmt.Sprintf("The %s is closed.", holder.name)
	}
	it.loc = locNowhere
	return "Thank you very much. That hit the spot."
}

// -- scoring ----------------------------------------------------------------

func (g *Game) scoreLine() string {
	max := g.MaxScore()
	return fmt.Sprintf("Your score is %d of a possible %d, in %s. This gives you the rank of %s.",
		g.score, max, plural(g.moves, "move"), rank(g.score, max))
}

func rank(score, max int) string {
	switch pct := score * 100 / max; {
	case pct <= 0:
		return "Beginner"
	case pct < 20:
		return "Amateur Adventurer"
	case pct < 40:
		return "Novice Adventurer"
	case pct < 60:
		return "Junior Adventurer"
	case pct < 80:
		return "Adventurer"
	case pct < 100:
		return "Master Adventurer"
	default:
		return "Master of the Great Underground API Server"
	}
}

const helpText = `Type commands in plain English. The ones this game understands are:

  Moving   north, south, east, west, up, down, in, out (n, s, e, w, u, d),
           go <direction>, climb tree, enter, exit, exits
  Looking  look (l), examine <thing> (x), read <thing>, inventory (i)
  Doing    take <thing>, take all, drop <thing>, put <thing> in <thing>,
           open <thing>, close <thing>, move <thing>, turn on <thing>,
           turn off <thing>, attack <thing> with <thing>, eat, drink, wait
  Meta     score, diagnose, version, help, restart, quit

Several commands may be given at once, separated by periods:
"open mailbox. take leaflet. read leaflet".

The object of the game is to find the treasures of the underground empire and
put them in the trophy case in the living room of the white house. Rooms below
ground are dark, and moving about in the dark is how adventures end.`
