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

// The map below is an original homage to the cave-crawling text adventures of
// the late 1970s. The shape of the world will look familiar to anyone who has
// ever been eaten by a grue, but every line of prose in it is ours, and the
// jokes are the ones an API server would make.

// direction is one of the ways out of a room.
type direction string

const (
	north     direction = "north"
	south     direction = "south"
	east      direction = "east"
	west      direction = "west"
	northeast direction = "northeast"
	northwest direction = "northwest"
	southeast direction = "southeast"
	southwest direction = "southwest"
	up        direction = "up"
	down      direction = "down"
	in        direction = "in"
	out       direction = "out"
)

// allDirections is the order directions are reported in, e.g. by "exits".
var allDirections = []direction{north, south, east, west, northeast, northwest, southeast, southwest, up, down, in, out}

// Pseudo-locations. Every item lives either in a room, inside another item, in
// the player's hands, or nowhere at all (which is where items wait until the
// story needs them).
const (
	locCarried = "@carried"
	locNowhere = "@nowhere"
)

// startRoom is where every game begins.
const startRoom = "west-of-house"

// winBonus is awarded once every treasure is in the trophy case.
const winBonus = 10

// exit describes what happens when the player walks in a given direction.
type exit struct {
	// to is the room this exit leads to. An empty to means the exit is
	// only there to explain itself, using message.
	to string
	// message is printed instead of moving. It is required when to is empty.
	message string
	// via names an item that has to be open before the exit can be used,
	// such as the kitchen window or the trap door. An item that is still
	// hidden makes the exit invisible.
	via string
	// viaClosed is printed when via exists but is closed.
	viaClosed string
}

// room is a single location on the map.
type room struct {
	id string
	// name is the heading printed on arrival, e.g. "Living Room".
	name string
	// desc is the full description, printed on the first visit and by "look".
	desc string
	// dark rooms need a light source before anything can be seen in them,
	// and are where grues wait.
	dark bool
	// value is the score awarded for arriving here for the first time.
	value int
	exits map[direction]exit
}

// item is anything in the world that is not a room: things to carry, things to
// open, scenery to examine and, in one case, something that would rather you
// did not walk past it.
type item struct {
	id string
	// name is how the item is printed once the player knows about it.
	name string
	// nouns are the words the parser accepts for this item.
	nouns []string
	// loc is a room id, another item's id, locCarried or locNowhere.
	loc string
	// initial is the sentence used to list the item in a room description
	// while it is still where the story left it. Items without one, and
	// items the player has picked up at least once, are listed as
	// "There is a <name> here."
	initial string
	// moved is set the first time the item is picked up, after which it is
	// no longer described as hanging over the mantelpiece it used to hang
	// over.
	moved bool
	// desc is the answer to "examine".
	desc string
	// text is the answer to "read". Items without one cannot be read.
	text string
	// takeable items can be picked up; cantTake explains the ones that
	// cannot in their own words.
	takeable bool
	cantTake string
	// scenery items are part of their room's description and are never
	// listed separately.
	scenery bool
	// containers can hold other items. Closed containers hide their
	// contents unless they are transparent.
	container   bool
	openable    bool
	open        bool
	transparent bool
	// lightSource items can be turned on and off, and lit ones keep grues
	// at a distance.
	lightSource bool
	lit         bool
	// weapons are worth swinging at a villain.
	weapon bool
	// treasures are what the trophy case is for. takeValue is scored the
	// first time the treasure is picked up, caseValue when it is deposited.
	treasure  bool
	takeValue int
	caseValue int
	// villain items block the exits of their room until they are dead.
	villain  bool
	dead     bool
	guardMsg string
	// hits counts the blows the villain has taken so far.
	hits int
	// edible and drinkable items can be consumed, once.
	edible    bool
	drinkable bool
}

// world is the mutable state of one game's map. Every game gets its own copy,
// so nothing here is shared between sessions.
type world struct {
	rooms map[string]*room
	items map[string]*item
	// order is the declaration order of items, so that room listings and
	// inventories come out in a stable order rather than a map's.
	order []string
}

func newWorld() *world {
	rooms := []*room{
		{
			id:   "west-of-house",
			name: "West of House",
			desc: "You are standing in an open field west of a white house with a boarded front door. A path runs around the house to the north and south, and the forest begins to the west.",
			exits: map[direction]exit{
				north: {to: "north-of-house"},
				south: {to: "south-of-house"},
				west:  {to: "forest"},
				east:  {message: "The front door is boarded shut, and the boards are in no mood to be argued with."},
				in:    {message: "The front door is boarded shut, and the boards are in no mood to be argued with."},
			},
		},
		{
			id:   "north-of-house",
			name: "North of House",
			desc: "You are on the north side of the white house. Every window on this side is boarded over. A narrow path winds north into the trees.",
			exits: map[direction]exit{
				north: {to: "forest-path"},
				west:  {to: "west-of-house"},
				east:  {to: "behind-house"},
				south: {message: "The boards over the windows are nailed on with real conviction."},
			},
		},
		{
			id:   "south-of-house",
			name: "South of House",
			desc: "You are on the south side of the white house. There is no door here, and the windows are boarded over.",
			exits: map[direction]exit{
				west:  {to: "west-of-house"},
				east:  {to: "behind-house"},
				south: {to: "forest"},
				north: {message: "The boards over the windows are nailed on with real conviction."},
			},
		},
		{
			id:   "behind-house",
			name: "Behind House",
			desc: "You are behind the white house. A path leads east into the forest. One small window at the corner of the house has been left slightly ajar, as if by an administrator in a hurry.",
			exits: map[direction]exit{
				north: {to: "north-of-house"},
				south: {to: "south-of-house"},
				east:  {to: "clearing"},
				west:  {to: "kitchen", via: "window", viaClosed: "The window is closed."},
				in:    {to: "kitchen", via: "window", viaClosed: "The window is closed."},
			},
		},
		{
			id:   "forest",
			name: "Forest",
			desc: "This is a forest of tall trees, all of them equally uninterested in you. There is light to the east.",
			exits: map[direction]exit{
				east:  {to: "west-of-house"},
				north: {to: "forest-path"},
				west:  {message: "The trees close ranks to the west. You would need a machete, and you do not have one."},
				south: {message: "The trees close ranks to the south. You would need a machete, and you do not have one."},
				up:    {message: "These trees have no branches you could reach."},
			},
		},
		{
			id:   "forest-path",
			name: "Forest Path",
			desc: "A path runs north and south through a dim forest. One enormous tree beside the path has branches low enough to climb.",
			exits: map[direction]exit{
				north: {to: "clearing"},
				south: {to: "north-of-house"},
				west:  {to: "forest"},
				east:  {to: "forest"},
				up:    {to: "up-a-tree"},
			},
		},
		{
			id:   "up-a-tree",
			name: "Up a Tree",
			desc: "You are ten feet off the ground, wedged comfortably among the branches of the great tree. The next branch up is out of reach, and the path is a long way down.",
			exits: map[direction]exit{
				down: {to: "forest-path"},
				up:   {message: "The branches above you are thin and full of opinions. Better not."},
			},
		},
		{
			id:   "clearing",
			name: "Clearing",
			desc: "A quiet clearing, ringed by forest. A path leads south, and the white house is somewhere back to the west.",
			exits: map[direction]exit{
				south: {to: "forest-path"},
				west:  {to: "behind-house"},
				north: {to: "forest"},
				east:  {to: "forest"},
			},
		},
		{
			id:   "kitchen",
			name: "Kitchen",
			desc: "You are in the kitchen of the white house. A table here was used, not long ago, to prepare a meal. A passage leads west, a dark staircase climbs up, and the window to the east stands open.",
			exits: map[direction]exit{
				west: {to: "living-room"},
				up:   {to: "attic"},
				east: {to: "behind-house"},
				out:  {to: "behind-house"},
				down: {message: "Only the very determined go down a chimney, and only in one direction."},
			},
		},
		{
			id:   "attic",
			name: "Attic",
			desc: "This is the attic. There is no light here at all, and the only way out is the staircase going down.",
			dark: true,
			exits: map[direction]exit{
				down: {to: "kitchen"},
			},
		},
		{
			id:   "living-room",
			name: "Living Room",
			desc: "You are in the living room. A doorway leads east to the kitchen, and a wooden door to the west, carved over with strange gothic lettering, has been nailed shut. A large oriental rug lies in the middle of the floor, and a trophy case stands against the wall.",
			exits: map[direction]exit{
				east: {to: "kitchen"},
				west: {message: "The gothic door is nailed shut. The lettering reads \"THIS DOOR IS NOT PART OF THE SUPPORTED API\"."},
				down: {to: "cellar", via: "trap-door", viaClosed: "The trap door is closed."},
			},
		},
		{
			id:    "cellar",
			name:  "Cellar",
			desc:  "You are in a dark and damp cellar. A narrow passage runs north and a low crawlway leads south. To the west is the bottom of a steep metal ramp, far too smooth to climb.",
			dark:  true,
			value: 25,
			exits: map[direction]exit{
				north: {to: "troll-room"},
				south: {to: "east-of-chasm"},
				west:  {message: "The ramp is much too steep and much too smooth."},
				up:    {to: "living-room", via: "trap-door", viaClosed: "The trap door is closed."},
			},
		},
		{
			id:   "troll-room",
			name: "The Troll Room",
			desc: "This is a small chamber with passages leading east and south. The walls are scored with deep gouges, some of them recent, all of them made by something with an axe and a short attention span.",
			dark: true,
			exits: map[direction]exit{
				south: {to: "cellar"},
				east:  {to: "east-west-passage"},
				west:  {message: "That way is a crack in the rock, far too narrow for anything your shape."},
			},
		},
		{
			id:   "east-west-passage",
			name: "East-West Passage",
			desc: "A long, narrow passage running east and west. Water runs somewhere below, patiently, the way water does.",
			dark: true,
			exits: map[direction]exit{
				west: {to: "troll-room"},
				east: {message: "The passage east has collapsed into a heap of rubble."},
				down: {message: "The sound of water is below you, but there is no way down."},
			},
		},
		{
			id:   "east-of-chasm",
			name: "East of Chasm",
			desc: "You are on the east rim of a chasm whose bottom is out of sight and, on reflection, out of scope. A passage leads north and the ledge continues east.",
			dark: true,
			exits: map[direction]exit{
				north: {to: "cellar"},
				east:  {to: "gallery"},
				down:  {message: "The chasm is deep, and the landing at the end of it is not one you would walk away from."},
			},
		},
		{
			id:   "gallery",
			name: "Gallery",
			desc: "This room was once a gallery, and someone has left the lamps burning. Empty hooks line the walls, where a collection used to hang. A passage leads west and a doorway opens north.",
			exits: map[direction]exit{
				west:  {to: "east-of-chasm"},
				north: {to: "studio"},
			},
		},
		{
			id:   "studio",
			name: "Studio",
			desc: "An artist worked here once and left in a hurry. Paint has dried in every jar. A wide chimney rises from the fireplace, blackened but climbable by someone with both hands nearly free.",
			dark: true,
			exits: map[direction]exit{
				south: {to: "gallery"},
				up:    {to: "kitchen"},
			},
		},
	}

	items := []*item{
		{
			id:        "mailbox",
			name:      "small mailbox",
			nouns:     []string{"mailbox", "mail", "box"},
			loc:       "west-of-house",
			initial:   "There is a small mailbox here.",
			desc:      "It is a small mailbox, of the kind that holds exactly one disappointing thing.",
			cantTake:  "The mailbox is securely anchored to its post.",
			container: true,
			openable:  true,
		},
		{
			id:       "leaflet",
			name:     "leaflet",
			nouns:    []string{"leaflet", "pamphlet", "flyer"},
			loc:      "mailbox",
			desc:     "A single sheet of paper, folded once, printed on both sides.",
			takeable: true,
			text:     "\"WELCOME TO ZORK!\n\nZORK is a game of adventure, danger and low cunning, now running inside a\nKubernetes API server, which is a game of adventure, danger and low cunning.\nNo computer should be without one. Beware of grues.\"",
		},
		{
			id:       "window",
			name:     "kitchen window",
			nouns:    []string{"window"},
			loc:      "behind-house",
			desc:     "A small window, ajar, wide enough to climb through once it is properly open.",
			scenery:  true,
			openable: true,
			cantTake: "The window is part of the house, and the house is not going anywhere.",
		},
		{
			id:        "sack",
			name:      "brown sack",
			nouns:     []string{"sack", "bag"},
			loc:       "kitchen",
			initial:   "On the table is a brown paper sack, smelling faintly of hot peppers.",
			desc:      "An ordinary brown paper sack, folded shut at the top.",
			takeable:  true,
			container: true,
			openable:  true,
		},
		{
			id:       "lunch",
			name:     "hot pepper sandwich",
			nouns:    []string{"lunch", "sandwich", "food", "dinner"},
			loc:      "sack",
			desc:     "A sandwich of considerable ambition. The peppers are visible from outside.",
			takeable: true,
			edible:   true,
		},
		{
			id:       "garlic",
			name:     "clove of garlic",
			nouns:    []string{"garlic", "clove"},
			loc:      "sack",
			desc:     "One clove of garlic. Adventurers swear by it. So does everyone standing near an adventurer.",
			takeable: true,
			edible:   true,
		},
		{
			id:          "bottle",
			name:        "glass bottle",
			nouns:       []string{"bottle", "glass"},
			loc:         "kitchen",
			initial:     "A glass bottle is standing on the table.",
			desc:        "A clear glass bottle. There is water in it.",
			takeable:    true,
			container:   true,
			openable:    true,
			transparent: true,
		},
		{
			id:        "water",
			name:      "quantity of water",
			nouns:     []string{"water"},
			loc:       "bottle",
			desc:      "Water. Wet, unremarkable, and entirely uninterested in your quest.",
			cantTake:  "The water runs out of your hands as fast as you can gather it.",
			drinkable: true,
		},
		{
			id:       "rope",
			name:     "coil of rope",
			nouns:    []string{"rope", "coil"},
			loc:      "attic",
			desc:     "A long coil of sturdy rope, tied off at both ends.",
			takeable: true,
		},
		{
			id:       "knife",
			name:     "nasty-looking knife",
			nouns:    []string{"knife"},
			loc:      "attic",
			desc:     "A knife with a blade that has clearly been used for something it should not have been.",
			takeable: true,
			weapon:   true,
		},
		{
			id:          "lamp",
			name:        "brass lantern",
			nouns:       []string{"lamp", "lantern", "light"},
			loc:         "living-room",
			initial:     "A battery-powered brass lantern is sitting on the trophy case.",
			desc:        "A brass lantern with a switch on the side.",
			takeable:    true,
			lightSource: true,
		},
		{
			id:       "sword",
			name:     "elvish sword",
			nouns:    []string{"sword"},
			loc:      "living-room",
			initial:  "An elvish sword of no small antiquity hangs above the mantelpiece.",
			desc:     "An elvish sword, old and beautifully balanced. The blade glows a faint blue when trouble is nearby.",
			takeable: true,
			weapon:   true,
		},
		{
			id:       "rug",
			name:     "oriental rug",
			nouns:    []string{"rug", "carpet"},
			loc:      "living-room",
			desc:     "A large oriental rug, worn in a rectangular pattern near the middle, as if something under it were regularly stepped on.",
			scenery:  true,
			cantTake: "The rug is far too heavy to lift, though you could probably drag it aside.",
		},
		{
			id:        "trophy-case",
			name:      "trophy case",
			nouns:     []string{"case", "trophy"},
			loc:       "living-room",
			desc:      "A handsome glass-fronted trophy case, empty and expectant. A small brass plate reads \"DESIRED STATE\".",
			scenery:   true,
			container: true,
			openable:  true,
			cantTake:  "The trophy case is bolted to the wall.",
		},
		{
			id:       "trap-door",
			name:     "trap door",
			nouns:    []string{"trapdoor", "trap", "door"},
			loc:      locNowhere,
			desc:     "A heavy wooden trap door, set flush into the floor.",
			scenery:  true,
			openable: true,
			cantTake: "The trap door is set into the floor and stays there.",
		},
		{
			id:       "newspaper",
			name:     "issue of the Underground News",
			nouns:    []string{"newspaper", "news", "issue"},
			loc:      "living-room",
			initial:  "A yellowed issue of the Underground News has been left on the floor.",
			desc:     "A newspaper, yellowed with age, folded to the classifieds.",
			takeable: true,
			text:     "\"UNDERGROUND NEWS -- LATE EDITION\n\nCELLAR DEEMED UNSAFE, DECLARED READY ANYWAY. Officials confirm the trap door\nis working exactly as designed and that anyone barring it from above is doing\nso on their own initiative.\n\nCLASSIFIEDS: Lantern, brass, one careful owner. Buyer collects.\"",
		},
		{
			id:        "nest",
			name:      "birds nest",
			nouns:     []string{"nest"},
			loc:       "up-a-tree",
			initial:   "A small birds nest is balanced on the branch beside you.",
			desc:      "A small nest, woven out of twigs and, on closer inspection, one length of somebody's fibre optic cable.",
			takeable:  true,
			container: true,
			open:      true,
		},
		{
			id:        "egg",
			name:      "jewel-encrusted egg",
			nouns:     []string{"egg"},
			loc:       "nest",
			desc:      "An egg the size of your fist, encrusted with jewels and worked in gold. Something rattles softly inside it.",
			takeable:  true,
			treasure:  true,
			takeValue: 5,
			caseValue: 5,
		},
		{
			id:       "troll",
			name:     "troll",
			nouns:    []string{"troll"},
			loc:      "troll-room",
			initial:  "A nasty-looking troll, brandishing a bloody axe, blocks all passages out of the room.",
			desc:     "The troll is large, green and entirely convinced that this room is his. He is holding an axe you would rather he put down.",
			villain:  true,
			guardMsg: "The troll fends you off with a swipe of his axe and a look of great patience.",
			cantTake: "The troll declines to be picked up, at length.",
		},
		{
			id:       "axe",
			name:     "bloody axe",
			nouns:    []string{"axe", "ax"},
			loc:      locNowhere,
			initial:  "The troll's bloody axe lies on the floor where he fell.",
			desc:     "A heavy axe, well used, and not fussy about what it is used on.",
			takeable: true,
			weapon:   true,
		},
		{
			id:        "painting",
			name:      "painting",
			nouns:     []string{"painting", "picture", "art"},
			loc:       "gallery",
			initial:   "A painting of unusual beauty hangs alone on the far wall.",
			desc:      "The painting shows a white house in an open field, seen from the west. The signature is illegible and the frame is worth something on its own.",
			takeable:  true,
			treasure:  true,
			takeValue: 4,
			caseValue: 6,
		},
		{
			id:        "bar",
			name:      "platinum bar",
			nouns:     []string{"bar", "platinum"},
			loc:       "east-west-passage",
			initial:   "A bar of platinum, absurdly heavy for its size, lies against the wall.",
			desc:      "A bar of solid platinum. Somebody has stamped it with a serial number and the word RESERVED.",
			takeable:  true,
			treasure:  true,
			takeValue: 10,
			caseValue: 5,
		},
		{
			id:       "manual",
			name:     "owner's manual",
			nouns:    []string{"manual", "documentation", "docs"},
			loc:      "studio",
			initial:  "A slim owner's manual has been left on the workbench.",
			desc:     "A thin manual, thumbed to softness at the first page and pristine everywhere after it.",
			takeable: true,
			text:     "\"ZORK OWNER'S MANUAL\n\nCongratulations on your purchase. To restore a saved game, do not lose it.\nThe chimney will take you and the lantern and one other thing, and will not\nnegotiate. Rooms below ground are dark, and the dark is not empty.\"",
		},
	}

	w := &world{
		rooms: make(map[string]*room, len(rooms)),
		items: make(map[string]*item, len(items)),
		order: make([]string, 0, len(items)),
	}
	for _, r := range rooms {
		w.rooms[r.id] = r
	}
	for _, i := range items {
		w.items[i.id] = i
		w.order = append(w.order, i.id)
	}
	return w
}

// itemsIn returns the items whose location is loc, in declaration order.
func (w *world) itemsIn(loc string) []*item {
	var found []*item
	for _, id := range w.order {
		if w.items[id].loc == loc {
			found = append(found, w.items[id])
		}
	}
	return found
}

// treasures returns every treasure in the world, in declaration order.
func (w *world) treasures() []*item {
	var found []*item
	for _, id := range w.order {
		if w.items[id].treasure {
			found = append(found, w.items[id])
		}
	}
	return found
}

// maxScore is the score of a perfect game: every room worth points visited,
// every treasure taken and deposited, plus the bonus for finishing.
func (w *world) maxScore() int {
	total := winBonus
	for _, r := range w.rooms {
		total += r.value
	}
	for _, i := range w.treasures() {
		total += i.takeValue + i.caseValue
	}
	return total
}
