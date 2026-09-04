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
	"unicode"
)

// splitCommands breaks one line of input into the commands it contains.
// Periods and semicolons separate commands, as does the word "then".
func splitCommands(input string) []string {
	var cmds []string
	for _, part := range strings.FieldsFunc(input, func(r rune) bool {
		return r == '.' || r == ';' || r == '\n' || r == '\r'
	}) {
		for _, cmd := range strings.Split(part, " then ") {
			if cmd = strings.TrimSpace(cmd); cmd != "" {
				cmds = append(cmds, cmd)
			}
		}
	}
	return cmds
}

// noiseWords are dropped before a command is interpreted, so that "take the
// lamp" and "take lamp" are the same command.
var noiseWords = map[string]bool{
	"the": true, "a": true, "an": true, "my": true, "some": true,
	"please": true, "of": true, "to": true, "and": true,
}

// tokenize lowercases a command, throws away punctuation and noise words, and
// returns what is left.
func tokenize(input string) []string {
	cleaned := strings.Map(func(r rune) rune {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			return unicode.ToLower(r)
		}
		return ' '
	}, input)
	var words []string
	for _, w := range strings.Fields(cleaned) {
		if noiseWords[w] {
			continue
		}
		words = append(words, w)
	}
	return words
}

// directionWords maps every word the parser accepts to a direction.
var directionWords = map[string]direction{
	"north": north, "n": north,
	"south": south, "s": south,
	"east": east, "e": east,
	"west": west, "w": west,
	"northeast": northeast, "ne": northeast,
	"northwest": northwest, "nw": northwest,
	"southeast": southeast, "se": southeast,
	"southwest": southwest, "sw": southwest,
	"up": up, "u": up, "upward": up, "upstairs": up,
	"down": down, "d": down, "downward": down, "downstairs": down,
	"in": in, "inside": in, "enter": in,
	"out": out, "outside": out, "exit": out, "leave": out,
}

func asDirection(word string) (direction, bool) {
	d, ok := directionWords[word]
	return d, ok
}

// splitAt divides a phrase around the first of the given prepositions.
func splitAt(words []string, preps ...string) (before, after []string, found bool) {
	for i, w := range words {
		for _, p := range preps {
			if w == p {
				return words[:i], words[i+1:], true
			}
		}
	}
	return words, nil, false
}

// resolve finds the item a phrase refers to. Only things the player could
// reasonably be holding or seeing are considered: what is carried, what is in
// this room, and the contents of any container that is open (or transparent
// enough not to need opening). When nothing matches, the second return value
// is the refusal to show the player.
func (g *Game) resolve(words []string) (*item, string) {
	if len(words) == 0 {
		return nil, "I do not know what you are referring to."
	}
	dark := g.room().dark && !g.hasLight()

	var scope []*item
	add := func(candidates []*item, requireVisible bool) {
		for _, it := range candidates {
			if requireVisible && dark {
				continue
			}
			scope = append(scope, it)
			if it.container && (it.open || it.transparent) {
				for _, inner := range g.w.itemsIn(it.id) {
					scope = append(scope, inner)
				}
			}
		}
	}
	// What you are holding can be found by touch, even in the dark.
	add(g.carried(), false)
	add(g.w.itemsIn(g.here), true)

	for _, it := range scope {
		if it.matches(words) {
			return it, ""
		}
	}
	if dark {
		return nil, "It is too dark to see anything."
	}
	return nil, fmt.Sprintf("You cannot see any %s here.", words[len(words)-1])
}

// matches reports whether any word of the phrase names this item, either as one
// of its nouns or as a word of its name.
func (i *item) matches(words []string) bool {
	for _, w := range words {
		for _, n := range i.nouns {
			if n == w {
				return true
			}
		}
		for _, n := range strings.Fields(i.name) {
			if strings.Trim(n, "'s") == w {
				return true
			}
		}
	}
	return false
}

// listing is the sentence that puts this item in a room description.
func (i *item) listing() string {
	if i.initial != "" && !i.moved {
		return i.initial
	}
	return fmt.Sprintf("There is %s here.", withArticle(i.name))
}

// contentsListing describes what is inside an open container, indented under
// the line that introduced it. Closed opaque containers keep their secrets.
func contentsListing(w *world, it *item, indent string) string {
	if !it.container || !(it.open || it.transparent) {
		return ""
	}
	inside := w.itemsIn(it.id)
	if len(inside) == 0 {
		return ""
	}
	var b strings.Builder
	b.WriteString(fmt.Sprintf("\n%sThe %s contains:", indent, it.name))
	for _, c := range inside {
		b.WriteString(fmt.Sprintf("\n%s  %s", indent, capitalize(withArticle(c.name))))
	}
	return b.String()
}

// withArticle puts "a" or "an" in front of a name, unless the name already
// begins with a word that does the job.
func withArticle(name string) string {
	switch first := strings.ToLower(strings.Fields(name)[0]); first {
	case "a", "an", "the", "some":
		return name
	}
	if strings.ContainsRune("aeiou", rune(strings.ToLower(name)[0])) {
		return "an " + name
	}
	return "a " + name
}

func capitalize(s string) string {
	if s == "" {
		return s
	}
	return strings.ToUpper(s[:1]) + s[1:]
}

// joinList renders a list of names as "x", "x and y" or "x, y and z".
func joinList(names []string) string {
	switch len(names) {
	case 0:
		return "nothing"
	case 1:
		return names[0]
	case 2:
		return names[0] + " and " + names[1]
	default:
		return strings.Join(names[:len(names)-1], ", ") + " and " + names[len(names)-1]
	}
}

func plural(n int, noun string) string {
	if n == 1 {
		return fmt.Sprintf("%d %s", n, noun)
	}
	return fmt.Sprintf("%d %ss", n, noun)
}
