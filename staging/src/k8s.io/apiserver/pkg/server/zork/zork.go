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

// Package zork serves a text adventure from an API server.
//
// The endpoint is one command per request, which is the only interaction model
// available to a client like "kubectl get --raw":
//
//	kubectl get --raw '/zork'
//	kubectl get --raw '/zork?session=6f1c2a7d9b40&cmd=open+mailbox'
//	curl -X POST "$APISERVER/zork?session=6f1c2a7d9b40" --data 'take leaflet'
//
// Games are held in memory, one per session, and a session is forgotten once it
// has been idle for a while or once the server is restarted -- which is to say
// the game has no save file, and neither does it need one. Because the games
// live in one process, a player whose requests are spread over several API
// servers will find a different game behind each of them.
//
// The endpoint is registered unlisted, so it does not appear at "/" or in the
// paths reported by /statusz: no client discovering the API surface has to
// learn about it. It is served through the ordinary handler chain, so reaching
// it still requires an authenticated user authorized for the non-resource path
// "/zork".
package zork

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/klog/v2"
)

// DefaultZorkPath is where the game is served.
const DefaultZorkPath = "/zork"

const (
	// maxCommandBytes is the longest command accepted in one request.
	maxCommandBytes = 512
	// maxSessionIDBytes is how much of a session id is worth reading, and
	// worth quoting back when there is no such game.
	maxSessionIDBytes = 64
	// defaultMaxSessions bounds how many games the server holds at once.
	// Reaching the limit retires the game nobody has touched for longest.
	defaultMaxSessions = 64
	// defaultIdleTimeout is how long an untouched game is kept.
	defaultIdleTimeout = 30 * time.Minute
)

// mux is the part of a PathRecorderMux this package needs.
type mux interface {
	UnlistedHandle(path string, handler http.Handler)
}

// Install registers the zork endpoint on the given mux.
func Install(m mux, opts ...Option) {
	m.UnlistedHandle(DefaultZorkPath, NewHandler(opts...))
}

// Option configures the handler.
type Option func(*handler)

// WithMaxSessions sets how many games may be in progress at once.
func WithMaxSessions(n int) Option {
	return func(h *handler) { h.maxSessions = n }
}

// WithIdleTimeout sets how long a game survives without being played.
func WithIdleTimeout(d time.Duration) Option {
	return func(h *handler) { h.idleTimeout = d }
}

// withClock replaces the handler's idea of time, for tests.
func withClock(now func() time.Time) Option {
	return func(h *handler) { h.now = now }
}

// Response is the JSON form of a turn, for clients that would rather parse than
// read. Ask for it with "Accept: application/json" or "?format=json".
type Response struct {
	Session  string `json:"session"`
	Command  string `json:"command,omitempty"`
	Output   string `json:"output"`
	Score    int    `json:"score"`
	MaxScore int    `json:"maxScore"`
	Moves    int    `json:"moves"`
	GameOver bool   `json:"gameOver"`
}

// session is one game in progress, plus who it belongs to.
type session struct {
	id string
	// owner is the name of the authenticated user whose default game this
	// is, or "" for a game only reachable by its session id.
	owner string
	// lock serializes the commands of this one game. Game is not safe for
	// concurrent use, and a player who fires two requests at once should
	// get two turns rather than a data race.
	lock sync.Mutex
	game *Game

	// lastUsed is guarded by the handler's lock, not by the session's.
	lastUsed time.Time
}

type handler struct {
	lock sync.Mutex
	// byID holds every live game, keyed by its session id.
	byID map[string]*session
	// byOwner maps an authenticated user name to their default game's id,
	// so that "kubectl get --raw /zork" resumes where they left off.
	byOwner map[string]string

	maxSessions int
	idleTimeout time.Duration
	now         func() time.Time
	// newID mints session ids. It is a field so tests can make them
	// predictable.
	newID func() string
}

// NewHandler returns the handler serving the game, for callers that want to
// mount it somewhere other than the API server's own mux.
func NewHandler(opts ...Option) http.Handler {
	h := &handler{
		byID:        map[string]*session{},
		byOwner:     map[string]string{},
		maxSessions: defaultMaxSessions,
		idleTimeout: defaultIdleTimeout,
		now:         time.Now,
		newID:       randomID,
	}
	for _, opt := range opts {
		opt(h)
	}
	if h.maxSessions < 1 {
		h.maxSessions = 1
	}
	return h
}

func randomID() string {
	var b [6]byte
	if _, err := rand.Read(b[:]); err != nil {
		// The game is not a security boundary; a readable clock is a
		// good enough source of a distinct id if the random one fails.
		return fmt.Sprintf("%012x", time.Now().UnixNano())
	}
	return hex.EncodeToString(b[:])
}

func (h *handler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	switch req.Method {
	case http.MethodGet, http.MethodPost, http.MethodPut:
	default:
		w.Header().Set("Allow", "GET, POST, PUT")
		http.Error(w, "The dungeon accepts GET, POST and PUT.", http.StatusMethodNotAllowed)
		return
	}

	cmd, err := commandFrom(req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	s, fresh, err := h.session(req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	s.lock.Lock()
	defer s.lock.Unlock()

	var parts []string
	if fresh {
		parts = append(parts, s.game.Banner(), s.game.Describe())
	}
	switch {
	case cmd != "":
		parts = append(parts, "> "+cmd+"\n"+s.game.Execute(cmd))
	case !fresh:
		// A request without a command is a free look around.
		parts = append(parts, s.game.Describe())
	}
	output := strings.Join(parts, "\n\n")

	if wantsJSON(req) {
		writeJSON(w, Response{
			Session:  s.id,
			Command:  cmd,
			Output:   output,
			Score:    s.game.Score(),
			MaxScore: s.game.MaxScore(),
			Moves:    s.game.Moves(),
			GameOver: s.game.IsOver(),
		})
		return
	}
	writeText(w, s, output, fresh)
}

// session finds the game this request is for, starting one if the player has
// none yet. An explicit session id that the server has never minted, or has
// already forgotten, is an error: session ids are handed out, not chosen.
func (h *handler) session(req *http.Request) (s *session, fresh bool, err error) {
	id := req.URL.Query().Get("session")
	if len(id) > maxSessionIDBytes {
		// Session ids are handed out by this server and are short. Do not
		// echo an arbitrarily long one back in the answer.
		id = id[:maxSessionIDBytes] + "..."
	}

	h.lock.Lock()
	defer h.lock.Unlock()
	h.expireLocked()

	if id != "" {
		s, ok := h.byID[id]
		if !ok {
			return nil, false, fmt.Errorf("there is no game %q here. It may have been idle too long, or the server may have restarted since. Request %s without a session to begin again", id, DefaultZorkPath)
		}
		s.lastUsed = h.now()
		return s, false, nil
	}

	owner := userName(req)
	if owner != "" {
		if id, ok := h.byOwner[owner]; ok {
			s := h.byID[id]
			s.lastUsed = h.now()
			return s, false, nil
		}
	}

	s = &session{id: h.newID(), owner: owner, game: New(), lastUsed: h.now()}
	h.evictLocked(h.maxSessions - 1)
	h.byID[s.id] = s
	if owner != "" {
		h.byOwner[owner] = s.id
	}
	klog.V(4).InfoS("Started a new game of zork", "session", s.id, "user", owner)
	return s, true, nil
}

// expireLocked forgets games nobody has played for idleTimeout.
func (h *handler) expireLocked() {
	if h.idleTimeout <= 0 {
		return
	}
	cutoff := h.now().Add(-h.idleTimeout)
	for id, s := range h.byID {
		if s.lastUsed.Before(cutoff) {
			h.forgetLocked(id, s)
		}
	}
}

// evictLocked retires least recently played games until at most keep remain.
func (h *handler) evictLocked(keep int) {
	if keep < 0 {
		keep = 0
	}
	for len(h.byID) > keep {
		var oldestID string
		var oldest *session
		for id, s := range h.byID {
			if oldest == nil || s.lastUsed.Before(oldest.lastUsed) {
				oldestID, oldest = id, s
			}
		}
		if oldest == nil {
			return
		}
		klog.V(4).InfoS("Retiring an idle game of zork to make room", "session", oldestID)
		h.forgetLocked(oldestID, oldest)
	}
}

func (h *handler) forgetLocked(id string, s *session) {
	delete(h.byID, id)
	if s.owner != "" && h.byOwner[s.owner] == id {
		delete(h.byOwner, s.owner)
	}
}

// userName returns the authenticated user this request belongs to, if the
// handler chain has identified one.
func userName(req *http.Request) string {
	u, ok := request.UserFrom(req.Context())
	if !ok || u == nil {
		return ""
	}
	return u.GetName()
}

// commandFrom pulls the command out of the query string, or out of the body of
// a POST or PUT.
func commandFrom(req *http.Request) (string, error) {
	query := req.URL.Query()
	cmd := query.Get("cmd")
	if cmd == "" {
		cmd = query.Get("command")
	}
	if cmd == "" && req.Body != nil && (req.Method == http.MethodPost || req.Method == http.MethodPut) {
		body, err := io.ReadAll(io.LimitReader(req.Body, maxCommandBytes+1))
		if err != nil {
			return "", fmt.Errorf("could not read the command: %v", err)
		}
		cmd = string(body)
	}
	cmd = strings.TrimSpace(cmd)
	if len(cmd) > maxCommandBytes {
		return "", fmt.Errorf("that command is %d characters long; %d is the most this game can hear at once", len(cmd), maxCommandBytes)
	}
	return cmd, nil
}

func wantsJSON(req *http.Request) bool {
	if format := req.URL.Query().Get("format"); format != "" {
		return strings.EqualFold(format, "json")
	}
	for _, accept := range strings.Split(req.Header.Get("Accept"), ",") {
		if strings.EqualFold(strings.TrimSpace(strings.SplitN(accept, ";", 2)[0]), "application/json") {
			return true
		}
	}
	return false
}

func writeJSON(w http.ResponseWriter, resp Response) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		klog.ErrorS(err, "Failed to write a zork response")
	}
}

func writeText(w http.ResponseWriter, s *session, output string, fresh bool) {
	var b strings.Builder
	b.WriteString(output)
	b.WriteString("\n\n--\n")
	b.WriteString(fmt.Sprintf("session %s | score %d of %d | %s\n", s.id, s.game.Score(), s.game.MaxScore(), plural(s.game.Moves(), "move")))
	if fresh {
		b.WriteString(fmt.Sprintf("Play on with:\n  kubectl get --raw '%s?session=%s&cmd=open+mailbox'\n", DefaultZorkPath, s.id))
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("X-Zork-Session", s.id)
	w.WriteHeader(http.StatusOK)
	if _, err := io.WriteString(w, b.String()); err != nil {
		klog.ErrorS(err, "Failed to write a zork response")
	}
}
