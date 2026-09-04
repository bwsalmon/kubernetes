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
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"k8s.io/apiserver/pkg/authentication/user"
	"k8s.io/apiserver/pkg/endpoints/request"
)

// ask sends one request to the handler and returns the recorder.
func ask(h http.Handler, req *http.Request) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

// get plays one turn of the session, or starts a game if session is empty.
func get(h http.Handler, session, cmd string) *httptest.ResponseRecorder {
	url := DefaultZorkPath
	query := make([]string, 0, 2)
	if session != "" {
		query = append(query, "session="+session)
	}
	if cmd != "" {
		query = append(query, "cmd="+strings.ReplaceAll(cmd, " ", "+"))
	}
	if len(query) > 0 {
		url += "?" + strings.Join(query, "&")
	}
	return ask(h, httptest.NewRequest(http.MethodGet, url, nil))
}

func sessionOf(t *testing.T, rec *httptest.ResponseRecorder) string {
	t.Helper()
	id := rec.Header().Get("X-Zork-Session")
	if id == "" {
		t.Fatalf("response carries no session id:\n%s", rec.Body.String())
	}
	return id
}

func TestStartingAGame(t *testing.T) {
	h := NewHandler()
	rec := get(h, "", "")

	if rec.Code != http.StatusOK {
		t.Fatalf("starting a game returned %d, want %d", rec.Code, http.StatusOK)
	}
	if got := rec.Header().Get("Content-Type"); got != "text/plain; charset=utf-8" {
		t.Errorf("Content-Type is %q, want text/plain", got)
	}
	body := rec.Body.String()
	for _, want := range []string{
		"ZORK: The Great Underground API Server",
		"West of House",
		"There is a small mailbox here.",
		"score 0 of 70",
		"kubectl get --raw",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("the opening response does not contain %q:\n%s", want, body)
		}
	}
	if id := sessionOf(t, rec); !strings.Contains(body, id) {
		t.Errorf("the opening response does not mention session %q so the player cannot continue:\n%s", id, body)
	}
}

func TestPlayingAcrossRequests(t *testing.T) {
	h := NewHandler()
	id := sessionOf(t, get(h, "", ""))

	rec := get(h, id, "open mailbox")
	if !strings.Contains(rec.Body.String(), "reveals a leaflet") {
		t.Fatalf("the first command did not take effect:\n%s", rec.Body.String())
	}
	if got := sessionOf(t, rec); got != id {
		t.Errorf("session id changed from %q to %q mid-game", id, got)
	}
	rec = get(h, id, "take leaflet. read leaflet")
	body := rec.Body.String()
	for _, want := range []string{"> take leaflet. read leaflet", "Taken.", "WELCOME TO ZORK!", "3 moves"} {
		if !strings.Contains(body, want) {
			t.Errorf("the second request does not contain %q:\n%s", want, body)
		}
	}
	// A request with no command is a free look around: same room, same moves.
	body = get(h, id, "").Body.String()
	if !strings.Contains(body, "West of House") || !strings.Contains(body, "3 moves") {
		t.Errorf("a commandless request should look around without spending a move:\n%s", body)
	}
}

func TestUnknownSession(t *testing.T) {
	h := NewHandler()
	rec := get(h, "0123456789ab", "look")
	if rec.Code != http.StatusNotFound {
		t.Fatalf("an unknown session returned %d, want %d", rec.Code, http.StatusNotFound)
	}
	if body := rec.Body.String(); !strings.Contains(body, "no game") {
		t.Errorf("an unknown session should say so plainly, got %q", body)
	}
}

func TestCommandInRequestBody(t *testing.T) {
	h := NewHandler()
	id := sessionOf(t, get(h, "", ""))

	req := httptest.NewRequest(http.MethodPost, DefaultZorkPath+"?session="+id, strings.NewReader("open mailbox\n"))
	if body := ask(h, req).Body.String(); !strings.Contains(body, "reveals a leaflet") {
		t.Errorf("a command posted in the body was not played:\n%s", body)
	}

	req = httptest.NewRequest(http.MethodPut, DefaultZorkPath+"?session="+id, strings.NewReader("take leaflet"))
	if body := ask(h, req).Body.String(); !strings.Contains(body, "Taken.") {
		t.Errorf("a command sent by PUT was not played:\n%s", body)
	}

	// The query string wins, so that a body left over from a proxy cannot
	// take a turn the player did not ask for.
	req = httptest.NewRequest(http.MethodPost, DefaultZorkPath+"?session="+id+"&cmd=read+leaflet", strings.NewReader("drop leaflet"))
	body := ask(h, req).Body.String()
	if !strings.Contains(body, "WELCOME TO ZORK!") || strings.Contains(body, "Dropped.") {
		t.Errorf("the query string should win over the body:\n%s", body)
	}
}

func TestCommandTooLong(t *testing.T) {
	h := NewHandler()
	id := sessionOf(t, get(h, "", ""))
	req := httptest.NewRequest(http.MethodPost, DefaultZorkPath+"?session="+id, strings.NewReader(strings.Repeat("look. ", 200)))
	rec := ask(h, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("an over-long command returned %d, want %d", rec.Code, http.StatusBadRequest)
	}
	if body := rec.Body.String(); !strings.Contains(body, "512") {
		t.Errorf("the refusal should say how much the game can hear, got %q", body)
	}
}

func TestMethodNotAllowed(t *testing.T) {
	h := NewHandler()
	rec := ask(h, httptest.NewRequest(http.MethodDelete, DefaultZorkPath, nil))
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("DELETE returned %d, want %d", rec.Code, http.StatusMethodNotAllowed)
	}
	if got := rec.Header().Get("Allow"); got != "GET, POST, PUT" {
		t.Errorf("Allow header is %q, want %q", got, "GET, POST, PUT")
	}
}

func TestJSONResponse(t *testing.T) {
	h := NewHandler()
	id := sessionOf(t, get(h, "", ""))
	get(h, id, "open mailbox")

	for name, req := range map[string]*http.Request{
		"format parameter": httptest.NewRequest(http.MethodGet, DefaultZorkPath+"?session="+id+"&cmd=take+leaflet&format=json", nil),
		"accept header":    httptest.NewRequest(http.MethodGet, DefaultZorkPath+"?session="+id+"&cmd=read+leaflet", nil),
	} {
		if name == "accept header" {
			req.Header.Set("Accept", "application/json, */*")
		}
		t.Run(name, func(t *testing.T) {
			rec := ask(h, req)
			if got := rec.Header().Get("Content-Type"); got != "application/json" {
				t.Errorf("Content-Type is %q, want application/json", got)
			}
			var resp Response
			if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
				t.Fatalf("response is not JSON: %v\n%s", err, rec.Body.String())
			}
			if resp.Session != id {
				t.Errorf("session is %q, want %q", resp.Session, id)
			}
			if resp.MaxScore != 70 || resp.GameOver {
				t.Errorf("unexpected maxScore/gameOver: %+v", resp)
			}
			if resp.Output == "" || resp.Command == "" {
				t.Errorf("output and command should both be filled in: %+v", resp)
			}
		})
	}
}

// TestSessionPerUser checks that an authenticated player can leave the session
// id out entirely: their game is found again by who they are.
func TestSessionPerUser(t *testing.T) {
	h := NewHandler()

	asUser := func(name, cmd string) *httptest.ResponseRecorder {
		url := DefaultZorkPath
		if cmd != "" {
			url += "?cmd=" + strings.ReplaceAll(cmd, " ", "+")
		}
		req := httptest.NewRequest(http.MethodGet, url, nil)
		ctx := request.WithUser(req.Context(), &user.DefaultInfo{Name: name})
		return ask(h, req.WithContext(ctx))
	}

	alice := sessionOf(t, asUser("alice", ""))
	if body := asUser("alice", "open mailbox").Body.String(); !strings.Contains(body, "reveals a leaflet") {
		t.Fatalf("alice's second request did not reach her game:\n%s", body)
	}
	if got := sessionOf(t, asUser("alice", "look")); got != alice {
		t.Errorf("alice was given session %q, want her first one %q", got, alice)
	}

	bob := sessionOf(t, asUser("bob", ""))
	if bob == alice {
		t.Fatal("two players should not share one game")
	}
	if body := asUser("bob", "look").Body.String(); !strings.Contains(body, "There is a small mailbox here.") {
		t.Errorf("bob's mailbox should still be shut; he is not playing alice's game:\n%s", body)
	}
}

func TestIdleGamesAreForgotten(t *testing.T) {
	now := time.Now()
	h := NewHandler(WithIdleTimeout(time.Hour), withClock(func() time.Time { return now }))
	id := sessionOf(t, get(h, "", ""))

	now = now.Add(59 * time.Minute)
	if rec := get(h, id, "look"); rec.Code != http.StatusOK {
		t.Fatalf("a game idle for 59 minutes returned %d, want it to still be there", rec.Code)
	}
	now = now.Add(61 * time.Minute)
	if rec := get(h, id, "look"); rec.Code != http.StatusNotFound {
		t.Fatalf("a game idle for over an hour returned %d, want %d", rec.Code, http.StatusNotFound)
	}
}

func TestOldestGameIsRetiredWhenFull(t *testing.T) {
	now := time.Now()
	tick := func() time.Time { now = now.Add(time.Second); return now }
	h := NewHandler(WithMaxSessions(2), withClock(tick))

	first := sessionOf(t, get(h, "", ""))
	second := sessionOf(t, get(h, "", ""))
	// Touching the first one makes the second the least recently played.
	if rec := get(h, first, "look"); rec.Code != http.StatusOK {
		t.Fatalf("the first game should still be playable, got %d", rec.Code)
	}
	third := sessionOf(t, get(h, "", ""))

	if rec := get(h, second, "look"); rec.Code != http.StatusNotFound {
		t.Errorf("the least recently played game should have been retired, got %d", rec.Code)
	}
	for _, id := range []string{first, third} {
		if rec := get(h, id, "look"); rec.Code != http.StatusOK {
			t.Errorf("game %q should still be playable, got %d", id, rec.Code)
		}
	}
}

// TestConcurrentPlay is the race detector's test: one game, many requests.
func TestConcurrentPlay(t *testing.T) {
	h := NewHandler()
	id := sessionOf(t, get(h, "", ""))

	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			cmds := []string{"look", "open mailbox", "take leaflet", "inventory", "score", "north"}
			if rec := get(h, id, cmds[i%len(cmds)]); rec.Code != http.StatusOK {
				t.Errorf("concurrent request returned %d", rec.Code)
			}
			// New games are being started alongside the shared one.
			if rec := get(h, "", "look"); rec.Code != http.StatusOK {
				t.Errorf("concurrent new game returned %d", rec.Code)
			}
		}(i)
	}
	wg.Wait()
}

// recordingMux stands in for the API server's PathRecorderMux.
type recordingMux struct {
	unlisted map[string]http.Handler
}

func (m *recordingMux) UnlistedHandle(path string, handler http.Handler) {
	if m.unlisted == nil {
		m.unlisted = map[string]http.Handler{}
	}
	m.unlisted[path] = handler
}

func TestInstallRegistersAnUnlistedPath(t *testing.T) {
	m := &recordingMux{}
	Install(m)
	h, ok := m.unlisted[DefaultZorkPath]
	if !ok {
		t.Fatalf("Install did not register %q, only %v", DefaultZorkPath, m.unlisted)
	}
	if body := ask(h, httptest.NewRequest(http.MethodGet, DefaultZorkPath, nil)).Body.String(); !strings.Contains(body, "West of House") {
		t.Errorf("the installed handler does not serve the game:\n%s", body)
	}
}

func TestGameOverIsReported(t *testing.T) {
	h := NewHandler()
	id := sessionOf(t, get(h, "", ""))
	// The shortest game there is: into the dark, and then one step too far.
	for _, cmd := range []string{"north", "east", "open window", "west", "up", "down"} {
		get(h, id, cmd)
	}
	req := httptest.NewRequest(http.MethodGet, fmt.Sprintf("%s?session=%s&cmd=look&format=json", DefaultZorkPath, id), nil)
	var resp Response
	if err := json.Unmarshal(ask(h, req).Body.Bytes(), &resp); err != nil {
		t.Fatalf("response is not JSON: %v", err)
	}
	if !resp.GameOver {
		t.Errorf("the player has been eaten and the response should say so: %+v", resp)
	}
}
