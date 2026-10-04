// Tests for the HTTP API, run against a real Queue and a temporary SQLite file.

package api

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	jobq "github.com/Mikkail04/Job-Queue"
)

const testKey = "test-key"

type env struct {
	t  *testing.T
	ts *httptest.Server
}

// newEnv starts a queue with two handlers ("ok" always succeeds, "bad" always
// fails) and an HTTP server in front of it. Jobs get one attempt, so "bad" jobs
// go straight to dead.
func newEnv(t *testing.T) *env {
	t.Helper()
	store, err := jobq.OpenSQLStore(filepath.Join(t.TempDir(), "jobs.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })

	q := jobq.NewWithStore(store, 1, time.Millisecond)
	q.Handle("ok", func(context.Context, []byte) error { return nil })
	q.Handle("bad", func(context.Context, []byte) error { return errors.New("boom") })
	srv := New(q, store, Config{APIKey: testKey})

	ctx, cancel := context.WithCancel(context.Background())
	q.Start(ctx, 2)
	t.Cleanup(func() { cancel(); q.Wait() })

	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)
	t.Cleanup(srv.Stop) // cleanups run last-in first-out, so open streams end before ts.Close waits on them
	return &env{t: t, ts: ts}
}

// call sends a request with the API key and returns the status code and body.
func (e *env) call(method, path, body string) (int, []byte) {
	e.t.Helper()
	req, err := http.NewRequest(method, e.ts.URL+path, strings.NewReader(body))
	if err != nil {
		e.t.Fatal(err)
	}
	req.Header.Set("X-API-Key", testKey)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		e.t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, b
}

type jobResp struct {
	ID        int             `json:"id"`
	Type      string          `json:"type"`
	Payload   json.RawMessage `json:"payload"`
	Status    string          `json:"status"`
	Attempts  int             `json:"attempts"`
	LastError *string         `json:"lastError"`
}

func (e *env) list(query string) []jobResp {
	e.t.Helper()
	code, b := e.call("GET", "/jobs"+query, "")
	if code != http.StatusOK {
		e.t.Fatalf("GET /jobs%s = %d: %s", query, code, b)
	}
	var r struct {
		Jobs []jobResp `json:"jobs"`
	}
	if err := json.Unmarshal(b, &r); err != nil {
		e.t.Fatal(err)
	}
	return r.Jobs
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func TestAuthAndCORS(t *testing.T) {
	e := newEnv(t)

	for name, key := range map[string]string{"no key": "", "wrong key": "nope"} {
		req, _ := http.NewRequest("GET", e.ts.URL+"/stats", nil)
		if key != "" {
			req.Header.Set("X-API-Key", key)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusUnauthorized {
			t.Fatalf("%s: got %d, want 401", name, resp.StatusCode)
		}
	}

	// The health check needs no key, so hosting platforms can poll it.
	resp, err := http.Get(e.ts.URL + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("/healthz = %d, want 200", resp.StatusCode)
	}

	// A browser preflight can't carry the key, so it must succeed without one.
	req, _ := http.NewRequest("OPTIONS", e.ts.URL+"/jobs", nil)
	req.Header.Set("Origin", "http://localhost:5173")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent ||
		resp.Header.Get("Access-Control-Allow-Origin") != "*" ||
		!strings.Contains(resp.Header.Get("Access-Control-Allow-Headers"), "X-API-Key") {
		t.Fatalf("preflight = %d with headers %v", resp.StatusCode, resp.Header)
	}
}

func TestEnqueueListAndStats(t *testing.T) {
	e := newEnv(t)

	code, b := e.call("POST", "/jobs", `{"type":"ok","payload":{"a":1}}`)
	if code != http.StatusCreated {
		t.Fatalf("POST /jobs = %d: %s", code, b)
	}
	var job jobResp
	if err := json.Unmarshal(b, &job); err != nil {
		t.Fatal(err)
	}
	if job.ID == 0 || job.Type != "ok" || string(job.Payload) != `{"a":1}` {
		t.Fatalf("created job = %+v", job)
	}

	waitFor(t, "the job to succeed", func() bool { return len(e.list("?status=succeeded")) == 1 })

	_, b = e.call("GET", "/stats", "")
	var stats map[string]int
	if err := json.Unmarshal(b, &stats); err != nil {
		t.Fatal(err)
	}
	if stats["succeeded"] != 1 || stats["dead"] != 0 || len(stats) != 4 {
		t.Fatalf("stats = %v", stats)
	}
}

func TestRetryDeadJob(t *testing.T) {
	e := newEnv(t)

	_, b := e.call("POST", "/jobs", `{"type":"bad"}`)
	var bad jobResp
	if err := json.Unmarshal(b, &bad); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "the job to die", func() bool { return len(e.list("?status=dead")) == 1 })

	dead := e.list("?status=dead")[0]
	if dead.LastError == nil || *dead.LastError != "boom" || dead.Attempts != 1 {
		t.Fatalf("dead job = %+v", dead)
	}

	code, b := e.call("POST", "/jobs/"+itoa(bad.ID)+"/retry", "")
	if code != http.StatusOK {
		t.Fatalf("retry = %d: %s", code, b)
	}
	var retried jobResp
	if err := json.Unmarshal(b, &retried); err != nil {
		t.Fatal(err)
	}
	if retried.Status != "pending" || retried.Attempts != 0 || retried.LastError != nil {
		t.Fatalf("retried job = %+v", retried)
	}

	// A job that already succeeded can't be retried.
	_, b = e.call("POST", "/jobs", `{"type":"ok"}`)
	var good jobResp
	if err := json.Unmarshal(b, &good); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "the ok job to succeed", func() bool { return len(e.list("?status=succeeded")) == 1 })
	if code, _ := e.call("POST", "/jobs/"+itoa(good.ID)+"/retry", ""); code != http.StatusConflict {
		t.Fatalf("retry of a succeeded job = %d, want 409", code)
	}
}

func itoa(n int) string { return strconv.Itoa(n) }

func TestBadRequests(t *testing.T) {
	e := newEnv(t)
	tests := []struct {
		name, method, path, body string
		want                     int
	}{
		{"not JSON", "POST", "/jobs", "not json", 400},
		{"missing type", "POST", "/jobs", `{}`, 400},
		{"unknown status", "GET", "/jobs?status=nope", "", 400},
		{"bad limit", "GET", "/jobs?limit=abc", "", 400},
		{"bad job id", "POST", "/jobs/abc/retry", "", 400},
		{"unknown job", "POST", "/jobs/9999/retry", "", 404},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if code, b := e.call(tc.method, tc.path, tc.body); code != tc.want {
				t.Fatalf("got %d, want %d: %s", code, tc.want, b)
			}
		})
	}
}

func TestEventsStream(t *testing.T) {
	e := newEnv(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, "GET", e.ts.URL+"/events", nil)
	req.Header.Set("X-API-Key", testKey)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if ct := resp.Header.Get("Content-Type"); ct != "text/event-stream" {
		t.Fatalf("Content-Type = %q", ct)
	}

	lines := make(chan string, 100)
	go func() {
		sc := bufio.NewScanner(resp.Body)
		for sc.Scan() {
			lines <- sc.Text()
		}
		close(lines)
	}()

	// The server subscribes before it sends headers, so this job's events can't be missed.
	e.call("POST", "/jobs", `{"type":"ok"}`)

	start := time.Now()
	seen := map[string]bool{}
	timeout := time.After(3 * time.Second)
	for !(seen["enqueued"] && seen["started"] && seen["succeeded"]) {
		select {
		case line, ok := <-lines:
			if !ok {
				t.Fatalf("stream closed early; saw %v", seen)
			}
			t.Logf("%v stream line: %q", time.Since(start).Round(time.Millisecond), line)
			if data, found := strings.CutPrefix(line, "data: "); found {
				var ev struct {
					Type string `json:"type"`
				}
				if err := json.Unmarshal([]byte(data), &ev); err != nil {
					t.Fatal(err)
				}
				seen[ev.Type] = true
			}
		case <-timeout:
			code, b := e.call("GET", "/jobs", "")
			t.Fatalf("timed out; saw only %v\nGET /jobs = %d: %s", seen, code, b)
		}
	}
}
