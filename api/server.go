// Package api exposes a Queue over HTTP: enqueue, list, stats, requeue, and a
// live event stream (Server-Sent Events).
package api

import (
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"sync"
	"time"

	jobq "github.com/Mikkail04/Job-Queue"
)

// Config holds the server settings.
type Config struct {
	APIKey        string // required; clients send it in the X-API-Key header
	AllowedOrigin string // CORS origin like "https://my-app.vercel.app"; default "*" allows any
}

// Server is the HTTP layer for a Queue.
type Server struct {
	q        *jobq.Queue
	insp     jobq.Inspector
	cfg      Config
	hub      *hub
	stop     chan struct{} // closed by Stop, to end open event streams
	stopOnce sync.Once
}

// New creates a Server. It sets q.OnEvent, so call it before q.Start.
func New(q *jobq.Queue, insp jobq.Inspector, cfg Config) *Server {
	if cfg.AllowedOrigin == "" {
		cfg.AllowedOrigin = "*"
	}
	s := &Server{q: q, insp: insp, cfg: cfg, hub: newHub(), stop: make(chan struct{})}
	q.OnEvent = s.onEvent
	return s
}

// Stop ends all open event streams. Register it with http.Server.RegisterOnShutdown,
// because Shutdown otherwise waits forever for streams that never finish.
func (s *Server) Stop() { s.stopOnce.Do(func() { close(s.stop) }) }

// Handler returns the routes, wrapped in CORS and API key checks.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("POST /jobs", s.enqueue)
	mux.HandleFunc("GET /jobs", s.list)
	mux.HandleFunc("GET /stats", s.stats)
	mux.HandleFunc("POST /jobs/{id}/retry", s.retry)
	mux.HandleFunc("GET /events", s.events)
	return s.cors(s.auth(mux))
}

// cors adds CORS headers and answers browser preflight (OPTIONS) requests.
// It runs before auth, because preflight requests can't carry the API key.
func (s *Server) cors(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h := w.Header()
		h.Set("Access-Control-Allow-Origin", s.cfg.AllowedOrigin)
		h.Set("Access-Control-Allow-Headers", "Content-Type, X-API-Key")
		h.Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		if s.cfg.AllowedOrigin != "*" {
			h.Add("Vary", "Origin")
		}
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// auth requires the X-API-Key header on everything except /healthz.
// With no key configured it rejects everything, so a missing setting can't
// leave the API open.
func (s *Server) auth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/healthz" {
			next.ServeHTTP(w, r)
			return
		}
		got := r.Header.Get("X-API-Key")
		// ConstantTimeCompare takes the same time however many characters match,
		// so response timing can't be used to guess the key.
		if s.cfg.APIKey == "" || subtle.ConstantTimeCompare([]byte(got), []byte(s.cfg.APIKey)) != 1 {
			writeError(w, http.StatusUnauthorized, "missing or invalid API key")
			return
		}
		next.ServeHTTP(w, r)
	})
}

// POST /jobs  {"type": "send-email", "payload": {...}}
func (s *Server) enqueue(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20) // 1 MB limit
	var req struct {
		Type    string          `json:"type"`
		Payload json.RawMessage `json:"payload"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	if req.Type == "" || len(req.Type) > 100 {
		writeError(w, http.StatusBadRequest, "type is required (max 100 characters)")
		return
	}
	payload := []byte(req.Payload)
	if len(payload) == 0 || string(payload) == "null" {
		payload = []byte("{}")
	}

	id, err := s.q.Enqueue(req.Type, payload)
	if err != nil {
		slog.Error("enqueue failed", "err", err)
		writeError(w, http.StatusInternalServerError, "could not enqueue job")
		return
	}
	job, err := s.insp.Get(id)
	if err != nil {
		slog.Error("reading back new job failed", "id", id, "err", err)
		writeError(w, http.StatusInternalServerError, "job was queued but could not be read back")
		return
	}
	writeJSON(w, http.StatusCreated, toJSON(*job))
}

// GET /jobs?status=dead&limit=50
func (s *Server) list(w http.ResponseWriter, r *http.Request) {
	status := jobq.Status(r.URL.Query().Get("status"))
	switch status {
	case "", jobq.StatusPending, jobq.StatusRunning, jobq.StatusSucceeded, jobq.StatusDead:
	default:
		writeError(w, http.StatusBadRequest, "status must be pending, running, succeeded, or dead")
		return
	}
	limit := 0 // 0 means the store's default
	if v := r.URL.Query().Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 {
			writeError(w, http.StatusBadRequest, "limit must be a positive number")
			return
		}
		limit = n
	}

	jobs, err := s.insp.List(status, limit)
	if err != nil {
		slog.Error("list failed", "err", err)
		writeError(w, http.StatusInternalServerError, "could not list jobs")
		return
	}
	out := make([]jobJSON, len(jobs)) // never nil, so the JSON is [] instead of null
	for i := range jobs {
		out[i] = toJSON(jobs[i])
	}
	writeJSON(w, http.StatusOK, map[string]any{"jobs": out})
}

// GET /stats
func (s *Server) stats(w http.ResponseWriter, r *http.Request) {
	counts, err := s.insp.Stats()
	if err != nil {
		slog.Error("stats failed", "err", err)
		writeError(w, http.StatusInternalServerError, "could not read stats")
		return
	}
	writeJSON(w, http.StatusOK, counts)
}

// POST /jobs/{id}/retry
func (s *Server) retry(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.Atoi(r.PathValue("id"))
	if err != nil || id < 1 {
		writeError(w, http.StatusBadRequest, "invalid job id")
		return
	}
	job, err := s.insp.Requeue(id)
	switch {
	case errors.Is(err, jobq.ErrNotFound):
		writeError(w, http.StatusNotFound, "job not found")
		return
	case errors.Is(err, jobq.ErrNotDead):
		writeError(w, http.StatusConflict, "only dead jobs can be retried")
		return
	case err != nil:
		slog.Error("requeue failed", "id", id, "err", err)
		writeError(w, http.StatusInternalServerError, "could not requeue job")
		return
	}
	s.onEvent(jobq.Event{Type: jobq.EventRequeued, Job: *job}) // the Queue doesn't know about this change
	writeJSON(w, http.StatusOK, toJSON(*job))
}

// GET /events streams job changes as Server-Sent Events.
func (s *Server) events(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeError(w, http.StatusInternalServerError, "streaming is not supported")
		return
	}
	// Subscribe before sending headers, so any client that has received the
	// response is already guaranteed to be listening.
	ch := s.hub.subscribe()
	defer s.hub.unsubscribe(ch)

	h := w.Header()
	h.Set("Content-Type", "text/event-stream")
	h.Set("Cache-Control", "no-cache")
	h.Set("X-Accel-Buffering", "no") // stops nginx-style proxies from buffering the stream
	w.WriteHeader(http.StatusOK)
	fmt.Fprint(w, ": connected\n\n")
	flusher.Flush()

	keepalive := time.NewTicker(15 * time.Second) // stops idle proxies from closing the connection
	defer keepalive.Stop()
	for {
		select {
		case <-r.Context().Done(): // the client went away
			return
		case <-s.stop: // the server is shutting down
			return
		case msg := <-ch:
			if _, err := fmt.Fprintf(w, "data: %s\n\n", msg); err != nil {
				return
			}
			flusher.Flush()
		case <-keepalive.C:
			if _, err := fmt.Fprint(w, ": keepalive\n\n"); err != nil {
				return
			}
			flusher.Flush()
		}
	}
}

// onEvent is the Queue's OnEvent callback: it turns an event into JSON and
// hands it to every connected stream.
func (s *Server) onEvent(e jobq.Event) {
	msg, err := json.Marshal(struct {
		Type string  `json:"type"`
		Job  jobJSON `json:"job"`
	}{string(e.Type), toJSON(e.Job)})
	if err != nil {
		slog.Error("encoding event failed", "err", err)
		return
	}
	s.hub.publish(msg)
}

// jobJSON is how a job looks on the wire (camelCase, matching the TypeScript types).
type jobJSON struct {
	ID        int             `json:"id"`
	Type      string          `json:"type"`
	Payload   json.RawMessage `json:"payload"`
	Status    string          `json:"status"`
	Attempts  int             `json:"attempts"`
	LastError *string         `json:"lastError"` // null when there's no error
	CreatedAt time.Time       `json:"createdAt"`
	UpdatedAt time.Time       `json:"updatedAt"`
}

func toJSON(j jobq.Job) jobJSON {
	payload := json.RawMessage(j.Payload)
	switch {
	case len(payload) == 0:
		payload = json.RawMessage("null")
	case !json.Valid(payload): // jobs enqueued from Go code may hold plain text
		b, _ := json.Marshal(string(j.Payload))
		payload = b
	}
	var lastErr *string
	if j.LastErr != "" {
		lastErr = &j.LastErr
	}
	return jobJSON{
		ID: j.ID, Type: j.Type, Payload: payload, Status: string(j.Status),
		Attempts: j.Attempts, LastError: lastErr, CreatedAt: j.CreatedAt, UpdatedAt: j.UpdatedAt,
	}
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}

// hub fans each event out to every connected /events stream.
type hub struct {
	mu   sync.Mutex
	subs map[chan []byte]struct{}
}

func newHub() *hub { return &hub{subs: map[chan []byte]struct{}{}} }

func (h *hub) subscribe() chan []byte {
	ch := make(chan []byte, 64)
	h.mu.Lock()
	h.subs[ch] = struct{}{}
	h.mu.Unlock()
	return ch
}

func (h *hub) unsubscribe(ch chan []byte) {
	h.mu.Lock()
	delete(h.subs, ch)
	h.mu.Unlock()
}

// publish never blocks. A client that falls 64 events behind misses events
// instead of slowing down the workers.
func (h *hub) publish(msg []byte) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for ch := range h.subs {
		select {
		case ch <- msg:
		default:
		}
	}
}
