// Package jobq is a small background job queue with retries, dead-lettering,
// and optional SQLite persistence.
package jobq

import (
	"context"
	"errors"
)

// ErrFull is returned when the store has no room for another job.
var ErrFull = errors.New("jobq: queue is full")

// Handler runs one job. Returning an error triggers a retry.
type Handler func(ctx context.Context, payload []byte) error

// Job is a unit of work.
type Job struct {
	ID       int    // assigned by the store
	Type     string // picks which Handler runs it
	Payload  []byte // data for the handler, usually JSON
	Attempts int    // how many times it has run
	LastErr  string // most recent failure message
}
