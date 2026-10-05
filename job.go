// Package jobq is a small background job queue with retries, dead-lettering,
// and optional SQLite persistence.
package jobq

import (
	"context"
	"errors"
	"time"
)

// ErrFull is returned when the store has no room for another job.
var ErrFull = errors.New("jobq: queue is full")

// Handler runs one job. Returning an error triggers a retry.
type Handler func(ctx context.Context, payload []byte) error

// Status is where a job is in its life.
type Status string

const (
	StatusPending   Status = "pending"   // waiting to run, or waiting to retry
	StatusRunning   Status = "running"   // a worker has it
	StatusSucceeded Status = "succeeded" // finished successfully
	StatusDead      Status = "dead"      // ran out of attempts
)

// Job is a unit of work.
type Job struct {
	ID        int       // assigned by the store
	Type      string    // picks which Handler runs it
	Payload   []byte    // data for the handler, usually JSON
	Status    Status    // filled in by SQLStore only; empty for MemoryStore jobs
	Attempts  int       // how many times it has run
	LastErr   string    // most recent failure message
	CreatedAt time.Time // filled in by SQLStore only
	UpdatedAt time.Time // filled in by SQLStore only
}
