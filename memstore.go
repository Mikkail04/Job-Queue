// This file is the in-memory Store: fast and simple, but jobs are lost on exit.
package jobq

import (
	"context"
	"sync"
	"time"
)

// Compile-time check that MemoryStore satisfies Store.
var _ Store = (*MemoryStore)(nil)

// MemoryStore keeps waiting jobs in a buffered channel.
type MemoryStore struct {
	jobs chan *Job // the queue itself

	mu     sync.Mutex // guards the fields below
	nextID int
	dead   []*Job
}

// NewMemoryStore creates a store that holds up to size waiting jobs.
func NewMemoryStore(size int) *MemoryStore {
	return &MemoryStore{jobs: make(chan *Job, size)}
}

// Add assigns the job an ID and queues it, or returns ErrFull without blocking.
func (s *MemoryStore) Add(job *Job) error {
	s.mu.Lock()
	s.nextID++
	job.ID = s.nextID
	s.mu.Unlock() // released early: the send below doesn't need the lock

	select {
	case s.jobs <- job:
		return nil
	default: // channel full
		return ErrFull
	}
}

// Next blocks until a job arrives or ctx is cancelled.
func (s *MemoryStore) Next(ctx context.Context) (*Job, error) {
	select {
	case job := <-s.jobs:
		return job, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// Complete does nothing here: a finished job has already left the channel.
func (s *MemoryStore) Complete(job *Job) error { return nil }

// Retry re-queues the job at time at, using a timer. Timers are lost on exit.
func (s *MemoryStore) Retry(job *Job, at time.Time) error {
	time.AfterFunc(time.Until(at), func() {
		select {
		case s.jobs <- job:
		default: // no room to retry, so dead-letter it instead of losing it
			job.LastErr += " (queue full on retry)"
			_ = s.Bury(job)
		}
	})
	return nil
}

// Bury moves a job to the dead-letter list.
func (s *MemoryStore) Bury(job *Job) error {
	s.mu.Lock()
	s.dead = append(s.dead, job)
	s.mu.Unlock()
	return nil
}

// Dead returns copies of the dead-lettered jobs, so callers can't alter the originals.
func (s *MemoryStore) Dead() []Job {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]Job, len(s.dead))
	for i, j := range s.dead {
		out[i] = *j
	}
	return out
}
