// This file defines Queue, the public API: it connects a Store to a pool of workers.

package jobq

import (
	"context"
	"sync"
	"time"
)

// Queue runs handlers on jobs pulled from a Store. Create one with New or NewWithStore.
type Queue struct {
	// JobTimeout limits how long one attempt may run. Set it before Start.
	JobTimeout time.Duration

	store       Store              // where jobs are saved and fetched
	handlers    map[string]Handler // job type -> function that runs it
	maxAttempts int                // a job is dead-lettered after this many tries
	baseDelay   time.Duration      // first retry delay; doubles each attempt
	wg          sync.WaitGroup     // tracks running workers
}

// New creates a queue that keeps up to size waiting jobs in memory.
func New(size, maxAttempts int, baseDelay time.Duration) *Queue {
	return NewWithStore(NewMemoryStore(size), maxAttempts, baseDelay)
}

// NewWithStore creates a queue backed by any Store.
func NewWithStore(store Store, maxAttempts int, baseDelay time.Duration) *Queue {
	return &Queue{
		JobTimeout:  30 * time.Second,
		store:       store,
		handlers:    map[string]Handler{}, // a nil map would panic on write
		maxAttempts: maxAttempts,
		baseDelay:   baseDelay,
	}
}

// Handle registers the function that runs jobs of type typ. Call before Start.
func (q *Queue) Handle(typ string, h Handler) { q.handlers[typ] = h }

// Enqueue adds a job and returns its ID.
func (q *Queue) Enqueue(typ string, payload []byte) (int, error) {
	job := &Job{Type: typ, Payload: payload} // the store assigns the ID
	if err := q.store.Add(job); err != nil {
		return 0, err
	}
	return job.ID, nil
}

// Start launches n workers. Each runs one job at a time, so at most n jobs
// run at once. Cancelling ctx stops them from taking new jobs.
func (q *Queue) Start(ctx context.Context, n int) {
	for i := 0; i < n; i++ {
		q.wg.Add(1) // before the goroutine starts, so Wait can't miss it
		go func() {
			defer q.wg.Done()
			for {
				job, err := q.store.Next(ctx)
				if err != nil {
					return // Next only fails when ctx is cancelled
				}
				// WithoutCancel lets a running job finish during shutdown.
				q.run(context.WithoutCancel(ctx), job)
			}
		}()
	}
}

// Wait blocks until all workers have exited. Cancel the context first.
func (q *Queue) Wait() { q.wg.Wait() }

// Dead returns the jobs that ran out of attempts.
func (q *Queue) Dead() []Job { return q.store.Dead() }
