// This file defines Store, the interface that lets the queue use any storage
// backend (in memory or SQLite).
package jobq

import (
	"context"
	"time"
)

// Store holds jobs for workers. It must be safe for concurrent use and give
// each job to only one worker at a time.
type Store interface {
	// Add saves a new job and assigns its ID. It returns ErrFull if there is no room.
	Add(job *Job) error

	// Next waits for the next job that is ready to run. It returns an error
	// when ctx is cancelled.
	Next(ctx context.Context) (*Job, error)

	// Complete is called when a job succeeded.
	Complete(job *Job) error

	// Retry puts a failed job back to run again at time `at`.
	Retry(job *Job, at time.Time) error

	// Bury moves a job to the dead-letter list.
	Bury(job *Job) error

	// Dead returns copies of all dead-lettered jobs.
	Dead() []Job
}
