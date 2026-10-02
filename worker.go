// This file decides what happens to a job after a worker takes it:
// run the handler, then complete, retry, or dead-letter it.
package jobq

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"time"
)

// maxRetryDelay caps how long a job ever waits between attempts.
const maxRetryDelay = 5 * time.Minute

// run makes one attempt at a job, then reports the outcome to the store.
func (q *Queue) run(ctx context.Context, job *Job) {
	job.Attempts++          // counted before the handler runs, so the first attempt is 1
	err := q.call(ctx, job) // run the handler; nil means success
	if err == nil {
		// Tell the store it's done. logErr logs if that call fails.
		logErr("complete", job, q.store.Complete(job))
		return
	}
	job.LastErr = err.Error() // save the failure reason (a string, so a database can store it)

	// Out of attempts: dead-letter it.
	if job.Attempts >= q.maxAttempts {
		logErr("bury", job, q.store.Bury(job))
		return
	}
	// Otherwise retry later. The store gets a clock time, not a delay,
	// because a database can save "run at 3:05:02" but not "wait 2s".
	retryAt := time.Now().Add(q.retryDelay(job.Attempts))
	logErr("retry", job, q.store.Retry(job, retryAt))
}

// logErr reports a failed store operation instead of silently dropping it.
func logErr(op string, job *Job, err error) {
	if err != nil {
		slog.Error("store operation failed", "op", op, "job", job.ID, "err", err)
	}
}

// call runs the job's handler, turning panics and missing handlers into errors.
func (q *Queue) call(ctx context.Context, job *Job) (err error) {
	h, ok := q.handlers[job.Type]
	if !ok {
		return fmt.Errorf("no handler for %q", job.Type)
	}
	ctx, cancel := context.WithTimeout(ctx, q.JobTimeout)
	defer cancel() // always release the timer
	defer func() {
		// A panicking handler becomes a normal failure instead of crashing the program.
		if r := recover(); r != nil {
			err = fmt.Errorf("panic: %v", r)
		}
	}()
	return h(ctx, job.Payload)
}

// retryDelay doubles with each attempt (capped at 5 minutes), then picks a
// random time between half of that and the full amount. The randomness stops
// a burst of failed jobs from all retrying at the same instant.
// Attempt must be at least 1.
func (q *Queue) retryDelay(attempt int) time.Duration {
	d := q.baseDelay << (attempt - 1) // shift left = double per attempt
	if d <= 0 || d > maxRetryDelay {  // <= 0 catches overflow from a huge shift
		d = maxRetryDelay
	}
	half := d / 2
	return half + time.Duration(rand.Int64N(int64(half)+1)) // +1 so the max is included
}
