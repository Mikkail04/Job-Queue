// Tests for Queue using the in-memory store. Run with: go test -race ./...
package jobq

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// waitForDead polls until at least n jobs are dead-lettered, or fails the test.
// Polling is needed because nothing signals "a job just died".
func waitForDead(t *testing.T, q *Queue, n int) []Job {
	t.Helper() // failures point at the calling test, not this helper
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if dead := q.Dead(); len(dead) >= n {
			return dead
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %d dead jobs", n)
	return nil // unreachable, but the compiler needs a return
}

// A job that fails twice and then succeeds should run exactly 3 times.
func TestRetriesUntilSuccess(t *testing.T) {
	q := New(10, 5, time.Millisecond) // tiny retry delay keeps the test fast
	var calls atomic.Int32            // atomic: the handler runs on another goroutine
	done := make(chan struct{})       // closed as a "succeeded" signal
	q.Handle("t", func(context.Context, []byte) error {
		if calls.Add(1) < 3 { // Add returns the new count: calls 1 and 2 fail
			return errors.New("transient")
		}
		close(done)
		return nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	q.Start(ctx, 2)
	q.Enqueue("t", nil)

	// Wait for success, but give up after 2s so a bug fails instead of hanging.
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("job never succeeded")
	}
	cancel()
	q.Wait()
	if n := calls.Load(); n != 3 {
		t.Fatalf("handler called %d times, want 3", n)
	}
}

// A job that always fails should be dead-lettered after maxAttempts tries.
func TestDeadLetterAfterMaxAttempts(t *testing.T) {
	q := New(10, 3, time.Millisecond)
	q.Handle("bad", func(context.Context, []byte) error { return errors.New("boom") })
	ctx, cancel := context.WithCancel(context.Background())
	q.Start(ctx, 1)
	q.Enqueue("bad", nil)

	deadline := time.Now().Add(2 * time.Second)
	for len(q.Dead()) == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	q.Wait()

	// Exactly one dead job, tried 3 times, with the handler's error saved.
	dead := q.Dead()
	if len(dead) != 1 || dead[0].Attempts != 3 || dead[0].LastErr != "boom" {
		t.Fatalf("unexpected dead letters: %+v", dead)
	}
}

// Cancelling the context must not interrupt a job that is already running.
func TestShutdownWaitsForInFlightJob(t *testing.T) {
	q := New(10, 1, time.Millisecond)
	started := make(chan struct{})
	var finished atomic.Bool
	q.Handle("slow", func(context.Context, []byte) error {
		close(started) // tell the test the job is running
		time.Sleep(50 * time.Millisecond)
		finished.Store(true)
		return nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	q.Start(ctx, 1)
	q.Enqueue("slow", nil)

	<-started // without this, we might cancel before the job begins
	cancel()  // the "Ctrl-C" moment
	q.Wait()
	if !finished.Load() {
		t.Fatal("Wait returned before the in-flight job finished")
	}
}

// Enqueueing into a full queue returns ErrFull instead of blocking.
func TestQueueFull(t *testing.T) {
	q := New(1, 1, time.Millisecond) // no workers started, so nothing drains the buffer
	if _, err := q.Enqueue("x", nil); err != nil {
		t.Fatal(err)
	}
	if _, err := q.Enqueue("x", nil); !errors.Is(err, ErrFull) {
		t.Fatalf("got %v, want ErrFull", err)
	}
}

// Jitter is random, so check a range over many samples instead of one value.
func TestRetryDelay(t *testing.T) {
	base := 100 * time.Millisecond
	q := New(1, 10, base)

	// Each delay should land between half and the full doubled amount.
	for attempt := 1; attempt <= 5; attempt++ {
		full := base << (attempt - 1) // 100ms, 200ms, 400ms, ...
		for i := 0; i < 200; i++ {
			got := q.retryDelay(attempt)
			if got < full/2 || got > full {
				t.Fatalf("attempt %d: delay %v outside [%v, %v]", attempt, got, full/2, full)
			}
		}
	}

	// A huge attempt number would overflow without the cap.
	if got := q.retryDelay(70); got <= 0 || got > maxRetryDelay {
		t.Fatalf("delay %v not in (0, %v]", got, maxRetryDelay)
	}
}

// A handler that outlives JobTimeout gets its context cancelled.
func TestJobTimeout(t *testing.T) {
	q := New(10, 1, time.Millisecond)
	q.JobTimeout = 20 * time.Millisecond
	q.Handle("stuck", func(ctx context.Context, _ []byte) error {
		<-ctx.Done() // waits until the timeout fires
		return ctx.Err()
	})
	ctx, cancel := context.WithCancel(context.Background())
	q.Start(ctx, 1)
	q.Enqueue("stuck", nil)

	deadline := time.Now().Add(2 * time.Second)
	for len(q.Dead()) == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	q.Wait()

	dead := q.Dead()
	if len(dead) != 1 || !strings.Contains(dead[0].LastErr, "deadline exceeded") {
		t.Fatalf("unexpected dead letters: %+v", dead)
	}
}

// A panicking handler must not kill the worker; it counts as a failure.
func TestPanicIsRecovered(t *testing.T) {
	q := New(10, 1, time.Millisecond) // 1 attempt, so it dies right away
	q.Handle("panic", func(context.Context, []byte) error {
		panic("kaboom")
	})
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() { cancel(); q.Wait() }) // runs when the test ends, even on failure
	q.Start(ctx, 1)
	q.Enqueue("panic", nil)

	dead := waitForDead(t, q, 1) // would time out if the worker had crashed
	if dead[0].LastErr != "panic: kaboom" {
		t.Fatalf("LastErr = %q, want %q", dead[0].LastErr, "panic: kaboom")
	}
}

// A job with no registered handler is retried, then dead-lettered with a clear reason.
func TestUnknownJobTypeIsDeadLettered(t *testing.T) {
	q := New(10, 2, time.Millisecond) // note: no handlers registered
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() { cancel(); q.Wait() })
	q.Start(ctx, 1)
	q.Enqueue("nope", nil)

	dead := waitForDead(t, q, 1)
	if !strings.Contains(dead[0].LastErr, "no handler") {
		t.Fatalf("LastErr = %q, want it to mention a missing handler", dead[0].LastErr)
	}
}
