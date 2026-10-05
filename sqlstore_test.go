// Tests for SQLStore: persistence, crash recovery, and safe claiming.
// Each test uses its own temporary database file. Run with: go test -race ./...
package jobq

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// openTestStore opens a SQLite file at path and closes it when the test ends.
func openTestStore(t *testing.T, path string) *SQLStore {
	t.Helper() // failures point at the calling test, not this helper
	s, err := OpenSQLStore(path)
	if err != nil {
		t.Fatal(err)
	}
	s.pollInterval = 10 * time.Millisecond // poll fast so tests don't wait 100ms per check
	// Closing twice is harmless, so tests can also Close early to simulate a restart.
	t.Cleanup(func() { s.Close() })
	return s
}

// A job added to the store comes back from Next with its data intact.
func TestSQLStoreAddAndNext(t *testing.T) {
	// t.TempDir is deleted automatically when the test ends.
	s := openTestStore(t, filepath.Join(t.TempDir(), "jobs.db"))
	job := &Job{Type: "email", Payload: []byte("hello")}
	if err := s.Add(job); err != nil {
		t.Fatal(err)
	}
	if job.ID == 0 {
		t.Fatal("Add did not assign an ID")
	}

	// The timeout makes a bug fail the test instead of hanging it.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	got, err := s.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != job.ID || got.Type != "email" || string(got.Payload) != "hello" {
		t.Fatalf("got %+v", got)
	}
}

// Next on an empty store waits, then returns an error once ctx ends.
// Workers rely on this to shut down.
func TestSQLStoreNextStopsWhenContextEnds(t *testing.T) {
	s := openTestStore(t, filepath.Join(t.TempDir(), "jobs.db"))
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, err := s.Next(ctx); err == nil {
		t.Fatal("expected an error from an empty store once ctx expires")
	}
}

// Two workers compete for one job, and only one may get it.
func TestSQLStoreClaimsEachJobOnce(t *testing.T) {
	s := openTestStore(t, filepath.Join(t.TempDir(), "jobs.db"))
	if err := s.Add(&Job{Type: "x"}); err != nil {
		t.Fatal(err)
	}

	// The loser keeps polling an empty table until this timeout ends it.
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	var wg sync.WaitGroup
	var claimed atomic.Int32 // atomic: both goroutines add to it
	for i := 0; i < 2; i++ { // two "workers" compete for one job
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := s.Next(ctx); err == nil { // only a successful claim counts
				claimed.Add(1)
			}
		}()
	}
	wg.Wait()
	if n := claimed.Load(); n != 1 {
		t.Fatalf("job was claimed %d times, want exactly 1", n)
	}
}

// A job saved to the file is still there after closing and reopening it.
func TestSQLStoreJobsSurviveReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "jobs.db")
	first := openTestStore(t, path)
	if err := first.Add(&Job{Type: "email", Payload: []byte("still here")}); err != nil {
		t.Fatal(err)
	}
	first.Close() // simulate the program exiting

	second := openTestStore(t, path) // a fresh store on the same file
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	got, err := second.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got.Type != "email" || string(got.Payload) != "still here" {
		t.Fatalf("got %+v", got)
	}
}

// A job claimed by a worker that dies mid-run is handed out again after a restart.
func TestSQLStoreRunningJobsRecoveredAfterCrash(t *testing.T) {
	path := filepath.Join(t.TempDir(), "jobs.db")
	first := openTestStore(t, path)
	job := &Job{Type: "x"}
	if err := first.Add(job); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, err := first.Next(ctx); err != nil { // a worker claims it...
		t.Fatal(err)
	}
	first.Close() // ...and the program dies before finishing

	// Opening the file resets 'running' jobs to 'pending', so it's claimable again.
	second := openTestStore(t, path)
	got, err := second.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != job.ID {
		t.Fatalf("got job %d, want the abandoned job %d", got.ID, job.ID)
	}
}

// End to end: jobs enqueued in one run are processed by a fresh queue in the next.
func TestQueueWithSQLStoreRunsJobsAfterRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "jobs.db")

	// First "run" of the program: enqueue jobs, then exit before any worker starts.
	first := openTestStore(t, path)
	q1 := NewWithStore(first, 3, time.Millisecond)
	for i := 0; i < 3; i++ {
		if _, err := q1.Enqueue("count", nil); err != nil {
			t.Fatal(err)
		}
	}
	first.Close()

	// Second run: a fresh queue on the same file picks them up.
	second := openTestStore(t, path)
	q2 := NewWithStore(second, 3, time.Millisecond)
	var ran atomic.Int32        // atomic: handlers run on worker goroutines
	done := make(chan struct{}) // closed once all 3 jobs have run
	q2.Handle("count", func(context.Context, []byte) error {
		if ran.Add(1) == 3 { // Add returns the new count
			close(done)
		}
		return nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() { cancel(); q2.Wait() }) // stop workers when the test ends
	q2.Start(ctx, 2)

	// Wait for all three, but fail with a count instead of hanging forever.
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatalf("only %d of 3 jobs ran after restart", ran.Load())
	}
}

// A dead-lettered job, with its attempts, error, and payload, survives a restart.
func TestSQLStoreDeadLettersPersist(t *testing.T) {
	path := filepath.Join(t.TempDir(), "jobs.db")
	store := openTestStore(t, path)
	q := NewWithStore(store, 2, time.Millisecond) // 2 attempts, so it dies quickly
	q.Handle("bad", func(context.Context, []byte) error { return errors.New("boom") })

	ctx, cancel := context.WithCancel(context.Background())
	q.Start(ctx, 1)
	if _, err := q.Enqueue("bad", []byte("x")); err != nil {
		t.Fatal(err)
	}
	waitForDead(t, q, 1) // helper from queue_test.go
	cancel()
	q.Wait() // make sure no worker is touching the file before we close it
	store.Close()

	// Read the dead letters back from the file with a brand-new store.
	reopened := openTestStore(t, path)
	dead := reopened.Dead()
	if len(dead) != 1 || dead[0].Attempts != 2 || dead[0].LastErr != "boom" || string(dead[0].Payload) != "x" {
		t.Fatalf("unexpected dead letters after reopen: %+v", dead)
	}
}

// Add and Next fill in each job's status and timestamps.
func TestSQLStoreSetsStatusAndTimes(t *testing.T) {
	s := openTestStore(t, filepath.Join(t.TempDir(), "jobs.db"))
	job := &Job{Type: "x"}
	if err := s.Add(job); err != nil {
		t.Fatal(err)
	}
	if job.Status != StatusPending || job.CreatedAt.IsZero() {
		t.Fatalf("after Add: %+v", job)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	got, err := s.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != StatusRunning || got.CreatedAt.IsZero() || got.UpdatedAt.Before(got.CreatedAt) {
		t.Fatalf("after Next: %+v", got)
	}
}
