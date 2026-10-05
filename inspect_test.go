// Tests for SQLStore's List, Stats, and Requeue.

package jobq

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"
)

// seedJobs adds three jobs and moves two of them along, giving one dead job,
// one succeeded job, and one still pending. It returns the dead and succeeded IDs.
func seedJobs(t *testing.T, s *SQLStore) (deadID, okID int) {
	t.Helper()
	for i := 0; i < 3; i++ {
		if err := s.Add(&Job{Type: "x"}); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Next hands out the oldest job first, so these are jobs 1 and 2.
	dead, err := s.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	dead.Attempts, dead.LastErr = 3, "boom"
	if err := s.Bury(dead); err != nil {
		t.Fatal(err)
	}

	ok, err := s.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	ok.Attempts = 1
	if err := s.Complete(ok); err != nil {
		t.Fatal(err)
	}
	return dead.ID, ok.ID
}

func TestSQLStoreList(t *testing.T) {
	s := openTestStore(t, filepath.Join(t.TempDir(), "jobs.db"))
	deadID, _ := seedJobs(t, s)

	all, err := s.List("", 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 3 || all[0].ID < all[1].ID || all[1].ID < all[2].ID {
		t.Fatalf("want 3 jobs, newest first, got %+v", all)
	}

	dead, err := s.List(StatusDead, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(dead) != 1 || dead[0].ID != deadID || dead[0].LastErr != "boom" || dead[0].Attempts != 3 {
		t.Fatalf("dead list = %+v", dead)
	}

	limited, err := s.List("", 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(limited) != 2 {
		t.Fatalf("limit 2 returned %d jobs", len(limited))
	}

	none, err := s.List(StatusRunning, 0)
	if err != nil {
		t.Fatal(err)
	}
	if none == nil || len(none) != 0 {
		t.Fatalf("want an empty, non-nil slice, got %#v", none)
	}
}

func TestSQLStoreStats(t *testing.T) {
	s := openTestStore(t, filepath.Join(t.TempDir(), "jobs.db"))

	// An empty store still reports every status.
	empty, err := s.Stats()
	if err != nil {
		t.Fatal(err)
	}
	if len(empty) != 4 {
		t.Fatalf("empty stats = %v, want all four statuses", empty)
	}

	seedJobs(t, s)
	got, err := s.Stats()
	if err != nil {
		t.Fatal(err)
	}
	want := map[Status]int{StatusPending: 1, StatusRunning: 0, StatusSucceeded: 1, StatusDead: 1}
	for status, n := range want {
		if got[status] != n {
			t.Fatalf("stats = %v, want %v", got, want)
		}
	}
}

func TestSQLStoreRequeue(t *testing.T) {
	s := openTestStore(t, filepath.Join(t.TempDir(), "jobs.db"))
	deadID, okID := seedJobs(t, s)

	job, err := s.Requeue(deadID)
	if err != nil {
		t.Fatal(err)
	}
	if job.Status != StatusPending || job.Attempts != 0 || job.LastErr != "" {
		t.Fatalf("requeued job = %+v", job)
	}
	if dead, _ := s.List(StatusDead, 0); len(dead) != 0 {
		t.Fatalf("job is still dead: %+v", dead)
	}
	if pending, _ := s.List(StatusPending, 0); len(pending) != 2 {
		t.Fatalf("want 2 pending jobs, got %d", len(pending))
	}

	// A job that isn't dead can't be requeued, and an unknown ID is a different error.
	if _, err := s.Requeue(okID); !errors.Is(err, ErrNotDead) {
		t.Fatalf("requeue of a succeeded job: got %v, want ErrNotDead", err)
	}
	if _, err := s.Requeue(9999); !errors.Is(err, ErrNotFound) {
		t.Fatalf("requeue of an unknown job: got %v, want ErrNotFound", err)
	}
}

func TestSQLStoreGet(t *testing.T) {
	s := openTestStore(t, filepath.Join(t.TempDir(), "jobs.db"))
	job := &Job{Type: "x", Payload: []byte(`{"a":1}`)}
	if err := s.Add(job); err != nil {
		t.Fatal(err)
	}

	got, err := s.Get(job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != job.ID || got.Type != "x" || string(got.Payload) != `{"a":1}` || got.Status != StatusPending {
		t.Fatalf("got %+v", got)
	}
	if _, err := s.Get(9999); !errors.Is(err, ErrNotFound) {
		t.Fatalf("unknown ID: got %v, want ErrNotFound", err)
	}
}
