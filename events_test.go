// Tests for the OnEvent hook.

package jobq

import (
	"context"
	"errors"
	"testing"
	"time"
)

// nextEvent waits for one event, or fails the test after 2 seconds.
func nextEvent(t *testing.T, ch <-chan Event) Event {
	t.Helper()
	select {
	case e := <-ch:
		return e
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for an event")
		return Event{}
	}
}

// A job that succeeds produces enqueued, started, succeeded, in that order.
func TestEventsForSuccess(t *testing.T) {
	q := New(10, 3, time.Millisecond)
	events := make(chan Event, 20)
	q.OnEvent = func(e Event) { events <- e }
	q.Handle("ok", func(context.Context, []byte) error { return nil })

	// Enqueue before Start, so the "enqueued" event is guaranteed to come first.
	id, err := q.Enqueue("ok", nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() { cancel(); q.Wait() })
	q.Start(ctx, 1)

	wantTypes := []EventType{EventEnqueued, EventStarted, EventSucceeded}
	wantStatus := []Status{StatusPending, StatusRunning, StatusSucceeded}
	for i := range wantTypes {
		e := nextEvent(t, events)
		if e.Type != wantTypes[i] || e.Job.Status != wantStatus[i] || e.Job.ID != id {
			t.Fatalf("event %d = %+v, want type %s, status %s, job %d", i, e, wantTypes[i], wantStatus[i], id)
		}
	}
}

// A job that always fails goes enqueued, started, retrying, started, dead.
func TestEventsForRetryThenDead(t *testing.T) {
	q := New(10, 2, time.Millisecond)
	events := make(chan Event, 20)
	q.OnEvent = func(e Event) { events <- e }
	q.Handle("bad", func(context.Context, []byte) error { return errors.New("boom") })

	if _, err := q.Enqueue("bad", nil); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() { cancel(); q.Wait() })
	q.Start(ctx, 1) // one worker, so events arrive in a fixed order

	wantTypes := []EventType{EventEnqueued, EventStarted, EventRetrying, EventStarted, EventDead}
	wantAttempts := []int{0, 1, 1, 2, 2}
	var last Event
	for i := range wantTypes {
		last = nextEvent(t, events)
		if last.Type != wantTypes[i] || last.Job.Attempts != wantAttempts[i] {
			t.Fatalf("event %d = %s with %d attempts, want %s with %d",
				i, last.Type, last.Job.Attempts, wantTypes[i], wantAttempts[i])
		}
	}
	if last.Job.Status != StatusDead || last.Job.LastErr != "boom" {
		t.Fatalf("final event job = %+v", last.Job)
	}
}

// A callback that panics must not break Enqueue or kill the workers.
func TestPanickingEventCallbackIsContained(t *testing.T) {
	q := New(10, 1, time.Millisecond)
	q.OnEvent = func(Event) { panic("bad callback") }
	done := make(chan struct{})
	q.Handle("ok", func(context.Context, []byte) error {
		close(done)
		return nil
	})

	if _, err := q.Enqueue("ok", nil); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() { cancel(); q.Wait() })
	q.Start(ctx, 1)

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("job never ran after the callback panicked")
	}
}
