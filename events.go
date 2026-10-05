// This file defines the events a Queue announces as jobs change state.

package jobq

import (
	"log/slog"
	"time"
)

// EventType says what just happened to a job.
type EventType string

const (
	EventEnqueued  EventType = "enqueued"  // added to the queue
	EventStarted   EventType = "started"   // a worker began an attempt
	EventSucceeded EventType = "succeeded" // finished successfully
	EventRetrying  EventType = "retrying"  // failed, will run again later
	EventDead      EventType = "dead"      // failed for the last time
	EventRequeued  EventType = "requeued"  // a dead job was requeued (sent by the API, not the Queue)
)

// Event reports one state change. Job is a copy, so receivers can keep it.
type Event struct {
	Type EventType
	Job  Job
}

// The status a job has right after each event. Setting it here means events
// carry a correct Status even with the memory store, which doesn't track one.
var eventStatus = map[EventType]Status{
	EventEnqueued:  StatusPending,
	EventStarted:   StatusRunning,
	EventSucceeded: StatusSucceeded,
	EventRetrying:  StatusPending,
	EventDead:      StatusDead,
}

// emit sends an event to q.OnEvent, if one is set.
func (q *Queue) emit(typ EventType, job *Job) {
	if q.OnEvent == nil {
		return
	}
	j := *job // copy, so later changes to the job don't alter the event
	j.Status = eventStatus[typ]
	j.UpdatedAt = time.Now()

	// A panicking callback must not crash the worker (or the caller of Enqueue).
	defer func() {
		if r := recover(); r != nil {
			slog.Error("OnEvent callback panicked", "panic", r)
		}
	}()
	q.OnEvent(Event{Type: typ, Job: j})
}
