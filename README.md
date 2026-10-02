# Job Queue

A small background job queue for Go. You hand it work, and a pool of workers runs it in the background, retrying anything that fails. Jobs can live in memory or in a SQLite file, so they survive restarts.

## Features

- Worker pool with retries (exponential backoff and jitter)
- Dead-letter list for jobs that keep failing
- Per-job timeouts and panic recovery
- Graceful shutdown: running jobs finish first
- Storage you can swap: in-memory or SQLite
- Crash recovery: jobs interrupted mid-run are run again

## Quick start

    go test -race ./...
    go run ./cmd/demo

The demo enqueues fake emails that fail about half the time and saves them in `jobs.db`. Press Ctrl-C partway through, then finish the leftovers:

    go run ./cmd/demo -n 0

## Usage

```go
// In memory (jobs are lost on exit)
q := jobq.New(100, 4, time.Second) // buffer size, max attempts, base retry delay

// Or SQLite (jobs survive restarts)
store, _ := jobq.OpenSQLStore("jobs.db")
defer store.Close()
q := jobq.NewWithStore(store, 4, time.Second)

q.Handle("email", func(ctx context.Context, payload []byte) error {
    return sendEmail(ctx, string(payload)) // return an error to retry
})
q.Start(ctx, 4)                            // 4 workers
q.Enqueue("email", []byte("a@b.com"))

cancel() // on shutdown...
q.Wait() // ...wait for running jobs to finish
```

Register handlers before `Start`. Set `q.JobTimeout` (default 30s) to limit how long one attempt may run.

## Design notes

- **Storage is an interface**, so the queue doesn't care whether jobs are in memory or SQLite.
- **Retries are stored as a time**, so in SQLite a retry is just a row with a future `run_at` and survives a restart.
- **Jitter** spreads out retries so failing jobs don't all retry at the same moment.
- **Claiming is one SQL statement**, so two workers can never take the same job.
- **Delivery is at-least-once**: a job may run twice if the program crashes mid-run, so handlers should be safe to repeat.

## Limitations

- SQLite: one process per database file, and finished jobs are never deleted.
- The memory store loses jobs and pending retries on exit.
- A handler that ignores its context can't be stopped by the timeout.
- No scheduled jobs, priorities, or metrics yet.
