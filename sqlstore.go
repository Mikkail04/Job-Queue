// This file is the SQLite Store: jobs are saved in a file, so they survive restarts.

package jobq

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"time"

	_ "modernc.org/sqlite" // registers the "sqlite" driver
)

// Each column matches a Job field, plus status and run_at. A row can't "move"
// like a job in a channel, so its state is written down in status:
// pending, running, done, or dead.
const schema = `
CREATE TABLE IF NOT EXISTS jobs (
	id       INTEGER PRIMARY KEY AUTOINCREMENT,
	type     TEXT    NOT NULL,
	payload  BLOB    NOT NULL,
	status   TEXT    NOT NULL DEFAULT 'pending',
	attempts INTEGER NOT NULL DEFAULT 0,
	last_err TEXT    NOT NULL DEFAULT '',
	run_at   INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS jobs_ready ON jobs (status, run_at);
`

// SQLStore keeps jobs in a SQLite file. Use one process per file.
type SQLStore struct {
	db           *sql.DB       // handle to the database file; safe for concurrent use
	pollInterval time.Duration // how long Next sleeps between checks (a field so tests can shorten it)
}

// OpenSQLStore opens (or creates) the database file at path.
func OpenSQLStore(path string) (*SQLStore, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	// SQLite allows one writer at a time. A single connection makes
	// concurrent workers wait their turn instead of failing with
	// "database is locked".
	db.SetMaxOpenConns(1)

	if _, err := db.Exec(schema); err != nil {
		db.Close()
		return nil, err
	}

	// A job still marked 'running' belonged to a worker that died with the
	// previous process. Make it runnable again. (This assumes only one
	// process uses the file at a time.)
	if _, err := db.Exec(`UPDATE jobs SET status = 'pending' WHERE status = 'running'`); err != nil {
		db.Close()
		return nil, err
	}
	return &SQLStore{db: db, pollInterval: 100 * time.Millisecond}, nil
}

// Close closes the database file.
func (s *SQLStore) Close() error { return s.db.Close() }

// Add saves a new job as pending, ready to run immediately, and sets job.ID
// to the ID the database assigns.
func (s *SQLStore) Add(job *Job) error {
	// The payload column is NOT NULL, but the driver stores a nil slice as
	// NULL. Swap in an empty slice so jobs with no payload can be saved.
	payload := job.Payload
	if payload == nil {
		payload = []byte{}
	}

	// The ? placeholders keep values separate from the SQL, which prevents
	// injection. status and attempts are left out so they take their defaults
	// ('pending' and 0), and run_at is now so the job is ready immediately.
	res, err := s.db.Exec(
		`INSERT INTO jobs (type, payload, run_at) VALUES (?, ?, ?)`,
		job.Type, payload, time.Now().UnixMilli(),
	)
	if err != nil {
		return err
	}

	// Write the new row's ID onto job so Enqueue can return it.
	id, err := res.LastInsertId()
	if err != nil {
		return err
	}
	job.ID = int(id)
	return nil
}

// Next polls until a job is ready, claims it, and returns it. It returns an
// error only when ctx is cancelled, because the worker loop treats any error
// from Next as "shut down".
func (s *SQLStore) Next(ctx context.Context) (*Job, error) {
	for {
		job, err := s.claim(ctx)
		if err == nil {
			return job, nil
		}

		// Shutdown: the only error Next is allowed to return.
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		// ErrNoRows just means nothing is ready, which is normal. Anything
		// else is a real database problem: log it and keep going, so a
		// temporary error doesn't kill the worker.
		if !errors.Is(err, sql.ErrNoRows) {
			slog.Error("claiming a job failed", "err", err)
		}

		// Wait before polling again, but wake right away on shutdown.
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(s.pollInterval):
		}
	}
}

// claim marks one ready job as running and returns it, in a single statement.
// Doing it in one statement is what stops two workers from grabbing the same job.
func (s *SQLStore) claim(ctx context.Context) (*Job, error) {
	var job Job
	// Inside out: the SELECT finds the oldest due pending job, the UPDATE marks
	// it running, and RETURNING hands its columns back.
	err := s.db.QueryRowContext(ctx, `
		UPDATE jobs SET status = 'running'
		WHERE id = (
			SELECT id FROM jobs
			WHERE status = 'pending' AND run_at <= ?
			ORDER BY run_at, id
			LIMIT 1
		)
		RETURNING id, type, payload, attempts, last_err`,
		time.Now().UnixMilli(),
	).Scan(&job.ID, &job.Type, &job.Payload, &job.Attempts, &job.LastErr)
	if err != nil {
		return nil, err // sql.ErrNoRows when nothing is ready
	}
	return &job, nil
}

// Compile-time check that SQLStore satisfies Store.
var _ Store = (*SQLStore)(nil)

// Complete marks a job as finished. The row is kept as a record.
func (s *SQLStore) Complete(job *Job) error {
	_, err := s.db.Exec(
		`UPDATE jobs SET status = 'done', attempts = ? WHERE id = ?`,
		job.Attempts, job.ID,
	)
	return err
}

// Retry puts a failed job back to pending, to run again at time at.
// No timer is needed: claim skips rows whose run_at is still in the future.
func (s *SQLStore) Retry(job *Job, at time.Time) error {
	_, err := s.db.Exec(
		`UPDATE jobs SET status = 'pending', attempts = ?, last_err = ?, run_at = ? WHERE id = ?`,
		job.Attempts, job.LastErr, at.UnixMilli(), job.ID,
	)
	return err
}

// Bury moves a job to the dead-letter state once it has used up all its attempts.
func (s *SQLStore) Bury(job *Job) error {
	_, err := s.db.Exec(
		`UPDATE jobs SET status = 'dead', attempts = ?, last_err = ? WHERE id = ?`,
		job.Attempts, job.LastErr, job.ID,
	)
	return err
}

// Dead returns the dead-lettered jobs, oldest first. The Store interface has
// no error return, so failures are logged and the result may be incomplete.
func (s *SQLStore) Dead() []Job {
	rows, err := s.db.Query(
		`SELECT id, type, payload, attempts, last_err FROM jobs WHERE status = 'dead' ORDER BY id`,
	)
	if err != nil {
		slog.Error("listing dead jobs failed", "err", err)
		return nil
	}
	// Always close rows: an unclosed result set keeps holding the only
	// connection, which would freeze the whole store.
	defer rows.Close()

	// Copy each row into a Job. If a row can't be read, stop and return what
	// we have so far.
	var out []Job
	for rows.Next() {
		var j Job
		if err := rows.Scan(&j.ID, &j.Type, &j.Payload, &j.Attempts, &j.LastErr); err != nil {
			slog.Error("reading a dead job failed", "err", err)
			return out
		}
		out = append(out, j)
	}

	// rows.Next also returns false when something went wrong, not only at the
	// end, so check here whether the loop stopped early.
	if err := rows.Err(); err != nil {
		slog.Error("listing dead jobs failed", "err", err)
	}
	return out
}
