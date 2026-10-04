// This file adds read and requeue operations for the HTTP API. Only SQLStore
// supports them, so they live in their own interface instead of Store.

package jobq

import (
	"database/sql"
	"errors"
	"time"
)

var (
	// ErrNotFound is returned when no job has the given ID.
	ErrNotFound = errors.New("jobq: job not found")
	// ErrNotDead is returned when Requeue is asked to retry a job that isn't dead.
	ErrNotDead = errors.New("jobq: job is not dead")
)

// Inspector lets callers look at jobs and requeue dead ones.
type Inspector interface {
	// List returns up to limit jobs, newest first. An empty status means all statuses.
	List(status Status, limit int) ([]Job, error)

	// Stats returns how many jobs are in each status. All four statuses are
	// always present, even when their count is zero.
	Stats() (map[Status]int, error)

	// Requeue gives a dead job a fresh start: pending, with zero attempts.
	Requeue(id int) (*Job, error)

	// Get returns one job by ID, or ErrNotFound.
	Get(id int) (*Job, error)
}

// Compile-time check that SQLStore satisfies Inspector.
var _ Inspector = (*SQLStore)(nil)

const (
	defaultListLimit = 100
	maxListLimit     = 1000
)

// List returns up to limit jobs, newest first, optionally filtered by status.
func (s *SQLStore) List(status Status, limit int) ([]Job, error) {
	if limit <= 0 {
		limit = defaultListLimit
	}
	if limit > maxListLimit {
		limit = maxListLimit
	}

	// Only fixed text is joined into the SQL. Values still go through placeholders.
	query := `SELECT ` + jobColumns + ` FROM jobs`
	var args []any
	if status != "" {
		query += ` WHERE status = ?`
		args = append(args, string(status))
	}
	query += ` ORDER BY id DESC LIMIT ?`
	args = append(args, limit)

	rows, err := s.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close() // an unclosed result set would hold the only connection

	out := []Job{} // not nil, so the API can return [] instead of null
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *j)
	}
	return out, rows.Err()
}

// Stats returns the number of jobs in each status.
func (s *SQLStore) Stats() (map[Status]int, error) {
	rows, err := s.db.Query(`SELECT status, COUNT(*) FROM jobs GROUP BY status`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	counts := map[Status]int{
		StatusPending:   0,
		StatusRunning:   0,
		StatusSucceeded: 0,
		StatusDead:      0,
	}
	for rows.Next() {
		var status string
		var n int
		if err := rows.Scan(&status, &n); err != nil {
			return nil, err
		}
		counts[Status(status)] = n
	}
	return counts, rows.Err()
}

// Requeue turns a dead job back into a pending one that runs immediately,
// with its attempts and last error cleared so it gets a full set of retries.
func (s *SQLStore) Requeue(id int) (*Job, error) {
	nowMS := time.Now().UnixMilli()
	row := s.db.QueryRow(`
		UPDATE jobs SET status = 'pending', attempts = 0, last_err = '', run_at = ?, updated_at = ?
		WHERE id = ? AND status = 'dead'
		RETURNING `+jobColumns,
		nowMS, nowMS, id,
	)
	job, err := scanJob(row)
	if !errors.Is(err, sql.ErrNoRows) {
		return job, err // success, or a real database error
	}

	// No dead job matched. Work out whether the job is missing or just not dead.
	var exists int
	err = s.db.QueryRow(`SELECT 1 FROM jobs WHERE id = ?`, id).Scan(&exists)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return nil, ErrNotDead
}

// Get returns one job by ID, or ErrNotFound.
func (s *SQLStore) Get(id int) (*Job, error) {
	job, err := scanJob(s.db.QueryRow(`SELECT `+jobColumns+` FROM jobs WHERE id = ?`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	return job, err
}
