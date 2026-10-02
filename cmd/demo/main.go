// Command demo enqueues fake emails and processes them with flaky retries,
// saving unfinished jobs to SQLite between runs.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"math/rand/v2"
	"os/signal"
	"syscall"
	"time"

	"github.com/Mikkail04/jobq"
)

func main() {
	// Command-line flags. These are pointers, so read them with * after Parse.
	dbPath := flag.String("db", "jobs.db", "path to the SQLite file")
	count := flag.Int("n", 10, "number of emails to enqueue (0 = just resume old jobs)")
	flag.Parse()

	// ctx is cancelled when the user presses Ctrl-C (or the OS asks us to stop).
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Open the database. Jobs left over from a previous run are picked up here.
	store, err := jobq.OpenSQLStore(*dbPath)
	if err != nil {
		log.Fatal(err)
	}
	defer store.Close()

	// 4 max attempts per job, retry delay starts around 200ms and doubles.
	q := jobq.NewWithStore(store, 4, 200*time.Millisecond)

	// Handler for "email" jobs. Returning an error triggers a retry.
	q.Handle("email", func(ctx context.Context, payload []byte) error {
		time.Sleep(time.Second) // fake work, slow enough to press Ctrl-C mid-job

		// Fail about half the time so retries and dead letters show up.
		if rand.Float64() < 0.5 {
			return errors.New("smtp temporarily unavailable")
		}
		fmt.Printf("sent email to %s\n", payload)
		return nil
	})

	// Launch 3 workers in the background.
	q.Start(ctx, 3)

	// Add new jobs. With -n 0 this loop is skipped and only old jobs run.
	for i := 1; i <= *count; i++ {
		to := fmt.Sprintf("user%d@example.com", i)
		if _, err := q.Enqueue("email", []byte(to)); err != nil {
			fmt.Println("enqueue failed:", err)
		}
	}

	// Block until Ctrl-C and let running jobs finish before exiting
	fmt.Println("running; press Ctrl-C to stop")
	<-ctx.Done()
	q.Wait()

	// Report jobs that used up all their attempts.
	for _, j := range q.Dead() {
		fmt.Printf("DEAD job %d after %d attempts: %s\n", j.ID, j.Attempts, j.LastErr)
	}
}
