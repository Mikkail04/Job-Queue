// Command server runs the job queue with its HTTP API, backed by SQLite.
//
// Set the API key first:   $env:JOBQ_API_KEY = "dev-key"
// Then run:                go run ./cmd/server -demo
package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	jobq "github.com/Mikkail04/Job-Queue"
	"github.com/Mikkail04/Job-Queue/api"
)

func main() {
	// run returns an error instead of calling log.Fatal itself, because
	// log.Fatal would skip the deferred cleanup inside run.
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	// Hosting platforms like Render tell you which port to use through PORT.
	defaultAddr := ":8080"
	if p := os.Getenv("PORT"); p != "" {
		defaultAddr = ":" + p
	}
	addr := flag.String("addr", defaultAddr, "HTTP listen address")
	dbPath := flag.String("db", "jobs.db", "path to the SQLite file")
	workers := flag.Int("workers", 4, "number of concurrent workers")
	origin := flag.String("origin", "*", "allowed CORS origin, e.g. https://my-app.vercel.app")
	demo := flag.Bool("demo", false, `register a flaky "send-email" handler so the dashboard has activity`)
	flag.Parse()

	apiKey := os.Getenv("JOBQ_API_KEY")
	if apiKey == "" {
		return errors.New("set the JOBQ_API_KEY environment variable")
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	store, err := jobq.OpenSQLStore(*dbPath)
	if err != nil {
		return err
	}
	defer store.Close()

	q := jobq.NewWithStore(store, 4, 500*time.Millisecond)
	if *demo {
		q.Handle("send-email", flakyEmail) // handlers must be registered before Start
	}
	apiSrv := api.New(q, store, api.Config{APIKey: apiKey, AllowedOrigin: *origin})
	q.Start(ctx, *workers)

	httpSrv := &http.Server{
		Addr:              *addr,
		Handler:           apiSrv.Handler(),
		ReadHeaderTimeout: 5 * time.Second, // no WriteTimeout: it would cut off /events streams
	}
	httpSrv.RegisterOnShutdown(apiSrv.Stop)

	errc := make(chan error, 1)
	go func() { errc <- httpSrv.ListenAndServe() }()
	slog.Info("listening", "addr", *addr)

	var serveErr error
	select {
	case <-ctx.Done(): // Ctrl-C
	case serveErr = <-errc: // the server failed, for example because the port is taken
		stop() // make sure the workers stop too
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := httpSrv.Shutdown(shutdownCtx); err != nil {
		slog.Error("http shutdown", "err", err)
	}
	q.Wait() // let running jobs finish
	if errors.Is(serveErr, http.ErrServerClosed) {
		serveErr = nil
	}
	return serveErr
}

// flakyEmail pretends to send an email: slow, and it fails 30% of the time.
func flakyEmail(ctx context.Context, payload []byte) error {
	select {
	case <-time.After(time.Duration(300+rand.IntN(1200)) * time.Millisecond):
	case <-ctx.Done():
		return ctx.Err()
	}
	if rand.Float64() < 0.3 {
		return errors.New("smtp temporarily unavailable")
	}
	return nil
}
