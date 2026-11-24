// Command pipeline generates and ingests a transactions file through a bounded, cancellable pipeline.
//
//	pipeline generate -n 100000 -o data/transactions.jsonl
//	pipeline run -i data/transactions.jsonl -workers 16
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"syscall"
	"time"

	"github.com/edybackend/go-pipeline/internal/fx"
	"github.com/edybackend/go-pipeline/internal/generate"
	"github.com/edybackend/go-pipeline/internal/pipeline"
	"github.com/edybackend/go-pipeline/internal/store"
)

const exitInterrupted = 130

func main() {
	if len(os.Args) < 2 {
		usage(os.Stderr)
		os.Exit(2)
	}
	var err error
	switch os.Args[1] {
	case "generate":
		err = runGenerate(os.Args[2:])
	case "run":
		err = runPipeline(os.Args[2:])
	case "-h", "--help", "help":
		usage(os.Stdout)
		return
	default:
		usage(os.Stderr)
		os.Exit(2)
	}
	switch {
	case errors.Is(err, context.Canceled):
		os.Exit(exitInterrupted)
	case err != nil:
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func usage(w io.Writer) {
	fmt.Fprintln(w, "usage: pipeline generate [-n 100000] [-o data/transactions.jsonl] [-seed 42]")
	fmt.Fprintln(w, "       pipeline run [-i data/transactions.jsonl] [-db data/pipeline.db] [-rejects data/rejects.jsonl] [-workers N] ...")
}

func runGenerate(args []string) error {
	fs := flag.NewFlagSet("generate", flag.ContinueOnError)
	n := fs.Int("n", 100_000, "number of lines")
	out := fs.String("o", "data/transactions.jsonl", "output file")
	seed := fs.Uint64("seed", 42, "random seed (same seed, same file)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	f, err := create(*out)
	if err != nil {
		return err
	}
	if err := generate.Write(f, *n, *seed); err != nil {
		return errors.Join(err, f.Close())
	}
	if err := f.Close(); err != nil {
		return err
	}
	fmt.Printf("wrote %d lines to %s\n", *n, *out)
	return nil
}

func runPipeline(args []string) error {
	fs := flag.NewFlagSet("run", flag.ContinueOnError)
	input := fs.String("i", "data/transactions.jsonl", "input file (JSON Lines)")
	dbPath := fs.String("db", "data/pipeline.db", "SQLite database")
	rejectsPath := fs.String("rejects", "data/rejects.jsonl", "file for rejected lines")
	workers := fs.Int("workers", 4*runtime.NumCPU(), "enrichment workers (rate lookups are I/O bound)")
	channel := fs.Int("buffer", 256, "capacity of each channel between stages")
	batch := fs.Int("batch", 500, "rows per database transaction")
	latency := fs.Duration("fx-latency", 40*time.Millisecond, "simulated rate service latency")
	failures := fs.Float64("fx-failures", 0.05, "simulated rate service transient failure rate (0..1)")
	timeout := fs.Duration("timeout", 0, "stop after this long (0 = no limit)")
	if err := fs.Parse(args); err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if *timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, *timeout)
		defer cancel()
	}

	in, err := os.Open(*input)
	if err != nil {
		return fmt.Errorf("open input (run `pipeline generate` first?): %w", err)
	}
	defer func() { _ = in.Close() }()
	db, err := store.Open(ctx, *dbPath)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()
	rejectsFile, err := create(*rejectsPath)
	if err != nil {
		return err
	}
	defer func() { _ = rejectsFile.Close() }()
	rejects := json.NewEncoder(rejectsFile)

	source := &fx.Simulated{Latency: *latency, FailureRate: *failures}
	rates := fx.NewClient(source, 5, 20*time.Millisecond)
	fmt.Printf("processing %s with %d workers (fx latency %s, %.0f %% transient failures)…\n", *input, *workers, *latency, *failures*100)

	report, runErr := pipeline.Run(ctx, in, rates, db, func(r pipeline.Rejection) error { return rejects.Encode(r) },
		pipeline.Config{Workers: *workers, ChannelSize: *channel, BatchSize: *batch})
	printReport(context.WithoutCancel(ctx), report, rates.Stats(), source.Calls(), db)
	if runErr != nil && !errors.Is(runErr, context.Canceled) && !errors.Is(runErr, context.DeadlineExceeded) {
		return runErr
	}
	if report.Interrupted {
		return context.Canceled
	}
	return nil
}

func printReport(ctx context.Context, r pipeline.Report, s fx.Stats, sourceCalls int64, db *store.SQLite) {
	status := "completed"
	if r.Interrupted {
		status = "INTERRUPTED (rerun the same command: writes are idempotent)"
	}
	fmt.Printf("\n%s in %s\n", status, r.Duration.Round(time.Millisecond))
	fmt.Printf("  lines read        %8d  (%.0f lines/s)\n", r.Read, float64(r.Read)/r.Duration.Seconds())
	fmt.Printf("  stored            %8d\n", r.Stored)
	fmt.Printf("  already stored    %8d  (duplicates skipped)\n", r.Duplicates)
	fmt.Printf("  rejected          %8d  (see the rejects file)\n", r.Rejected)
	fmt.Printf("  rate lookups      %8d  calls to the source %d, retries %d\n", s.Lookups, sourceCalls, s.Retries)
	fmt.Printf("  cache hits        %8d  coalesced in flight %d\n", s.CacheHits, s.Coalesced)
	if total, err := db.Count(ctx); err == nil {
		fmt.Printf("  rows in database  %8d\n", total)
	}
	top, err := db.TopAccounts(ctx, 5)
	if err != nil || len(top) == 0 {
		return
	}
	fmt.Println("\ntop accounts by volume (BRL)")
	for _, a := range top {
		fmt.Printf("  %s  %6d tx  R$ %14.2f\n", a.Account, a.Transactions, float64(a.TotalBRL)/100)
	}
}

func create(path string) (*os.File, error) {
	if dir := filepath.Dir(path); dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, fmt.Errorf("create directory for %s: %w", path, err)
		}
	}
	return os.Create(path)
}
