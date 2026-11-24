package pipeline

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/edybackend/go-pipeline/internal/fx"
	"github.com/edybackend/go-pipeline/internal/generate"
	"github.com/edybackend/go-pipeline/internal/record"
)

// memWriter is an idempotent in-memory sink. block, if set, holds every write until it is closed.
type memWriter struct {
	mu      sync.Mutex
	rows    map[string]record.Enriched
	block   chan struct{}
	failOn  int // fail the n-th batch (0 = never)
	batches int
}

func newMemWriter() *memWriter { return &memWriter{rows: map[string]record.Enriched{}} }

func (w *memWriter) WriteBatch(ctx context.Context, batch []record.Enriched) (int, error) {
	if w.block != nil {
		select {
		case <-w.block:
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	w.batches++
	if w.failOn > 0 && w.batches == w.failOn {
		return 0, errors.New("disk full")
	}
	inserted := 0
	for _, e := range batch {
		if _, ok := w.rows[e.ID]; !ok {
			w.rows[e.ID] = e
			inserted++
		}
	}
	return inserted, nil
}

func (w *memWriter) count() int { w.mu.Lock(); defer w.mu.Unlock(); return len(w.rows) }

// rates answers instantly (or after delay), and fails for the currencies in unavailable.
type rates struct {
	delay       time.Duration
	unavailable string
	calls       atomic.Int64
}

func (r *rates) Rate(ctx context.Context, currency, _ string) (float64, error) {
	r.calls.Add(1)
	if r.delay > 0 {
		select {
		case <-time.After(r.delay):
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}
	if currency == r.unavailable {
		return 0, fmt.Errorf("%w: down for %s", fx.ErrUnavailable, currency)
	}
	return 5.4321, nil
}

func sample(t *testing.T, n int) []byte {
	t.Helper()
	var buf bytes.Buffer
	if err := generate.Write(&buf, n, 7); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

// validLines counts the lines record.Parse accepts.
func validLines(data []byte) int {
	n := 0
	for _, line := range bytes.Split(bytes.TrimSpace(data), []byte("\n")) {
		if _, err := record.Parse(line); err == nil {
			n++
		}
	}
	return n
}

var cfg = Config{Workers: 8, ChannelSize: 16, BatchSize: 50}

func collect() (RejectFunc, *[]Rejection) {
	var mu sync.Mutex
	var list []Rejection
	return func(r Rejection) error { mu.Lock(); defer mu.Unlock(); list = append(list, r); return nil }, &list
}

func TestEveryLineIsStoredOrRejected(t *testing.T) {
	data := sample(t, 2_000)
	w := newMemWriter()
	reject, rejected := collect()

	report, err := Run(t.Context(), bytes.NewReader(data), &rates{}, w, reject, cfg)

	if err != nil {
		t.Fatal(err)
	}
	valid := validLines(data)
	if report.Read != 2_000 || int(report.Stored) != valid || int(report.Rejected) != 2_000-valid || w.count() != valid {
		t.Fatalf("report %+v, stored rows %d; want 2000 read, %d stored, %d rejected", report, w.count(), valid, 2_000-valid)
	}
	if len(*rejected) == 0 || (*rejected)[0].Line == 0 || (*rejected)[0].Reason == "" {
		t.Fatalf("rejections should carry the line number and the reason: %+v", (*rejected)[:min(1, len(*rejected))])
	}
	if e := w.rows["tx-0000001"]; e.AmountBRL != toBRL(e.Amount, 5.4321) {
		t.Fatalf("conversion: %+v", e)
	}
}

func TestARerunStoresNothingTwice(t *testing.T) {
	data := sample(t, 1_000)
	w := newMemWriter()
	reject, _ := collect()
	first, _ := Run(t.Context(), bytes.NewReader(data), &rates{}, w, reject, cfg)

	second, err := Run(t.Context(), bytes.NewReader(data), &rates{}, w, reject, cfg)

	if err != nil || second.Stored != 0 || second.Duplicates != first.Stored || w.count() != int(first.Stored) {
		t.Fatalf("second run %+v (err %v); want 0 stored and %d duplicates", second, err, first.Stored)
	}
}

func TestAnInterruptedRunCanBeRerunToCompletion(t *testing.T) {
	data := sample(t, 3_000)
	w := newMemWriter()
	reject, _ := collect()
	ctx, cancel := context.WithTimeout(t.Context(), 150*time.Millisecond)
	defer cancel()

	interrupted, err := Run(ctx, bytes.NewReader(data), &rates{delay: 2 * time.Millisecond}, w, reject, Config{Workers: 2, ChannelSize: 8, BatchSize: 20})

	if !errors.Is(err, context.DeadlineExceeded) || !interrupted.Interrupted || interrupted.Stored == 0 || int(interrupted.Stored) >= validLines(data) {
		t.Fatalf("first run %+v (err %v); want a partial, interrupted run", interrupted, err)
	}
	if w.count() != int(interrupted.Stored) {
		t.Fatalf("the batch in hand must be committed on interruption: %d rows, report says %d", w.count(), interrupted.Stored)
	}

	rerun, err := Run(t.Context(), bytes.NewReader(data), &rates{}, w, reject, cfg)

	if err != nil || w.count() != validLines(data) || rerun.Duplicates != interrupted.Stored {
		t.Fatalf("rerun %+v (err %v), rows %d; want every valid line exactly once", rerun, err, w.count())
	}
}

func TestAFatalErrorStopsEveryStageAndLeaksNoGoroutine(t *testing.T) {
	before := runtime.NumGoroutine()
	w := newMemWriter()
	w.failOn = 3
	reject, _ := collect()

	_, err := Run(t.Context(), bytes.NewReader(sample(t, 5_000)), &rates{}, w, reject, cfg)

	if err == nil || !bytes.Contains([]byte(err.Error()), []byte("disk full")) {
		t.Fatalf("err = %v; want the writer's error", err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for runtime.NumGoroutine() > before && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if after := runtime.NumGoroutine(); after > before {
		t.Fatalf("goroutines: %d before, %d after", before, after)
	}
}

func TestARecordWhoseRateNeverComesIsRejectedAndTheRunContinues(t *testing.T) {
	data := sample(t, 1_000)
	w := newMemWriter()
	reject, rejected := collect()

	report, err := Run(t.Context(), bytes.NewReader(data), &rates{unavailable: "GBP"}, w, reject, cfg)

	if err != nil || report.Stored == 0 {
		t.Fatalf("report %+v err %v; want the run to finish", report, err)
	}
	gbp := 0
	for _, r := range *rejected {
		if bytes.Contains([]byte(r.Reason), []byte("down for GBP")) {
			gbp++
		}
	}
	if gbp == 0 || int(report.Stored)+int(report.Rejected) != 1_000 {
		t.Fatalf("%d GBP rejections, report %+v", gbp, report)
	}
}

func TestBackpressureBoundsHowFarTheReaderGets(t *testing.T) {
	w := newMemWriter()
	w.block = make(chan struct{})
	reject, _ := collect()
	small := Config{Workers: 2, ChannelSize: 10, BatchSize: 5}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan Report, 1)
	go func() {
		report, _ := Run(ctx, bytes.NewReader(sample(t, 5_000)), &rates{}, w, reject, small)
		done <- report
	}()

	time.Sleep(200 * time.Millisecond)
	cancel()
	close(w.block)
	report := <-done

	// 3 channels of 10, 2 workers, a batch of 5, plus one line held by each stage: well under 100 of 5,000.
	if report.Read > 100 {
		t.Fatalf("read %d lines while the writer was stuck; backpressure should keep it near 50", report.Read)
	}
}
