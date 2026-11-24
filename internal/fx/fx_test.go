package fx

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// scripted fails the first `failures` calls, then answers 5.0 after `delay`.
type scripted struct {
	failures int64
	delay    time.Duration
	calls    atomic.Int64
	err      error
}

func (s *scripted) Rate(ctx context.Context, _, _ string) (float64, error) {
	n := s.calls.Add(1)
	select {
	case <-time.After(s.delay):
	case <-ctx.Done():
		return 0, ctx.Err()
	}
	if n <= s.failures {
		if s.err != nil {
			return 0, s.err
		}
		return 0, ErrUnavailable
	}
	return 5.0, nil
}

func TestConcurrentLookupsOfOneKeyMakeOneCall(t *testing.T) {
	source := &scripted{delay: 50 * time.Millisecond}
	client := NewClient(source, 3, time.Millisecond)

	var wg sync.WaitGroup
	for range 50 {
		wg.Go(func() {
			if rate, err := client.Rate(t.Context(), "USD", "2026-11-01"); err != nil || rate != 5.0 {
				t.Errorf("Rate = %v, %v", rate, err)
			}
		})
	}
	wg.Wait()
	_, _ = client.Rate(t.Context(), "USD", "2026-11-01")

	if got := source.calls.Load(); got != 1 {
		t.Fatalf("source called %d times; want 1", got)
	}
	if s := client.Stats(); s.Lookups != 1 || s.CacheHits < 1 || s.Coalesced+s.CacheHits != 50 {
		t.Fatalf("stats = %+v; want 1 lookup, the other 50 coalesced or cached", s)
	}
}

func TestTransientFailuresAreRetriedWithBackoff(t *testing.T) {
	tests := []struct {
		name      string
		failures  int64
		wantErr   bool
		wantCalls int64
	}{
		{"recovers on the third attempt", 2, false, 3},
		{"gives up after five attempts", 10, true, 5},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			source := &scripted{failures: tt.failures}
			client := NewClient(source, 5, time.Millisecond)

			_, err := client.Rate(t.Context(), "EUR", "2026-11-01")

			if (err != nil) != tt.wantErr || source.calls.Load() != tt.wantCalls {
				t.Fatalf("err = %v, calls = %d; want error %v, calls %d", err, source.calls.Load(), tt.wantErr, tt.wantCalls)
			}
			if tt.wantErr && !errors.Is(err, ErrUnavailable) {
				t.Fatalf("error should wrap ErrUnavailable: %v", err)
			}
		})
	}
}

func TestANonTransientErrorIsNotRetried(t *testing.T) {
	source := &scripted{failures: 1, err: errors.New("401 unauthorized")}
	client := NewClient(source, 5, time.Millisecond)

	if _, err := client.Rate(t.Context(), "GBP", "2026-11-01"); err == nil || source.calls.Load() != 1 {
		t.Fatalf("err = %v, calls = %d; want the error after 1 call", err, source.calls.Load())
	}
}

func TestCancellationStopsTheRetries(t *testing.T) {
	source := &scripted{failures: 100}
	client := NewClient(source, 100, 50*time.Millisecond)
	ctx, cancel := context.WithTimeout(t.Context(), 120*time.Millisecond)
	defer cancel()

	_, err := client.Rate(ctx, "USD", "2026-11-01")

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v; want context.DeadlineExceeded", err)
	}
	if source.calls.Load() > 4 {
		t.Fatalf("source called %d times after the deadline", source.calls.Load())
	}
}

func TestSimulatedRatesAreStablePerDay(t *testing.T) {
	s := &Simulated{}
	a, _ := s.Rate(t.Context(), "USD", "2026-11-01")
	b, _ := s.Rate(t.Context(), "USD", "2026-11-01")
	c, _ := s.Rate(t.Context(), "USD", "2026-11-02")
	if a != b || a == c || a < 5.29 || a > 5.51 {
		t.Fatalf("rates %v %v %v: want stable per day, different across days, within ±2 %% of 5.40", a, b, c)
	}
	if brl, _ := s.Rate(t.Context(), "BRL", "2026-11-01"); brl != 1 {
		t.Fatalf("BRL rate = %v; want 1", brl)
	}
}
