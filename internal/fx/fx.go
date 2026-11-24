// Package fx looks up exchange rates: a slow, unreliable source behind a cache, request coalescing and retries.
package fx

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"math"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/singleflight"
)

// ErrUnavailable is a transient failure of the rate source: worth retrying.
var ErrUnavailable = errors.New("rate source unavailable")

// Source returns how many BRL one unit of currency was worth on day (YYYY-MM-DD).
type Source interface {
	Rate(ctx context.Context, currency, day string) (float64, error)
}

// Simulated is a rate service with latency and random transient failures. Rates are deterministic per (currency, day).
type Simulated struct {
	Latency     time.Duration
	FailureRate float64 // 0..1
	calls       atomic.Int64
}

// Calls counts how many requests reached the source.
func (s *Simulated) Calls() int64 { return s.calls.Load() }

// Rate implements Source.
func (s *Simulated) Rate(ctx context.Context, currency, day string) (float64, error) {
	s.calls.Add(1)
	select {
	case <-time.After(s.Latency):
	case <-ctx.Done():
		return 0, ctx.Err()
	}
	if rand.Float64() < s.FailureRate {
		return 0, fmt.Errorf("%w: 503 from the rate service", ErrUnavailable)
	}
	base := map[string]float64{"BRL": 1, "USD": 5.40, "EUR": 5.85, "GBP": 6.85}[currency]
	if currency == "BRL" {
		return 1, nil
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(currency + day))
	variation := float64(h.Sum32()%400)/10000 - 0.02 // ±2 % per day, stable
	return math.Round(base*(1+variation)*10000) / 10000, nil
}

// Stats are counters for the report. Coalesced counts callers that waited for another caller's lookup.
type Stats struct {
	Lookups, CacheHits, Coalesced, Retries int64
}

// Client is what the pipeline uses: cache, singleflight and retries in front of a Source.
type Client struct {
	source      Source
	maxAttempts int
	baseBackoff time.Duration

	group     singleflight.Group
	mu        sync.RWMutex
	cache     map[string]float64
	lookups   atomic.Int64
	cacheHits atomic.Int64
	coalesced atomic.Int64
	retries   atomic.Int64
}

// NewClient returns a client with up to maxAttempts calls per lookup and exponential backoff from baseBackoff.
func NewClient(source Source, maxAttempts int, baseBackoff time.Duration) *Client {
	return &Client{source: source, maxAttempts: maxAttempts, baseBackoff: baseBackoff, cache: map[string]float64{}}
}

// Rate returns the rate for (currency, day). Concurrent calls for the same key share one lookup.
func (c *Client) Rate(ctx context.Context, currency, day string) (float64, error) {
	key := currency + "|" + day
	c.mu.RLock()
	rate, ok := c.cache[key]
	c.mu.RUnlock()
	if ok {
		c.cacheHits.Add(1)
		return rate, nil
	}

	ran := false // true only in the goroutine that performs the lookup; the others wait for its result
	v, err, _ := c.group.Do(key, func() (any, error) {
		ran = true
		// Check again: a lookup for this key may have finished between our cache miss and this call.
		c.mu.RLock()
		cached, ok := c.cache[key]
		c.mu.RUnlock()
		if ok {
			c.cacheHits.Add(1)
			return cached, nil
		}
		c.lookups.Add(1)
		rate, err := c.withRetry(ctx, currency, day)
		if err == nil {
			c.mu.Lock()
			c.cache[key] = rate
			c.mu.Unlock()
		}
		return rate, err
	})
	if !ran {
		c.coalesced.Add(1)
	}
	if err != nil {
		return 0, err
	}
	return v.(float64), nil
}

func (c *Client) withRetry(ctx context.Context, currency, day string) (float64, error) {
	var lastErr error
	for attempt := 1; attempt <= c.maxAttempts; attempt++ {
		rate, err := c.source.Rate(ctx, currency, day)
		if err == nil {
			return rate, nil
		}
		if !errors.Is(err, ErrUnavailable) {
			return 0, err // context cancelled or a non-transient error: do not retry
		}
		lastErr = err
		if attempt == c.maxAttempts {
			break
		}
		c.retries.Add(1)
		select {
		case <-time.After(c.baseBackoff << (attempt - 1)):
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}
	return 0, fmt.Errorf("rate %s on %s after %d attempts: %w", currency, day, c.maxAttempts, lastErr)
}

// Stats returns the counters.
func (c *Client) Stats() Stats {
	return Stats{Lookups: c.lookups.Load(), CacheHits: c.cacheHits.Load(), Coalesced: c.coalesced.Load(), Retries: c.retries.Load()}
}
