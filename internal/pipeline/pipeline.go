// Package pipeline streams a transactions file through bounded stages:
//
//	read ──▶ parse/validate ──▶ enrich (N workers) ──▶ write (batches)
//	              └──────────────┴──▶ rejects
//
// Every channel has a fixed capacity, so a slow stage makes the earlier ones wait (backpressure) instead of
// buffering the whole file. All stages share one errgroup context: a fatal error in any of them cancels the rest.
package pipeline

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/edybackend/go-pipeline/internal/fx"
	"github.com/edybackend/go-pipeline/internal/record"
)

// Rates looks up an exchange rate (fx.Client in production).
type Rates interface {
	Rate(ctx context.Context, currency, day string) (float64, error)
}

// Writer stores a batch and returns how many rows were new.
type Writer interface {
	WriteBatch(ctx context.Context, batch []record.Enriched) (int, error)
}

// Rejection is an input line that was not stored, and why.
type Rejection struct {
	Line   int64  `json:"line"`
	Reason string `json:"reason"`
	Raw    string `json:"raw"`
}

// RejectFunc receives every rejection, from a single goroutine.
type RejectFunc func(Rejection) error

// Config sizes the pipeline.
type Config struct {
	Workers     int // enrichment workers
	ChannelSize int // capacity of every channel between stages
	BatchSize   int // rows per database transaction
}

// Report is what happened.
type Report struct {
	Read        int64         `json:"read"`
	Stored      int64         `json:"stored"`
	Duplicates  int64         `json:"duplicates"`
	Rejected    int64         `json:"rejected"`
	Interrupted bool          `json:"interrupted"`
	Duration    time.Duration `json:"duration"`
}

type rawLine struct {
	number int64
	data   []byte
}

type parsedLine struct {
	number int64
	raw    []byte
	tx     record.Transaction
}

// Run processes in until it ends, the context is cancelled or a stage fails.
// On cancellation the batch being written is still committed, and the report says Interrupted.
func Run(ctx context.Context, in io.Reader, rates Rates, w Writer, reject RejectFunc, cfg Config) (Report, error) {
	if cfg.Workers < 1 || cfg.ChannelSize < 1 || cfg.BatchSize < 1 {
		return Report{}, errors.New("workers, channel size and batch size must be at least 1")
	}
	started := time.Now()
	var read, stored, duplicates, rejected atomic.Int64

	g, gctx := errgroup.WithContext(ctx)
	lines := make(chan rawLine, cfg.ChannelSize)
	parsed := make(chan parsedLine, cfg.ChannelSize)
	enriched := make(chan record.Enriched, cfg.ChannelSize)
	rejects := make(chan Rejection, cfg.ChannelSize)

	// send delivers v unless the pipeline is shutting down.
	send := func(ch chan<- Rejection, v Rejection) bool {
		select {
		case ch <- v:
			return true
		case <-gctx.Done():
			return false
		}
	}

	// 1. Read.
	g.Go(func() error {
		defer close(lines)
		scanner := bufio.NewScanner(in)
		scanner.Buffer(make([]byte, 64*1024), 1024*1024)
		var n int64
		for scanner.Scan() {
			n++
			data := append([]byte(nil), scanner.Bytes()...) // the scanner reuses its buffer
			select {
			case lines <- rawLine{n, data}:
				read.Add(1)
			case <-gctx.Done():
				return nil
			}
		}
		if err := scanner.Err(); err != nil {
			return fmt.Errorf("read input: %w", err)
		}
		return nil
	})

	// 2. Parse and validate. 3. Enrich. Both can reject; rejects closes when both are done.
	var producers sync.WaitGroup
	producers.Go(func() {
		defer close(parsed)
		for l := range lines {
			tx, err := record.Parse(l.data)
			if err != nil {
				if !send(rejects, Rejection{l.number, err.Error(), string(l.data)}) {
					return
				}
				continue
			}
			select {
			case parsed <- parsedLine{l.number, l.data, tx}:
			case <-gctx.Done():
				return
			}
		}
	})

	var workers sync.WaitGroup
	var fatal error
	var fatalOnce sync.Once
	for range cfg.Workers {
		workers.Go(func() {
			for p := range parsed {
				rate, err := rates.Rate(gctx, p.tx.Currency, p.tx.Day())
				switch {
				case errors.Is(err, fx.ErrUnavailable):
					if !send(rejects, Rejection{p.number, err.Error(), string(p.raw)}) {
						return
					}
					continue
				case err != nil && gctx.Err() != nil:
					return // shutting down
				case err != nil:
					fatalOnce.Do(func() { fatal = fmt.Errorf("enrich line %d: %w", p.number, err) })
					return
				}
				select {
				case enriched <- record.Enriched{Transaction: p.tx, Rate: rate, AmountBRL: toBRL(p.tx.Amount, rate)}:
				case <-gctx.Done():
					return
				}
			}
		})
	}
	producers.Go(func() {
		workers.Wait()
		close(enriched)
	})
	g.Go(func() error {
		producers.Wait()
		close(rejects)
		return fatal
	})

	// 4a. Rejects go to their own sink.
	g.Go(func() error {
		for r := range rejects {
			rejected.Add(1)
			if err := reject(r); err != nil {
				return fmt.Errorf("write reject: %w", err)
			}
		}
		return nil
	})

	// 4b. Write in batches. On shutdown the batch in hand is still committed.
	g.Go(func() error {
		batch := make([]record.Enriched, 0, cfg.BatchSize)
		flush := func(ctx context.Context) error {
			if len(batch) == 0 {
				return nil
			}
			n, err := w.WriteBatch(ctx, batch)
			if err != nil {
				return fmt.Errorf("write batch: %w", err)
			}
			stored.Add(int64(n))
			duplicates.Add(int64(len(batch) - n))
			batch = batch[:0]
			return nil
		}
		for {
			select {
			case e, ok := <-enriched:
				if !ok {
					return flush(gctx)
				}
				batch = append(batch, e)
				if len(batch) == cfg.BatchSize {
					if err := flush(gctx); err != nil {
						return err
					}
				}
			case <-gctx.Done():
				return flush(context.WithoutCancel(gctx))
			}
		}
	})

	err := g.Wait()
	// Drain whatever is left so no goroutine stays blocked on a send (only after a cancellation).
	for range enriched {
	}
	report := Report{
		Read:        read.Load(),
		Stored:      stored.Load(),
		Duplicates:  duplicates.Load(),
		Rejected:    rejected.Load(),
		Interrupted: ctx.Err() != nil,
		Duration:    time.Since(started),
	}
	if err == nil && ctx.Err() != nil {
		err = ctx.Err()
	}
	return report, err
}

// toBRL converts minor units with the rate at 4 decimal places, in integer arithmetic, rounding half up.
func toBRL(amount int64, rate float64) int64 {
	scaled := int64(math.Round(rate * 10_000))
	return (amount*scaled + 5_000) / 10_000
}
