// Package generate writes a deterministic sample file of transactions, with a few invalid lines on purpose.
package generate

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"math/rand/v2"
	"time"
)

// Write writes n JSON lines to w. The same seed always produces the same file.
// About 1 % of the lines are invalid (malformed JSON, negative amounts, unknown currencies, bad timestamps).
func Write(w io.Writer, n int, seed uint64) error {
	rng := rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15))
	out := bufio.NewWriter(w)
	currencies := []string{"BRL", "BRL", "USD", "USD", "EUR", "GBP"}
	start := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)

	for i := 1; i <= n; i++ {
		line := map[string]any{
			"id":        fmt.Sprintf("tx-%07d", i),
			"account":   fmt.Sprintf("acc-%04d", rng.IntN(500)+1),
			"amount":    rng.Int64N(500_000) + 100, // 1.00 to 5,001.00
			"currency":  currencies[rng.IntN(len(currencies))],
			"timestamp": start.Add(time.Duration(rng.Int64N(int64(30 * 24 * time.Hour)))).Format(time.RFC3339),
		}
		var data []byte
		switch rng.IntN(400) {
		case 0:
			data = []byte(`{"id":"tx-broken",`) // malformed JSON
		case 1:
			line["amount"] = -line["amount"].(int64)
		case 2:
			line["currency"] = "JPY"
		case 3:
			line["timestamp"] = "31/10/2026 10:00"
		}
		if data == nil {
			var err error
			if data, err = json.Marshal(line); err != nil {
				return fmt.Errorf("encode line %d: %w", i, err)
			}
		}
		if _, err := out.Write(append(data, '\n')); err != nil {
			return fmt.Errorf("write line %d: %w", i, err)
		}
	}
	return out.Flush()
}
