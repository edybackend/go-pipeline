// Package record parses and validates one transaction line.
package record

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

// Transaction is a validated input line.
type Transaction struct {
	ID       string
	Account  string
	Amount   int64 // minor units of Currency
	Currency string
	At       time.Time
}

// Day is the UTC date used to look up the exchange rate.
func (t Transaction) Day() string { return t.At.UTC().Format(time.DateOnly) }

// Enriched is a transaction converted to BRL.
type Enriched struct {
	Transaction
	Rate      float64 // BRL per unit of Currency, as published by the rate source
	AmountBRL int64   // minor units, rounded half away from zero
}

// Currencies the pipeline accepts.
var Currencies = map[string]bool{"BRL": true, "USD": true, "EUR": true, "GBP": true}

type line struct {
	ID        string      `json:"id"`
	Account   string      `json:"account"`
	Amount    json.Number `json:"amount"`
	Currency  string      `json:"currency"`
	Timestamp string      `json:"timestamp"`
}

// ErrInvalid wraps every validation failure, so callers can tell bad data from real errors.
var ErrInvalid = errors.New("invalid record")

// Parse decodes and validates one JSON line.
func Parse(raw []byte) (Transaction, error) {
	var l line
	if err := json.Unmarshal(raw, &l); err != nil {
		return Transaction{}, fmt.Errorf("%w: malformed JSON: %w", ErrInvalid, err)
	}
	var problems []string
	if strings.TrimSpace(l.ID) == "" {
		problems = append(problems, "id is required")
	}
	if strings.TrimSpace(l.Account) == "" {
		problems = append(problems, "account is required")
	}
	amount, err := l.Amount.Int64()
	if err != nil || amount <= 0 {
		problems = append(problems, "amount must be a positive integer in minor units")
	}
	if !Currencies[l.Currency] {
		problems = append(problems, fmt.Sprintf("unsupported currency %q", l.Currency))
	}
	at, err := time.Parse(time.RFC3339, l.Timestamp)
	if err != nil {
		problems = append(problems, "timestamp must be RFC 3339")
	}
	if len(problems) > 0 {
		return Transaction{}, fmt.Errorf("%w: %s", ErrInvalid, strings.Join(problems, "; "))
	}
	return Transaction{ID: l.ID, Account: l.Account, Amount: amount, Currency: l.Currency, At: at}, nil
}
