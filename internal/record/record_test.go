package record

import (
	"errors"
	"strings"
	"testing"
	"time"
)

func TestParse(t *testing.T) {
	tests := []struct {
		name    string
		line    string
		wantErr string
	}{
		{"valid", `{"id":"t1","account":"acc-1","amount":1990,"currency":"USD","timestamp":"2026-11-01T10:00:00Z"}`, ""},
		{"malformed", `{"id":`, "malformed JSON"},
		{"missing id", `{"account":"a","amount":1,"currency":"BRL","timestamp":"2026-11-01T10:00:00Z"}`, "id is required"},
		{"zero amount", `{"id":"t","account":"a","amount":0,"currency":"BRL","timestamp":"2026-11-01T10:00:00Z"}`, "positive integer"},
		{"decimal amount", `{"id":"t","account":"a","amount":19.9,"currency":"BRL","timestamp":"2026-11-01T10:00:00Z"}`, "positive integer"},
		{"unknown currency", `{"id":"t","account":"a","amount":1,"currency":"JPY","timestamp":"2026-11-01T10:00:00Z"}`, `unsupported currency "JPY"`},
		{"bad timestamp", `{"id":"t","account":"a","amount":1,"currency":"BRL","timestamp":"01/11/2026"}`, "RFC 3339"},
		{"several problems at once", `{"amount":-1,"currency":"XX","timestamp":"x"}`, "id is required; account is required"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tx, err := Parse([]byte(tt.line))
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if tx.Amount != 1990 || tx.Day() != "2026-11-01" || !tx.At.Equal(time.Date(2026, 11, 1, 10, 0, 0, 0, time.UTC)) {
					t.Fatalf("parsed %+v", tx)
				}
				return
			}
			if !errors.Is(err, ErrInvalid) || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("error = %v; want ErrInvalid containing %q", err, tt.wantErr)
			}
		})
	}
}
