package store

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/edybackend/go-pipeline/internal/record"
)

func tx(id, account string, brl int64) record.Enriched {
	return record.Enriched{
		Transaction: record.Transaction{ID: id, Account: account, Amount: brl, Currency: "BRL", At: time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)},
		Rate:        1,
		AmountBRL:   brl,
	}
}

func TestBatchesAreIdempotentAndTotalsComeFromSQL(t *testing.T) {
	db, err := Open(t.Context(), filepath.Join(t.TempDir(), "p.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })

	first, err := db.WriteBatch(t.Context(), []record.Enriched{tx("1", "a", 100), tx("2", "b", 500), tx("3", "a", 50)})
	if err != nil || first != 3 {
		t.Fatalf("first batch: %d, %v", first, err)
	}
	second, err := db.WriteBatch(t.Context(), []record.Enriched{tx("3", "a", 50), tx("4", "a", 25)})
	if err != nil || second != 1 {
		t.Fatalf("second batch: %d new, %v; want 1 (id 3 already stored)", second, err)
	}

	top, err := db.TopAccounts(t.Context(), 5)
	if err != nil {
		t.Fatal(err)
	}
	want := []AccountTotal{{"b", 1, 500}, {"a", 3, 175}}
	if len(top) != 2 || top[0] != want[0] || top[1] != want[1] {
		t.Fatalf("top = %+v; want %+v", top, want)
	}
}
