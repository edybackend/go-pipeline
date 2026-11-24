// Package store writes enriched transactions to SQLite in batches.
package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	_ "modernc.org/sqlite" // registers the "sqlite" driver

	"github.com/edybackend/go-pipeline/internal/record"
)

const schema = `
CREATE TABLE IF NOT EXISTS transactions (
	id          TEXT PRIMARY KEY,
	account     TEXT NOT NULL,
	amount      INTEGER NOT NULL,
	currency    TEXT NOT NULL,
	at          TEXT NOT NULL,
	rate        REAL NOT NULL,
	amount_brl  INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_transactions_account ON transactions (account);`

// SQLite is the pipeline's sink.
type SQLite struct {
	db *sql.DB
}

// Open opens (or creates) the database at path.
func Open(ctx context.Context, path string) (*SQLite, error) {
	if dir := filepath.Dir(path); dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, fmt.Errorf("create data directory: %w", err)
		}
	}
	db, err := sql.Open("sqlite", path+"?_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)&_pragma=busy_timeout(5000)")
	if err != nil {
		return nil, fmt.Errorf("open sqlite %s: %w", path, err)
	}
	db.SetMaxOpenConns(1) // one writer
	if _, err := db.ExecContext(ctx, schema); err != nil {
		return nil, errors.Join(fmt.Errorf("apply schema: %w", err), db.Close())
	}
	return &SQLite{db: db}, nil
}

// Close releases the database.
func (s *SQLite) Close() error { return s.db.Close() }

// WriteBatch stores a batch in one transaction and returns how many rows were new.
// The insert is idempotent (the id is the key), so rerunning a file after an interruption is safe.
func (s *SQLite) WriteBatch(ctx context.Context, batch []record.Enriched) (int, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("begin batch: %w", err)
	}
	defer func() { _ = tx.Rollback() }() // no-op after Commit

	stmt, err := tx.PrepareContext(ctx,
		`INSERT INTO transactions (id, account, amount, currency, at, rate, amount_brl)
		 VALUES (?, ?, ?, ?, ?, ?, ?) ON CONFLICT(id) DO NOTHING`)
	if err != nil {
		return 0, fmt.Errorf("prepare insert: %w", err)
	}
	defer func() { _ = stmt.Close() }()

	inserted := 0
	for _, e := range batch {
		res, err := stmt.ExecContext(ctx, e.ID, e.Account, e.Amount, e.Currency, e.At.UTC().Format(time.RFC3339), e.Rate, e.AmountBRL)
		if err != nil {
			return 0, fmt.Errorf("insert %s: %w", e.ID, err)
		}
		if n, _ := res.RowsAffected(); n == 1 {
			inserted++
		}
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("commit batch: %w", err)
	}
	return inserted, nil
}

// AccountTotal is a line of the report.
type AccountTotal struct {
	Account      string
	Transactions int64
	TotalBRL     int64
}

// TopAccounts returns the accounts with the largest BRL volume.
func (s *SQLite) TopAccounts(ctx context.Context, n int) ([]AccountTotal, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT account, COUNT(*), SUM(amount_brl) FROM transactions GROUP BY account ORDER BY SUM(amount_brl) DESC, account LIMIT ?`, n)
	if err != nil {
		return nil, fmt.Errorf("top accounts: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []AccountTotal
	for rows.Next() {
		var a AccountTotal
		if err := rows.Scan(&a.Account, &a.Transactions, &a.TotalBRL); err != nil {
			return nil, fmt.Errorf("scan account total: %w", err)
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

// Count returns how many transactions are stored.
func (s *SQLite) Count(ctx context.Context) (int64, error) {
	var n int64
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM transactions`).Scan(&n); err != nil {
		return 0, fmt.Errorf("count: %w", err)
	}
	return n, nil
}
