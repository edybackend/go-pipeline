# 001 · Ingest a transactions file

## Context
A partner sends a large JSON Lines file of card transactions in several currencies. Each one must be validated, converted to BRL with the exchange rate of its day, and stored. The file is too big to load at once and the rate service is slow and unreliable.

## Behavior
- `pipeline generate -n 100000 -o data/transactions.jsonl` writes a sample file (deterministic seed; about 1 % of the lines are invalid on purpose).
- `pipeline run -i data/transactions.jsonl -workers 16` processes it:
  1. **read** lines (streaming, bounded channel);
  2. **parse and validate**: id, account, positive amount in minor units, currency `BRL|USD|EUR|GBP`, RFC 3339 timestamp;
  3. **enrich** with a pool of workers: rate for (currency, day), amount in BRL;
  4. **write** in batches (one transaction per batch) to SQLite.
- Invalid lines go to `rejects.jsonl` with the line number and reason; they never stop the run.
- At the end it prints a report: read, stored, rejected, duplicates skipped, rate lookups vs cache hits, retries, duration and throughput. The top accounts by BRL volume come from SQL.

## Acceptance criteria
- Memory stays bounded: every stage talks through a channel with a fixed capacity, so a slow stage slows the readers (backpressure) instead of buffering the file.
- Rate lookups for the same (currency, day) are shared: concurrent requests make one call (`singleflight`), later ones hit the cache.
- A transient rate failure is retried with exponential backoff (max 5 attempts); a record whose rate never comes is rejected, the run continues.

## Out of scope
Other file formats, distributed processing.
