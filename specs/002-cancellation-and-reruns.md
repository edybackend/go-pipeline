# 002 · Cancellation, fatal errors and reruns

## Context
Long jobs get interrupted: a deploy, Ctrl+C, a full disk. An interruption must leave consistent data and a rerun must finish the job without duplicates.

## Behavior
- `SIGINT`/`SIGTERM` cancels the context: reading stops, in-flight records finish or are dropped, the current batch is committed, the report is printed with `interrupted: true`, exit code 130.
- A fatal error (e.g. the database cannot be written) cancels every stage through `errgroup` and is returned; no goroutine is left running.
- Writes are idempotent (`INSERT … ON CONFLICT(id) DO NOTHING`): running the same file again after an interruption stores only what was missing and reports the rest as duplicates.
- `-timeout` bounds the whole run.

## Acceptance criteria
- Cancel mid-run, then rerun: the database holds every valid record exactly once.
- A failing writer stops the pipeline with its error, and the goroutine count returns to the baseline.

## Out of scope
Exactly-once across systems, checkpoint files (idempotent writes make them unnecessary here).
