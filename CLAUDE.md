# Engineering rules for AI agents

This file is loaded by Claude Code in every session. It defines how changes are made in this repository.
Hooks in `.claude/settings.json` enforce the parts that can be automated.

## Workflow
1. **Spec first.** A new behavior starts as a spec in `specs/NNN-name.md` (context, behavior, acceptance criteria, out of scope).
2. **Small steps.** One concern per commit, [Conventional Commits](https://www.conventionalcommits.org/) (`feat(pipeline): ...`, `fix(fx): ...`).
3. **Definition of done** = the CI gates pass locally: `gofmt -l .` is empty, `go vet ./...`, `go test -race ./...`, `golangci-lint run`. The Stop hook blocks finishing while they are red.

## Layout
- `cmd/pipeline` parses flags, wires the stages, handles signals and prints the report. No logic.
- `internal/pipeline` owns the stages and their channels; it depends on small interfaces (`Rates`, `Writer`) declared there.
- `internal/fx` is the only place that talks to the rate source. `internal/store` is the only place that talks to SQLite.

## Concurrency rules
- **Every channel between stages is bounded**, sized from `Config`. Backpressure is a feature; a test proves the reader stops when the writer is stuck. A hook blocks huge literal buffers.
- **Every goroutine can be stopped.** Pass `ctx` in; never create `context.Background()`/`TODO()` in `internal/` (hook). Wait with `select` on `ctx.Done()`, never `time.Sleep` (hook).
- **Every goroutine has an owner** that waits for it (`errgroup`, `sync.WaitGroup`). The channel's sender closes it. Tests check the goroutine count after a fatal error.
- **Bad data is not an error.** Invalid lines and records whose rate never comes are rejected with a reason and the run continues; only infrastructure failures stop it.
- **Writes are idempotent** (`ON CONFLICT(id) DO NOTHING`), which is what makes a rerun after an interruption safe. Keep it that way.
- Money is an integer in minor units; conversions use integer arithmetic on a scaled rate.

## Go conventions
- Errors are returned and wrapped with `%w`; sentinel errors (`record.ErrInvalid`, `fx.ErrUnavailable`) mark what is expected. No `panic`.
- Table-driven tests; `go test -race` must pass. `cmd/` + `internal/`, no `pkg/`.

## Never
- Edit `data/`, the SQLite files or `go.sum` by hand. Blocked by hook.
- Read or write secrets (`.env`, keys). Blocked by permissions and hook.
- Disable a linter or a test to make the build pass.
- Add a dependency without a reason stated in the commit message.
