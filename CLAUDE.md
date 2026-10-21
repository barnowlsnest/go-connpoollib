# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

`connpool` is a single-package, dependency-free (testify only in tests) Go library: a bounded pool of reusable `net.Conn`. Module `github.com/brnowlsnest/go-connpoollib/v2` (the package lives in `pkg/connpool`, import path `github.com/brnowlsnest/go-connpoollib/v2/pkg/connpool`), `go 1.27` (deliberate; don't lower it). Pre-1.0, no tagged release.

## Commands

Uses [Task](https://taskfile.dev) (`Taskfile.yaml`) and golangci-lint v2 (CI pins v2.14.0).

```sh
task sanity     # go mod tidy, clean testcache, fmt, vet, golangci-lint run --fix, test
task go-test    # go test -v -race -cover ./...
task go-bench   # go test -run '^$' -bench . -benchtime 100x ./...
task go-lint    # golangci-lint run --fix

go test -race -run 'TestPoolSuite/TestAcquireOverTLS' ./...   # single test (suite/method)
go test -run '^$' -bench . -count 10 ./...                      # real benchmark numbers, compare with benchstat
```

Tests always run with `-race`. CI (`.github/workflows/`) runs build, test and a benchmark smoke run on ubuntu and macos, because `probe_unix.go` behaves differently per OS; lint runs on ubuntu.

## Architecture

All source files below live in `pkg/connpool/`. The pool (`pool.go`) is built entirely on channels, plus one mutex:

- `idleChan` (buffered, cap `MaxPoolSize`): idle `*Conn`s, reused oldest-first.
- `slots` (semaphore, cap `MaxPoolSize`): one token per open connection. Every open connection holds a slot; `dropConn` is the only place that frees it (and decrements `stats.open`).
- `dialTokens` (semaphore, cap `MaxConcurrency`): caps concurrent dials.
- `closedChan` / `closed` atomic: shutdown signal. `mux` only serializes `putIdle` against `Close` closing `idleChan`, so a release never sends on a closed channel.
- `refillChan` (cap 1): nudges the `maintainIdle` worker (started only when `MinIdle > 0`), which also evicts expired idle conns on a ticker of `min(IdleTimeout, LifespanTimeout)/2`.

**Lock ordering invariant:** always take a dial token *before* a slot. `dialIdle` additionally never blocks on the slot; holding a slot while waiting for a token deadlocks against an `Acquire` holding the token (covered by `TestFillIdleTakesDialTokenBeforeSlot`).

`Acquire` flow: non-blocking `acquireIdle` first (no timeout context), then loop `acquireOnce` under `AcquireTimeout`, selecting on idle conn vs. dial token → `dialIntoSlot` (idle conn vs. slot) → `openConnection` (retries with backoff from `Config.dialDelay`; cancelled when the pool closes). `errConnDiscarded` means "idle conn was unfit, try again".

`Conn` (`conn.go`) wraps `net.Conn`. `Close` returns it to the pool by putting a **fresh `*Conn` copy** into `idleChan` (in `release`), so the caller's handle stays `released` and returns `net.ErrClosed` forever, even after the underlying conn is lent out again. `Discard` always closes. Each conn's lifespan is shortened by up to 10% (crypto/rand jitter).

Health check (`Conn.reusable`, called via `pool.unfit`, which also counts close reasons into stats): clears deadlines, then `peekSocket` — a non-blocking `recv(MSG_PEEK)` on Unix (`probe_unix.go`), `sockUnknown` elsewhere (`probe_other.go`). Unknown state, or pending data under a wrapper with `NetConn()` (e.g. `*tls.Conn` holding TLS 1.3 session tickets), falls back to a 1 ms read that must time out.

`config.go`: functional options over defaults via `NewConfig`, validated by `Config.validate` (also re-run by `New`); all errors wrap `ErrInvalidConnPoolCfg`. `stats.go`: atomic counters, `Stats()` snapshot with lock-free gauges.

## Conventions

- Tests: testify `suite`/`require`/`assert` only, including `Eventually` for waits — no hand-rolled polling loops or sleeps as assertions. Tests for `x.go` live in `x_test.go`. Pool/conn/stats suites share `baseSuite` in `pool_test.go` (local listener, `newPool`, TLS configs, `accept`, `eventually`). Don't add one test per fix; cover real behavior and races.
- Comments: short, only where non-obvious.
- Lint: never add `//nolint` or config excludes; change the code instead. The one accepted exception is the existing `//nolint:gocyclo` on `Config.validate`. `govet` `fieldalignment` is on for non-test code, so order struct fields accordingly. Line length 140.
- Keep README's config/stats/error tables in sync with API changes.
