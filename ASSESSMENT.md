# connpool: performance and resource-leak assessment

Date: 2026-10-11, at commit `efc5a2c` (branch `refactor-inspect-performans`).

**Verdict: not production-ready yet.** The pool is correct and leak-free under
stress, but every checkout costs ~2.3 ms, which defeats the purpose of pooling.
The remaining gaps are smaller.

Method: a throwaway benchmark plus a leak stress test, run under `-race`
(Go 1.27.1, darwin/arm64).

## Performance

| Benchmark | Result |
|---|---|
| Acquire + Close, 1 goroutine | **2.35 ms/op**, 548 B, 11 allocs |
| Acquire + Close, 40 goroutines, pool of 64 | 59 µs/op aggregate, each call still waits ~2.3 ms |
| `reusable()` probe alone | **1.26 ms/op** |

### 1. The health probe blocks for its full timeout (critical)

`conn.go`, `reusable()`, sets a 1 ms read deadline and does a blocking `Read`.
On a healthy idle connection the read only returns when the deadline fires,
so it always waits the full 1 ms or more. It runs in `takeIdle` and again in
`Conn.Close`, so every borrow-and-return costs at least 2 ms. A single
goroutine tops out at ~425 checkouts/s, while a loopback round trip takes
microseconds.

The background eviction runs the same probe and holds each idle connection
for ~1 ms. Meanwhile `Acquire` callers see an empty idle set and dial extra
connections.

**Fix:** a non-blocking check through `SyscallConn().Read` with a non-blocking
read: `EAGAIN` means healthy, 0 bytes means the peer closed it, any data means
the connection is dirty. This is the approach of go-sql-driver/mysql's
`connCheck`. It costs ~1 µs and needs no deadlines. It needs a separate build
for Unix and Windows.

### 2. Per-call overhead

- Each `Acquire` creates a `context.WithTimeout` (and its timer), even when an
  idle connection is ready.
- Each dial starts an extra goroutine in `openConnection` only to watch for
  pool shutdown. `context.AfterFunc` avoids it.
- Each `Close` allocates a new `Conn` wrapper. This is required for the
  use-after-Close protection and is cheap.

### 3. Synchronized lifespan expiry (low)

`LifespanTimeout` has no jitter, so connections opened together expire
together and reconnect together. A ~10% per-connection jitter fixes it.

## Resource leaks

- **No leaks found.** Stress test: 20 rounds × 32 goroutines × 50
  acquire/close cycles, short idle and lifespan timeouts, pool closed
  mid-round, run 3× under `-race`. All slots were returned and the goroutine
  count went back to baseline.
- **Shutdown is clean.** Idle connections are drained and closed; checked-out
  connections are closed when their holders call `Close`; in-flight
  background dials are cancelled on pool close.
- **Caller-side leak.** A caller that never closes an acquired `Conn` loses
  its slot for good, and enough of them hang the pool until `AcquireTimeout`.
  Normal for pools, but there is no checkout tracking or stats to catch it.
- **Lock-order inversion (low).** `fillIdle` takes a slot then a dial token;
  `Acquire` takes a token then a slot. In a narrow window both can wait on each
  other until `AcquireTimeout`, failing an `Acquire` spuriously. Taking the
  token first in `fillIdle` removes it.

## Other gaps before production

- **No metrics.** Nothing reports in-use/idle counts, wait counts and
  durations, dials or evictions. A `Stats()` method like `sql.DBStats` is
  needed to size and debug the pool.
- **Only `*net.Dialer` is accepted.** No TLS and no custom dial function
  (proxy, mock). Accept `func(ctx, network, addr) (net.Conn, error)`.
- **Waiters are not FIFO.** Under contention some callers wait much longer
  than others.
- **`go 1.27.1` in `go.mod`** forces consumers onto the newest toolchain.
- **No README** and no tagged release.

## Strengths

The `Close` race is fixed, shutdown semantics are clear, and the test suite is
broad and runs under `-race`.

## Path to production

1. Non-blocking health probe (blocker).
2. `Stats()`.
3. Custom dial function / TLS, before other teams depend on the API.

## Follow-up: fixes for items 1–3

| Benchmark | Before | After |
|---|---|---|
| Acquire + Close, 1 goroutine | 2.35 ms/op, 548 B, 11 allocs | **833 ns/op**, 146 B, 7 allocs |
| Acquire + Close, 40 goroutines | 59 µs/op | **1.8 µs/op** |
| `reusable()` probe | 1.26 ms/op | **325 ns/op** |

1. **Probe:** `probe_unix.go` does a non-blocking read through `SyscallConn`.
   Platforms without it (`probe_other.go`, e.g. Windows) and connections
   without a file descriptor (e.g. `net.Pipe`) still use the 1 ms blocking
   read.
2. **Per-call overhead:** `Acquire` takes a ready idle connection before
   creating its timeout context. The per-dial watcher goroutine stays: a
   `context.AfterFunc` would need the pool to store a `context.Context`, and
   the goroutine costs ~1 µs next to a dial measured in milliseconds.
3. **Lifespan jitter:** each connection's lifespan is randomly up to 10%
   shorter than `LifespanTimeout`.

The leak stress test (200 rounds × 32 goroutines × 50 cycles, pool closed
mid-round, 15 runs, 5 under `-race`) still shows no leaked slots or
goroutines.
