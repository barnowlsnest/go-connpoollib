# connpool

[![Go Reference](https://pkg.go.dev/badge/github.com/dshlychkou/connpool.svg)](https://pkg.go.dev/github.com/dshlychkou/connpool)
[![Build and Test](https://github.com/dshlychkou/connpool/actions/workflows/build.yml/badge.svg?branch=main)](https://github.com/dshlychkou/connpool/actions/workflows/build.yml)
[![Lint](https://github.com/dshlychkou/connpool/actions/workflows/lint.yml/badge.svg?branch=main)](https://github.com/dshlychkou/connpool/actions/workflows/lint.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/dshlychkou/connpool)](https://goreportcard.com/report/github.com/dshlychkou/connpool)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

A small, dependency-free Go pool of reusable `net.Conn` connections for
TCP, TLS, Unix sockets, or anything you can dial.

- **Bounded size and bounded dialing.** At most `MaxPoolSize` open
  connections and `MaxConcurrency` dials in flight, so a burst of traffic
  can't flood the upstream with connects.
- **Warm idle set.** A background worker keeps `MinIdle` connections open
  and evicts expired ones.
- **Idle and lifespan expiry.** Each connection gets a lifespan up to 10%
  shorter than the configured one, so connections opened together don't
  all reconnect together.
- **Health check on reuse.** A non-blocking socket peek (sub-microsecond on
  Unix) drops connections the peer closed, or that hold unread data, both
  before they are handed out and when they are returned.
- **Dial retries** with capped exponential backoff and optional full jitter.
- **Any dialer.** `*net.Dialer`, `*tls.Dialer`, a SOCKS proxy, or a test
  double, through `WithDialFunc`.
- **Stats** in the style of `sql.DBStats`, ready to export as metrics.
- **Drop-in `net.Conn`.** `*connpool.Conn` implements `net.Conn`, and
  `Close` returns it to the pool.

## Install

```sh
go get github.com/dshlychkou/connpool
```

Requires Go 1.27 or newer.

## Quick start

```go
package main

import (
	"bufio"
	"context"
	"fmt"
	"log"

	"github.com/dshlychkou/connpool"
)

func main() {
	cfg, err := connpool.NewConfig(
		connpool.WithHost("127.0.0.1"),
		connpool.WithPort(6379),
		connpool.WithMaxPoolSize(32),
		connpool.WithMinIdle(4),
	)
	if err != nil {
		log.Fatal(err)
	}

	pool, err := connpool.New(cfg)
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Close()

	conn, err := pool.Acquire(context.Background())
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close() // returns the connection to the pool

	if _, err := conn.Write([]byte("PING\r\n")); err != nil {
		_ = conn.Discard()
		log.Fatal(err)
	}
	reply, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		_ = conn.Discard()
		log.Fatal(err)
	}
	fmt.Print(reply)
}
```

## Usage

### Close or Discard

Every connection returned by `Acquire` must be released exactly once:

- **`conn.Close()`** returns it to the pool. If it can't be reused (it
  expired, the peer closed it, it has unread data, or the pool is closed
  or full), it is closed instead.
- **`conn.Discard()`** always closes it and frees its slot. Call it after
  a write timeout, a protocol error, or anything else that leaves the
  connection in a state the health check can't see, such as a request
  written but its response never read.

After either call, the `Conn` returns `net.ErrClosed` from `Read`, `Write`
and the deadline setters, because the pool may already have lent the
underlying connection to someone else. A connection that is never released
keeps its slot forever, and once all slots are leaked, `Acquire` waits
until it times out.

Deadlines you set are cleared when the connection goes back to the pool.

### TLS

```go
dialer := &tls.Dialer{
	NetDialer: &net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second},
	Config:    &tls.Config{ServerName: "db.example.com"},
}
cfg, err := connpool.NewConfig(
	connpool.WithHost("db.example.com"),
	connpool.WithPort(5432),
	connpool.WithDialFunc(dialer.DialContext),
)
```

`conn.NetConn()` returns the underlying `*tls.Conn`, for example to read
`ConnectionState()`. Don't close it or use it after releasing the pooled
connection.

### Unix sockets

```go
cfg, err := connpool.NewConfig(
	connpool.WithNetwork("unix"),
	connpool.WithHost("/var/run/app.sock"), // the socket path; no port needed
)
```

### Timeouts and cancellation

`Acquire(ctx)` waits for an idle connection or a free slot until `ctx` is
done or `AcquireTimeout` passes, whichever comes first. Dial retries count
against the same deadline. An idle connection that is ready right away is
returned without starting the timer.

```go
ctx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
defer cancel()
conn, err := pool.Acquire(ctx)
```

### Stats

```go
s := pool.Stats()
log.Printf("open=%d in_use=%d idle=%d waits=%d wait=%s dial_errors=%d",
	s.Open, s.InUse, s.Idle, s.WaitCount, s.WaitDuration, s.DialErrors)
```

| Field | Meaning |
|---|---|
| `MaxOpen` | configured `MaxPoolSize` |
| `Open`, `InUse`, `Idle` | current connections: all, checked out, idle |
| `WaitCount`, `WaitDuration` | `Acquire` calls that found no idle connection, and the total time they spent waiting |
| `Dials`, `DialErrors` | successful dials, and dials that failed after all attempts |
| `IdleClosed`, `LifespanClosed` | connections closed for exceeding `IdleTimeout` or their lifespan |
| `UnhealthyClosed` | connections closed by the health check |
| `Discarded` | connections closed by `Conn.Discard` |

`Stats` has JSON tags, and you can call it after `Close`. The gauges are
read without a lock, so under load they can briefly disagree with each
other.

### Errors

```go
conn, err := pool.Acquire(ctx)
switch {
case errors.Is(err, connpool.ErrPoolAlreadyClosed):
	// shutting down
case errors.Is(err, connpool.ErrMaxRetriesExceeded):
	// upstream unreachable: every dial attempt failed
case errors.Is(err, connpool.ErrDialFailed):
	// dial cancelled by ctx or AcquireTimeout; the dial error is wrapped too
case errors.Is(err, context.DeadlineExceeded):
	// no slot freed up in time
}
```

| Error | Returned when |
|---|---|
| `ErrNilConfig` | `New(nil)` |
| `ErrInvalidConnPoolCfg` | `NewConfig` or `New` rejects the config; the message names the field |
| `ErrPoolAlreadyClosed` | `Acquire` or `Close` on a closed pool |
| `ErrDialFailed` | opening a new connection failed; wraps the last dial error |
| `ErrMaxRetriesExceeded` | joined into `ErrDialFailed` when all `DialMaxAttempts` failed |

## Configuration

`NewConfig` applies options over these defaults and validates the result.

| Option | Default | Description |
|---|---|---|
| `WithNetwork(string)` | `"tcp"` | network passed to the dialer: `tcp`, `tcp4`, `unix`, ... |
| `WithHost(string)` | `"127.0.0.1"` | host to dial, or the socket path for Unix networks |
| `WithPort(int)` | none | port to dial; **required** for non-Unix networks |
| `WithMaxPoolSize(int)` | `10` | max open connections, idle and checked out combined |
| `WithMinIdle(int)` | `1` | idle connections kept open in the background; `0` disables it |
| `WithMaxConcurrency(int)` | `2` | max dials in flight at once |
| `WithIdleTimeout(d)` | `30s` | close a connection after it sits idle this long |
| `WithLifespanTimeout(d)` | `5m` | max connection age, randomly shortened by up to 10% per connection |
| `WithAcquireTimeout(d)` | `5s` | max time `Acquire` waits, dial retries included |
| `WithDialer(*net.Dialer)` | 5s timeout, 30s keep-alive | dialer used to open connections |
| `WithDialFunc(DialFunc)` | none | custom dial function, e.g. `(*tls.Dialer).DialContext` |
| `WithDialMaxAttempts(int)` | `3` | total dial attempts per connection, the first included |
| `WithDialBackoff(base, max)` | `100ms`, `2s` | retries wait `base`, `2·base`, `4·base`, ..., capped at `max` |
| `WithDialBackoffJitter(bool)` | `true` | full jitter: each wait is uniformly random in `[0, delay]` |

`WithDialer` and `WithDialFunc` set the same dialer, so the last one passed
wins. Every setting has a getter on `*Config`, and `pool.Config()` returns
the config in use.

## How it works

The pool is built on channels: a buffered channel holds the idle
connections, one semaphore channel caps open connections (slots), and
another caps concurrent dials (dial tokens). `Acquire` takes a ready idle
connection without blocking if one is available. Otherwise it waits for
whichever comes first: an idle connection, or a dial token and then a free
slot to dial into.

Before a connection is handed out, and again when it is returned, the pool
checks it:

1. **Expiry:** has it outlived its lifespan or `IdleTimeout`?
2. **Health:** on Unix, a non-blocking `recv(MSG_PEEK)` on the socket.
   No data means healthy, EOF means the peer closed it, and pending bytes
   mean the connection is out of sync. For wrapped connections such as
   `*tls.Conn`, pending bytes may just be protocol records (like TLS 1.3
   session tickets), so the pool falls back to a 1 ms read through the
   wrapper.

On platforms without the socket peek (Windows, for example), and for
connections without a file descriptor (`net.Pipe`), the health check is
always the 1 ms read, which adds about 1 ms to each `Acquire` and `Close`.

When `MinIdle` is positive, a background worker tops up the idle set
whenever a connection is taken or closed, and every
`min(IdleTimeout, LifespanTimeout) / 2` (15 s by default) it also evicts
expired idle connections.

`Close` stops the background worker, closes the idle connections and makes
further `Acquire` calls fail. Connections still checked out are closed
when their holders release them.

## Limitations and known issues

The module has no tagged release yet and is pre-1.0, so the API may still
change.

**Behaviour**

- **Waiters are not served first-come-first-served.** Goroutines blocked in
  `Acquire` race for the next idle connection or slot, so under heavy
  contention some callers wait longer than others.
- **Idle connections are reused oldest-first.** Light, steady traffic
  rotates through every idle connection, so `IdleTimeout` rarely shrinks a
  pool that grew during a burst. `LifespanTimeout` still caps each
  connection's age.
- **The health check sees transport state only.** It can't tell that a
  protocol exchange was left half-done, such as a request written but its
  response never read. Call `Discard` in that case.
- **Leaked connections aren't tracked.** A `Conn` that is never closed or
  discarded holds its slot until the pool closes. `Stats().InUse` shows
  the leak, but not who holds the connection.

**Performance**

- **1 ms health check without a socket peek.** On non-Unix platforms
  (Windows, for example) and for connections without a file descriptor
  (`net.Pipe`), each `Acquire` of an idle connection and each `Close` adds
  about 1 ms. CI doesn't run on Windows.
- **1 ms health check on fresh TLS connections.** With TLS 1.3, unread
  session tickets usually sit on a new connection's socket, so its first
  check after the handshake takes the 1 ms read through `*tls.Conn`.
- **The background worker competes for dial capacity.** While it tops up
  the idle set it holds one of the `MaxConcurrency` dial tokens, for up to
  `AcquireTimeout` when the upstream is down. Callers of `Acquire` then
  have fewer dials available, so keep `MaxConcurrency` above 1 when
  `MinIdle` is positive.
- **A failed refill isn't retried right away.** After a failed dial, the
  background worker tries again only on its next trigger, so after an
  outage the idle set can stay empty for up to one eviction interval.
  `Acquire` still dials on demand.

**Stats**

- **Gauges can briefly disagree.** They are read without a lock, and
  `Open` and `InUse` don't count dials still in progress.

## Contributing

Issues and pull requests are welcome. The project uses
[Task](https://taskfile.dev) and
[golangci-lint](https://golangci-lint.run):

```sh
task sanity    # tidy, fmt, vet, lint, test with -race
task go-test   # tests only
task go-bench  # benchmarks, 100 iterations each
```

CI runs the tests on Linux and macOS, because the socket health check
behaves differently on each. It also runs the benchmarks for 100
iterations each, to check that they still build and run; it doesn't
compare timings, so check a hot-path change with
`go test -run '^$' -bench . -count 10` and
[benchstat](https://pkg.go.dev/golang.org/x/perf/cmd/benchstat).

## License

[MIT](LICENSE) © 2026 Denis Shlychkou
