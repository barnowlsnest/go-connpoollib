package connpool

import (
	"errors"
	"net"
	"sync/atomic"
	"time"
)

const (
	connCheckTimeout = time.Millisecond
	// lifespanJitterDiv shortens each connection's lifespan by up to 1/10, so
	// connections opened together do not all expire together.
	lifespanJitterDiv = 10
)

// Conn is a pooled connection. It wraps a net.Conn and returns it to the pool
// on Close instead of closing it. A Conn must not be used after Close.
type Conn struct {
	createdAt time.Time
	idleSince time.Time
	net.Conn
	pool     *ConnPool
	lifespan time.Duration
	released atomic.Bool
}

func newConn(conn net.Conn, pool *ConnPool) *Conn {
	lifespan := pool.cfg.LifespanTimeout()
	if jitter := lifespan / lifespanJitterDiv; jitter > 0 {
		lifespan -= randDuration(jitter)
	}
	return &Conn{
		Conn:      conn,
		pool:      pool,
		createdAt: time.Now(),
		lifespan:  lifespan,
	}
}

// expired reports whether the connection outlived its lifespan, or has been
// idle for longer than the configured idle timeout.
func (c *Conn) expired(now time.Time) bool {
	return c.lifespanEnded(now) || c.idleTooLong(now)
}

func (c *Conn) lifespanEnded(now time.Time) bool {
	return now.Sub(c.createdAt) >= c.lifespan
}

func (c *Conn) idleTooLong(now time.Time) bool {
	return !c.idleSince.IsZero() && now.Sub(c.idleSince) >= c.pool.cfg.IdleTimeout()
}

// Close returns the connection to the pool. A connection that is no longer
// reusable (unread data, peer closed, pool closed or full) is closed instead.
// Calling Close more than once returns net.ErrClosed.
func (c *Conn) Close() error {
	if c.released.Swap(true) {
		return net.ErrClosed
	}
	// Skip the reusable probe when release would reject the conn anyway.
	if c.pool.closed.Load() || c.pool.unfit(c, time.Now()) {
		return c.pool.closeConn(c)
	}

	return c.pool.release(c)
}

// Read returns net.ErrClosed after Close: the connection may be lent out again.
func (c *Conn) Read(b []byte) (int, error) {
	if c.released.Load() {
		return 0, net.ErrClosed
	}
	return c.Conn.Read(b)
}

// Write returns net.ErrClosed after Close: the connection may be lent out again.
func (c *Conn) Write(b []byte) (int, error) {
	if c.released.Load() {
		return 0, net.ErrClosed
	}
	return c.Conn.Write(b)
}

// SetDeadline returns net.ErrClosed after Close.
func (c *Conn) SetDeadline(t time.Time) error {
	if c.released.Load() {
		return net.ErrClosed
	}
	return c.Conn.SetDeadline(t)
}

// SetReadDeadline returns net.ErrClosed after Close.
func (c *Conn) SetReadDeadline(t time.Time) error {
	if c.released.Load() {
		return net.ErrClosed
	}
	return c.Conn.SetReadDeadline(t)
}

// SetWriteDeadline returns net.ErrClosed after Close.
func (c *Conn) SetWriteDeadline(t time.Time) error {
	if c.released.Load() {
		return net.ErrClosed
	}
	return c.Conn.SetWriteDeadline(t)
}

// sockState is the result of a non-blocking peek at a socket.
type sockState int

const (
	sockUnknown sockState = iota // no file descriptor or unsupported platform
	sockIdle                     // nothing to read
	sockData                     // unread bytes
	sockClosed                   // EOF or error
)

// reusable reports whether the connection is idle and healthy: nothing is
// waiting to be read and the peer has not closed it. Deadlines are cleared.
func (c *Conn) reusable() bool {
	// Clear deadlines first: an expired one would fail the probe.
	if err := c.Conn.SetDeadline(time.Time{}); err != nil {
		return false
	}

	sock, wrapped := c.Conn, false
	if w, ok := sock.(interface{ NetConn() net.Conn }); ok { // e.g. *tls.Conn
		sock, wrapped = w.NetConn(), true
	}
	switch peekSocket(sock) {
	case sockIdle:
		return true
	case sockClosed:
		return false
	case sockData:
		// Under a wrapper the bytes may be protocol records (e.g. TLS session
		// tickets) that only the wrapper's Read can tell from caller data.
		if !wrapped {
			return false
		}
	case sockUnknown:
	}

	// Fallback: a short blocking read that must time out.
	if err := c.Conn.SetReadDeadline(time.Now().Add(connCheckTimeout)); err != nil {
		return false
	}
	_, err := c.Conn.Read(make([]byte, 1))
	var nerr net.Error
	if !errors.As(err, &nerr) || !nerr.Timeout() {
		return false
	}

	return c.Conn.SetDeadline(time.Time{}) == nil
}
