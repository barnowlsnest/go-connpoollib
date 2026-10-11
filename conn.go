package connpool

import (
	"errors"
	"math/rand/v2"
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
		lifespan -= rand.N(jitter) //nolint:gosec // jitter, not security
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
	if now.Sub(c.createdAt) >= c.lifespan {
		return true
	}
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
	if c.pool.closed.Load() || c.expired(time.Now()) || !c.reusable() {
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

// reusable reports whether the connection is idle and healthy: nothing is
// waiting to be read and the peer has not closed it. Deadlines are cleared.
func (c *Conn) reusable() bool {
	// Clear deadlines first: an expired one would fail the probe.
	if err := c.Conn.SetDeadline(time.Time{}); err != nil {
		return false
	}
	if idle, ok := probeIdle(c.Conn); ok {
		return idle
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
