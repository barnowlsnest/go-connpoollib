package connpool

import (
	"errors"
	"net"
	"sync/atomic"
	"time"
)

const connCheckTimeout = time.Millisecond

// Conn is a pooled connection. It wraps a net.Conn and returns it to the pool
// on Close instead of closing it. A Conn must not be used after Close.
type Conn struct {
	createdAt time.Time
	idleSince time.Time
	net.Conn
	pool     *ConnPool
	released atomic.Bool
}

func newConn(conn net.Conn, pool *ConnPool) *Conn {
	return &Conn{
		Conn:      conn,
		pool:      pool,
		createdAt: time.Now(),
	}
}

// expired reports whether the connection outlived the configured lifespan, or
// has been idle for longer than the configured idle timeout.
func (c *Conn) expired(now time.Time) bool {
	cfg := c.pool.cfg
	if now.Sub(c.createdAt) >= cfg.LifespanTimeout() {
		return true
	}
	return !c.idleSince.IsZero() && now.Sub(c.idleSince) >= cfg.IdleTimeout()
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
