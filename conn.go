package connpool

import (
	"errors"
	"net"
	"sync/atomic"
	"time"
)

const connCheckTimeout = time.Millisecond

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
	if c.expired(time.Now()) || !c.reusable() {
		return c.pool.closeConn(c)
	}

	return c.pool.release(c)
}

// reusable reports whether the connection is idle and healthy: nothing is
// waiting to be read and the peer has not closed it. Deadlines are cleared.
func (c *Conn) reusable() bool {
	if err := c.SetReadDeadline(time.Now().Add(connCheckTimeout)); err != nil {
		return false
	}
	_, err := c.Read(make([]byte, 1))
	var nerr net.Error
	if !errors.As(err, &nerr) || !nerr.Timeout() {
		return false
	}

	return c.SetDeadline(time.Time{}) == nil
}
