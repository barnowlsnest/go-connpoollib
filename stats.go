package connpool

import (
	"sync/atomic"
	"time"
)

// Stats is a snapshot of pool statistics. Gauges are read without a lock, so
// they may be briefly inconsistent with each other under load.
type Stats struct {
	// MaxOpen is the configured MaxPoolSize.
	MaxOpen int `json:"max_open"`
	// Open is the number of established connections, in use or idle.
	Open int `json:"open"`
	// InUse is the number of connections checked out by callers.
	InUse int `json:"in_use"`
	// Idle is the number of connections waiting in the idle set.
	Idle int `json:"idle"`

	// WaitCount is the number of Acquire calls that found no idle connection
	// ready and had to dial or wait.
	WaitCount int64 `json:"wait_count"`
	// WaitDuration is the total time those calls spent in Acquire.
	WaitDuration time.Duration `json:"wait_duration_ns"`

	// Dials is the number of connections dialed successfully.
	Dials int64 `json:"dials"`
	// DialErrors is the number of dials that failed after all attempts.
	DialErrors int64 `json:"dial_errors"`

	// IdleClosed is the number of connections closed for exceeding IdleTimeout.
	IdleClosed int64 `json:"idle_closed"`
	// LifespanClosed is the number of connections closed for exceeding LifespanTimeout.
	LifespanClosed int64 `json:"lifespan_closed"`
	// UnhealthyClosed is the number of connections closed by a failed health
	// probe: unread data, or closed by the peer.
	UnhealthyClosed int64 `json:"unhealthy_closed"`
}

type poolStats struct {
	open            atomic.Int64
	waitCount       atomic.Int64
	waitDuration    atomic.Int64
	dials           atomic.Int64
	dialErrors      atomic.Int64
	idleClosed      atomic.Int64
	lifespanClosed  atomic.Int64
	unhealthyClosed atomic.Int64
}

// Stats returns a snapshot of the pool statistics. It is safe to call after Close.
func (pool *ConnPool) Stats() Stats {
	open := int(pool.stats.open.Load())
	idle := len(pool.idleChan)
	return Stats{
		MaxOpen:         pool.cfg.MaxPoolSize(),
		Open:            open,
		InUse:           max(open-idle, 0),
		Idle:            idle,
		WaitCount:       pool.stats.waitCount.Load(),
		WaitDuration:    time.Duration(pool.stats.waitDuration.Load()),
		Dials:           pool.stats.dials.Load(),
		DialErrors:      pool.stats.dialErrors.Load(),
		IdleClosed:      pool.stats.idleClosed.Load(),
		LifespanClosed:  pool.stats.lifespanClosed.Load(),
		UnhealthyClosed: pool.stats.unhealthyClosed.Load(),
	}
}

func (s *poolStats) recordWait(start time.Time) {
	s.waitCount.Add(1)
	s.waitDuration.Add(int64(time.Since(start)))
}

// unfit reports whether conn must be closed instead of reused, counting why.
func (pool *ConnPool) unfit(conn *Conn, now time.Time) bool {
	switch {
	case conn.lifespanEnded(now):
		pool.stats.lifespanClosed.Add(1)
	case conn.idleTooLong(now):
		pool.stats.idleClosed.Add(1)
	case !conn.reusable():
		pool.stats.unhealthyClosed.Add(1)
	default:
		return false
	}
	return true
}
