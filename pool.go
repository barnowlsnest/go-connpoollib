package connpool

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/barnowlsnest/go-asynctasklib/v2/pkg/task"
)

// ConnPool is a pool of reusable connections. It maintains a set of idle connections and dials new ones on demand,
// up to MaxPoolSize open connections and MaxConcurrency concurrent dials.
// It is safe for concurrent use.
// Connections are closed when they expire or become unfit for reuse.
type ConnPool struct {
	idleChan   chan *Conn
	closedChan chan struct{}
	refillChan chan struct{}
	slots      chan struct{}
	dialTokens chan struct{}
	cfg        *Config
	mux        sync.Mutex
	wg         sync.WaitGroup
	closed     atomic.Bool
}

// errConnDiscarded reports that an idle connection was unfit and closed; the
// caller should try again.
var errConnDiscarded = errors.New("idle connection discarded")

// New returns a pool for cfg and, if MinIdle is positive, starts filling it
// with idle connections in the background. Call Close to release resources.
func New(cfg *Config) (*ConnPool, error) {
	if cfg == nil {
		return nil, ErrNilConfig
	}
	if err := cfg.validate(); err != nil {
		return nil, err
	}

	pool := &ConnPool{
		closedChan: make(chan struct{}),
		refillChan: make(chan struct{}, 1),
		idleChan:   make(chan *Conn, cfg.MaxPoolSize()),
		slots:      make(chan struct{}, cfg.MaxPoolSize()),
		dialTokens: make(chan struct{}, cfg.MaxConcurrency()),
		cfg:        cfg,
	}
	if cfg.MinIdle() > 0 {
		pool.wg.Go(pool.maintainIdle)
	}

	return pool, nil
}

// Config returns the pool configuration.
func (pool *ConnPool) Config() *Config {
	return pool.cfg
}

// Acquire returns an idle connection or dials a new one. It waits up to
// AcquireTimeout, or until ctx is done, for a free slot. The caller must Close
// the returned connection to return it to the pool.
func (pool *ConnPool) Acquire(ctx context.Context) (*Conn, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	// An idle connection needs no timeout context.
	if conn, err := pool.acquireIdle(); conn != nil || err != nil {
		return conn, err
	}

	ctx, cancel := context.WithTimeout(ctx, pool.cfg.AcquireTimeout())
	defer cancel()

	for {
		conn, err := pool.acquireOnce(ctx)
		if errors.Is(err, errConnDiscarded) {
			continue
		}
		return conn, err
	}
}

func (pool *ConnPool) acquireOnce(ctx context.Context) (*Conn, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	// Prefer an idle connection over dialing a new one.
	if conn, err := pool.acquireIdle(); conn != nil || err != nil {
		return conn, err
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-pool.closedChan:
		return nil, ErrPoolAlreadyClosed
	case idle, ok := <-pool.idleChan:
		return pool.takeIdle(idle, ok)
	case pool.dialTokens <- struct{}{}:
		defer func() { <-pool.dialTokens }()
		return pool.dialIntoSlot(ctx)
	}
}

// acquireIdle takes an idle connection without blocking, discarding unfit ones.
// It returns nil, nil if none is ready.
func (pool *ConnPool) acquireIdle() (*Conn, error) {
	for {
		select {
		case idle, ok := <-pool.idleChan:
			conn, err := pool.takeIdle(idle, ok)
			if errors.Is(err, errConnDiscarded) {
				continue
			}
			return conn, err
		default:
			return nil, nil
		}
	}
}

// dialIntoSlot waits for a free slot to dial into, or for an idle connection.
func (pool *ConnPool) dialIntoSlot(ctx context.Context) (*Conn, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-pool.closedChan:
		return nil, ErrPoolAlreadyClosed
	case idle, ok := <-pool.idleChan:
		return pool.takeIdle(idle, ok)
	case pool.slots <- struct{}{}:
		conn, err := pool.openConnection(ctx)
		if err != nil {
			<-pool.slots
			return nil, err
		}
		return conn, nil
	}
}

// takeIdle handles a receive from idleChan. It returns ErrPoolAlreadyClosed if
// the pool is closed, or errConnDiscarded if the connection was unfit.
func (pool *ConnPool) takeIdle(conn *Conn, ok bool) (*Conn, error) {
	if !ok {
		return nil, ErrPoolAlreadyClosed
	}
	// A closed idleChan still yields buffered conns until Close drains it.
	if pool.closed.Load() {
		_ = pool.closeConn(conn)
		return nil, ErrPoolAlreadyClosed
	}
	if conn.expired(time.Now()) || !conn.reusable() {
		_ = pool.closeConn(conn)
		return nil, errConnDiscarded
	}
	conn.idleSince = time.Time{}
	pool.requestRefill()
	return conn, nil
}

// closeConn closes the underlying connection and frees its pool slot.
func (pool *ConnPool) closeConn(conn *Conn) error {
	err := conn.Conn.Close()
	<-pool.slots
	pool.requestRefill()
	return err
}

// Close shuts the pool down and closes all idle connections. Connections that
// are still checked out are closed when their holders call Conn.Close.
func (pool *ConnPool) Close() error {
	if !pool.closed.CompareAndSwap(false, true) {
		return ErrPoolAlreadyClosed
	}

	// Taking the mutex waits out any in-flight release before the channel closes.
	pool.mux.Lock()
	close(pool.closedChan)
	close(pool.idleChan)
	pool.mux.Unlock()
	pool.wg.Wait()

	var errs []error
	for conn := range pool.idleChan {
		if err := pool.closeConn(conn); err != nil {
			errs = append(errs, err)
		}
	}

	return errors.Join(errs...)
}

// openConnection dials with retries until ctx is done or the pool is closed.
func (pool *ConnPool) openConnection(ctx context.Context) (*Conn, error) {
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	go func() {
		select {
		case <-pool.closedChan:
			cancel(ErrPoolAlreadyClosed)
		case <-ctx.Done():
		}
	}()

	var lastErr error
	for attempt := range pool.cfg.DialMaxAttempts() {
		conn, err := pool.cfg.dialer.DialContext(ctx, pool.cfg.Network(), pool.cfg.Addr())
		if err == nil {
			if pool.closed.Load() {
				_ = conn.Close()
				return nil, fmt.Errorf("%w %s: %w", ErrDialFailed, pool.cfg.Addr(), ErrPoolAlreadyClosed)
			}
			return newConn(conn, pool), nil
		}
		lastErr = err
		if ctx.Err() != nil {
			break
		}

		if d := pool.cfg.dialBackoff.Delay(attempt); d > 0 {
			timer := time.NewTimer(d)
			select {
			case <-timer.C:
			case <-ctx.Done():
				timer.Stop()
			}
		}
		if ctx.Err() != nil {
			break
		}
	}

	if ctx.Err() != nil {
		return nil, fmt.Errorf("%w %s: %w", ErrDialFailed, pool.cfg.Addr(), errors.Join(context.Cause(ctx), lastErr))
	}
	return nil, fmt.Errorf("%w %s: %w", ErrDialFailed, pool.cfg.Addr(), errors.Join(task.ErrMaxRetriesExceeded, lastErr))
}

// release returns the connection to the idle set. The connection is closed
// instead if the pool is closed or already holds MaxPoolSize idle connections.
func (pool *ConnPool) release(conn *Conn) error {
	idle := &Conn{
		Conn:      conn.Conn,
		pool:      pool,
		createdAt: conn.createdAt,
		lifespan:  conn.lifespan,
		idleSince: time.Now(),
	}
	if pool.putIdle(idle) {
		return nil
	}
	return pool.closeConn(conn)
}

// putIdle reports false if the pool is closed or the idle set is full.
func (pool *ConnPool) putIdle(conn *Conn) bool {
	pool.mux.Lock()
	defer pool.mux.Unlock()
	if pool.closed.Load() {
		return false
	}
	select {
	case pool.idleChan <- conn:
		return true
	default:
		return false
	}
}

func (pool *ConnPool) requestRefill() {
	select {
	case pool.refillChan <- struct{}{}:
	default:
	}
}

// maintainIdle keeps MinIdle healthy idle connections until the pool closes.
func (pool *ConnPool) maintainIdle() {
	ticker := time.NewTicker(min(pool.cfg.IdleTimeout(), pool.cfg.LifespanTimeout()) / 2)
	defer ticker.Stop()

	pool.fillIdle()
	for {
		select {
		case <-pool.closedChan:
			return
		case <-pool.refillChan:
		case <-ticker.C:
			pool.evictIdle()
		}
		pool.fillIdle()
	}
}

func (pool *ConnPool) fillIdle() {
	for len(pool.idleChan) < pool.cfg.MinIdle() {
		select {
		case pool.slots <- struct{}{}:
		default:
			return
		}

		ctx, cancel := context.WithTimeout(context.Background(), pool.cfg.AcquireTimeout())
		conn, err := pool.dialWithToken(ctx)
		cancel()
		if err != nil {
			<-pool.slots
			return
		}
		conn.idleSince = time.Now()
		if !pool.putIdle(conn) {
			_ = conn.Conn.Close()
			<-pool.slots
			return
		}
	}
}

func (pool *ConnPool) dialWithToken(ctx context.Context) (*Conn, error) {
	select {
	case pool.dialTokens <- struct{}{}:
		defer func() { <-pool.dialTokens }()
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-pool.closedChan:
		return nil, ErrPoolAlreadyClosed
	}
	return pool.openConnection(ctx)
}

func (pool *ConnPool) evictIdle() {
	for range len(pool.idleChan) {
		var conn *Conn
		select {
		case c, ok := <-pool.idleChan:
			if !ok {
				return
			}
			conn = c
		default:
			return
		}
		if conn.expired(time.Now()) || !conn.reusable() || !pool.putIdle(conn) {
			_ = pool.closeConn(conn)
		}
	}
}
