package connpool

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/barnowlsnest/go-asynctasklib/v2/pkg/semaphore"
	"github.com/barnowlsnest/go-asynctasklib/v2/pkg/task"
	"golang.org/x/sync/errgroup"
)

const taskConnName = "connpool.conn"

type (
	ConnPool struct {
		idleChan   chan *Conn
		closedChan chan struct{}
		slots      chan struct{}
		sem        *semaphore.Semaphore
		cfg        *Config
		dialSeq    atomic.Uint64
		mux        sync.Mutex
		closed     atomic.Bool
	}
)

func New(cfg *Config) (*ConnPool, error) {
	if cfg == nil {
		return nil, ErrNilConfig
	}

	return &ConnPool{
		closedChan: make(chan struct{}),
		idleChan:   make(chan *Conn, cfg.MaxPoolSize()),
		slots:      make(chan struct{}, cfg.MaxPoolSize()),
		sem:        semaphore.NewSemaphore(cfg.MaxConcurrency()),
		cfg:        cfg,
	}, nil
}

func (pool *ConnPool) Config() *Config {
	return pool.cfg
}

func (pool *ConnPool) Acquire(ctx context.Context) (*Conn, error) {
	ctx, cancel := context.WithTimeout(ctx, pool.cfg.AcquireTimeout())
	defer cancel()

	if err := pool.sem.AcquireWithContext(ctx); err != nil {
		return nil, err
	}
	defer pool.sem.Release()

	for {
		// Prefer an idle connection over dialing a new one.
		select {
		case idle, ok := <-pool.idleChan:
			if conn, err := pool.takeIdle(idle, ok); conn != nil || err != nil {
				return conn, err
			}
			continue
		default:
		}

		// Otherwise wait for an idle connection or a free slot to dial into.
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-pool.closedChan:
			return nil, ErrPoolAlreadyClosed
		case idle, ok := <-pool.idleChan:
			if conn, err := pool.takeIdle(idle, ok); conn != nil || err != nil {
				return conn, err
			}
		case pool.slots <- struct{}{}:
			conn, err := pool.openConnection(ctx)
			if err != nil {
				<-pool.slots
				return nil, err
			}
			return conn, nil
		}
	}
}

// takeIdle handles a receive from idleChan. It returns the connection if it is
// still fit for reuse, ErrPoolAlreadyClosed if the channel was closed, or
// (nil, nil) after discarding a connection that expired or was dropped by the
// peer, in which case the caller should look again.
func (pool *ConnPool) takeIdle(conn *Conn, ok bool) (*Conn, error) {
	if !ok {
		return nil, ErrPoolAlreadyClosed
	}
	if conn.expired(time.Now()) || !conn.reusable() {
		_ = pool.closeConn(conn)
		return nil, nil
	}
	return conn, nil
}

// closeConn closes the underlying connection and frees its pool slot.
func (pool *ConnPool) closeConn(conn *Conn) error {
	err := conn.Conn.Close()
	<-pool.slots
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

	var errs []error
	for conn := range pool.idleChan {
		if err := pool.closeConn(conn); err != nil {
			errs = append(errs, err)
		}
	}

	return errors.Join(errs...)
}

func (pool *ConnPool) connectionAttempt(ptr *atomic.Pointer[Conn]) func(*task.Run) error {
	return func(run *task.Run) error {
		conn, err := pool.cfg.dialer.DialContext(run.Context(), pool.cfg.Network(), pool.cfg.Addr())
		if err != nil {
			return err
		}

		// The task may have timed out or been canceled while dialing; nobody
		// would pick this connection up.
		if err := run.Context().Err(); err != nil {
			_ = conn.Close()
			return err
		}

		ptr.Store(newConn(conn, pool))

		return nil
	}
}

func (pool *ConnPool) openConnection(ctx context.Context) (*Conn, error) {
	var dialed atomic.Pointer[Conn]
	b := task.NewBuilder(
		task.WithID(pool.dialSeq.Add(1)),
		task.WithName(taskConnName),
		task.WithMaxRetries(pool.cfg.DialMaxAttempts()),
		task.WithMaxDuration(pool.cfg.AcquireTimeout()),
		task.WithRetryStrategy(pool.cfg.dialBackoff),
		task.WithTaskFn(pool.connectionAttempt(&dialed)),
	)
	def, err := b.Build()
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrDialFailed, err)
	}

	t := task.New(*def)
	g, ctx := errgroup.WithContext(ctx)
	done := make(chan struct{})
	g.Go(func() error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-pool.closedChan:
			t.Cancel()
			return ErrPoolAlreadyClosed
		case <-done:
			return nil
		}
	})
	g.Go(func() error {
		defer close(done)
		return t.GoRetry(ctx)
	})
	if err := g.Wait(); err != nil {
		// The dial may have succeeded just before the watcher gave up.
		if conn := dialed.Swap(nil); conn != nil {
			_ = conn.Conn.Close()
		}
		return nil, fmt.Errorf("%w %s: %w", ErrDialFailed, pool.cfg.Addr(), err)
	}
	if conn := dialed.Load(); conn != nil {
		return conn, nil
	}

	return nil, fmt.Errorf("%w %s: %w", ErrDialFailed, pool.cfg.Addr(), ctx.Err())
}

// release returns the connection to the idle set. The connection is closed
// instead if the pool is closed or already holds MaxPoolSize idle connections.
func (pool *ConnPool) release(conn *Conn) error {
	pool.mux.Lock()
	if !pool.closed.Load() {
		idle := &Conn{Conn: conn.Conn, pool: pool, createdAt: conn.createdAt, idleSince: time.Now()}
		select {
		case pool.idleChan <- idle:
			pool.mux.Unlock()
			return nil
		default:
		}
	}
	pool.mux.Unlock()

	return pool.closeConn(conn)
}
