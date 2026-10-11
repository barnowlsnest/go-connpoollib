package connpool

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	
	"github.com/barnowlsnest/go-asynctasklib/v2/pkg/semaphore"
	"github.com/barnowlsnest/go-asynctasklib/v2/pkg/task"
	"golang.org/x/sync/errgroup"
)

const taskConnName = "connpool.conn"

type (
	ConnFactory func(ctx context.Context) (*Conn, error)
	
	ConnPool struct {
		mux        sync.Mutex
		onceClose  sync.Once
		idleChan   chan *Conn
		closedChan chan struct{}
		active     []*Conn
		sem        *semaphore.Semaphore
		cfg        *Config
		dialSeq    atomic.Uint64
	}
)

func New(cfg *Config) (*ConnPool, error) {
	if cfg == nil {
		return nil, ErrNilConfig
	}
	
	return &ConnPool{
		closedChan: make(chan struct{}),
		idleChan:   make(chan *Conn, cfg.MaxPoolSize()),
		active:     make([]*Conn, 0, cfg.MaxPoolSize()),
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
	
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case conn, ok := <-pool.idleChan:
		if !ok {
			return nil, ErrPoolAlreadyClosed
		}
		pool.toActive(conn)
		return conn, nil
	default:
		return pool.openConnection(ctx)
	}
}

func (pool *ConnPool) Close() error {
	pool.onceClose.Do(func() {
		close(pool.idleChan)
		close(pool.closedChan)
	})
	
	return nil
}

func (pool *ConnPool) connectionAttempt(ptr *atomic.Pointer[Conn]) func(*task.Run) error {
	return func(run *task.Run) error {
		conn, err := pool.cfg.dialer.DialContext(run.Context(), pool.cfg.Network(), pool.cfg.Addr())
		if err != nil {
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
		return nil, fmt.Errorf("%w %s: %w", ErrDialFailed, pool.cfg.Addr(), err)
	}
	if conn := dialed.Load(); conn != nil {
		pool.toActive(conn)
		return conn, nil
	}
	
	return nil, fmt.Errorf("%w %s: %w", ErrDialFailed, pool.cfg.Addr(), ctx.Err())
}

func (pool *ConnPool) totalConnections() uint64 {
	pool.mux.Lock()
	defer pool.mux.Unlock()
	active := uint64(len(pool.active))
	idle := uint64(len(pool.idleChan))
	return active + idle
}

func (pool *ConnPool) isExhausted() bool {
	return pool.totalConnections() >= uint64(pool.cfg.MaxPoolSize())
}

func (pool *ConnPool) toActive(conn *Conn) {
	pool.mux.Lock()
	defer pool.mux.Unlock()
	pool.active = append(pool.active, conn)
}

func (pool *ConnPool) release(conn *Conn) {
	if conn == nil {
		return
	}
	pool.idleChan <- conn
	pool.mux.Lock()
	defer pool.mux.Unlock()
	for i, c := range pool.active {
		if c == conn {
			pool.active = append(pool.active[:i], pool.active[i+1:]...)
			break
		}
	}
}
