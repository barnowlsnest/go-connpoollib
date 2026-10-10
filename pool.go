package connpool

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	
	"github.com/barnowlsnest/go-asynctasklib/v2/pkg/semaphore"
	"github.com/barnowlsnest/go-asynctasklib/v2/pkg/task"
)

const taskConnName = "connpool.conn"

type (
	ConnFactory func(ctx context.Context) (*Conn, error)
	
	ConnPool struct {
		idleChan chan *Conn
		sem      *semaphore.Semaphore
		cfg      *Config
		dialSeq  atomic.Uint64
		closed   bool
	}
)

func New(cfg *Config) (*ConnPool, error) {
	if cfg == nil {
		return nil, ErrNilConfig
	}
	
	return &ConnPool{
		idleChan: make(chan *Conn, cfg.MaxPoolSize()),
		sem:      semaphore.NewSemaphore(cfg.MaxConcurrency()),
		cfg:      cfg,
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
		return conn, nil
	default:
		return pool.openConnection(ctx)
	}
}

func (pool *ConnPool) Close() error {
	return errors.New("not implemented")
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
	def, err := task.NewBuilder(
		task.WithID(pool.dialSeq.Add(1)),
		task.WithName(taskConnName),
		task.WithMaxRetries(pool.cfg.DialMaxAttempts()),
		task.WithRetryStrategy(pool.cfg.dialBackoff),
		task.WithTaskFn(pool.connectionAttempt(&dialed)),
	).Build()
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrDialFailed, err)
	}
	if err := task.New(*def).GoRetry(ctx); err != nil {
		return nil, fmt.Errorf("%w %s: %w", ErrDialFailed, pool.cfg.Addr(), err)
	}
	if conn := dialed.Load(); conn != nil {
		return conn, nil
	}
	
	return nil, fmt.Errorf("%w %s: %w", ErrDialFailed, pool.cfg.Addr(), ctx.Err())
}
