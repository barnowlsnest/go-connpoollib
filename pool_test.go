package connpool

import (
	"context"
	"net"
	"strconv"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
	
	"github.com/barnowlsnest/go-asynctasklib/v2/pkg/task"
	"github.com/stretchr/testify/suite"
)

type PoolSuite struct {
	suite.Suite
	ln       net.Listener
	port     int
	attempts atomic.Int32
	// onAttempt, when set, runs before each connect with the 1-based attempt number.
	onAttempt func(n int32)
}

func TestPoolSuite(t *testing.T) {
	suite.Run(t, new(PoolSuite))
}

func (s *PoolSuite) SetupTest() {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	s.Require().NoError(err)
	s.ln = ln
	s.port = ln.Addr().(*net.TCPAddr).Port
	s.attempts.Store(0)
	s.onAttempt = nil
}

func (s *PoolSuite) TearDownTest() {
	if s.ln != nil {
		_ = s.ln.Close()
	}
}

// stopListener closes the test listener so dials to s.port are refused.
func (s *PoolSuite) stopListener() {
	s.Require().NoError(s.ln.Close())
	s.ln = nil
}

func (s *PoolSuite) dialer() *net.Dialer {
	return &net.Dialer{
		Timeout: time.Second,
		Control: func(string, string, syscall.RawConn) error {
			n := s.attempts.Add(1)
			if s.onAttempt != nil {
				s.onAttempt(n)
			}
			return nil
		},
	}
}

// newPool builds a pool aimed at the test listener; opts override the defaults.
func (s *PoolSuite) newPool(opts ...Option) *ConnPool {
	base := []Option{
		WithPort(s.port),
		WithDialer(s.dialer()),
		WithDialBackoff(time.Millisecond, 5*time.Millisecond),
	}
	cfg, err := NewConfig(append(base, opts...)...)
	s.Require().NoError(err)
	pool, err := New(cfg)
	s.Require().NoError(err)
	return pool
}

func (s *PoolSuite) closeConn(conn *Conn) {
	s.Require().NotNil(conn)
	s.NoError(conn.Conn.Close())
}

func (s *PoolSuite) TestNewNilConfig() {
	pool, err := New(nil)
	s.Nil(pool)
	s.ErrorIs(err, ErrNilConfig)
}

func (s *PoolSuite) TestNewAppliesConfig() {
	pool := s.newPool(WithMaxPoolSize(7), WithMaxConcurrency(3))
	
	s.Equal(7, cap(pool.idleChan))
	s.Equal(3, pool.sem.Limit())
	s.Equal(7, pool.Config().MaxPoolSize())
	s.Equal(3, pool.Config().MaxConcurrency())
}

func (s *PoolSuite) TestAcquireDialsWhenNoIdle() {
	pool := s.newPool()
	
	conn, err := pool.Acquire(context.Background())
	s.Require().NoError(err)
	defer s.closeConn(conn)
	
	s.Same(pool, conn.pool)
	s.Equal(s.ln.Addr().String(), conn.RemoteAddr().String())
	s.Equal(int32(1), s.attempts.Load())
	s.Equal(0, pool.sem.Acquired())
}

func (s *PoolSuite) TestAcquireReturnsIdleWithoutDial() {
	pool := s.newPool()
	idle, err := pool.openConnection(context.Background())
	s.Require().NoError(err)
	defer s.closeConn(idle)
	s.attempts.Store(0)
	
	pool.idleChan <- idle
	
	conn, err := pool.Acquire(context.Background())
	s.Require().NoError(err)
	s.Same(idle, conn)
	s.Equal(int32(0), s.attempts.Load())
	s.Equal(0, pool.sem.Acquired())
}

func (s *PoolSuite) TestAcquireAfterPoolClose() {
	pool := s.newPool()
	s.Require().NoError(pool.Close())
	
	conn, err := pool.Acquire(context.Background())
	s.Nil(conn)
	s.ErrorIs(err, ErrPoolAlreadyClosed)
	s.Equal(0, pool.sem.Acquired())
}

func (s *PoolSuite) TestAcquireRetriesUntilSuccess() {
	s.stopListener()
	s.onAttempt = func(n int32) {
		// Bring the server up right before the third connect.
		if n == 3 {
			ln, err := net.Listen("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(s.port)))
			s.Require().NoError(err)
			s.ln = ln
		}
	}
	pool := s.newPool(WithDialMaxAttempts(3))
	
	conn, err := pool.Acquire(context.Background())
	s.Require().NoError(err)
	defer s.closeConn(conn)
	
	s.Equal(int32(3), s.attempts.Load())
}

func (s *PoolSuite) TestAcquireGivesUpAfterMaxAttempts() {
	s.stopListener()
	pool := s.newPool(WithDialMaxAttempts(4))
	
	conn, err := pool.Acquire(context.Background())
	s.Nil(conn)
	s.ErrorIs(err, ErrDialFailed)
	s.ErrorIs(err, task.ErrMaxRetriesExceeded)
	s.ErrorIs(err, syscall.ECONNREFUSED)
	s.Equal(int32(4), s.attempts.Load())
	s.Equal(0, pool.sem.Acquired())
}

func (s *PoolSuite) TestAcquireBackoffHonorsAcquireTimeout() {
	s.stopListener()
	pool := s.newPool(
		WithDialMaxAttempts(10),
		WithDialBackoff(time.Hour, time.Hour),
		WithDialBackoffJitter(false),
		WithAcquireTimeout(50*time.Millisecond),
	)
	
	start := time.Now()
	conn, err := pool.Acquire(context.Background())
	s.Nil(conn)
	s.ErrorIs(err, ErrDialFailed)
	s.ErrorIs(err, context.DeadlineExceeded)
	s.Less(time.Since(start), time.Second)
	s.Equal(int32(1), s.attempts.Load())
}

func (s *PoolSuite) TestAcquireCanceledContext() {
	pool := s.newPool()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	
	conn, err := pool.Acquire(ctx)
	s.Nil(conn)
	s.ErrorIs(err, context.Canceled)
	s.Equal(int32(0), s.attempts.Load())
	s.Equal(0, pool.sem.Acquired())
}

func (s *PoolSuite) TestAcquireSemaphoreTimeoutKeepsForeignSlots() {
	pool := s.newPool(WithMaxConcurrency(2), WithAcquireTimeout(20*time.Millisecond))
	pool.sem.Acquire()
	pool.sem.Acquire()
	
	conn, err := pool.Acquire(context.Background())
	s.Nil(conn)
	s.ErrorIs(err, context.DeadlineExceeded)
	s.Equal(int32(0), s.attempts.Load())
	// A failed Acquire must not release slots it never held.
	s.Equal(2, pool.sem.Acquired())
}

func (s *PoolSuite) TestOpenConnectionIDsAreUnique() {
	pool := s.newPool()
	
	for range 3 {
		conn, err := pool.openConnection(context.Background())
		s.Require().NoError(err)
		s.closeConn(conn)
	}
	s.Equal(uint64(3), pool.dialSeq.Load())
}

// accept returns the server side of the next accepted connection.
func (s *PoolSuite) accept() <-chan net.Conn {
	ch := make(chan net.Conn, 1)
	go func() {
		conn, err := s.ln.Accept()
		if err == nil {
			ch <- conn
		}
		close(ch)
	}()
	return ch
}

func (s *PoolSuite) TestConnCloseReturnsToPool() {
	pool := s.newPool()
	
	conn, err := pool.Acquire(context.Background())
	s.Require().NoError(err)
	local := conn.LocalAddr().String()
	s.Require().NoError(conn.Close())
	s.Len(pool.idleChan, 1)
	
	again, err := pool.Acquire(context.Background())
	s.Require().NoError(err)
	defer s.closeConn(again)
	
	s.NotSame(conn, again)
	s.Equal(local, again.LocalAddr().String())
	s.Equal(int32(1), s.attempts.Load())
}

func (s *PoolSuite) TestConnDoubleCloseDoesNotDuplicate() {
	pool := s.newPool()
	
	conn, err := pool.Acquire(context.Background())
	s.Require().NoError(err)
	s.Require().NoError(conn.Close())
	
	s.ErrorIs(conn.Close(), net.ErrClosed)
	s.Len(pool.idleChan, 1)
}

func (s *PoolSuite) TestConnCloseResetsDeadline() {
	pool := s.newPool()
	
	conn, err := pool.Acquire(context.Background())
	s.Require().NoError(err)
	s.Require().NoError(conn.SetDeadline(time.Now().Add(-time.Second)))
	s.Require().NoError(conn.Close())
	
	again, err := pool.Acquire(context.Background())
	s.Require().NoError(err)
	defer s.closeConn(again)
	
	_, err = again.Write([]byte("ping"))
	s.NoError(err)
}

func (s *PoolSuite) TestConnCloseDiscardsUnreadData() {
	pool := s.newPool()
	server := s.accept()
	
	conn, err := pool.Acquire(context.Background())
	s.Require().NoError(err)
	srv := <-server
	s.Require().NotNil(srv)
	defer srv.Close()
	
	_, err = srv.Write([]byte("ab"))
	s.Require().NoError(err)
	// Read only part of the response, leaving "b" in the socket.
	_, err = conn.Read(make([]byte, 1))
	s.Require().NoError(err)
	
	s.NoError(conn.Close())
	s.Empty(pool.idleChan)
	_, err = conn.Conn.Read(make([]byte, 1))
	s.ErrorIs(err, net.ErrClosed)
}

func (s *PoolSuite) TestConnCloseWhenPoolFull() {
	pool := s.newPool(WithMaxPoolSize(1), WithMaxConcurrency(2))
	
	first, err := pool.Acquire(context.Background())
	s.Require().NoError(err)
	second, err := pool.Acquire(context.Background())
	s.Require().NoError(err)
	
	s.Require().NoError(first.Close())
	s.Require().NoError(second.Close())
	
	s.Len(pool.idleChan, 1)
	_, err = second.Conn.Read(make([]byte, 1))
	s.ErrorIs(err, net.ErrClosed)
}

func (s *PoolSuite) TestConnCloseAfterPoolClose() {
	pool := s.newPool()
	
	conn, err := pool.Acquire(context.Background())
	s.Require().NoError(err)
	s.Require().NoError(pool.Close())
	
	s.NoError(conn.Close())
	_, err = conn.Conn.Read(make([]byte, 1))
	s.ErrorIs(err, net.ErrClosed)
}

func (s *PoolSuite) TestPoolCloseClosesIdle() {
	pool := s.newPool()
	
	conn, err := pool.Acquire(context.Background())
	s.Require().NoError(err)
	s.Require().NoError(conn.Close())
	idle := <-pool.idleChan
	pool.idleChan <- idle
	
	s.NoError(pool.Close())
	_, err = idle.Conn.Read(make([]byte, 1))
	s.ErrorIs(err, net.ErrClosed)
}

func (s *PoolSuite) TestPoolCloseTwice() {
	pool := s.newPool()
	
	s.NoError(pool.Close())
	s.ErrorIs(pool.Close(), ErrPoolAlreadyClosed)
}
