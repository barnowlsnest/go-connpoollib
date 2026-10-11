package connpool

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"math/big"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/barnowlsnest/go-asynctasklib/v2/pkg/task"
	"github.com/stretchr/testify/suite"
)

type baseSuite struct {
	suite.Suite
	ln       net.Listener
	port     int
	attempts atomic.Int32
	// onAttempt runs before each connect with the 1-based attempt number.
	onAttempt func(n int32)
}

func (s *baseSuite) SetupTest() {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	s.Require().NoError(err)
	s.ln = ln
	s.port = ln.Addr().(*net.TCPAddr).Port
	s.attempts.Store(0)
	s.onAttempt = nil
}

func (s *baseSuite) TearDownTest() {
	if s.ln != nil {
		_ = s.ln.Close()
	}
}

func (s *baseSuite) stopListener() {
	s.Require().NoError(s.ln.Close())
	s.ln = nil
}

func (s *baseSuite) dialer() *net.Dialer {
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

// newPool targets the test listener with no warm-up, so dials can be counted.
func (s *baseSuite) newPool(opts ...Option) *ConnPool {
	base := []Option{
		WithMinIdle(0),
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

// tlsConfigs returns a TLS 1.3 server config with a self-signed certificate
// and a client config that trusts it.
func (s *baseSuite) tlsConfigs() (server, client *tls.Config) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	s.Require().NoError(err)
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		DNSNames:     []string{"localhost"},
		NotAfter:     time.Now().Add(time.Hour),
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	s.Require().NoError(err)
	cert, err := x509.ParseCertificate(der)
	s.Require().NoError(err)
	roots := x509.NewCertPool()
	roots.AddCert(cert)

	server = &tls.Config{
		Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key}},
		MinVersion:   tls.VersionTLS13,
	}
	client = &tls.Config{RootCAs: roots, ServerName: "localhost", MinVersion: tls.VersionTLS13}
	return server, client
}

func (s *baseSuite) accept() <-chan net.Conn {
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

func (s *baseSuite) closeConn(conn *Conn) {
	s.Require().NotNil(conn)
	s.NoError(conn.Conn.Close())
}

func (s *baseSuite) eventually(cond func() bool) {
	s.Eventually(cond, time.Second, 5*time.Millisecond)
}

type PoolSuite struct {
	baseSuite
}

func TestPoolSuite(t *testing.T) {
	suite.Run(t, new(PoolSuite))
}

func (s *PoolSuite) TestNew() {
	s.Run("nil config", func() {
		pool, err := New(nil)
		s.Nil(pool)
		s.ErrorIs(err, ErrNilConfig)
	})
	s.Run("invalid config", func() {
		pool, err := New(&Config{})
		s.Nil(pool)
		s.ErrorIs(err, ErrInvalidConnPoolCfg)
	})
	s.Run("applies config", func() {
		pool := s.newPool(WithMaxPoolSize(7), WithMaxConcurrency(3))
		s.Equal(7, cap(pool.idleChan))
		s.Equal(7, cap(pool.slots))
		s.Equal(3, cap(pool.dialTokens))
	})
}

func (s *PoolSuite) TestAcquireDialsWhenNoIdle() {
	pool := s.newPool()

	conn, err := pool.Acquire(context.Background())
	s.Require().NoError(err)
	defer s.closeConn(conn)

	s.Equal(s.ln.Addr().String(), conn.RemoteAddr().String())
	s.Equal(int32(1), s.attempts.Load())
	s.Len(pool.slots, 1)
	s.Empty(pool.dialTokens)
}

func (s *PoolSuite) TestAcquireAfterPoolClose() {
	pool := s.newPool()
	s.Require().NoError(pool.Close())

	conn, err := pool.Acquire(context.Background())
	s.Nil(conn)
	s.ErrorIs(err, ErrPoolAlreadyClosed)
}

func (s *PoolSuite) TestAcquireRetriesUntilSuccess() {
	s.stopListener()
	s.onAttempt = func(n int32) {
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
	s.Empty(pool.slots)
	s.Empty(pool.dialTokens)
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
}

func (s *PoolSuite) TestAcquireTimeoutMidDialFreesSlot() {
	pool := s.newPool(WithMaxPoolSize(1), WithAcquireTimeout(20*time.Millisecond))
	s.onAttempt = func(int32) { time.Sleep(50 * time.Millisecond) }

	conn, err := pool.Acquire(context.Background())
	s.Nil(conn)
	s.ErrorIs(err, ErrDialFailed)
	s.ErrorIs(err, context.DeadlineExceeded)
	s.Empty(pool.slots)
	s.Empty(pool.dialTokens)

	s.onAttempt = nil
	conn, err = pool.Acquire(context.Background())
	s.Require().NoError(err)
	s.closeConn(conn)
}

func (s *PoolSuite) TestAcquireTimesOutWhenDialsBusy() {
	pool := s.newPool(WithMaxConcurrency(2), WithAcquireTimeout(20*time.Millisecond))
	pool.dialTokens <- struct{}{}
	pool.dialTokens <- struct{}{}

	conn, err := pool.Acquire(context.Background())
	s.Nil(conn)
	s.ErrorIs(err, context.DeadlineExceeded)
	s.Equal(int32(0), s.attempts.Load())
	s.Len(pool.dialTokens, 2)
}

func (s *PoolSuite) TestAcquireTakesIdleWhileDialsBusy() {
	pool := s.newPool(WithMaxConcurrency(1))

	held, err := pool.Acquire(context.Background())
	s.Require().NoError(err)
	local := held.LocalAddr().String()
	pool.dialTokens <- struct{}{}
	defer func() { <-pool.dialTokens }()
	time.AfterFunc(20*time.Millisecond, func() { _ = held.Close() })

	conn, err := pool.Acquire(context.Background())
	s.Require().NoError(err)
	defer s.closeConn(conn)
	s.Equal(local, conn.LocalAddr().String())
	s.Equal(int32(1), s.attempts.Load())
}

func (s *PoolSuite) TestAcquireBlocksWhenPoolExhausted() {
	pool := s.newPool(WithMaxPoolSize(1), WithAcquireTimeout(50*time.Millisecond))

	first, err := pool.Acquire(context.Background())
	s.Require().NoError(err)

	_, err = pool.Acquire(context.Background())
	s.ErrorIs(err, context.DeadlineExceeded)
	s.Equal(int32(1), s.attempts.Load())

	local := first.LocalAddr().String()
	time.AfterFunc(10*time.Millisecond, func() { _ = first.Close() })
	again, err := pool.Acquire(context.Background())
	s.Require().NoError(err)
	defer s.closeConn(again)
	s.Equal(local, again.LocalAddr().String())
}

func (s *PoolSuite) TestAcquireWaiterUnblockedByPoolClose() {
	pool := s.newPool(WithMaxPoolSize(1), WithAcquireTimeout(time.Second))
	held, err := pool.Acquire(context.Background())
	s.Require().NoError(err)
	time.AfterFunc(20*time.Millisecond, func() { _ = pool.Close() })

	start := time.Now()
	conn, err := pool.Acquire(context.Background())
	s.Nil(conn)
	s.ErrorIs(err, ErrPoolAlreadyClosed)
	s.Less(time.Since(start), 500*time.Millisecond)

	s.Require().NoError(held.Close())
	s.Empty(pool.slots)
}

func (s *PoolSuite) TestAcquireEvictsIdleTimedOutConn() {
	pool := s.newPool(WithIdleTimeout(time.Minute))

	conn, err := pool.Acquire(context.Background())
	s.Require().NoError(err)
	s.Require().NoError(conn.Close())
	idle := <-pool.idleChan
	idle.idleSince = time.Now().Add(-time.Minute)
	pool.idleChan <- idle

	again, err := pool.Acquire(context.Background())
	s.Require().NoError(err)
	defer s.closeConn(again)
	s.NotEqual(conn.LocalAddr().String(), again.LocalAddr().String())
	s.Equal(int32(2), s.attempts.Load())
	s.Len(pool.slots, 1)
}

func (s *PoolSuite) TestAcquireEvictsConnClosedByPeer() {
	pool := s.newPool()
	server := s.accept()

	conn, err := pool.Acquire(context.Background())
	s.Require().NoError(err)
	srv := <-server
	s.Require().NotNil(srv)
	s.Require().NoError(conn.Close())
	s.Require().NoError(srv.Close())
	s.eventually(func() bool {
		idle := <-pool.idleChan
		defer func() { pool.idleChan <- idle }()
		return !idle.reusable()
	})

	again, err := pool.Acquire(context.Background())
	s.Require().NoError(err)
	defer s.closeConn(again)
	s.Equal(int32(2), s.attempts.Load())
	s.Len(pool.slots, 1)
}

func (s *PoolSuite) TestAcquireResetsIdleTime() {
	pool := s.newPool(WithIdleTimeout(time.Minute))

	conn, err := pool.Acquire(context.Background())
	s.Require().NoError(err)
	s.Require().NoError(conn.Close())
	idle := <-pool.idleChan
	idle.idleSince = time.Now().Add(-59 * time.Second)
	pool.idleChan <- idle

	again, err := pool.Acquire(context.Background())
	s.Require().NoError(err)
	s.False(again.expired(time.Now().Add(2 * time.Second)))
	s.Require().NoError(again.Close())
	s.Len(pool.idleChan, 1)
}

func (s *PoolSuite) TestPoolCloseDuringDial() {
	pool := s.newPool()
	s.onAttempt = func(int32) { time.Sleep(50 * time.Millisecond) }
	time.AfterFunc(10*time.Millisecond, func() { _ = pool.Close() })

	conn, err := pool.Acquire(context.Background())
	s.Nil(conn)
	s.ErrorIs(err, ErrPoolAlreadyClosed)
	s.Empty(pool.slots)
}

func (s *PoolSuite) TestPoolCloseClosesIdle() {
	pool := s.newPool()

	conn, err := pool.Acquire(context.Background())
	s.Require().NoError(err)
	s.Require().NoError(conn.Close())
	idle := <-pool.idleChan
	pool.idleChan <- idle

	s.NoError(pool.Close())
	_, err = idle.Read(make([]byte, 1))
	s.ErrorIs(err, net.ErrClosed)
	s.Empty(pool.slots)
}

func (s *PoolSuite) TestAcquireDuringPoolCloseDrain() {
	pool := s.newPool()

	conn, err := pool.Acquire(context.Background())
	s.Require().NoError(err)
	s.Require().NoError(conn.Close())
	idle := <-pool.idleChan
	pool.idleChan <- idle
	// Close's state before it drains idleChan.
	pool.closed.Store(true)
	close(pool.closedChan)
	close(pool.idleChan)

	_, err = pool.Acquire(context.Background())
	s.Require().ErrorIs(err, ErrPoolAlreadyClosed)
	_, err = idle.Read(make([]byte, 1))
	s.ErrorIs(err, net.ErrClosed)
	s.Empty(pool.slots)
}

func (s *PoolSuite) TestPoolCloseWithCheckedOutConns() {
	pool := s.newPool()
	a, err := pool.Acquire(context.Background())
	s.Require().NoError(err)
	b, err := pool.Acquire(context.Background())
	s.Require().NoError(err)

	s.Require().NoError(pool.Close())
	s.Len(pool.slots, 2)

	s.NoError(a.Close())
	s.NoError(b.Close())
	s.Empty(pool.slots)
	_, err = a.Conn.Read(make([]byte, 1))
	s.ErrorIs(err, net.ErrClosed)
}

func (s *PoolSuite) TestPoolCloseTwice() {
	pool := s.newPool()

	s.NoError(pool.Close())
	s.ErrorIs(pool.Close(), ErrPoolAlreadyClosed)
}

func (s *PoolSuite) TestMinIdleWarmsUpAndRefills() {
	pool := s.newPool(WithMinIdle(2))
	defer func() { s.NoError(pool.Close()) }()

	s.eventually(func() bool { return len(pool.idleChan) == 2 })
	s.Equal(int32(2), s.attempts.Load())

	conn, err := pool.Acquire(context.Background())
	s.Require().NoError(err)
	defer func() { s.NoError(conn.Close()) }()

	s.eventually(func() bool { return len(pool.idleChan) == 2 })
	s.Equal(int32(3), s.attempts.Load())
}

func (s *PoolSuite) TestMinIdleRespectsMaxPoolSize() {
	pool := s.newPool(WithMinIdle(2), WithMaxPoolSize(2))
	defer func() { s.NoError(pool.Close()) }()
	s.eventually(func() bool { return len(pool.idleChan) == 2 })

	a, err := pool.Acquire(context.Background())
	s.Require().NoError(err)
	b, err := pool.Acquire(context.Background())
	s.Require().NoError(err)
	defer s.closeConn(a)
	defer s.closeConn(b)

	s.Never(func() bool { return s.attempts.Load() > 2 }, 50*time.Millisecond, 5*time.Millisecond)
	s.Len(pool.slots, 2)
}

func (s *PoolSuite) TestMinIdleReplacesExpired() {
	pool := s.newPool(WithMinIdle(1), WithIdleTimeout(40*time.Millisecond))
	defer func() { s.NoError(pool.Close()) }()
	s.eventually(func() bool { return len(pool.idleChan) == 1 })

	s.eventually(func() bool { return s.attempts.Load() >= 2 && len(pool.idleChan) == 1 })
	s.Len(pool.slots, 1)
}

func (s *PoolSuite) TestFillIdleTakesDialTokenBeforeSlot() {
	pool := s.newPool(WithMaxPoolSize(1), WithMaxConcurrency(1))
	pool.cfg.minIdle = 1
	pool.dialTokens <- struct{}{} // held by an Acquire waiting for a slot

	var wg sync.WaitGroup
	wg.Go(pool.fillIdle)
	s.Never(func() bool { return len(pool.slots) > 0 }, 50*time.Millisecond, 5*time.Millisecond)

	<-pool.dialTokens
	wg.Wait()
	s.Len(pool.idleChan, 1)
	s.Len(pool.slots, 1)
	s.NoError(pool.Close())
	s.Empty(pool.slots)
}

func (s *PoolSuite) TestAcquireOverTLS() {
	serverCfg, clientCfg := s.tlsConfigs()
	ln := s.ln
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				tc := tls.Server(conn, serverCfg)
				defer tc.Close()
				_, _ = io.Copy(tc, tc)
			}()
		}
	}()
	tlsDialer := &tls.Dialer{NetDialer: s.dialer(), Config: clientCfg}
	pool := s.newPool(WithDialFunc(tlsDialer.DialContext), WithMaxPoolSize(1))
	defer func() { s.NoError(pool.Close()) }()

	for range 3 {
		conn, err := pool.Acquire(context.Background())
		s.Require().NoError(err)
		s.IsType(&tls.Conn{}, conn.Conn)
		_, err = conn.Write([]byte("ping"))
		s.Require().NoError(err)
		buf := make([]byte, 4)
		_, err = io.ReadFull(conn, buf)
		s.Require().NoError(err)
		s.Equal("ping", string(buf))
		s.Require().NoError(conn.Close())
	}
	s.Equal(int32(1), s.attempts.Load())
	s.Len(pool.idleChan, 1)
}

func (s *PoolSuite) TestMinIdleCloseDuringWarmUp() {
	s.onAttempt = func(int32) { time.Sleep(50 * time.Millisecond) }
	pool := s.newPool(WithMinIdle(2))
	s.eventually(func() bool { return s.attempts.Load() == 1 })

	s.Require().NoError(pool.Close())
	s.Empty(pool.slots)
	s.Empty(pool.idleChan)
	s.Empty(pool.dialTokens)
}

func (s *PoolSuite) TestConcurrentAcquireRelease() {
	const maxPool = 3
	pool := s.newPool(WithMaxPoolSize(maxPool), WithMaxConcurrency(2))
	defer func() { s.NoError(pool.Close()) }()

	var held, peak atomic.Int32
	var wg sync.WaitGroup
	for range 32 {
		wg.Go(func() {
			for range 20 {
				conn, err := pool.Acquire(context.Background())
				if !s.NoError(err) {
					return
				}
				n := held.Add(1)
				for p := peak.Load(); n > p && !peak.CompareAndSwap(p, n); p = peak.Load() {
				}
				_, err = conn.Write([]byte("x"))
				s.NoError(err)
				held.Add(-1)
				s.NoError(conn.Close())
			}
		})
	}
	wg.Wait()

	s.LessOrEqual(peak.Load(), int32(maxPool))
	s.LessOrEqual(s.attempts.Load(), int32(maxPool))
	s.LessOrEqual(len(pool.slots), maxPool)
	s.Empty(pool.dialTokens)
}

func (s *PoolSuite) TestConcurrentPoolClose() {
	pool := s.newPool(WithMaxPoolSize(4))

	var wg sync.WaitGroup
	for range 16 {
		wg.Go(func() {
			for {
				conn, err := pool.Acquire(context.Background())
				if err != nil {
					s.True(errors.Is(err, ErrPoolAlreadyClosed), err)
					return
				}
				s.NoError(conn.Close())
			}
		})
	}
	time.AfterFunc(20*time.Millisecond, func() { s.NoError(pool.Close()) })
	wg.Wait()

	s.Empty(pool.slots)
	s.Empty(pool.dialTokens)
}
