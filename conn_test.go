package connpool

import (
	"context"
	"crypto/tls"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
)

type ConnSuite struct {
	baseSuite
}

func TestConnSuite(t *testing.T) {
	suite.Run(t, new(ConnSuite))
}

func (s *ConnSuite) TestExpired() {
	pool := s.newPool(WithIdleTimeout(time.Minute), WithLifespanTimeout(time.Hour))
	now := time.Now()

	tests := map[string]struct {
		createdAt time.Time
		idleSince time.Time
		expired   bool
	}{
		"fresh":          {createdAt: now},
		"checked out":    {createdAt: now.Add(-50 * time.Minute)},
		"idle in time":   {createdAt: now, idleSince: now.Add(-59 * time.Second)},
		"idle too long":  {createdAt: now, idleSince: now.Add(-time.Minute), expired: true},
		"lifespan ended": {createdAt: now.Add(-time.Hour), expired: true},
	}
	for name, tt := range tests {
		s.Run(name, func() {
			conn := &Conn{pool: pool, createdAt: tt.createdAt, idleSince: tt.idleSince, lifespan: time.Hour}
			s.Equal(tt.expired, conn.expired(now))
		})
	}
}

func (s *ConnSuite) TestLifespanJitter() {
	pool := s.newPool(WithLifespanTimeout(time.Hour))

	seen := map[time.Duration]bool{}
	for range 100 {
		lifespan := newConn(nil, pool).lifespan
		s.Greater(lifespan, 54*time.Minute)
		s.LessOrEqual(lifespan, time.Hour)
		seen[lifespan] = true
	}
	s.Greater(len(seen), 1)
}

func (s *ConnSuite) TestCloseReturnsToPool() {
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

func (s *ConnSuite) TestDoubleClose() {
	pool := s.newPool()

	conn, err := pool.Acquire(context.Background())
	s.Require().NoError(err)
	s.Require().NoError(conn.Close())

	s.ErrorIs(conn.Close(), net.ErrClosed)
	s.Len(pool.idleChan, 1)
	s.Len(pool.slots, 1)
}

func (s *ConnSuite) TestReleasedHandleRejectsIO() {
	pool := s.newPool()
	server := s.accept()

	conn, err := pool.Acquire(context.Background())
	s.Require().NoError(err)
	srv := <-server
	s.Require().NotNil(srv)
	defer srv.Close()
	s.Require().NoError(conn.Close())

	again, err := pool.Acquire(context.Background())
	s.Require().NoError(err)
	defer s.closeConn(again)

	_, err = conn.Write([]byte("stale"))
	s.ErrorIs(err, net.ErrClosed)
	_, err = conn.Read(make([]byte, 1))
	s.ErrorIs(err, net.ErrClosed)
	s.ErrorIs(conn.SetDeadline(time.Now()), net.ErrClosed)
	s.ErrorIs(conn.SetReadDeadline(time.Now()), net.ErrClosed)
	s.ErrorIs(conn.SetWriteDeadline(time.Now()), net.ErrClosed)

	_, err = again.Write([]byte("ok"))
	s.Require().NoError(err)
	buf := make([]byte, 8)
	s.Require().NoError(srv.SetReadDeadline(time.Now().Add(time.Second)))
	n, err := srv.Read(buf)
	s.Require().NoError(err)
	s.Equal("ok", string(buf[:n]))
}

func (s *ConnSuite) TestCloseResetsDeadline() {
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
	s.Equal(int32(1), s.attempts.Load())
}

func (s *ConnSuite) TestCloseDiscardsUnreadData() {
	pool := s.newPool()
	server := s.accept()

	conn, err := pool.Acquire(context.Background())
	s.Require().NoError(err)
	srv := <-server
	s.Require().NotNil(srv)
	defer srv.Close()

	_, err = srv.Write([]byte("ab"))
	s.Require().NoError(err)
	_, err = conn.Read(make([]byte, 1))
	s.Require().NoError(err)

	s.NoError(conn.Close())
	s.Empty(pool.idleChan)
	s.Empty(pool.slots)
}

func (s *ConnSuite) TestCloseDiscardsLifespanExpired() {
	pool := s.newPool(WithMaxPoolSize(1), WithLifespanTimeout(time.Hour))

	conn, err := pool.Acquire(context.Background())
	s.Require().NoError(err)
	conn.createdAt = time.Now().Add(-time.Hour)
	s.Require().NoError(conn.Close())

	s.Empty(pool.idleChan)
	s.Empty(pool.slots)
	_, err = conn.Conn.Read(make([]byte, 1))
	s.ErrorIs(err, net.ErrClosed)

	again, err := pool.Acquire(context.Background())
	s.Require().NoError(err)
	defer s.closeConn(again)
	s.Equal(int32(2), s.attempts.Load())
}

func (s *ConnSuite) TestCloseAfterPoolClose() {
	pool := s.newPool()

	conn, err := pool.Acquire(context.Background())
	s.Require().NoError(err)
	s.Require().NoError(pool.Close())

	s.NoError(conn.Close())
	s.Empty(pool.slots)
	_, err = conn.Conn.Read(make([]byte, 1))
	s.ErrorIs(err, net.ErrClosed)
}

func (s *ConnSuite) TestReusable() {
	pool := s.newPool()

	s.Run("idle", func() {
		client, server := net.Pipe()
		defer server.Close()
		s.True(newConn(client, pool).reusable())
	})
	s.Run("peer closed", func() {
		client, server := net.Pipe()
		s.Require().NoError(server.Close())
		s.False(newConn(client, pool).reusable())
	})

	serverCfg, clientCfg := s.tlsConfigs()
	tlsPair := func() (*Conn, *tls.Conn) {
		accepted := s.accept()
		raw, err := net.Dial("tcp", s.ln.Addr().String())
		s.Require().NoError(err)
		srvRaw := <-accepted
		s.Require().NotNil(srvRaw)
		server := tls.Server(srvRaw, serverCfg)
		go func() { _ = server.Handshake() }()
		client := tls.Client(raw, clientCfg)
		s.Require().NoError(client.Handshake())
		return newConn(client, pool), server
	}
	s.Run("tls idle with session tickets", func() {
		conn, server := tlsPair()
		defer server.Close()
		defer conn.Conn.Close()
		s.True(conn.reusable())
		s.True(conn.reusable())
	})
	s.Run("tls unread data", func() {
		conn, server := tlsPair()
		defer server.Close()
		defer conn.Conn.Close()
		_, err := server.Write([]byte("x"))
		s.Require().NoError(err)
		s.eventually(func() bool { return !conn.reusable() })
	})
	s.Run("tls peer closed", func() {
		conn, server := tlsPair()
		defer conn.Conn.Close()
		s.Require().NoError(server.Close())
		s.eventually(func() bool { return !conn.reusable() })
	})
}
