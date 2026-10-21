package connpool

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
)

type StatsSuite struct {
	baseSuite
}

func TestStatsSuite(t *testing.T) {
	suite.Run(t, new(StatsSuite))
}

func (s *StatsSuite) TestLifecycle() {
	pool := s.newPool(WithMaxPoolSize(3))
	s.Equal(Stats{MaxOpen: 3}, pool.Stats())

	conn, err := pool.Acquire(context.Background())
	s.Require().NoError(err)
	st := pool.Stats()
	s.Equal(1, st.Open)
	s.Equal(1, st.InUse)
	s.Equal(0, st.Idle)
	s.Equal(int64(1), st.Dials)
	s.Equal(int64(1), st.WaitCount)
	s.Positive(st.WaitDuration)

	s.Require().NoError(conn.Close())
	st = pool.Stats()
	s.Equal(1, st.Open)
	s.Equal(0, st.InUse)
	s.Equal(1, st.Idle)

	// Taking an idle connection is not a wait.
	conn, err = pool.Acquire(context.Background())
	s.Require().NoError(err)
	s.Require().NoError(conn.Close())
	st = pool.Stats()
	s.Equal(int64(1), st.WaitCount)
	s.Equal(int64(1), st.Dials)

	s.Require().NoError(pool.Close())
	st = pool.Stats()
	s.Equal(0, st.Open)
	s.Equal(0, st.Idle)
}

func (s *StatsSuite) TestDialErrors() {
	s.stopListener()
	pool := s.newPool(WithDialMaxAttempts(2))

	_, err := pool.Acquire(context.Background())
	s.Require().ErrorIs(err, ErrDialFailed)
	st := pool.Stats()
	s.Equal(int64(1), st.DialErrors)
	s.Equal(int64(0), st.Dials)
	s.Equal(0, st.Open)
	s.Equal(int64(1), st.WaitCount)
}

func (s *StatsSuite) TestCloseReasons() {
	pool := s.newPool(WithIdleTimeout(time.Minute), WithLifespanTimeout(time.Hour))
	past := time.Now().Add(-2 * time.Hour)

	tests := map[string]struct {
		prepare func(conn *Conn, peer net.Conn)
		want    Stats
	}{
		"lifespan ended": {
			prepare: func(conn *Conn, _ net.Conn) { conn.createdAt = past },
			want:    Stats{LifespanClosed: 1},
		},
		"idle too long": {
			prepare: func(conn *Conn, _ net.Conn) { conn.idleSince = past },
			want:    Stats{IdleClosed: 1},
		},
		"peer closed": {
			prepare: func(_ *Conn, peer net.Conn) { s.NoError(peer.Close()) },
			want:    Stats{UnhealthyClosed: 1},
		},
	}
	for name, tt := range tests {
		s.Run(name, func() {
			before := pool.Stats()
			accepted := s.accept()
			conn, err := pool.Acquire(context.Background())
			s.Require().NoError(err)
			server := <-accepted
			s.Require().NotNil(server)
			defer server.Close()

			tt.prepare(conn, server)
			s.eventually(func() bool { return pool.unfit(conn, time.Now()) })
			_ = pool.dropConn(conn)

			st := pool.Stats()
			s.Equal(tt.want.LifespanClosed, st.LifespanClosed-before.LifespanClosed)
			s.Equal(tt.want.IdleClosed, st.IdleClosed-before.IdleClosed)
			s.Equal(tt.want.UnhealthyClosed, st.UnhealthyClosed-before.UnhealthyClosed)
		})
	}
}

func (s *StatsSuite) TestConcurrentAccounting() {
	pool := s.newPool(WithMinIdle(2), WithMaxPoolSize(8))

	var wg sync.WaitGroup
	for range 32 {
		wg.Go(func() {
			for range 50 {
				conn, err := pool.Acquire(context.Background())
				if !s.NoError(err) {
					return
				}
				s.NoError(conn.Close())
			}
		})
	}
	wg.Wait()

	// The background filler may still be dialing.
	s.eventually(func() bool {
		st := pool.Stats()
		return st.Open == len(pool.slots) && st.InUse == 0 && st.Idle == st.Open
	})
	s.LessOrEqual(pool.Stats().Dials, int64(8))

	s.Require().NoError(pool.Close())
	s.Equal(0, pool.Stats().Open)
}
