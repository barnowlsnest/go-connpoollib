package connpool

import (
	"errors"
	"net"
	"sync/atomic"
	"time"
)

const connCheckTimeout = time.Millisecond

type Conn struct {
	net.Conn
	pool     *ConnPool
	released atomic.Bool
}

func newConn(conn net.Conn, pool *ConnPool) *Conn {
	return &Conn{
		Conn: conn,
		pool: pool,
	}
}

func (c *Conn) Close() error {
	return errors.New("not implemented")
}
