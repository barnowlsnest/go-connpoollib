//go:build unix

package connpool

import (
	"errors"
	"net"
	"syscall"
)

// probeIdle does a non-blocking read on the socket. idle is true only if the
// read would block: no unread data and no EOF from the peer. supported is
// false if conn does not expose its file descriptor.
func probeIdle(conn net.Conn) (idle, supported bool) {
	sc, ok := conn.(syscall.Conn)
	if !ok {
		return false, false
	}
	raw, err := sc.SyscallConn()
	if err != nil {
		return false, false
	}

	err = raw.Read(func(fd uintptr) bool {
		var buf [1]byte
		_, rerr := syscall.Read(int(fd), buf[:])
		idle = errors.Is(rerr, syscall.EAGAIN) || errors.Is(rerr, syscall.EWOULDBLOCK)
		return true
	})

	return err == nil && idle, true
}
