//go:build unix

package connpool

import (
	"errors"
	"net"
	"syscall"
)

// peekSocket peeks at conn's socket without blocking or consuming data.
func peekSocket(conn net.Conn) sockState {
	sc, ok := conn.(syscall.Conn)
	if !ok {
		return sockUnknown
	}
	raw, err := sc.SyscallConn()
	if err != nil {
		return sockUnknown
	}

	state := sockClosed
	err = raw.Read(func(fd uintptr) bool {
		var buf [1]byte
		n, _, rerr := syscall.Recvfrom(int(fd), buf[:], syscall.MSG_PEEK)
		switch {
		case n > 0:
			state = sockData
		case errors.Is(rerr, syscall.EAGAIN) || errors.Is(rerr, syscall.EWOULDBLOCK):
			state = sockIdle
		}
		return true
	})
	if err != nil {
		return sockClosed
	}
	return state
}
