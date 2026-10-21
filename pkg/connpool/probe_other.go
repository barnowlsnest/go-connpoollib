//go:build !unix

package connpool

import "net"

// peekSocket is not supported on this platform; reusable falls back to a short
// blocking read.
func peekSocket(net.Conn) sockState {
	return sockUnknown
}
