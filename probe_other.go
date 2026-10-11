//go:build !unix

package connpool

import "net"

// probeIdle is not supported on this platform; reusable falls back to a short
// blocking read.
func probeIdle(net.Conn) (idle, supported bool) {
	return false, false
}
