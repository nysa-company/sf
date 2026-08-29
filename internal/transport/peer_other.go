//go:build !darwin && !linux

package transport

import (
	"fmt"
	"net"
)

func peerUID(*net.UnixConn) (uint32, error) {
	return 0, fmt.Errorf("peer credentials are unsupported on this platform")
}
