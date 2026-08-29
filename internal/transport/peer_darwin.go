//go:build darwin

package transport

import (
	"fmt"
	"net"

	"golang.org/x/sys/unix"
)

func peerUID(connection *net.UnixConn) (uint32, error) {
	raw, err := connection.SyscallConn()
	if err != nil {
		return 0, fmt.Errorf("access peer socket: %w", err)
	}
	var uid uint32
	var controlErr error
	if err := raw.Control(func(fd uintptr) {
		credential, err := unix.GetsockoptXucred(int(fd), unix.SOL_LOCAL, unix.LOCAL_PEERCRED)
		if err != nil {
			controlErr = err
			return
		}
		uid = credential.Uid
	}); err != nil {
		return 0, fmt.Errorf("inspect peer socket: %w", err)
	}
	if controlErr != nil {
		return 0, fmt.Errorf("read peer credentials: %w", controlErr)
	}
	return uid, nil
}
