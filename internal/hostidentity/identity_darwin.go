//go:build darwin

package hostidentity

import (
	"context"
	"io"
	"os/exec"
	"time"

	"golang.org/x/sys/unix"
)

// Observe uses only fixed OS sources, with no inherited command environment.
// It never returns raw hardware identifiers or command output in errors.
func Observe(ctx context.Context) (Identity, error) {
	if ctx == nil || ctx.Err() != nil {
		return Identity{}, ErrUnavailable
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	boot, err := unix.Sysctl("kern.bootsessionuuid")
	if err != nil {
		return Identity{}, ErrUnavailable
	}
	boot, err = canonicalUUID(boot)
	if err != nil {
		return Identity{}, err
	}
	command := exec.CommandContext(ctx, "/usr/sbin/ioreg", "-rd1", "-c", "IOPlatformExpertDevice")
	command.Env = []string{"LANG=C"}
	command.WaitDelay = time.Second
	var output boundedOutput
	command.Stdout = &output
	command.Stderr = io.Discard
	if err := command.Run(); err != nil || ctx.Err() != nil {
		return Identity{}, ErrUnavailable
	}
	machine, err := platformMachineDigest(output.Bytes())
	if err != nil {
		return Identity{}, err
	}
	return Identity{MachineDigest: machine, BootID: boot}, nil
}
