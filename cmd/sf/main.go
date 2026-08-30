package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/nysa-company/sf/internal/cli"
	"github.com/nysa-company/sf/internal/config"
	"github.com/nysa-company/sf/internal/daemon"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/version"
)

func main() {
	if len(os.Args) >= 3 && os.Args[1] == "__provider_gate" {
		// FD 3 is held by the supervisor until the launch PID/PGID is durably
		// recorded. EOF means the parent died before authority was published.
		gate := os.NewFile(uintptr(3), "provider-launch-gate")
		if gate == nil {
			os.Exit(125)
		}
		var one [1]byte
		if _, err := gate.Read(one[:]); err != nil {
			os.Exit(125)
		}
		if err := syscall.Exec(os.Args[2], os.Args[2:], os.Environ()); err != nil {
			os.Exit(126)
		}
		return
	}
	home, err := os.UserHomeDir()
	if err != nil {
		home = ""
	}
	channel := domain.Channel(version.Channel)
	if !channel.Valid() {
		channel = domain.ChannelStable
	}
	paths, _ := config.PathsFor(home, channel)
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	runDaemon := func(runCtx context.Context) error {
		return daemon.Run(runCtx, daemon.Config{
			Channel: channel, Paths: paths,
			DaemonIdentity: fmt.Sprintf("sf/%s/%s", version.Version, version.Commit),
		})
	}
	os.Exit(cli.ExecuteWithDaemon(ctx, os.Args[1:], os.Stdout, os.Stderr, cli.SocketClient{Path: paths.Socket}, runDaemon))
}
