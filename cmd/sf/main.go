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
