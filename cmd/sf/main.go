package main

import (
	"context"
	"os"

	"github.com/nysa-company/sf/internal/cli"
	"github.com/nysa-company/sf/internal/config"
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
	os.Exit(cli.Execute(context.Background(), os.Args[1:], os.Stdout, os.Stderr, cli.SocketClient{Path: paths.Socket}))
}
