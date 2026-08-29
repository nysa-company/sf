package config

import (
	"fmt"
	"path/filepath"

	"github.com/nysa-company/sf/internal/domain"
)

type ChannelPaths struct {
	Root      string
	Database  string
	Machine   string
	Socket    string
	Logs      string
	Events    string
	Worktrees string
	Backups   string
}

func PathsFor(home string, channel domain.Channel) (ChannelPaths, error) {
	if home == "" {
		return ChannelPaths{}, fmt.Errorf("home directory is required")
	}
	if !channel.Valid() {
		return ChannelPaths{}, fmt.Errorf("invalid channel %q", channel)
	}
	root := filepath.Join(home, "Library", "Application Support", "sf", string(channel))
	return ChannelPaths{
		Root:      root,
		Database:  filepath.Join(root, "sf.sqlite"),
		Machine:   filepath.Join(root, "machine.toml"),
		Socket:    filepath.Join(root, "run", "sf.sock"),
		Logs:      filepath.Join(root, "logs"),
		Events:    filepath.Join(root, "events"),
		Worktrees: filepath.Join(root, "worktrees"),
		Backups:   filepath.Join(root, "backups"),
	}, nil
}
