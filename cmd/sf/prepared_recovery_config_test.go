package main

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/nysa-company/sf/internal/config"
	"github.com/nysa-company/sf/internal/daemon"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/runtimeassets"
)

func TestProductionDaemonPreparedRecoveryConfiguration(t *testing.T) {
	for _, channel := range []domain.Channel{domain.ChannelStable, domain.ChannelDev} {
		t.Run(string(channel), func(t *testing.T) {
			root, err := filepath.EvalSymlinks(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			primary, helper := "sf", "sf-git-exec"
			if channel == domain.ChannelDev {
				primary, helper = "sf-dev", "sf-git-exec-dev"
			}
			executable := filepath.Join(root, primary)
			paths, err := config.PathsFor(root, channel)
			if err != nil {
				t.Fatal(err)
			}
			// Use the same configuration constructor as main. No bundle exists
			// yet: idle startup configuration must not resolve it eagerly.
			configuration := productionDaemonConfig(daemon.Config{Channel: channel, Paths: paths}, executable)
			if configuration.PreparedCommitRunnerFactory == nil || configuration.GitRunner != nil || configuration.PreparedCommitObserver != nil {
				t.Fatal("production recovery is not lazily wired")
			}
			if _, err := configuration.PreparedCommitRunnerFactory(); !errors.Is(err, runtimeassets.ErrUnsafeBundle) {
				t.Fatalf("missing bundle accepted: %v", err)
			}
			for _, name := range []string{primary, helper} {
				if err := os.WriteFile(filepath.Join(root, name), []byte("test bundle asset\n"), 0o700); err != nil {
					t.Fatal(err)
				}
			}
			runner, err := configuration.PreparedCommitRunnerFactory()
			if err != nil {
				t.Fatal(err)
			}
			if runner.Binary != "/usr/bin/git" || runner.ExecHelper != filepath.Join(root, helper) || runner.Home != filepath.Join(filepath.Dir(paths.Socket), "git-home") || runner.MutationAuthority != nil || runner.CredentialHelper != "" || runner.GHConfigDir != "" {
				t.Fatal("recovery runner changed trusted paths or acquired mutation/publication authority")
			}
			if err := os.Chmod(filepath.Join(root, helper), 0o777); err != nil {
				t.Fatal(err)
			}
			if _, err := configuration.PreparedCommitRunnerFactory(); !errors.Is(err, runtimeassets.ErrUnsafeBundle) {
				t.Fatalf("unsafe helper accepted: %v", err)
			}
		})
	}
}
