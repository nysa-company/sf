package cli

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/nysa-company/sf/internal/config"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/pythonprepare"
)

func TestPythonPreparationPreviewIsReadOnlyAndHumanReadable(t *testing.T) {
	home := t.TempDir()
	response := runPythonPreparation(t.Context(), domain.ChannelDev, home, "darwin", "arm64", false, func(context.Context, string) (pythonprepare.Result, error) {
		t.Fatal("preview prepared runtime")
		return pythonprepare.Result{}, nil
	})
	if !response.OK || response.Mutation.Attempted {
		t.Fatal(response)
	}
	entries, err := os.ReadDir(home)
	if err != nil || len(entries) != 0 {
		t.Fatal("preview changed HOME", err)
	}
	var human, encoded bytes.Buffer
	if err := Render(&human, response, false); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(human.String(), "Preparation: preview") || !strings.Contains(human.String(), "sf-dev runtimes prepare python --download") || strings.Contains(human.String(), "sha256:") {
		t.Fatal(human.String())
	}
	if err := Render(&encoded, response, true); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(encoded.String(), "environment_digest") {
		t.Fatal("JSON omitted exact identity")
	}
}

func TestPythonPreparationIsChannelScopedAndDoesNotOpenDatabase(t *testing.T) {
	for _, channel := range []domain.Channel{domain.ChannelDev, domain.ChannelStable} {
		t.Run(string(channel), func(t *testing.T) {
			home := t.TempDir()
			catalog, _ := pythonprepare.DefaultCatalog("darwin", "arm64")
			calls := 0
			response := runPythonPreparation(t.Context(), channel, home, "darwin", "arm64", true, func(_ context.Context, root string) (pythonprepare.Result, error) {
				calls++
				if !strings.HasSuffix(root, filepath.Join("sf", string(channel), "runtimes", "python")) {
					t.Fatal(root)
				}
				return pythonprepare.Result{EnvironmentDigest: catalog.EnvironmentDigest, LockDigest: pythonprepare.LockDigest()}, nil
			})
			if !response.OK || !response.Mutation.Attempted || calls != 1 {
				t.Fatal(response, calls)
			}
			paths, _ := config.PathsFor(home, channel)
			for _, p := range []string{paths.Database, paths.Machine, paths.Socket} {
				if _, err := os.Lstat(p); !os.IsNotExist(err) {
					t.Fatal("created non-cache state", p, err)
				}
			}
			other := domain.ChannelDev
			if channel == other {
				other = domain.ChannelStable
			}
			otherPaths, _ := config.PathsFor(home, other)
			if _, err := os.Lstat(otherPaths.Root); !os.IsNotExist(err) {
				t.Fatal("changed other channel", err)
			}
		})
	}
}

func TestPythonPreparationReportsUnsupportedAndFailures(t *testing.T) {
	for _, kind := range []string{"unsupported", "failed", "mismatch", "cache"} {
		t.Run(kind, func(t *testing.T) {
			home := t.TempDir()
			arch := "arm64"
			if kind == "unsupported" {
				arch = "amd64"
			}
			calls := 0
			response := runPythonPreparation(t.Context(), domain.ChannelDev, home, "darwin", arch, true, func(context.Context, string) (pythonprepare.Result, error) {
				calls++
				if kind == "cache" {
					return pythonprepare.Result{}, pythonprepare.ErrCacheInvalid
				}
				if kind == "failed" {
					return pythonprepare.Result{}, errors.New("untrusted upstream details")
				}
				return pythonprepare.Result{}, nil
			})
			if response.OK || response.Error == nil || response.NextAction == nil {
				t.Fatal(response)
			}
			if strings.Contains(response.Error.Message, "untrusted") {
				t.Fatal("leaked upstream detail")
			}
			if kind == "cache" && (response.Error.Code != "runtime_cache_unverified" || strings.Contains(strings.Join(response.NextAction.Argv, " "), "--download")) {
				t.Fatal("corrupt cache encouraged blind retry", response)
			}
			if kind == "unsupported" {
				if calls != 0 || response.Mutation.Attempted || exitCode(response) != ExitCompatibility {
					t.Fatal(response, calls)
				}
			} else if calls != 1 || !response.Mutation.Attempted || exitCode(response) != ExitAction {
				t.Fatal(response, calls)
			}
		})
	}
}
