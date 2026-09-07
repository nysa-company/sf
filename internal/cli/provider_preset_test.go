package cli

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/nysa-company/sf/internal/config"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/store"
)

func TestInitProviderPresetFreezesPreferencesWithoutQualification(t *testing.T) {
	ctx := context.Background()
	repo, home := initializedRepository(t), t.TempDir()
	request := InitRequest{Channel: domain.ChannelDev, Project: "preset", Repo: repo, Home: home, ProviderPreset: "claude-codex"}
	response := RunInit(ctx, request)
	if !response.OK {
		t.Fatalf("init: %+v", response)
	}
	file := filepath.Join(repo, ".sf", "config.toml")
	before, err := os.ReadFile(file)
	if err != nil {
		t.Fatal(err)
	}
	paths, _ := config.PathsFor(home, domain.ChannelDev)
	db, err := store.OpenReadOnly(ctx, paths.Database)
	if err != nil {
		t.Fatal(err)
	}
	project, err := db.Project(ctx, domain.ChannelDev, "preset")
	if err != nil {
		t.Fatal(err)
	}
	effective, err := config.DecodeSnapshot(project.ConfigSnapshot, project.ConfigDigest)
	if err != nil {
		t.Fatal(err)
	}
	want, _ := config.ProviderPreset("claude-codex")
	if !reflect.DeepEqual(effective.Providers, want) {
		t.Fatal("wrong frozen provider preferences")
	}
	if _, err := db.ProviderPair(ctx, domain.ChannelDev); err == nil {
		t.Fatal("configuration minted qualification")
	}
	_ = db.Close()
	if replay := RunInit(ctx, request); !replay.OK || !replay.Mutation.Observed {
		t.Fatalf("replay: %+v", replay)
	}
	request.ProviderPreset = "codex-claude"
	if conflict := RunInit(ctx, request); conflict.OK {
		t.Fatal("conflicting preset overwrote config")
	}
	after, err := os.ReadFile(file)
	if err != nil || string(after) != string(before) {
		t.Fatal("existing configuration changed")
	}
}

func TestInitProviderPresetRejectsUnsupportedBeforeConfigCreation(t *testing.T) {
	repo := initializedRepository(t)
	response := RunInit(context.Background(), InitRequest{Channel: domain.ChannelDev, Project: "preset", Repo: repo, Home: t.TempDir(), ProviderPreset: "unknown-codex"})
	if response.OK {
		t.Fatal("unsupported preset accepted")
	}
	if _, err := os.Stat(filepath.Join(repo, ".sf", "config.toml")); !os.IsNotExist(err) {
		t.Fatal("unsupported preset created config")
	}
}

func TestInitProviderPresetRollbackOnRegistrationConflict(t *testing.T) {
	home := t.TempDir()
	request := InitRequest{Channel: domain.ChannelDev, Project: "preset", Repo: initializedRepository(t), Home: home}
	if response := RunInit(context.Background(), request); !response.OK {
		t.Fatal(response)
	}
	request.Repo, request.ProviderPreset = initializedRepository(t), "claude-codex"
	if response := RunInit(context.Background(), request); response.OK {
		t.Fatal("conflicting registration succeeded")
	}
	if _, err := os.Stat(filepath.Join(request.Repo, ".sf", "config.toml")); !os.IsNotExist(err) {
		t.Fatal("failed registration retained generated config")
	}
}

func TestInitCheckProviderPresetRemainsReadOnly(t *testing.T) {
	repo, home := initializedRepository(t), filepath.Join(t.TempDir(), "absent-home")
	response := RunInitCheck(context.Background(), InitRequest{Channel: domain.ChannelDev, Project: "preset", Repo: repo, Home: home, ProviderPreset: "claude-codex"})
	if response.OK || response.Mutation.Attempted {
		t.Fatal("check accepted a configuration mutation")
	}
	for _, path := range []string{home, filepath.Join(repo, ".sf")} {
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatalf("read-only check created %s", path)
		}
	}
}
