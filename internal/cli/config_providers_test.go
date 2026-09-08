package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/nysa-company/sf/internal/config"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/store"
)

func TestConfigProvidersCancelledPickerDoesNotAccessProject(t *testing.T) {
	for _, answer := range []string{"q\n", "", "invalid\n"} {
		a := newApp(nil, &bytes.Buffer{}, &bytes.Buffer{})
		a.input, a.interactive = strings.NewReader(answer), func() bool { return true }
		command := a.command()
		command.SetArgs([]string{"config", "providers", "--project", "must-not-be-looked-up", "--preset", "select"})
		if err := command.ExecuteContext(context.Background()); err == nil {
			t.Fatal("cancelled or invalid selection accepted")
		}
		if a.last != nil {
			t.Fatal("cancelled picker reached project edit handler")
		}
	}
}

func TestConfigProvidersEditsSourceWithoutApplyingGeneration(t *testing.T) {
	ctx := context.Background()
	repo, home := initializedRepository(t), t.TempDir()
	if response := RunInit(ctx, InitRequest{Channel: domain.ChannelDev, Project: "p", Repo: repo, Home: home, ProviderPreset: "codex-codex"}); !response.OK {
		t.Fatalf("init=%+v", response)
	}
	paths, _ := config.PathsFor(home, domain.ChannelDev)
	db, err := store.OpenReadOnly(ctx, paths.Database)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	before, err := db.Project(ctx, domain.ChannelDev, "p")
	if err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(repo, ".sf", "config.toml")
	source, err := os.ReadFile(file)
	if err != nil {
		t.Fatal(err)
	}
	request := ConfigProvidersRequest{ConfigApplyRequest: ConfigApplyRequest{Channel: domain.ChannelDev, Project: "p", Home: home}, Preset: "claude-codex"}
	response := RunConfigProviders(ctx, request)
	if !response.OK || !response.Mutation.Attempted || response.NextAction == nil || response.NextAction.Code != "config_apply_required" {
		t.Fatalf("edit=%+v", response)
	}
	var data struct {
		Edit struct {
			Backup  string `json:"backup_path"`
			Applied bool   `json:"applied"`
		} `json:"provider_configuration"`
	}
	if err := json.Unmarshal(response.Data, &data); err != nil {
		t.Fatal(err)
	}
	backup, err := os.ReadFile(data.Edit.Backup)
	if err != nil || string(backup) != string(source) || data.Edit.Applied {
		t.Fatal("backup/application semantics", err)
	}
	after, err := db.Project(ctx, domain.ChannelDev, "p")
	if err != nil || before.ConfigDigest != after.ConfigDigest || before.ConfigGeneration != after.ConfigGeneration {
		t.Fatal("edit applied authority", err)
	}
	if _, err := db.ProviderPair(ctx, domain.ChannelDev); err == nil {
		t.Fatal("edit qualified providers")
	}
	if replay := RunConfigProviders(ctx, request); !replay.OK || replay.Mutation.Attempted {
		t.Fatalf("replay=%+v", replay)
	}
	if applied := RunConfigApply(ctx, request.ConfigApplyRequest); !applied.OK {
		t.Fatalf("apply=%+v", applied)
	}
	after, err = db.Project(ctx, domain.ChannelDev, "p")
	if err != nil || after.ConfigGeneration != before.ConfigGeneration+1 {
		t.Fatal("explicit apply did not advance", err)
	}
	effective, err := config.DecodeSnapshot(after.ConfigSnapshot, after.ConfigDigest)
	if err != nil || effective.Providers.Builder[0] != "claude" {
		t.Fatal("wrong new snapshot", err)
	}
}
