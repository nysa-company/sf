package cli

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/api"
	"github.com/nysa-company/sf/internal/config"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/store"
	_ "modernc.org/sqlite"
)

func TestRunConfigApplyFreezesOptionalConfigAbsenceForFutureStarts(t *testing.T) {
	ctx := context.Background()
	repository := initializedRepository(t)
	home := t.TempDir()
	if response := RunInit(ctx, InitRequest{Channel: domain.ChannelDev, Project: "relay", Repo: repository, Home: home}); !response.OK {
		t.Fatalf("init=%+v", response)
	}
	if _, err := os.Lstat(filepath.Join(repository, ".sf")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("init unexpectedly created .sf: %v", err)
	}
	paths, err := config.PathsFor(home, domain.ChannelDev)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(paths.Machine, []byte("max_concurrent_tickets = 1\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	response := RunConfigApply(ctx, ConfigApplyRequest{Channel: domain.ChannelDev, Project: "relay", Home: home})
	if !response.OK || !response.Mutation.Attempted || response.Mutation.Observed {
		t.Fatalf("apply=%+v", response)
	}
	var result configApplyResult
	if err := json.Unmarshal(response.Data, &result); err != nil {
		t.Fatal(err)
	}
	if result.ConfigGeneration != 2 || result.Repository != repository {
		t.Fatalf("result=%+v", result)
	}
	if _, err := os.Lstat(filepath.Join(repository, ".sf")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("apply created repository configuration: %v", err)
	}
	database, err := store.OpenReadOnly(ctx, paths.Database)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	project, err := database.Project(ctx, domain.ChannelDev, "relay")
	if err != nil {
		t.Fatal(err)
	}
	effective, err := config.DecodeSnapshot(project.ConfigSnapshot, project.ConfigDigest)
	if err != nil || effective.MaxConcurrentTickets != 1 {
		t.Fatalf("effective=%+v err=%v", effective, err)
	}
	replay := RunConfigApply(ctx, ConfigApplyRequest{Channel: domain.ChannelDev, Project: "relay", Home: home})
	if !replay.OK || !replay.Mutation.Observed {
		t.Fatalf("replay=%+v", replay)
	}
}

func TestRunConfigApplyRejectsImmutableBaseRetarget(t *testing.T) {
	ctx := context.Background()
	repository := initializedRepository(t)
	home := t.TempDir()
	if response := RunInit(ctx, InitRequest{Channel: domain.ChannelDev, Project: "relay", Repo: repository, Home: home}); !response.OK {
		t.Fatalf("init=%+v", response)
	}
	if err := os.Mkdir(filepath.Join(repository, ".sf"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(repository, ".sf", "config.toml"), []byte("base_branch = \"trunk\"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	response := RunConfigApply(ctx, ConfigApplyRequest{Channel: domain.ChannelDev, Project: "relay", Home: home})
	if response.OK || response.Error == nil || response.Error.Code != "project_conflict" {
		t.Fatalf("response=%+v", response)
	}
}

func TestRunConfigApplyRejectsRepositoryPathReplacementAfterBaseVerification(t *testing.T) {
	ctx := context.Background()
	repository := initializedRepository(t)
	replacement := initializedRepository(t)
	home := t.TempDir()
	if response := RunInit(ctx, InitRequest{Channel: domain.ChannelDev, Project: "relay", Repo: repository, Home: home}); !response.OK {
		t.Fatalf("init=%+v", response)
	}
	paths, err := config.PathsFor(home, domain.ChannelDev)
	if err != nil {
		t.Fatal(err)
	}
	previous := afterConfigApplyBaseVerification
	afterConfigApplyBaseVerification = func() {
		afterConfigApplyBaseVerification = nil
		moved := repository + ".original"
		if err := os.Rename(repository, moved); err != nil {
			t.Errorf("move registered repository: %v", err)
			return
		}
		if err := os.Rename(replacement, repository); err != nil {
			t.Errorf("replace registered repository: %v", err)
		}
	}
	t.Cleanup(func() { afterConfigApplyBaseVerification = previous })
	response := RunConfigApply(ctx, ConfigApplyRequest{Channel: domain.ChannelDev, Project: "relay", Home: home})
	if response.OK || response.Error == nil || response.Error.Code != "invalid_configuration" {
		t.Fatalf("response=%+v", response)
	}
	database, err := store.OpenReadOnly(ctx, paths.Database)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	project, err := database.Project(ctx, domain.ChannelDev, "relay")
	if err != nil || project.ConfigGeneration != 1 {
		t.Fatalf("path replacement mutated registration=%+v err=%v", project, err)
	}
}

func TestRunConfigApplySerializesWithProfileInitBeforeFreezingSnapshot(t *testing.T) {
	ctx := context.Background()
	repository := initializedRepository(t)
	testPath := "apps/api/tests/config-generation.test.ts"
	entrypoint := filepath.Join(repository, filepath.FromSlash(testPath))
	if err := os.MkdirAll(filepath.Dir(entrypoint), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(entrypoint, []byte("import test from 'node:test'; test('config generation', () => {});\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	home := t.TempDir()
	if response := RunInit(ctx, InitRequest{Channel: domain.ChannelDev, Project: "relay", Repo: repository, Home: home}); !response.OK {
		t.Fatalf("dev init=%+v", response)
	}
	lockHeld := make(chan struct{})
	releaseInit := make(chan struct{})
	var releaseOnce sync.Once
	priorLockHook := afterInitConfigLockAcquired
	afterInitConfigLockAcquired = func() {
		close(lockHeld)
		<-releaseInit
	}
	t.Cleanup(func() {
		releaseOnce.Do(func() { close(releaseInit) })
		afterInitConfigLockAcquired = priorLockHook
	})
	initDone := make(chan api.Response, 1)
	go func() {
		initDone <- RunInit(ctx, InitRequest{Channel: domain.ChannelStable, Project: "nysa", Repo: repository, Home: home, Profile: config.NysaPureAPIV1Profile, TestPath: testPath})
	}()
	select {
	case <-lockHeld:
	case response := <-initDone:
		t.Fatalf("profile init returned before acquiring repository configuration lock: %+v", response)
	case <-time.After(10 * time.Second):
		t.Fatal("profile init did not acquire the repository configuration lock")
	}
	applyReachedStore := make(chan struct{})
	priorApplyHook := afterConfigApplyBeforeStore
	afterConfigApplyBeforeStore = func() { close(applyReachedStore) }
	t.Cleanup(func() { afterConfigApplyBeforeStore = priorApplyHook })
	applyDone := make(chan api.Response, 1)
	go func() {
		applyDone <- RunConfigApply(ctx, ConfigApplyRequest{Channel: domain.ChannelDev, Project: "relay", Home: home})
	}()
	select {
	case <-applyReachedStore:
		t.Fatal("config apply reached Store while supported profile init held the root lock")
	default:
	}
	releaseOnce.Do(func() { close(releaseInit) })
	select {
	case response := <-initDone:
		if !response.OK || !response.Mutation.Attempted {
			t.Fatalf("profile init=%+v", response)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("profile init did not complete after release")
	}
	select {
	case <-applyReachedStore:
	case <-time.After(10 * time.Second):
		t.Fatal("config apply did not proceed after profile init released the root lock")
	}
	select {
	case response := <-applyDone:
		if !response.OK || response.Mutation.Observed {
			t.Fatalf("apply=%+v", response)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("config apply did not finish")
	}
	paths, err := config.PathsFor(home, domain.ChannelDev)
	if err != nil {
		t.Fatal(err)
	}
	database, err := store.OpenReadOnly(ctx, paths.Database)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	project, err := database.Project(ctx, domain.ChannelDev, "relay")
	if err != nil {
		t.Fatal(err)
	}
	effective, err := config.DecodeSnapshot(project.ConfigSnapshot, project.ConfigDigest)
	if err != nil || effective.PhaseTimeout != time.Minute {
		t.Fatalf("apply did not freeze serialized profile configuration=%+v err=%v", effective, err)
	}
}

func TestRunConfigApplyFreezesLastValidatedMachineSample(t *testing.T) {
	ctx := context.Background()
	repository := initializedRepository(t)
	home := t.TempDir()
	if response := RunInit(ctx, InitRequest{Channel: domain.ChannelDev, Project: "relay", Repo: repository, Home: home}); !response.OK {
		t.Fatalf("init=%+v", response)
	}
	paths, err := config.PathsFor(home, domain.ChannelDev)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(paths.Machine, []byte("max_concurrent_tickets = 1\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	priorHook := afterConfigApplyBeforeStore
	afterConfigApplyBeforeStore = func() {
		if err := os.WriteFile(paths.Machine, []byte("max_concurrent_tickets = 2\n"), 0o600); err != nil {
			t.Errorf("change machine policy after final sample: %v", err)
		}
	}
	t.Cleanup(func() { afterConfigApplyBeforeStore = priorHook })
	response := RunConfigApply(ctx, ConfigApplyRequest{Channel: domain.ChannelDev, Project: "relay", Home: home})
	if !response.OK || response.Mutation.Observed {
		t.Fatalf("apply=%+v", response)
	}
	database, err := store.OpenReadOnly(ctx, paths.Database)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	project, err := database.Project(ctx, domain.ChannelDev, "relay")
	if err != nil {
		t.Fatal(err)
	}
	effective, err := config.DecodeSnapshot(project.ConfigSnapshot, project.ConfigDigest)
	if err != nil || effective.Machine.MaxConcurrentTickets != 1 || effective.MaxConcurrentTickets != 1 {
		t.Fatalf("snapshot did not preserve final machine sample effective=%+v err=%v", effective, err)
	}
}

func TestRunConfigApplyRequiresExistingCompatibleAuthority(t *testing.T) {
	response := RunConfigApply(context.Background(), ConfigApplyRequest{Channel: domain.ChannelDev, Project: "relay", Home: t.TempDir()})
	if response.OK || response.Error == nil || response.Error.Code != "not_configured" {
		t.Fatalf("response=%+v", response)
	}
}

func TestRunConfigApplyRefusesFutureSchemaBeforeAnyGenerationWrite(t *testing.T) {
	ctx := context.Background()
	repository := initializedRepository(t)
	home := t.TempDir()
	if response := RunInit(ctx, InitRequest{Channel: domain.ChannelDev, Project: "relay", Repo: repository, Home: home}); !response.OK {
		t.Fatalf("init=%+v", response)
	}
	paths, err := config.PathsFor(home, domain.ChannelDev)
	if err != nil {
		t.Fatal(err)
	}
	database, err := sql.Open("sqlite", paths.Database)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.ExecContext(ctx, `INSERT INTO schema_migrations(version, applied_at) VALUES (999, 'future')`); err != nil {
		_ = database.Close()
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	response := RunConfigApply(ctx, ConfigApplyRequest{Channel: domain.ChannelDev, Project: "relay", Home: home})
	if response.OK || response.Error == nil || response.Error.Code != "schema_mismatch" || response.Mutation.Attempted {
		t.Fatalf("response=%+v", response)
	}
}
