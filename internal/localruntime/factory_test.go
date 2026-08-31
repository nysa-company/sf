package localruntime

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/daemon"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/engine"
	"github.com/nysa-company/sf/internal/providercoord"
	"github.com/nysa-company/sf/internal/runtimeassets"
	"github.com/nysa-company/sf/internal/statemachine"
	"github.com/nysa-company/sf/internal/store"
	"github.com/nysa-company/sf/internal/testkit"
	"github.com/nysa-company/sf/internal/workflowruntime"
)

func TestFactoryLeavesUnqualifiedDaemonIdleWithoutResolvingAssets(t *testing.T) {
	database := openStore(t)
	called := false
	compose := factory(Config{Channel: domain.ChannelDev, GitHome: filepath.Join(t.TempDir(), "git-home"), PrePublishingOnly: true}, func(domain.Channel) (runtimeassets.Core, error) {
		called = true
		return runtimeassets.Core{}, errors.New("must not resolve")
	})
	components, err := compose(daemon.RuntimeDependencies{Store: database, Engine: engine.New(database, statemachine.Spec{})})
	if err != nil || components.Runtime != nil || components.Controller != nil || called {
		t.Fatalf("idle components=%+v called=%v err=%v", components, called, err)
	}
}

func TestFactoryComposesExactTwoWorkerRuntimeAndController(t *testing.T) {
	database := openStore(t)
	coordinator := readyCoordinator(t, database)
	root := t.TempDir()
	primary := executable(t, root, "sf-dev")
	helper := executable(t, root, "sf-git-exec-dev")
	gitParent := t.TempDir()
	if err := os.Chmod(gitParent, 0o700); err != nil {
		t.Fatal(err)
	}
	gitHome := filepath.Join(gitParent, "git-home")
	compose := factory(Config{Channel: domain.ChannelDev, GitHome: gitHome, PrePublishingOnly: true, Interval: 10 * time.Millisecond, Workers: 2}, func(channel domain.Channel) (runtimeassets.Core, error) {
		if channel != domain.ChannelDev {
			t.Fatalf("channel=%s", channel)
		}
		return runtimeassets.Core{Executable: primary, GitExec: helper}, nil
	})
	components, err := compose(daemon.RuntimeDependencies{Store: database, Engine: engine.New(database, statemachine.Spec{}), ProviderCoordinator: coordinator})
	if err != nil || components.Runtime == nil || components.Controller == nil {
		t.Fatalf("components=%+v err=%v", components, err)
	}
	info, err := os.Lstat(gitHome)
	if err != nil || !info.IsDir() || info.Mode().Perm()&0o077 != 0 {
		t.Fatalf("git HOME mode=%v err=%v", info, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	if err := components.Runtime.Start(ctx, domain.Fence{LeaderEpoch: 1}); err != nil {
		cancel()
		t.Fatal(err)
	}
	cancel()
	if err := components.Runtime.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestFactoryRejectsUnsafeGitHomeBeforeResolvingAssets(t *testing.T) {
	database := openStore(t)
	coordinator := readyCoordinator(t, database)
	root := t.TempDir()
	if err := os.Chmod(root, 0o700); err != nil {
		t.Fatal(err)
	}
	real := filepath.Join(root, "real")
	if err := os.Mkdir(real, 0o700); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(root, "link")
	if err := os.Symlink(real, link); err != nil {
		t.Fatal(err)
	}
	called := false
	compose := factory(Config{Channel: domain.ChannelDev, GitHome: link, PrePublishingOnly: true}, func(domain.Channel) (runtimeassets.Core, error) {
		called = true
		return runtimeassets.Core{}, nil
	})
	components, err := compose(daemon.RuntimeDependencies{Store: database, Engine: engine.New(database, statemachine.Spec{}), ProviderCoordinator: coordinator})
	if err == nil || components.Runtime != nil || components.Controller != nil || called {
		t.Fatalf("components=%+v called=%v err=%v", components, called, err)
	}
}

func TestFactoryRejectsPartialPublicationCapability(t *testing.T) {
	database := openStore(t)
	coordinator := readyCoordinator(t, database)
	root := t.TempDir()
	if err := os.Chmod(root, 0o700); err != nil {
		t.Fatal(err)
	}
	called := false
	compose := factory(Config{Channel: domain.ChannelDev, GitHome: filepath.Join(root, "git-home"), OwnerHome: root}, func(domain.Channel) (runtimeassets.Core, error) {
		called = true
		return runtimeassets.Core{}, nil
	})
	components, err := compose(daemon.RuntimeDependencies{Store: database, Engine: engine.New(database, statemachine.Spec{}), ProviderCoordinator: coordinator})
	if err == nil || components.Runtime != nil || components.Controller != nil || !called {
		t.Fatalf("partial publication capability components=%+v called=%v err=%v", components, called, err)
	}
}

func TestFactoryPrePublishingModeDisablesPublicationAdmission(t *testing.T) {
	database := openStore(t)
	coordinator := readyCoordinator(t, database)
	root := t.TempDir()
	if err := os.Chmod(root, 0o700); err != nil {
		t.Fatal(err)
	}
	compose := factory(Config{Channel: domain.ChannelDev, GitHome: filepath.Join(root, "git-home"), PrePublishingOnly: true}, func(domain.Channel) (runtimeassets.Core, error) {
		return runtimeassets.Core{Executable: executable(t, root, "sf-dev"), GitExec: executable(t, root, "sf-git-exec-dev")}, nil
	})
	components, err := compose(daemon.RuntimeDependencies{Store: database, Engine: engine.New(database, statemachine.Spec{}), ProviderCoordinator: coordinator})
	if err != nil {
		t.Fatal(err)
	}
	managed, ok := components.Runtime.(*managedRuntime)
	if !ok || managed.runtime.Scheduler.AdmitPublishing {
		t.Fatalf("pre-publishing runtime admitted publication: runtime=%T", components.Runtime)
	}
	if err := components.Runtime.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestFactoryPrePublishingBlocksPublishingTicketWithChannelAction(t *testing.T) {
	database := openStore(t)
	if err := database.CreateProject(t.Context(), store.Project{Channel: domain.ChannelDev, ID: "nysa", Path: "/tmp/nysa", BaseRef: "main"}); err != nil {
		t.Fatal(err)
	}
	coordinator := readyCoordinator(t, database)
	root := t.TempDir()
	if err := os.Chmod(root, 0o700); err != nil {
		t.Fatal(err)
	}
	spec, err := statemachine.LoadEmbeddedApproved()
	if err != nil {
		t.Fatal(err)
	}
	ref := domain.TicketRef{Channel: domain.ChannelDev, Project: "nysa", Ticket: "SF-factory-prepublishing"}
	if err := database.CreateTicket(t.Context(), store.Ticket{Ref: ref, State: domain.StatePublishing, SourceDigest: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", Type: domain.TicketFeature, MergeMode: domain.MergeGuarded, CreatedAt: time.Now().UTC()}); err != nil {
		t.Fatal(err)
	}
	leader, err := database.AcquireLeader(t.Context(), ref.Channel, "factory-prepublishing")
	if err != nil {
		t.Fatal(err)
	}
	compose := factory(Config{Channel: domain.ChannelDev, GitHome: filepath.Join(root, "git-home"), PrePublishingOnly: true}, func(domain.Channel) (runtimeassets.Core, error) {
		return runtimeassets.Core{Executable: executable(t, root, "sf-dev"), GitExec: executable(t, root, "sf-git-exec-dev")}, nil
	})
	components, err := compose(daemon.RuntimeDependencies{Store: database, Engine: engine.New(database, spec), ProviderCoordinator: coordinator})
	if err != nil {
		t.Fatal(err)
	}
	managed, ok := components.Runtime.(*managedRuntime)
	if !ok {
		t.Fatalf("runtime=%T", components.Runtime)
	}
	t.Cleanup(func() { _ = managed.Close() })
	result := managed.runtime.Scheduler.Tick(t.Context(), domain.Fence{LeaderEpoch: leader})
	if result.Outcome != workflowruntime.OutcomeInvoked {
		t.Fatalf("factory prepublishing tick=%+v", result)
	}
	blocked, err := database.Ticket(t.Context(), ref)
	if err != nil || blocked.State != domain.StateBlocked || blocked.ResumeState != domain.StatePublishing || blocked.BlockedCode != publishingUnavailableCode {
		t.Fatalf("factory blocked ticket=%+v err=%v", blocked, err)
	}
	events, err := database.Events(t.Context(), ref.Channel, 0, 10)
	if err != nil || len(events) != 2 || events[1].Trigger != "typed_blocker" {
		t.Fatalf("factory block events=%+v err=%v", events, err)
	}
	var payload struct {
		NextAction string `json:"next_action"`
		Guidance   string `json:"guidance"`
	}
	if json.Unmarshal([]byte(events[1].Payload), &payload) != nil || payload.NextAction != "sf-dev doctor" || payload.Guidance == "" {
		t.Fatalf("factory block payload=%s", events[1].Payload)
	}
}

func TestFactoryComposesSnapshotBoundPublicationCapability(t *testing.T) {
	database := openStore(t)
	coordinator := readyCoordinator(t, database)
	root := t.TempDir()
	if err := os.Chmod(root, 0o700); err != nil {
		t.Fatal(err)
	}
	primary := executable(t, root, "sf-dev")
	gitExec := executable(t, root, "sf-git-exec-dev")
	credential := executable(t, root, "sf-git-credential-dev")
	gh := executable(t, root, "gh")
	configDir := filepath.Join(root, "gh-config")
	if err := os.Mkdir(configDir, 0o700); err != nil {
		t.Fatal(err)
	}
	compose := factoryWithResolvers(Config{Channel: domain.ChannelDev, GitHome: filepath.Join(root, "git-home"), OwnerHome: root, GHConfigDir: configDir, GHBinary: gh, GHAuthenticated: true}, func(domain.Channel) (runtimeassets.Core, error) {
		return runtimeassets.Core{Executable: primary, GitExec: gitExec}, nil
	}, func(domain.Channel, string) (runtimeassets.Publication, error) {
		return runtimeassets.Publication{CredentialHelper: credential}, nil
	})
	components, err := compose(daemon.RuntimeDependencies{Store: database, Engine: engine.New(database, statemachine.Spec{}), ProviderCoordinator: coordinator})
	if err != nil {
		t.Fatal(err)
	}
	managed, ok := components.Runtime.(*managedRuntime)
	if !ok || managed.gh == nil {
		t.Fatalf("publication runtime=%T managed=%+v", components.Runtime, managed)
	}
	capability, err := managed.gh.CredentialCapability()
	if err != nil {
		t.Fatal(err)
	}
	dispatcher, ok := managed.runtime.Scheduler.Worker.(Worker)
	if !ok || dispatcher.Publication.Git.GHBinary != capability.Path || dispatcher.Publication.Git.GHBinaryDigest != capability.Digest {
		t.Fatalf("worker does not use gh snapshot: worker=%T capability=%+v", managed.runtime.Scheduler.Worker, capability)
	}
	if err := components.Runtime.Close(); err != nil {
		t.Fatal(err)
	}
}

func openStore(t *testing.T) *store.Store {
	t.Helper()
	database, err := store.Open(context.Background(), filepath.Join(t.TempDir(), "state.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	return database
}

func readyCoordinator(t *testing.T, database *store.Store) *providercoord.Coordinator {
	t.Helper()
	identity := domain.ProviderIdentity{Provider: "fixture", Model: "fixture-model", Family: "fixture-family", Version: "1"}
	provider := testkit.NewScriptedProvider(identity)
	registry := providercoord.NewRegistry()
	if err := registry.Register(context.Background(), provider); err != nil {
		t.Fatal(err)
	}
	routes := map[providercoord.Role]providercoord.Route{
		providercoord.RolePlanner:  {Primary: provider.Name(), Capacity: 1},
		providercoord.RoleBuilder:  {Primary: provider.Name(), Capacity: 1},
		providercoord.RoleReviewer: {Primary: provider.Name(), Capacity: 1},
	}
	coordinator, err := providercoord.New(registry, routes, database, nil, testkit.NewSupervisor())
	if err != nil {
		t.Fatal(err)
	}
	if err := coordinator.ReadyForPrePublishing(); err != nil {
		t.Fatal(err)
	}
	return coordinator
}

func executable(t *testing.T, root, name string) string {
	t.Helper()
	path := filepath.Join(root, name)
	if err := os.WriteFile(path, []byte("fixture"), 0o700); err != nil {
		t.Fatal(err)
	}
	return path
}
