//go:build sf_e2e

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/config"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/store"
)

// TestCompiledStableAndDevDaemonsCoexist proves the two local channels use
// separate roots, sockets, and SQLite authorities even when they share one
// operator HOME. It also replaces and restarts the dev executable while the
// stable daemon remains live, which is the minimum safe stable/dev improvement
// boundary for v1.
func TestCompiledStableAndDevDaemonsCoexist(t *testing.T) {
	if runtime.GOOS != "darwin" {
		t.Skip("compiled local-runtime channel coexistence is exercised on macOS")
	}

	stable, dev := compiledChannelCoexistenceBundle(t)
	home := compiledWalkingSkeletonHome(t)
	t.Cleanup(func() { _ = os.RemoveAll(home) })
	stablePaths, err := config.PathsFor(home, domain.ChannelStable)
	if err != nil {
		t.Fatal(err)
	}
	devPaths, err := config.PathsFor(home, domain.ChannelDev)
	if err != nil {
		t.Fatal(err)
	}
	if stablePaths.Root == devPaths.Root || stablePaths.Database == devPaths.Database || stablePaths.Socket == devPaths.Socket {
		t.Fatalf("channel paths overlap: stable=%+v dev=%+v", stablePaths, devPaths)
	}

	stableRoot, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	devRoot, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	stableRepository, _, _ := compiledWalkingSkeletonRepository(t, stableRoot)
	devRepository, _, _ := compiledWalkingSkeletonRepository(t, devRoot)

	compiledChannelCLI(t, stable, home, "init", "--project", "stable-app", "--repo", stableRepository, "--json")
	compiledChannelCLI(t, dev, home, "init", "--project", "dev-app", "--repo", devRepository, "--json")

	stableDaemon := compiledChannelStartDaemon(t, stable, home, stablePaths.Socket, "stable")
	devDaemon := compiledChannelStartDaemon(t, dev, home, devPaths.Socket, "dev")
	stablePID := stableDaemon.command.Process.Pid
	t.Cleanup(stableDaemon.Stop)
	t.Cleanup(devDaemon.Stop)

	stableFirst := compiledChannelSubmit(t, stable, home, "stable-app", "stable-first")
	devFirst := compiledChannelSubmit(t, dev, home, "dev-app", "dev-first")
	devSecond := compiledChannelSubmit(t, dev, home, "dev-app", "dev-second")
	if devSecond == devFirst {
		t.Fatalf("dev submit reused ticket identity: first=%s second=%s", devFirst, devSecond)
	}
	compiledChannelAssertTicketNotFound(t, stable, home, devSecond)

	stableSecond := compiledChannelSubmit(t, stable, home, "stable-app", "stable-second")
	stableThird := compiledChannelSubmit(t, stable, home, "stable-app", "stable-third")
	if stableThird == stableSecond || stableThird == stableFirst {
		t.Fatalf("stable submit reused ticket identity: first=%s second=%s third=%s", stableFirst, stableSecond, stableThird)
	}
	compiledChannelAssertTicketNotFound(t, dev, home, stableThird)

	// A dev rollout is constrained to the dev bundle. Stable keeps serving a
	// request while dev is down, then continues to use its original socket and
	// SQLite authority after the replacement dev executable restarts.
	devDaemon.Stop()
	if _, err := os.Lstat(devPaths.Socket); !os.IsNotExist(err) {
		t.Fatalf("dev socket remained after dev shutdown: %v", err)
	}
	compiledChannelAssertLive(t, stableDaemon, stable, home, stablePaths.Socket)
	compiledChannelSubmit(t, stable, home, "stable-app", "stable-while-dev-restarts")

	replacement := filepath.Join(filepath.Dir(dev), "sf-dev.next")
	compiledChannelBuild(t, replacement, ".", compiledChannelLDFlags(domain.ChannelDev))
	if err := os.Rename(replacement, dev); err != nil {
		t.Fatalf("activate rebuilt dev binary: %v", err)
	}
	devDaemon = compiledChannelStartDaemon(t, dev, home, devPaths.Socket, "rebuilt dev")
	t.Cleanup(devDaemon.Stop)
	compiledChannelAssertLive(t, stableDaemon, stable, home, stablePaths.Socket)
	if stableDaemon.command.Process.Pid != stablePID {
		t.Fatalf("stable daemon changed across dev replacement: before=%d after=%d", stablePID, stableDaemon.command.Process.Pid)
	}
	compiledChannelSubmit(t, dev, home, "dev-app", "dev-after-restart")

	compiledChannelAssertAuthorities(t, stablePaths.Database, devPaths.Database)
}

func compiledChannelCoexistenceBundle(t *testing.T) (stable, dev string) {
	t.Helper()
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	stableFlags := compiledChannelLDFlags(domain.ChannelStable)
	devFlags := compiledChannelLDFlags(domain.ChannelDev)
	for _, target := range []struct {
		name, packagePath, ldflags string
	}{
		{name: "sf", packagePath: ".", ldflags: stableFlags},
		{name: "sf-git-exec", packagePath: "../../cmd/sf-git-exec", ldflags: stableFlags},
		{name: "sf-dev", packagePath: ".", ldflags: devFlags},
		{name: "sf-git-exec-dev", packagePath: "../../cmd/sf-git-exec", ldflags: devFlags},
	} {
		compiledChannelBuild(t, filepath.Join(root, target.name), target.packagePath, target.ldflags)
	}
	return filepath.Join(root, "sf"), filepath.Join(root, "sf-dev")
}

func compiledChannelLDFlags(channel domain.Channel) string {
	version := "0.0.0-dev"
	if channel == domain.ChannelStable {
		version = "1.0.0"
	}
	const versionPackage = "github.com/nysa-company/sf/internal/version"
	const fixtureCommit = "0123456789abcdef0123456789abcdef01234567"
	return "-X " + versionPackage + ".Version=" + version + " -X " + versionPackage + ".Commit=" + fixtureCommit + " -X " + versionPackage + ".Channel=" + string(channel)
}

func compiledChannelBuild(t *testing.T, output, packagePath, ldflags string) {
	t.Helper()
	goBinary, err := exec.LookPath("go")
	if err != nil {
		t.Fatal(err)
	}
	args := []string{"build", "-tags", "sf_e2e"}
	if ldflags != "" {
		args = append(args, "-ldflags", ldflags)
	}
	args = append(args, "-o", output, packagePath)
	command := exec.Command(goBinary, args...)
	command.Dir = "."
	command.Env = compiledChannelBuildEnv(t, goBinary)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("build %s: %v\n%s", packagePath, err, output)
	}
}

func compiledChannelBuildEnv(t *testing.T, goBinary string) []string {
	t.Helper()
	// Runtime credentials stay isolated, but offline nested builds still need
	// the already-populated host module and object caches. Resolve those paths
	// explicitly before replacing HOME; never let Go derive an empty module
	// cache under the test home and then attempt network access.
	query := exec.Command(goBinary, "env", "-json", "GOCACHE", "GOMODCACHE")
	query.Env = os.Environ()
	output, err := query.Output()
	if err != nil {
		t.Fatalf("resolve Go build caches: %v", err)
	}
	var caches map[string]string
	if err := json.Unmarshal(output, &caches); err != nil {
		t.Fatalf("decode Go build caches: %v", err)
	}
	environment := compiledChannelEnv(t.TempDir())
	for _, key := range []string{"GOCACHE", "GOMODCACHE"} {
		value := caches[key]
		if !filepath.IsAbs(value) || filepath.Clean(value) != value {
			t.Fatalf("invalid %s path %q", key, value)
		}
		environment = append(environment, key+"="+value)
	}
	return append(environment, "GOPROXY=off", "GOSUMDB=off", "GOTOOLCHAIN=local")
}

type compiledChannelDaemon struct {
	t       *testing.T
	name    string
	command *exec.Cmd
	done    chan error
	output  compiledSafeBuffer
	socket  string
	stopped bool
}

func compiledChannelStartDaemon(t *testing.T, binary, home, socket, name string) *compiledChannelDaemon {
	t.Helper()
	daemon := &compiledChannelDaemon{t: t, name: name, socket: socket, done: make(chan error, 1)}
	daemon.command = exec.Command(binary, "daemon", "run")
	daemon.command.Env = compiledChannelEnv(home)
	daemon.command.Stdout, daemon.command.Stderr = &daemon.output, &daemon.output
	if err := daemon.command.Start(); err != nil {
		t.Fatalf("start compiled %s daemon: %v", name, err)
	}
	go func() { daemon.done <- daemon.command.Wait() }()
	compiledWalkingSkeletonWaitSocket(t, socket, daemon.done, &daemon.output, &daemon.stopped)
	return daemon
}

func (daemon *compiledChannelDaemon) Stop() {
	daemon.t.Helper()
	if daemon.stopped {
		return
	}
	daemon.stopped = true
	if daemon.command.Process != nil && daemon.command.ProcessState == nil {
		if err := daemon.command.Process.Signal(os.Interrupt); err != nil {
			daemon.t.Errorf("SIGINT compiled %s daemon: %v", daemon.name, err)
		}
	}
	select {
	case err := <-daemon.done:
		if err != nil {
			daemon.t.Errorf("compiled %s daemon graceful exit: %v\n%s", daemon.name, err, daemon.output.String())
		}
	case <-time.After(30 * time.Second):
		if daemon.command.Process != nil {
			_ = daemon.command.Process.Kill()
		}
		daemon.t.Errorf("compiled %s daemon did not stop within 30s: %s", daemon.name, daemon.output.String())
	}
}

func compiledChannelCLI(t *testing.T, binary, home string, args ...string) []byte {
	t.Helper()
	command := exec.Command(binary, args...)
	command.Env = compiledChannelEnv(home)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("compiled %s %s: %v\n%s", filepath.Base(binary), strings.Join(args, " "), err, output)
	}
	return output
}

func compiledChannelSubmit(t *testing.T, binary, home, project, title string) domain.TicketID {
	t.Helper()
	path := filepath.Join(home, title+".md")
	source := fmt.Sprintf("---\ntype: feature\nmerge: guarded\nmax_duration: 30m\nmax_cost_usd: 10\n---\n# %s\n\nChannel-isolation fixture.\n", title)
	if err := os.WriteFile(path, []byte(source), 0o600); err != nil {
		t.Fatal(err)
	}
	output := compiledChannelCLI(t, binary, home, "submit", path, "--project", project, "--json")
	var response struct {
		OK   bool `json:"ok"`
		Data struct {
			Ticket domain.TicketID `json:"ticket"`
		} `json:"data"`
	}
	if err := json.Unmarshal(output, &response); err != nil || !response.OK || response.Data.Ticket == "" {
		t.Fatalf("submit response=%s err=%v", output, err)
	}
	return response.Data.Ticket
}

func compiledChannelAssertTicketNotFound(t *testing.T, binary, home string, ticket domain.TicketID) {
	t.Helper()
	command := exec.Command(binary, "status", string(ticket), "--json")
	command.Env = compiledChannelEnv(home)
	output, err := command.CombinedOutput()
	if err == nil || !strings.Contains(string(output), `"code":"ticket_not_found"`) {
		t.Fatalf("%s status %s crossed channel boundary: err=%v output=%s", filepath.Base(binary), ticket, err, output)
	}
}

func compiledChannelEnv(home string) []string {
	blockedExact := map[string]struct{}{
		"HOME": {}, "PATH": {}, "GH_CONFIG_DIR": {}, "CODEX_HOME": {}, "XDG_CONFIG_HOME": {}, "GOCACHE": {}, "GOMODCACHE": {},
	}
	blockedPrefixes := []string{"GH_", "GITHUB_", "GIT_", "CODEX_", "SF_E2E_"}
	environment := make([]string, 0, len(os.Environ())+5)
	for _, item := range os.Environ() {
		key, _, ok := strings.Cut(item, "=")
		if !ok {
			continue
		}
		if _, blocked := blockedExact[key]; blocked {
			continue
		}
		skip := false
		for _, prefix := range blockedPrefixes {
			if strings.HasPrefix(key, prefix) {
				skip = true
				break
			}
		}
		if !skip {
			environment = append(environment, item)
		}
	}
	return append(environment,
		"HOME="+home,
		"PATH=/usr/bin:/bin:/usr/sbin:/sbin",
		"GH_CONFIG_DIR="+filepath.Join(home, ".gh-empty"),
		"CODEX_HOME="+filepath.Join(home, ".codex-empty"),
		"XDG_CONFIG_HOME="+filepath.Join(home, ".config-empty"),
	)
}

func compiledChannelAssertLive(t *testing.T, daemon *compiledChannelDaemon, binary, home, socket string) {
	t.Helper()
	select {
	case err := <-daemon.done:
		daemon.stopped = true
		t.Fatalf("compiled %s daemon exited unexpectedly: %v\n%s", daemon.name, err, daemon.output.String())
	default:
	}
	if _, err := os.Lstat(socket); err != nil {
		t.Fatalf("compiled %s socket unavailable: %v", daemon.name, err)
	}
	compiledChannelCLI(t, binary, home, "status", "--json")
}

func compiledChannelAssertAuthorities(t *testing.T, stablePath, devPath string) {
	t.Helper()
	stable, err := store.OpenReadOnly(context.Background(), stablePath)
	if err != nil {
		t.Fatalf("open stable authority: %v", err)
	}
	defer stable.Close()
	dev, err := store.OpenReadOnly(context.Background(), devPath)
	if err != nil {
		t.Fatalf("open dev authority: %v", err)
	}
	defer dev.Close()
	stableTickets, err := stable.Tickets(context.Background(), domain.ChannelStable, "stable-app", 100)
	if err != nil || len(stableTickets) != 4 {
		t.Fatalf("stable authority tickets=%+v err=%v, want exactly four", stableTickets, err)
	}
	devTickets, err := dev.Tickets(context.Background(), domain.ChannelDev, "dev-app", 100)
	if err != nil || len(devTickets) != 3 {
		t.Fatalf("dev authority tickets=%+v err=%v, want exactly three", devTickets, err)
	}
	if tickets, err := stable.Tickets(context.Background(), domain.ChannelDev, "", 100); err != nil || len(tickets) != 0 {
		t.Fatalf("stable authority exposed dev tickets=%+v err=%v", tickets, err)
	}
	if tickets, err := dev.Tickets(context.Background(), domain.ChannelStable, "", 100); err != nil || len(tickets) != 0 {
		t.Fatalf("dev authority exposed stable tickets=%+v err=%v", tickets, err)
	}
}
