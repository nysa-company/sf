package processsupervisor

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/executionpolicy"
	"github.com/nysa-company/sf/internal/pythonclosure"
)

func TestPreparedPythonIdentityUsesOnlyComposedRoot(t *testing.T) {
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(root, 0700); err != nil {
		t.Fatal(err)
	}
	stage := filepath.Join(root, "preparing")
	if err := os.Mkdir(stage, 0700); err != nil {
		t.Fatal(err)
	}
	manifests := make([]pythonclosure.Manifest, 2)
	for i, name := range []string{"runtime", "dependencies"} {
		dir := filepath.Join(stage, name)
		if err := os.Mkdir(dir, 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "entry"), []byte("identity fixture; never execute"), 0500); err != nil {
			t.Fatal(err)
		}
		fd, err := os.Open(dir)
		if err != nil {
			t.Fatal(err)
		}
		manifests[i], err = pythonclosure.CaptureDirectoryFD(t.Context(), int(fd.Fd()))
		fd.Close()
		if err != nil {
			t.Fatal(err)
		}
	}
	environment := pythonclosure.Environment{Version: pythonclosure.EnvironmentVersion, Runtime: manifests[0], Dependencies: manifests[1], Interpreter: "entry", LockDigest: "sha256:" + strings.Repeat("a", 64), BootstrapDigest: pythonclosure.BootstrapDigest()}
	data, digest, err := environment.Canonical()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(stage, "environment.json"), data, 0400); err != nil {
		t.Fatal(err)
	}
	prepared := filepath.Join(root, digest[7:])
	if err := os.Rename(stage, prepared); err != nil {
		t.Fatal(err)
	}
	argv := []string{"python3", pythonclosure.RecipeFlag, digest, environment.LockDigest, "tests"}
	s := RepositoryCommandSupervisor{PythonSnapshots: root}
	executable, got, err := s.CommandExecutableIdentity(t.Context(), argv)
	if err != nil || got != digest || executable != filepath.Join(prepared, "runtime/entry") {
		t.Fatalf("identity: %s %s %v", executable, got, err)
	}
	handle, launchPath, launchDigest, err := s.openPythonRuntime(t.Context(), argv)
	if err != nil {
		t.Fatal(err)
	}
	defer handle.Close()
	if launchPath != executable || launchDigest != got {
		t.Fatal("launch and claim identity differ")
	}
	paths := PythonSandboxPaths{Runtime: filepath.Join(prepared, "runtime"), Dependencies: filepath.Join(prepared, "dependencies")}
	if !pythonPreparedPathsMatch(handle, paths) {
		t.Fatal("matching retained roots refused")
	}
	held := paths.Runtime + "-held"
	if err := os.Rename(paths.Runtime, held); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(paths.Runtime, 0700); err != nil {
		t.Fatal(err)
	}
	if pythonPreparedPathsMatch(handle, paths) {
		t.Fatal("replacement path authenticated as retained root")
	}
	if err := os.Remove(paths.Runtime); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(held, paths.Runtime); err != nil {
		t.Fatal(err)
	}
	closed, finished, quarantined := 0, 0, 0
	artifacts := &stagedNodeArtifacts{runtime: func() { closed++; handle.Close() }}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	settleStagedNodeArtifacts(ctx, time.Millisecond, func() error { return ErrUnclear }, func(context.Context) error { finished++; return nil }, func() error { quarantined++; return nil }, artifacts)
	if closed != 0 || finished != 0 || quarantined != 1 || handle.Revalidate(t.Context()) != nil {
		t.Fatal("ambiguous drain released descriptors")
	}
	settleStagedNodeArtifacts(t.Context(), time.Millisecond, func() error { return nil }, func(context.Context) error { finished++; return nil }, func() error { quarantined++; return nil }, artifacts)
	artifacts.Close()
	if closed != 1 || finished != 1 || handle.RuntimeFD() != -1 {
		t.Fatal("proven drain did not release once")
	}
	if pythonPreparedPathsMatch(handle, paths) {
		t.Fatal("closed roots authenticated")
	}
	// The typed recipe is eligible only with explicit runtime composition.
	if (RepositoryCommandSupervisor{}).Preflight(contracts.CommandSpec{Argv: argv}) == nil || !executionpolicy.EvaluateRepositoryCommand(argv).Allowed {
		t.Fatal("prepared recipe lost its explicit composition requirement")
	}
	for _, other := range []string{"", t.TempDir(), root + "-missing"} {
		if _, _, err := (RepositoryCommandSupervisor{PythonSnapshots: other}).CommandExecutableIdentity(t.Context(), argv); err == nil {
			t.Fatal("used ambient or another root")
		}
	}
	if err := os.Chmod(root, 0755); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.CommandExecutableIdentity(t.Context(), argv); err == nil {
		t.Fatal("accepted public cache")
	}
	if err := os.Chmod(root, 0700); err != nil {
		t.Fatal(err)
	}
	entry := filepath.Join(prepared, "runtime/entry")
	if err := os.Chmod(entry, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(entry, []byte("changed"), 0500); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(entry, 0500); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.CommandExecutableIdentity(t.Context(), argv); err == nil {
		t.Fatal("accepted runtime mutation")
	}
}
