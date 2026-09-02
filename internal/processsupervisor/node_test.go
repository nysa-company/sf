package processsupervisor

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/nysapure"
)

func nodeDigest(data string) string {
	s := sha256.Sum256([]byte(data))
	return "sha256:" + hex.EncodeToString(s[:])
}

func TestNodeClosureIdentityBindsSourceAndSortedLibraries(t *testing.T) {
	base := nodeClosure{Executable: "/opt/homebrew/Cellar/node/22/bin/node", Digest: nodeDigest("node-v1"), Mode: 0o555, Entries: []nodeClosureEntry{
		{Load: "@rpath/libb.dylib", Path: "/opt/homebrew/lib/libb.dylib", Leaf: "libb.dylib", Digest: nodeDigest("b"), Mode: 0o444},
		{Load: "@rpath/liba.dylib", Path: "/opt/homebrew/lib/liba.dylib", Leaf: "liba.dylib", Digest: nodeDigest("a"), Mode: 0o444},
	}}
	one, err := nodeClosureIdentity(base)
	if err != nil {
		t.Fatal(err)
	}
	reordered := base
	reordered.Entries = []nodeClosureEntry{base.Entries[1], base.Entries[0]}
	two, err := nodeClosureIdentity(reordered)
	if err != nil || one != two {
		t.Fatalf("identity not canonical: %q %q err=%v", one, two, err)
	}
	replacedSource := base
	replacedSource.Digest = nodeDigest("node-v2")
	if got, _ := nodeClosureIdentity(replacedSource); got == one {
		t.Fatal("source Node replacement did not change closure identity")
	}
	replacedLibrary := base
	replacedLibrary.Entries = append([]nodeClosureEntry(nil), base.Entries...)
	replacedLibrary.Entries[0].Digest = nodeDigest("b2")
	if got, _ := nodeClosureIdentity(replacedLibrary); got == one {
		t.Fatal("dylib replacement did not change closure identity")
	}
}

func TestNodeClosureIdentityRejectsDuplicateLeafConflict(t *testing.T) {
	closure := nodeClosure{Executable: "/node", Digest: nodeDigest("node"), Mode: 0o555, Entries: []nodeClosureEntry{
		{Path: "/one/libsame.dylib", Leaf: "libsame.dylib", Digest: nodeDigest("one"), Mode: 0o444},
		{Path: "/two/libsame.dylib", Leaf: "libsame.dylib", Digest: nodeDigest("two"), Mode: 0o444},
	}}
	if _, err := nodeClosureIdentity(closure); err == nil {
		t.Fatal("duplicate dylib leaf was accepted")
	}
}

func TestNodeClosureIdentityRejectsCumulativeByteOverflow(t *testing.T) {
	closure := nodeClosure{Executable: "/node", Digest: nodeDigest("node"), Mode: 0o555, Size: 1, Bytes: 1, Entries: []nodeClosureEntry{
		{Path: "/lib/a.dylib", Leaf: "a.dylib", Digest: nodeDigest("a"), Mode: 0o444, Size: nodeClosureFileBytes},
		{Path: "/lib/b.dylib", Leaf: "b.dylib", Digest: nodeDigest("b"), Mode: 0o444, Size: nodeClosureFileBytes},
		{Path: "/lib/c.dylib", Leaf: "c.dylib", Digest: nodeDigest("c"), Mode: 0o444, Size: nodeClosureFileBytes},
		{Path: "/lib/d.dylib", Leaf: "d.dylib", Digest: nodeDigest("d"), Mode: 0o444, Size: nodeClosureFileBytes},
		{Path: "/lib/e.dylib", Leaf: "e.dylib", Digest: nodeDigest("e"), Mode: 0o444, Size: nodeClosureFileBytes},
	}}
	if _, err := nodeClosureIdentity(closure); err == nil {
		t.Fatal("aggregate closure byte overflow was accepted")
	}
}

func TestNysaPureRuntimeIdentityBindsNodeClosureAndFactoryLoader(t *testing.T) {
	closure := "node22-closure-v1:sha256:" + strings.Repeat("a", 64)
	identity, err := nysaPureRuntimeIdentity(closure)
	if err != nil || !strings.HasPrefix(identity, "nysa-api-pure-v1:sha256:") {
		t.Fatalf("identity=%q err=%v", identity, err)
	}
	if _, err := nysaPureRuntimeIdentity("sha256:" + strings.Repeat("a", 64)); err == nil {
		t.Fatal("plain executable digest accepted")
	}
	root := t.TempDir()
	loader, err := stageNysaPureLoader(root)
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(loader)
	if err != nil || !nysapure.EqualLoaderSource(data) {
		t.Fatalf("staged loader err=%v", err)
	}
}

func TestNode22VersionQualificationUsesRecipeMinimum(t *testing.T) {
	for version, want := range map[string]bool{
		"v22.7.0":      false,
		"v22.8.0":      true,
		"v22.23.2":     true,
		"v21.9.0":      false,
		"v23.0.0":      false,
		"v22.8.0-rc.1": false,
		"v22.+8.0":     false,
		"v22.08.0":     false,
	} {
		if got := node22VersionUsable(version); got != want {
			t.Errorf("node22VersionUsable(%q)=%v, want %v", version, got, want)
		}
	}
}

func TestNodeLaunchFlagsPureRequireLoaderWorkerWithoutBroaderPermissions(t *testing.T) {
	flags := nodeLaunchFlags(true, "/private/staged/loader.mjs", "apps/api/tests/retrieval-fusion.test.ts", "/private/source", "/private/staged")
	ordered, err := nodePureSourceReadFlags(flags, []string{"/private/source/apps/api/tests/retrieval-fusion.test.ts"})
	if err != nil {
		t.Fatal(err)
	}
	readIndex, testIndex := -1, -1
	for index, flag := range ordered {
		if flag == "--allow-fs-read=/private/source/apps/api/tests/retrieval-fusion.test.ts" {
			readIndex = index
		}
		if flag == "--test" {
			testIndex = index
		}
	}
	if readIndex < 0 || testIndex < 0 || readIndex >= testIndex {
		t.Fatalf("pure source read flags must precede --test: %v", ordered)
	}
	have := make(map[string]bool, len(flags))
	for _, flag := range flags {
		have[flag] = true
	}
	for _, flag := range []string{
		"--permission",
		"--allow-worker",
		"--allow-fs-read=/private/source",
		"--allow-fs-read=/private/staged",
		"--experimental-loader",
		"/private/staged/loader.mjs",
		"--test",
		"apps/api/tests/retrieval-fusion.test.ts",
	} {
		if !have[flag] {
			t.Errorf("pure Node flags missing %q: %v", flag, flags)
		}
	}
	for _, flag := range []string{
		"--allow-fs-read=/private/worktree",
		"--allow-child-process",
		"--allow-net",
		"--allow-fs-write",
		"--allow-addons",
	} {
		if have[flag] {
			t.Errorf("pure Node flags unexpectedly broaden permission with %q: %v", flag, flags)
		}
	}
}

func TestStagedNodeClosureTamperFails(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, "source-node")
	if err := os.WriteFile(source, []byte("node bytes"), 0o500); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(source)
	if err != nil {
		t.Fatal(err)
	}
	closure := nodeClosure{Executable: source, Digest: nodeDigest("node bytes"), Mode: info.Mode().Perm(), Size: info.Size(), Bytes: info.Size()}
	identity, err := nodeClosureIdentity(closure)
	if err != nil {
		t.Fatal(err)
	}
	staged, library, cleanup, err := stageNodeClosure(closure, identity)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	if err := os.Chmod(staged, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(staged, []byte("tampered"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := verifyStagedNodeClosure(closure, staged, library); err == nil {
		t.Fatal("tampered staged node was accepted")
	}
}

func TestStagedNodeClosureSealsRuntimeAndRejectsDirectoryReplacement(t *testing.T) {
	root := t.TempDir()
	nodeSource := filepath.Join(root, "source-node")
	libSource := filepath.Join(root, "source-lib.dylib")
	if err := os.WriteFile(nodeSource, []byte("node bytes"), 0o500); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(libSource, []byte("library bytes"), 0o500); err != nil {
		t.Fatal(err)
	}
	nodeInfo, err := os.Stat(nodeSource)
	if err != nil {
		t.Fatal(err)
	}
	libInfo, err := os.Stat(libSource)
	if err != nil {
		t.Fatal(err)
	}
	closure := nodeClosure{
		Executable: nodeSource,
		Digest:     nodeDigest("node bytes"),
		Mode:       nodeInfo.Mode().Perm(),
		Size:       nodeInfo.Size(),
		Bytes:      nodeInfo.Size() + libInfo.Size(),
		Entries: []nodeClosureEntry{{
			Path: libSource, Leaf: "source-lib.dylib", Digest: nodeDigest("library bytes"), Mode: libInfo.Mode().Perm(), Size: libInfo.Size(),
		}},
	}
	identity, err := nodeClosureIdentity(closure)
	if err != nil {
		t.Fatal(err)
	}
	staged, library, loader, cleanup, err := stageNysaPureNodeClosure(closure, identity)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	base := filepath.Dir(filepath.Dir(staged))
	for _, path := range []string{base, filepath.Dir(staged), library} {
		info, err := os.Lstat(path)
		if err != nil || info.Mode().Perm() != 0o500 {
			t.Fatalf("runtime directory %s was not sealed: info=%v err=%v", path, info, err)
		}
	}
	for _, path := range []string{staged, filepath.Join(library, "source-lib.dylib"), loader} {
		info, err := os.Lstat(path)
		if err != nil || info.Mode().Perm() != 0o500 {
			t.Fatalf("runtime file %s was not sealed: info=%v err=%v", path, info, err)
		}
	}
	if err := os.Chmod(base, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(library, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(library); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(library, 0o500); err != nil {
		t.Fatal(err)
	}
	if err := verifyStagedNodeClosure(closure, staged, library); err == nil {
		t.Fatal("replacement sealed runtime directory was accepted")
	}
}

func TestStagedNodeLifecycleClosesBothArtifactsOnce(t *testing.T) {
	var runtime, source, finish, quarantined int
	artifacts := &stagedNodeArtifacts{runtime: func() { runtime++ }, source: func() { source++ }}
	settleStagedNodeArtifacts(context.Background(), time.Millisecond, func() error { return nil }, func(context.Context) error {
		finish++
		return nil
	}, func() error {
		quarantined++
		return nil
	}, artifacts)
	artifacts.Close()
	if runtime != 1 || source != 1 || finish != 1 || quarantined != 0 {
		t.Fatalf("runtime=%d source=%d finish=%d quarantine=%d", runtime, source, finish, quarantined)
	}
}

func TestRepositoryLaunchFinisherMemoizesFinishOutcome(t *testing.T) {
	for _, test := range []struct {
		name string
		run  func(context.Context) error
		want error
	}{
		{name: "success", run: func(context.Context) error { return nil }},
		{name: "failure", run: func(context.Context) error { return errors.New("finish unavailable") }, want: errors.New("finish unavailable")},
	} {
		t.Run(test.name, func(t *testing.T) {
			calls := 0
			finisher := &repositoryLaunchFinisher{run: func(ctx context.Context) error {
				calls++
				return test.run(ctx)
			}}
			first := finisher.Finish(context.Background())
			second := finisher.Finish(context.Background())
			if calls != 1 || (first == nil) != (test.want == nil) || (second == nil) != (test.want == nil) {
				t.Fatalf("calls=%d first=%v second=%v want=%v", calls, first, second, test.want)
			}
			if test.want != nil && (first == nil || first.Error() != test.want.Error() || second.Error() != test.want.Error()) {
				t.Fatalf("first=%v second=%v want=%v", first, second, test.want)
			}
		})
	}
}

func TestStagedNodeLifecycleQuarantinesAndCleansAfterGoneFinishFailure(t *testing.T) {
	var runtime, source, finish, quarantined int
	artifacts := &stagedNodeArtifacts{runtime: func() { runtime++ }, source: func() { source++ }}
	settleStagedNodeArtifacts(context.Background(), time.Millisecond, func() error { return nil }, func(context.Context) error {
		finish++
		return errors.New("durable finish unavailable")
	}, func() error {
		quarantined++
		return nil
	}, artifacts)
	if runtime != 1 || source != 1 || finish != 1 || quarantined != 1 {
		t.Fatalf("runtime=%d source=%d finish=%d quarantine=%d", runtime, source, finish, quarantined)
	}
}

func TestStagedNodeLifecycleRetainsArtifactsUntilGoneProof(t *testing.T) {
	var runtime, source, finish, quarantined int
	artifacts := &stagedNodeArtifacts{runtime: func() { runtime++ }, source: func() { source++ }}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Millisecond)
	defer cancel()
	settleStagedNodeArtifacts(ctx, time.Millisecond, func() error {
		return errors.New("process identity still observable")
	}, func(context.Context) error {
		finish++
		return nil
	}, func() error {
		quarantined++
		return nil
	}, artifacts)
	if runtime != 0 || source != 0 || finish != 0 || quarantined != 1 {
		t.Fatalf("runtime=%d source=%d finish=%d quarantine=%d", runtime, source, finish, quarantined)
	}
}

func TestNode22IdentityUsesCodeOwnedCandidateWhenAvailable(t *testing.T) {
	if _, err := os.Stat("/opt/homebrew/bin/node"); err != nil {
		t.Skip("Homebrew Node is unavailable")
	}
	path, identity, err := node22Identity("node")
	if err != nil {
		t.Fatalf("Node 22 identity: %v", err)
	}
	if path == "/opt/homebrew/bin/node" || !strings.HasPrefix(identity, "node22-closure-v1:") {
		t.Fatalf("path=%q identity=%q", path, identity)
	}
}

func TestStagedNodePermissionFlagsAuthorizeWorktreeRead(t *testing.T) {
	if _, err := os.Stat("/opt/homebrew/bin/node"); err != nil {
		t.Skip("Homebrew Node is unavailable")
	}
	resolved, err := resolveFixedNodeExecutable("node")
	if err != nil {
		t.Fatal(err)
	}
	closure, err := nodeClosureFor(resolved)
	if err != nil {
		t.Fatal(err)
	}
	identity, err := nodeClosureIdentity(closure)
	if err != nil {
		t.Fatal(err)
	}
	staged, library, cleanup, err := stageNodeClosure(closure, identity)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	worktree := t.TempDir()
	if err := os.Mkdir(filepath.Join(worktree, "test"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(worktree, "fixture.txt"), []byte("ok"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(worktree, "test", "read.test.mjs"), []byte(`import test from 'node:test'; import fs from 'node:fs'; test('read', () => { if (fs.readFileSync(new URL('../fixture.txt', import.meta.url), 'utf8') !== 'ok') throw new Error('read'); });`), 0o600); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(staged, "--permission", "--allow-fs-read="+worktree, "--allow-fs-read="+filepath.Dir(library), "--no-addons", "--experimental-test-isolation=none", "--test-concurrency=1", "--test")
	cmd.Dir = worktree
	cmd.Env = []string{"PATH=/usr/bin:/bin", "DYLD_LIBRARY_PATH=" + library, "HOME=/var/empty", "TMPDIR=/var/empty"}
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("staged Node permission test: %v: %s", err, out)
	}
}
