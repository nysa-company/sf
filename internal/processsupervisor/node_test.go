package processsupervisor

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
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
