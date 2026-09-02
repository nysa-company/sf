package processsupervisor

// The Node recipe deliberately stages just the Mach-O runtime closure instead
// of accepting Homebrew/NVM/PATH as ambient authority. It is Darwin-only at
// execution time, but uses debug/macho so the parser and identity stay
// compile-tested on every host.

import (
	"context"
	"crypto/sha256"
	"debug/macho"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/executionpolicy"
	"github.com/nysa-company/sf/internal/nodeclosure"
	"github.com/nysa-company/sf/internal/nysapure"
)

const (
	nodeClosureVersion             = "node22-closure-v1"
	nodeClosureEntries             = 128
	nodeClosureBytes         int64 = 256 << 20
	nodeClosureFileBytes     int64 = 64 << 20
	nodeClosureDepth               = 16
	nodeClosureStageTime           = 15 * time.Second
	stagedNodeRecoveryWindow       = 30 * time.Second
	stagedNodeRecoveryPoll         = 250 * time.Millisecond
)

type nodeClosureEntry struct {
	Load, Path, Leaf, Digest string
	Mode                     os.FileMode
	Size                     int64
}

type nodeClosure struct {
	Executable, Digest string
	Mode               os.FileMode
	Size, Bytes        int64
	Entries            []nodeClosureEntry
}

func resolveFixedNodeExecutable(name string) (string, error) {
	if name == "" || filepath.Base(name) != "node" {
		return "", exec.ErrNotFound
	}
	for _, candidate := range []string{"/opt/homebrew/bin/node", "/usr/local/bin/node"} {
		resolved, err := filepath.EvalSymlinks(candidate)
		if err != nil {
			continue
		}
		if filepath.IsAbs(name) {
			provided, err := filepath.EvalSymlinks(name)
			if err != nil || provided != resolved {
				continue
			}
		}
		return resolved, nil
	}
	return "", exec.ErrNotFound
}

func nodeClosureFor(path string) (nodeClosure, error) {
	if !cleanAbsolute(path) {
		return nodeClosure{}, ErrUnclear
	}
	info, err := secureNodeSource(path)
	if err != nil {
		return nodeClosure{}, err
	}
	if info.Size() < 0 || info.Size() > nodeClosureFileBytes || info.Size() > nodeClosureBytes {
		return nodeClosure{}, ErrUnclear
	}
	digest, err := executableFileDigest(path)
	if err != nil {
		return nodeClosure{}, err
	}
	c := nodeClosure{Executable: path, Digest: digest, Mode: info.Mode().Perm(), Size: info.Size(), Bytes: info.Size()}
	seen := map[string]bool{path: true}
	byLeaf := map[string]string{}
	var visit func(string, int) error
	visit = func(current string, depth int) error {
		if depth > nodeClosureDepth || len(c.Entries) > nodeClosureEntries {
			return fmt.Errorf("closure bound at %s: %w", current, ErrUnclear)
		}
		f, err := macho.Open(current)
		if err != nil {
			return fmt.Errorf("parse %s: %w", current, ErrUnclear)
		}
		defer f.Close()
		loads, rpaths := machoLoads(f)
		for _, load := range loads {
			if trustedSystemDylib(load) {
				continue
			}
			resolved, err := resolveNodeDylib(load, current, c.Executable, rpaths)
			if err != nil || trustedSystemDylib(resolved) {
				if err != nil {
					return fmt.Errorf("resolve %s from %s: %w", load, current, ErrUnclear)
				}
				continue
			}
			entryInfo, err := secureNodeSource(resolved)
			if err != nil {
				return fmt.Errorf("authenticate %s from %s: %w", resolved, current, err)
			}
			leaf := filepath.Base(load)
			if leaf == "." || leaf == string(filepath.Separator) || strings.Contains(leaf, string(filepath.Separator)) {
				return fmt.Errorf("invalid leaf %s: %w", leaf, ErrUnclear)
			}
			if other, exists := byLeaf[leaf]; exists && other != resolved {
				return fmt.Errorf("duplicate leaf %s: %w", leaf, ErrUnclear)
			}
			byLeaf[leaf] = resolved
			if !seen[resolved] {
				if len(c.Entries) >= nodeClosureEntries {
					return fmt.Errorf("too many closure entries: %w", ErrUnclear)
				}
				if entryInfo.Size() < 0 || entryInfo.Size() > nodeClosureFileBytes || entryInfo.Size() > nodeClosureBytes-c.Bytes {
					return fmt.Errorf("closure byte bound: %w", ErrUnclear)
				}
				d, err := executableFileDigest(resolved)
				if err != nil {
					return err
				}
				c.Entries = append(c.Entries, nodeClosureEntry{Load: load, Path: resolved, Leaf: leaf, Digest: d, Mode: entryInfo.Mode().Perm(), Size: entryInfo.Size()})
				c.Bytes += entryInfo.Size()
				seen[resolved] = true
				if err := visit(resolved, depth+1); err != nil {
					return err
				}
			}
		}
		return nil
	}
	if err := visit(path, 0); err != nil {
		return nodeClosure{}, err
	}
	sort.Slice(c.Entries, func(i, j int) bool { return c.Entries[i].Path < c.Entries[j].Path })
	return c, nil
}

func machoLoads(f *macho.File) (loads, rpaths []string) {
	for _, load := range f.Loads {
		switch v := load.(type) {
		case *macho.Dylib:
			loads = append(loads, v.Name)
		case *macho.Rpath:
			rpaths = append(rpaths, v.Path)
		}
	}
	return loads, rpaths
}

func resolveNodeDylib(load, loader, executable string, rpaths []string) (string, error) {
	resolve := func(value string) (string, error) {
		value = strings.ReplaceAll(value, "@loader_path", filepath.Dir(loader))
		value = strings.ReplaceAll(value, "@executable_path", filepath.Dir(executable))
		value = filepath.Clean(value)
		if !filepath.IsAbs(value) {
			return "", ErrUnclear
		}
		// Homebrew's opt paths are aliases. Resolve them once into the actual
		// Cellar file, then reject symlinks at that final identity; we never
		// stage or later execute an alias pathname.
		resolved, err := filepath.EvalSymlinks(value)
		if err != nil || !cleanAbsolute(resolved) {
			return "", ErrUnclear
		}
		return resolved, nil
	}
	if strings.HasPrefix(load, "@rpath/") {
		for _, rp := range rpaths {
			// Do not filepath.Join before expanding @loader_path: Join cleans the
			// symbolic prefix's .. component as though it were a real directory.
			candidate, err := resolve(rp + "/" + strings.TrimPrefix(load, "@rpath/"))
			if err != nil {
				continue
			}
			if _, err := os.Lstat(candidate); err == nil {
				return candidate, nil
			}
		}
		return "", ErrUnclear
	}
	return resolve(load)
}

func trustedSystemDylib(path string) bool {
	return path == "/usr/lib" || strings.HasPrefix(path, "/usr/lib/") || path == "/System" || strings.HasPrefix(path, "/System/")
}

func secureNodeSource(path string) (os.FileInfo, error) {
	info, err := os.Lstat(path)
	if err != nil || info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() || info.Mode()&0o022 != 0 || !trustedOwner(info) {
		return nil, ErrUnclear
	}
	return info, nil
}

// nodeClosureIdentity names both the executable and all non-system libraries;
// its canonical JSON form is deliberately not a bare aggregate dylib hash.
func nodeClosureIdentity(c nodeClosure) (string, error) {
	if c.Executable == "" || c.Digest == "" || len(c.Entries) > nodeClosureEntries || c.Size < 0 || c.Size > nodeClosureFileBytes || c.Bytes < c.Size || c.Bytes > nodeClosureBytes {
		return "", ErrUnclear
	}
	entries := append([]nodeClosureEntry(nil), c.Entries...)
	sort.Slice(entries, func(i, j int) bool { return entries[i].Path < entries[j].Path })
	seenLeaves := map[string]string{}
	total := c.Size
	for _, entry := range entries {
		if entry.Path == "" || entry.Leaf == "" || entry.Digest == "" || entry.Size < 0 || entry.Size > nodeClosureFileBytes || entry.Size > nodeClosureBytes-total {
			return "", ErrUnclear
		}
		if previous, exists := seenLeaves[entry.Leaf]; exists && previous != entry.Path {
			return "", ErrUnclear
		}
		seenLeaves[entry.Leaf] = entry.Path
		total += entry.Size
	}
	if total > nodeClosureBytes || total != c.Bytes {
		return "", ErrUnclear
	}
	v := struct {
		Version, Executable, Digest string
		Mode                        os.FileMode
		Size, Bytes                 int64
		Entries                     []nodeClosureEntry
	}{nodeClosureVersion, c.Executable, c.Digest, c.Mode, c.Size, c.Bytes, entries}
	b, err := json.Marshal(v)
	if err != nil {
		return "", err
	}
	s := sha256.Sum256(b)
	return nodeClosureVersion + ":sha256:" + hex.EncodeToString(s[:]), nil
}

// stageNodeClosure copies every executable dependency into writable private
// directories, then seals the entire runtime closure before returning it.
func stageNodeClosure(source nodeClosure, want string) (node, library string, cleanup func(), err error) {
	node, library, _, cleanup, err = stageNodeClosureWithLoader(source, want, false)
	return node, library, cleanup, err
}

// stageNysaPureNodeClosure installs the factory loader before the shared
// closure is sealed. A launch can consequently never reopen lib to add it.
func stageNysaPureNodeClosure(source nodeClosure, want string) (node, library, loader string, cleanup func(), err error) {
	return stageNodeClosureWithLoader(source, want, true)
}

func stageNodeClosureWithLoader(source nodeClosure, want string, withLoader bool) (node, library, loader string, cleanup func(), err error) {
	identity, err := nodeClosureIdentity(source)
	if err != nil || identity != want {
		return "", "", "", nil, ErrUnclear
	}
	base, err := os.MkdirTemp("", "sf-node22-closure-")
	if err != nil {
		return "", "", "", nil, err
	}
	// Seatbelt matches macOS's /private/var spelling. Use the canonical path
	// for the stage, DYLD path, and profile so an alias cannot make the exact
	// staged launch allowance miss its own executable.
	created := base
	base, err = filepath.EvalSymlinks(base)
	if err != nil {
		_ = os.RemoveAll(created)
		return "", "", "", nil, err
	}
	if err := os.Chmod(base, 0o700); err != nil {
		_ = os.RemoveAll(base)
		return "", "", "", nil, err
	}
	cleanup = func() { removeSealedNodeStage(base) }
	bin, lib := filepath.Join(base, "bin"), filepath.Join(base, "lib")
	if err := os.Mkdir(bin, 0o700); err != nil {
		cleanup()
		return "", "", "", nil, err
	}
	if err := os.Mkdir(lib, 0o700); err != nil {
		cleanup()
		return "", "", "", nil, err
	}
	deadline := time.Now().Add(nodeClosureStageTime)
	copied := int64(0)
	if err := copyNodeClosureFile(source.Executable, filepath.Join(bin, "node"), source.Digest, source.Size, &copied, deadline); err != nil {
		cleanup()
		return "", "", "", nil, err
	}
	for _, entry := range source.Entries {
		if err := copyNodeClosureFile(entry.Path, filepath.Join(lib, entry.Leaf), entry.Digest, entry.Size, &copied, deadline); err != nil {
			cleanup()
			return "", "", "", nil, err
		}
	}
	node = filepath.Join(bin, "node")
	if copied != source.Bytes || copied > nodeClosureBytes {
		cleanup()
		return "", "", "", nil, ErrUnclear
	}
	if withLoader {
		loader, err = stageNysaPureLoader(lib)
		if err != nil {
			cleanup()
			return "", "", "", nil, err
		}
	}
	if err := sealNodeClosureStage(base, bin, lib, node, loader, source); err != nil {
		cleanup()
		return "", "", "", nil, err
	}
	return node, lib, loader, cleanup, nil
}

func verifyStagedNodeClosure(source nodeClosure, node, library string) error {
	base := filepath.Dir(filepath.Dir(node))
	if filepath.Dir(node) != filepath.Join(base, "bin") || library != filepath.Join(base, "lib") || !sealedNodeDirectory(base) || !sealedNodeDirectory(filepath.Dir(node)) || !sealedNodeDirectory(library) {
		return ErrUnclear
	}
	if !sealedNodeFile(node) {
		return ErrUnclear
	}
	if got, err := executableFileDigest(node); err != nil || got != source.Digest {
		return ErrUnclear
	}
	for _, entry := range source.Entries {
		path := filepath.Join(library, entry.Leaf)
		if !sealedNodeFile(path) {
			return ErrUnclear
		}
		if got, err := executableFileDigest(path); err != nil || got != entry.Digest {
			return ErrUnclear
		}
	}
	return nil
}

func sealedNodeDirectory(path string) bool {
	info, err := os.Lstat(path)
	return err == nil && info.Mode()&os.ModeSymlink == 0 && info.IsDir() && info.Mode().Perm() == 0o500 && trustedOwner(info)
}

func sealedNodeFile(path string) bool {
	info, err := os.Lstat(path)
	return err == nil && info.Mode()&os.ModeSymlink == 0 && info.Mode().IsRegular() && info.Mode().Perm() == 0o500 && trustedOwner(info)
}

func removeSealedNodeStage(root string) {
	if !cleanAbsolute(root) {
		return
	}
	_ = filepath.WalkDir(root, func(current string, entry os.DirEntry, err error) error {
		if err == nil && entry.IsDir() && entry.Type()&os.ModeSymlink == 0 {
			_ = os.Chmod(current, 0o700)
		}
		return nil
	})
	_ = os.RemoveAll(root)
}

func sealNodeClosureStage(base, bin, library, node, loader string, source nodeClosure) error {
	paths := []string{node}
	for _, entry := range source.Entries {
		paths = append(paths, filepath.Join(library, entry.Leaf))
	}
	if loader != "" {
		paths = append(paths, loader)
	}
	for _, path := range paths {
		if err := os.Chmod(path, 0o500); err != nil || !sealedNodeFile(path) {
			return ErrUnclear
		}
	}
	for _, directory := range []string{bin, library, base} {
		if err := os.Chmod(directory, 0o500); err != nil || !sealedNodeDirectory(directory) {
			return ErrUnclear
		}
	}
	if err := verifyStagedNodeClosure(source, node, library); err != nil {
		return err
	}
	if loader != "" {
		data, err := os.ReadFile(loader)
		if err != nil || !nysapure.EqualLoaderSource(data) {
			return ErrUnclear
		}
	}
	return nil
}

func copyNodeClosureFile(source, target, want string, wantSize int64, copied *int64, deadline time.Time) error {
	if time.Now().After(deadline) {
		return ErrUnclear
	}
	info, err := secureNodeSource(source)
	if err != nil {
		return err
	}
	in, err := os.Open(source)
	if err != nil {
		return err
	}
	defer in.Close()
	opened, err := in.Stat()
	if err != nil || !os.SameFile(info, opened) || info.Size() != wantSize || info.Size() > nodeClosureFileBytes || copied == nil || info.Size() > nodeClosureBytes-*copied {
		return ErrUnclear
	}
	out, err := os.OpenFile(target, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	h := sha256.New()
	n, copyErr := copyRepositoryBytes(io.MultiWriter(out, h), in, nodeClosureFileBytes, deadline)
	closeErr := out.Close()
	if copyErr != nil || closeErr != nil || n != info.Size() || want != "sha256:"+hex.EncodeToString(h.Sum(nil)) {
		return ErrUnclear
	}
	staged, err := secureNodeSource(target)
	if err != nil || staged.Mode().Perm() != 0o600 {
		return ErrUnclear
	}
	got, err := executableFileDigest(target)
	if err != nil || got != want {
		return ErrUnclear
	}
	*copied += n
	return nil
}

// node22VersionUsable accepts the exact Node 22 release family required by
// the recipe. Node's experimental test isolation flag was added in 22.8.0;
// accepting an earlier v22 would defer that compatibility failure until the
// gated child is launched.
func node22VersionUsable(version string) bool {
	version = strings.TrimSpace(version)
	if !strings.HasPrefix(version, "v") {
		return false
	}
	parts := strings.Split(strings.TrimPrefix(version, "v"), ".")
	if len(parts) != 3 {
		return false
	}
	parsePart := func(part string) (int, bool) {
		if part == "" || (len(part) > 1 && part[0] == '0') {
			return 0, false
		}
		for index := range part {
			if part[index] < '0' || part[index] > '9' {
				return 0, false
			}
		}
		parsed, err := strconv.Atoi(part)
		return parsed, err == nil
	}
	major, validMajor := parsePart(parts[0])
	minor, validMinor := parsePart(parts[1])
	patch, validPatch := parsePart(parts[2])
	return validMajor && validMinor && validPatch && major == 22 && (minor > 8 || (minor == 8 && patch >= 0))
}

func probeStagedNode22(path, library string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, path, "--version")
	cmd.Env = []string{"PATH=/usr/bin:/bin", "DYLD_LIBRARY_PATH=" + library, "HOME=/var/empty", "TMPDIR=/var/empty"}
	out, err := cmd.Output()
	if err != nil || !node22VersionUsable(string(out)) {
		return ErrUnclear
	}
	return nil
}

func node22Identity(name string) (string, string, error) {
	path, err := resolveFixedNodeExecutable(name)
	if err != nil {
		return "", "", err
	}
	closure, err := nodeClosureFor(path)
	if err != nil {
		return "", "", fmt.Errorf("resolve Node Mach-O closure: %w", err)
	}
	identity, err := nodeClosureIdentity(closure)
	if err != nil {
		return "", "", err
	}
	node, lib, cleanup, err := stageNodeClosure(closure, identity)
	if err != nil {
		return "", "", fmt.Errorf("stage Node Mach-O closure: %w", err)
	}
	defer cleanup()
	if err := probeStagedNode22(node, lib); err != nil {
		return "", "", fmt.Errorf("probe staged Node 22: %w", err)
	}
	return path, identity, nil
}

// nysaPureRuntimeIdentity binds the staged Node closure and the factory-owned
// resolver. A frozen command therefore cannot silently start using a changed
// TypeScript resolution policy merely because the selected Node binary stayed
// the same.
func nysaPureRuntimeIdentity(nodeIdentity string) (string, error) {
	if !strings.HasPrefix(nodeIdentity, nodeClosureVersion+":sha256:") {
		return "", ErrUnclear
	}
	data, err := json.Marshal(struct {
		Version, Node, Loader string
	}{"nysa-api-pure-v1", nodeIdentity, nysapure.LoaderIdentity()})
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(data)
	return "nysa-api-pure-v1:sha256:" + hex.EncodeToString(sum[:]), nil
}

func stageNysaPureLoader(directory string) (string, error) {
	if !cleanAbsolute(directory) {
		return "", ErrUnclear
	}
	path := filepath.Join(directory, "nysa_api_pure_loader.mjs")
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o500)
	if err != nil {
		return "", err
	}
	_, writeErr := io.WriteString(file, nysapure.LoaderSource)
	syncErr := file.Sync()
	closeErr := file.Close()
	if writeErr != nil || syncErr != nil || closeErr != nil {
		_ = os.Remove(path)
		return "", ErrUnclear
	}
	info, err := os.Lstat(path)
	if err != nil || info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() || info.Mode().Perm() != 0o500 || !trustedOwner(info) {
		return "", ErrUnclear
	}
	data, err := os.ReadFile(path)
	if err != nil || !nysapure.EqualLoaderSource(data) {
		return "", ErrUnclear
	}
	return path, nil
}

func nodeRecipeValidate(worktree string, argv []string, pure bool) error {
	if pure {
		if len(argv) != 3 || !nysapure.ValidTestPath(argv[2]) {
			return ErrUnclear
		}
		return nysapure.Validate(worktree, argv[2])
	}
	return nodeclosure.Validate(worktree)
}

func nodeRecipeValidateDirectoryFD(rootFD int, argv []string, pure bool) ([]nysapure.SourceFile, error) {
	if pure {
		if len(argv) != 3 || !nysapure.ValidTestPath(argv[2]) {
			return nil, ErrUnclear
		}
		return nysapure.ValidateDirectoryFDSources(rootFD, argv[2])
	}
	return nil, nodeclosure.ValidateDirectoryFD(rootFD)
}

func sameStringList(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func nodeLaunchFlags(pure bool, loader, testPath, worktree, closureRoot string) []string {
	// Node's test runner must traverse the launch root to resolve a relative
	// test path. In the pure recipe this is the private sealed source stage,
	// which contains exactly the authenticated closure rather than the mutable
	// worktree. Seatbelt still grants only the individual staged source files.
	flags := []string{"--permission", "--allow-fs-read=" + worktree}
	flags = append(flags, "--allow-fs-read="+closureRoot, "--no-addons", "--experimental-test-isolation=none", "--test-concurrency=1", "--no-experimental-fetch", "--no-experimental-websocket", "--no-experimental-sqlite", "--no-global-search-paths")
	if pure {
		// Node's permission model runs an experimental loader in a worker. The
		// worker permission is narrowly paired with Seatbelt's process-fork and
		// process-exec denials at the repository gate.
		flags = append(flags, "--allow-worker", "--experimental-transform-types", "--experimental-loader", loader, "--test", testPath)
	} else {
		flags = append(flags, "--no-experimental-strip-types", "--test")
	}
	return flags
}

func nodePureSourceReadFlags(flags, files []string) ([]string, error) {
	if len(flags) < 2 || flags[len(flags)-2] != "--test" || flags[len(flags)-1] == "" {
		return nil, ErrUnclear
	}
	tail := append([]string(nil), flags[len(flags)-2:]...)
	result := append([]string(nil), flags[:len(flags)-2]...)
	for _, file := range files {
		if !cleanAbsolute(file) {
			return nil, ErrUnclear
		}
		result = append(result, "--allow-fs-read="+file)
	}
	return append(result, tail...), nil
}

// stagedNodeArtifacts owns every private path granted to the one staged Node
// process. It intentionally has one close transition: source and runtime are
// released together, only after the recorded launch is durably finished.
type stagedNodeArtifacts struct {
	runtime, source func()
	once            sync.Once
}

func (a *stagedNodeArtifacts) Close() {
	if a == nil {
		return
	}
	a.once.Do(func() {
		if a.source != nil {
			a.source()
		}
		if a.runtime != nil {
			a.runtime()
		}
	})
}

// repositoryLaunchFinisher shares one durable Finish attempt between the
// normal wait path and bounded recovery. A returned error is deliberately
// memoized: retrying an uncertain persistence result could create a second
// transition after a store commit that the first caller failed to observe.
type repositoryLaunchFinisher struct {
	once sync.Once
	run  func(context.Context) error
	err  error
}

func (f *repositoryLaunchFinisher) Finish(ctx context.Context) error {
	if f == nil || f.run == nil {
		return ErrUnclear
	}
	f.once.Do(func() {
		f.err = f.run(ctx)
	})
	return f.err
}

// drainStagedNodeLifecycle has one bounded recovery owner for a recorded
// launch. A failed Finish is not retried by a second stage finisher: it is
// quarantined, then both stages are released only after identity-pinned
// disappearance has already proved that no process can still use them.
func (s RepositoryCommandSupervisor) drainStagedNodeLifecycle(launch contracts.RepositoryCommandLaunch, lease contracts.RepositoryCommandLease, artifacts *stagedNodeArtifacts, finish func(context.Context) error) {
	if artifacts == nil || lease == nil || finish == nil || launch.PID <= 0 || launch.PGID <= 0 {
		return
	}
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), stagedNodeRecoveryWindow)
		defer cancel()
		settleStagedNodeArtifacts(ctx, stagedNodeRecoveryPoll, func() error {
			return s.ensureGone(launch, false)
		}, finish, lease.Quarantine, artifacts)
	}()
}

// settleStagedNodeArtifacts performs a single Finish transition. It is kept
// dependency-injected so regressions can prove that a failed finish is
// quarantined and cannot consume a second artifact cleanup path.
func settleStagedNodeArtifacts(ctx context.Context, poll time.Duration, gone func() error, finish func(context.Context) error, quarantine func() error, artifacts *stagedNodeArtifacts) {
	if ctx == nil || gone == nil || finish == nil || quarantine == nil || artifacts == nil || poll <= 0 {
		return
	}
	ticker := time.NewTicker(poll)
	defer ticker.Stop()
	for {
		if gone() == nil {
			finishCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			err := finish(finishCtx)
			cancel()
			if err != nil {
				_ = quarantine()
			}
			// A successful gone proof makes the private paths inert even when the
			// Store transition could not be persisted. Quarantine retains the
			// authority evidence; retaining executable/source files would not.
			artifacts.Close()
			return
		}
		select {
		case <-ctx.Done():
			_ = quarantine()
			return
		case <-ticker.C:
		}
	}
}

// runNode uses the same durable Store lease and fd-pinned worktree as Go, but
// never inserts Node test wrappers. Seatbelt denies network and
// process-fork/exec; Node's own permissions/flags add child, worker, write,
// and addon defense in depth.
func (s RepositoryCommandSupervisor) runNode(ctx context.Context, claim contracts.RepositoryCommandClaim, spec contracts.CommandSpec, policy executionpolicy.CommandSnapshot, lease contracts.RepositoryCommandLease) (contracts.CommandResult, error) {
	pure := len(spec.Argv) == 3 && spec.Argv[0] == "node" && spec.Argv[1] == nysapure.RecipeFlag && nysapure.ValidTestPath(spec.Argv[2])
	normal := len(spec.Argv) == 2 && spec.Argv[0] == "node" && spec.Argv[1] == "--test"
	if lease == nil || spec.Profile != contracts.ProfileGuarded || (!normal && !pure) || spec.Directory != claim.Worktree || spec.Timeout <= 0 || (pure && spec.Timeout > 60*time.Second) || (!pure && spec.Timeout > 45*time.Minute) || s.SoftDrain > 30*time.Second || s.HardDrain > 30*time.Second || policy.Authorize(spec.Argv) != nil || policy.Digest() != claim.PolicyDigest {
		return contracts.CommandResult{}, ErrUnclear
	}
	if err := nodeRecipeValidate(spec.Directory, spec.Argv, pure); err != nil {
		return contracts.CommandResult{}, ErrUnclear
	}
	argvBytes, err := json.Marshal(spec.Argv)
	if err != nil {
		return contracts.CommandResult{}, err
	}
	argvSum := sha256.Sum256(argvBytes)
	if claim.CommandDigest != "sha256:"+hex.EncodeToString(argvSum[:]) {
		return contracts.CommandResult{}, ErrUnclear
	}
	empty := sha256.Sum256(nil)
	specBytes, err := json.Marshal(struct {
		Argv        []string
		Directory   string
		Timeout     int64
		Profile     contracts.ExecutionProfile
		StdinDigest string
	}{spec.Argv, spec.Directory, spec.Timeout.Nanoseconds(), spec.Profile, "sha256:" + hex.EncodeToString(empty[:])})
	if err != nil {
		return contracts.CommandResult{}, err
	}
	specSum := sha256.Sum256(specBytes)
	if claim.SpecDigest != "sha256:"+hex.EncodeToString(specSum[:]) {
		return contracts.CommandResult{}, ErrUnclear
	}
	resolved, err := resolveFixedNodeExecutable(spec.Argv[0])
	if err != nil {
		return contracts.CommandResult{}, err
	}
	closure, err := nodeClosureFor(resolved)
	if err != nil {
		return contracts.CommandResult{}, err
	}
	closureIdentity, err := nodeClosureIdentity(closure)
	claimIdentity := closureIdentity
	if pure && err == nil {
		claimIdentity, err = nysaPureRuntimeIdentity(closureIdentity)
	}
	if err != nil || claim.ExecutablePath != resolved || claim.ExecutableDigest != claimIdentity {
		return contracts.CommandResult{}, ErrUnclear
	}
	var staged, library, loader string
	var cleanup func()
	if pure {
		staged, library, loader, cleanup, err = stageNysaPureNodeClosure(closure, closureIdentity)
	} else {
		staged, library, cleanup, err = stageNodeClosure(closure, closureIdentity)
	}
	if err != nil {
		return contracts.CommandResult{}, err
	}
	artifacts := &stagedNodeArtifacts{runtime: cleanup}
	stageStarted, stageReleased := false, false
	var launch contracts.RepositoryCommandLaunch
	launchRecorded := false
	finisher := &repositoryLaunchFinisher{run: func(_ context.Context) error {
		finishCtx, finishCancel := repositoryLeasePersistenceContext()
		defer finishCancel()
		return lease.FinishRepositoryCommandLaunch(finishCtx, launch)
	}}
	defer func() {
		if !stageStarted || stageReleased || !launchRecorded {
			artifacts.Close()
			return
		}
		// One recovery owner guards both source and runtime. It releases them
		// only after identity-pinned disappearance, and quarantines a failed
		// durable Finish without another consumer racing that transition.
		s.drainStagedNodeLifecycle(launch, lease, artifacts, finisher.Finish)
	}()
	if err := probeStagedNode22(staged, library); err != nil {
		return contracts.CommandResult{}, err
	}
	if err := verifyStagedNodeClosure(closure, staged, library); err != nil {
		return contracts.CommandResult{}, ErrUnclear
	}
	parsed, err := parseRepositoryIdentity(claim)
	if err != nil {
		return contracts.CommandResult{}, ErrUnclear
	}
	if spec.Stdin != nil {
		return contracts.CommandResult{}, ErrUnclear
	}
	self := s.Executable
	if self == "" {
		self, err = os.Executable()
		if err != nil {
			return contracts.CommandResult{}, err
		}
	}
	self, err = stageRepositoryGate(self)
	if err != nil {
		return contracts.CommandResult{}, err
	}
	defer os.RemoveAll(filepath.Dir(self))
	worktreeFD, err := s.openAuthenticatedWorktree(claim, parsed)
	if err != nil {
		return contracts.CommandResult{}, ErrUnclear
	}
	defer worktreeFD.Close()
	if s.GitRunner.ExecHelper == "" || s.GitRunner.Home == "" {
		return contracts.CommandResult{}, ErrUnclear
	}
	runCtx, cancel := context.WithTimeout(ctx, spec.Timeout)
	defer cancel()
	if err := s.GitRunner.Reauthenticate(runCtx, parsed); err != nil {
		return contracts.CommandResult{}, fmt.Errorf("reauthenticate repository command worktree: %w", err)
	}
	env, err := executionpolicy.MinimalEnvironment(filepath.Dir(library), filepath.Dir(library))
	if err != nil {
		return contracts.CommandResult{}, err
	}
	closureSources, err := nodeRecipeValidateDirectoryFD(int(worktreeFD.Fd()), spec.Argv, pure)
	if err != nil {
		return contracts.CommandResult{}, ErrUnclear
	}
	var sourceStage *nysapure.SourceStage
	if pure {
		sourceStage, err = nysapure.StageDirectoryFD(int(worktreeFD.Fd()), spec.Argv[2], closureSources)
		if err != nil {
			return contracts.CommandResult{}, ErrUnclear
		}
		artifacts.source = sourceStage.Close
	}
	testPath := ""
	if pure {
		testPath = spec.Argv[2]
	}
	launchRoot := claim.Worktree
	if pure {
		launchRoot = sourceStage.Root
	}
	flags := nodeLaunchFlags(pure, loader, testPath, launchRoot, filepath.Dir(library))
	env = append(env, "DYLD_LIBRARY_PATH="+library, "OPENSSL_CONF=/dev/null", "SF_REPOSITORY_NODE_WORKTREE="+launchRoot, "SF_REPOSITORY_NODE_CLOSURE="+filepath.Dir(library))
	if pure {
		env = append(env, "SF_NYSA_API_PURE_ROOT="+sourceStage.Root, "SF_NYSA_API_PURE_MANIFEST="+string(sourceStage.Manifest), "SF_NYSA_API_PURE_MANIFEST_DIGEST="+sourceStage.ManifestDigest)
		allowed := append([]string(nil), sourceStage.Files...)
		flags, err = nodePureSourceReadFlags(flags, allowed)
		if err != nil {
			return contracts.CommandResult{}, ErrUnclear
		}
		encoded, encodeErr := json.Marshal(allowed)
		if encodeErr != nil {
			return contracts.CommandResult{}, ErrUnclear
		}
		env = append(env, "SF_REPOSITORY_NODE_FILES="+string(encoded))
	}
	launchFD := worktreeFD
	if pure {
		launchFD, err = os.Open(sourceStage.Root)
		if err != nil {
			return contracts.CommandResult{}, err
		}
		defer launchFD.Close()
	}
	gateRead, gateWrite, err := os.Pipe()
	if err != nil {
		return contracts.CommandResult{}, err
	}
	defer gateWrite.Close()
	cmd := exec.CommandContext(runCtx, self, append([]string{"__repository_node_command_gate", staged}, flags...)...)
	cmd.Cancel = func() error { return nil }
	cmd.WaitDelay = s.drainHard()
	cmd.Dir = string(filepath.Separator)
	cmd.Env = env
	cmd.ExtraFiles = []*os.File{gateRead, launchFD}
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	var stdout, stderr repositoryBuffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	started := time.Now()
	if err := cmd.Start(); err != nil {
		_ = gateRead.Close()
		return contracts.CommandResult{}, err
	}
	stageStarted = true
	_ = gateRead.Close()
	startID, e1 := processStartIdentity(cmd.Process.Pid)
	bootID, e2 := hostBootIdentity()
	launch = contracts.RepositoryCommandLaunch{PID: cmd.Process.Pid, PGID: cmd.Process.Pid, BootIdentity: bootID, ProcessStartIdentity: startID}
	if e1 != nil || e2 != nil || lease.RecordRepositoryCommandLaunch(ctx, launch) != nil {
		_ = signalGroup(cmd.Process.Pid, syscall.SIGKILL)
		_ = cmd.Wait()
		_ = s.ensureGone(launch, false)
		_ = lease.Quarantine()
		return contracts.CommandResult{}, ErrUnclear
	}
	launchRecorded = true
	if err := lease.Check(ctx); err != nil {
		_ = signalGroup(cmd.Process.Pid, syscall.SIGKILL)
		_ = cmd.Wait()
		_ = lease.Quarantine()
		return contracts.CommandResult{}, ErrUnclear
	}
	// Pure source admission has already copied the FD-authenticated closure to
	// an owner-only immutable stage. The gate receives that stage descriptor,
	// never the mutable worktree descriptor.
	if pure && sourceStage.Validate() != nil {
		_ = signalGroup(cmd.Process.Pid, syscall.SIGKILL)
		_ = cmd.Wait()
		_ = lease.Quarantine()
		return contracts.CommandResult{}, ErrUnclear
	}
	if _, err := gateWrite.Write([]byte{1}); err != nil {
		_ = signalGroup(cmd.Process.Pid, syscall.SIGKILL)
		_ = cmd.Wait()
		_ = s.ensureGone(launch, false)
		_ = lease.Quarantine()
		return contracts.CommandResult{}, ErrUnclear
	}
	_ = gateWrite.Close()
	wait := make(chan error, 1)
	go func() { wait <- cmd.Wait() }()
	var waitErr error
	select {
	case waitErr = <-wait:
	case <-runCtx.Done():
		if pgid, err := syscall.Getpgid(launch.PID); err != nil || pgid != launch.PGID {
			_ = lease.Quarantine()
			return contracts.CommandResult{}, ErrUnclear
		}
		_ = signalGroup(launch.PGID, syscall.SIGTERM)
		select {
		case waitErr = <-wait:
		case <-time.After(s.drainSoft()):
			_ = signalGroup(launch.PGID, syscall.SIGKILL)
			select {
			case waitErr = <-wait:
			case <-time.After(s.drainHard() + 250*time.Millisecond):
				_ = lease.Quarantine()
				return contracts.CommandResult{}, ErrUnclear
			}
		}
	}
	if err := s.ensureGone(launch, false); err != nil {
		_ = lease.Quarantine()
		return contracts.CommandResult{}, fmt.Errorf("repository ensure gone: %w", err)
	}
	finishErr := finisher.Finish(ctx)
	if finishErr != nil {
		return contracts.CommandResult{}, ErrUnclear
	}
	stageReleased = true
	artifacts.Close()
	if stdout.overflow || stderr.overflow {
		return contracts.CommandResult{}, errors.New("repository command output exceeds limit")
	}
	result := contracts.CommandResult{Stdout: stdout.Bytes(), Stderr: stderr.Bytes(), Duration: time.Since(started), ExitCode: 0, Observed: true, ObservedAt: time.Now().UTC()}
	if waitErr != nil {
		if exit, ok := waitErr.(*exec.ExitError); ok {
			result.ExitCode = exit.ExitCode()
		} else {
			result.ExitCode = -1
		}
		if runCtx.Err() != nil {
			return result, runCtx.Err()
		}
		return result, waitErr
	}
	return result, nil
}
