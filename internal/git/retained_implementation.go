package git

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"reflect"
	"sort"
	"strings"
	"time"
)

type retainedImplementation struct {
	Schema           string
	Identity         Identity
	Checkpoint       string
	Protected        []string
	Staged, Unstaged []byte
	Files            []retainedFile
}

type retainedImplementationObservation struct {
	Implementation           retainedImplementation
	Head                     string
	AllFiles                 []retainedFile
	FullStaged, FullUnstaged []byte
}

// InspectRetainedImplementation binds nonprotected bytes and their index
// partition to an original checkpoint. The caller authenticates that checkpoint
// and the frozen protected scope through Store. Current HEAD is observed for
// stability, but excluded from the digest so a protected-only verification
// commit does not change implementation identity. This does not authorize a
// writer or claim containment against concurrent hostile same-UID processes.
// The complete tracked/untracked inventory, including unchanged source, is
// bounded to 256 files, 1 MiB per file, and 8 MiB per observation including
// diffs. Larger repositories refuse rather than silently omit implementation.
func (r Runner) InspectRetainedImplementation(ctx context.Context, worktree Worktree, originalCheckpoint string, protected []string) (string, error) {
	if !validOID(originalCheckpoint) || len(protected) == 0 || len(protected) > 256 {
		return "", ErrIdentityMismatch
	}
	scope := append([]string(nil), protected...)
	sort.Strings(scope)
	for index, path := range scope {
		if !validRepoPath(path) || len(path) > 4096 || path == "." || index > 0 && path == scope[index-1] {
			return "", ErrUnsafeWorktree
		}
	}
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	root, err := openPinnedDirectory(worktree.Path)
	if err != nil {
		return "", err
	}
	defer root.Close()
	if root.dev() != worktree.Identity.WorktreeDev || root.ino() != worktree.Identity.WorktreeIno {
		return "", ErrIdentityMismatch
	}
	first, err := r.retainedImplementationObservation(ctx, worktree, root, originalCheckpoint, scope)
	if err != nil {
		return "", err
	}
	second, err := r.retainedImplementationObservation(ctx, worktree, root, originalCheckpoint, scope)
	if err != nil {
		return "", err
	}
	if !reflect.DeepEqual(first, second) {
		return "", ErrUnsafeWorktree
	}
	if err := root.verify(); err != nil {
		return "", err
	}
	if err := r.InspectWorktree(ctx, worktree); err != nil {
		return "", err
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	encoded, err := json.Marshal(first.Implementation)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(encoded)
	return "sha256:" + hex.EncodeToString(sum[:]), nil
}

func retainedProtectedPath(path string, protected []string) bool {
	for _, root := range protected {
		if path == root || strings.HasPrefix(path, root+"/") {
			return true
		}
	}
	return false
}

func (r Runner) retainedImplementationObservation(ctx context.Context, worktree Worktree, root *pinnedDirectory, checkpoint string, protected []string) (retainedImplementationObservation, error) {
	var result retainedImplementationObservation
	if err := r.InspectWorktree(ctx, worktree); err != nil {
		return result, err
	}
	read := func(args ...string) ([]byte, error) {
		return r.commandExpected(ctx, worktree.Path, worktree.Identity.WorktreeDev, worktree.Identity.WorktreeIno, args...)
	}
	head, err := r.oneExpected(ctx, worktree.Path, worktree.Identity.WorktreeDev, worktree.Identity.WorktreeIno, "rev-parse", "--verify", "HEAD^{commit}")
	if err != nil || !validOID(head) {
		return result, ErrIdentityMismatch
	}
	resolved, err := r.oneExpected(ctx, worktree.Path, worktree.Identity.WorktreeDev, worktree.Identity.WorktreeIno, "rev-parse", "--verify", checkpoint+"^{commit}")
	if err != nil || resolved != checkpoint {
		return result, ErrIdentityMismatch
	}
	if _, err := read("merge-base", "--is-ancestor", checkpoint, head); err != nil {
		return result, ErrIdentityMismatch
	}
	result.Head = head
	result.Implementation = retainedImplementation{Schema: "sf.retained-implementation/v1", Identity: worktree.Identity, Checkpoint: checkpoint, Protected: protected}
	ignored, err := read("ls-files", "--others", "--ignored", "--exclude-standard", "-z")
	if err != nil {
		return result, err
	}
	if len(ignored) != 0 {
		return result, ErrUnsafeWorktree
	}
	// Index vs original checkpoint survives later HEAD changes. The second diff
	// binds the independent worktree/index partition, including staged-only edits.
	staged := []string{"diff", "--cached", "--binary", "--no-ext-diff", "--no-textconv", "--no-renames", checkpoint, "--"}
	unstaged := []string{"diff", "--binary", "--no-ext-diff", "--no-textconv", "--no-renames", "--"}
	result.FullStaged, err = read(staged...)
	if err != nil {
		return result, err
	}
	result.FullUnstaged, err = read(unstaged...)
	if err != nil {
		return result, err
	}
	// Literal pathspecs preserve directory-prefix semantics without interpreting
	// glob characters, leading colons, or magic supplied in a repository path.
	exclusions := []string{"."}
	for _, path := range protected {
		exclusions = append(exclusions, ":(top,literal,exclude)"+path)
	}
	result.Implementation.Staged, err = read(append(staged, exclusions...)...)
	if err != nil {
		return result, err
	}
	result.Implementation.Unstaged, err = read(append(unstaged, exclusions...)...)
	if err != nil {
		return result, err
	}
	paths := map[string]bool{}
	var inventories []struct {
		argv   []string
		output []byte
	}
	for _, args := range [][]string{
		{"ls-files", "--cached", "-z"},
		{"diff", "--cached", "--name-only", "--no-ext-diff", "--no-textconv", "--no-renames", "-z", checkpoint, "--"},
		{"diff", "--name-only", "--no-ext-diff", "--no-textconv", "--no-renames", "-z", "--"},
		{"ls-files", "--others", "--exclude-standard", "-z"},
	} {
		output, err := read(args...)
		if err != nil {
			return result, err
		}
		inventories = append(inventories, struct {
			argv   []string
			output []byte
		}{args, output})
		for _, path := range splitNUL(output) {
			if !validRepoPath(path) {
				return result, ErrUnsafeWorktree
			}
			paths[path] = true
			if len(paths) > 256 {
				return result, ErrUnsafeWorktree
			}
		}
	}
	ordered := make([]string, 0, len(paths))
	for path := range paths {
		ordered = append(ordered, path)
	}
	sort.Strings(ordered)
	remaining := retainedWorktreeMaxBytes - len(result.FullStaged) - len(result.FullUnstaged) - len(result.Implementation.Staged) - len(result.Implementation.Unstaged)
	for _, path := range ordered {
		file, err := readRetainedFile(ctx, int(root.file.Fd()), path, &remaining)
		if err != nil {
			return result, err
		}
		result.AllFiles = append(result.AllFiles, file)
		if !retainedProtectedPath(path, protected) {
			result.Implementation.Files = append(result.Implementation.Files, file)
		}
	}
	if remaining < 0 {
		return result, ErrUnsafeWorktree
	}
	afterHead, err := r.oneExpected(ctx, worktree.Path, worktree.Identity.WorktreeDev, worktree.Identity.WorktreeIno, "rev-parse", "--verify", "HEAD^{commit}")
	if err != nil || afterHead != head {
		return result, ErrIdentityMismatch
	}
	ignored, err = read("ls-files", "--others", "--ignored", "--exclude-standard", "-z")
	if err != nil {
		return result, err
	}
	if len(ignored) != 0 {
		return result, ErrUnsafeWorktree
	}
	inventories = append(inventories, struct {
		argv   []string
		output []byte
	}{staged, result.FullStaged}, struct {
		argv   []string
		output []byte
	}{unstaged, result.FullUnstaged})
	for _, inventory := range inventories {
		got, err := read(inventory.argv...)
		if err != nil {
			return result, err
		}
		if !bytes.Equal(got, inventory.output) {
			return result, ErrUnsafeWorktree
		}
	}
	return result, nil
}
