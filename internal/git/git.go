// Package git is the credential-free boundary for sf-owned Git mutations.
// It intentionally exposes no generic shell command execution.
package git

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/nysa-company/sf/internal/domain"
)

var (
	ErrIdentityMismatch = errors.New("git repository identity mismatch")
	ErrUnsafeWorktree   = errors.New("git worktree is unsafe")
	ErrUnexpectedRemote = errors.New("remote branch head is unexpected")
)

// Allocator persists an unguessable branch per channel/project/ticket. A
// separate 0600 record is used instead of a process-local map so restarts and
// concurrent daemons reuse exactly the same ref.
type Allocator struct{ Root string }

func (a Allocator) Allocate(channel domain.Channel, project domain.ProjectID, ticket domain.TicketID) (string, error) {
	if !channel.Valid() || project == "" || ticket == "" || a.Root == "" {
		return "", fmt.Errorf("allocator requires channel, project, ticket, and root")
	}
	dir := filepath.Join(a.Root, "branches", string(channel), safePart(string(project)))
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return "", err
	}
	path := filepath.Join(dir, safePart(string(ticket))+".ref")
	if data, err := os.ReadFile(path); err == nil {
		return validateBranch(strings.TrimSpace(string(data)))
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", err
	}
	for {
		var random [16]byte
		if _, err := rand.Read(random[:]); err != nil {
			return "", fmt.Errorf("random branch suffix: %w", err)
		}
		branch := fmt.Sprintf("sf/%s/%s/%s-%s", channel, safePart(string(project)), safePart(string(ticket)), hex.EncodeToString(random[:]))
		file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
		if errors.Is(err, os.ErrExist) {
			data, readErr := os.ReadFile(path)
			if readErr == nil {
				return validateBranch(strings.TrimSpace(string(data)))
			}
			continue
		}
		if err != nil {
			return "", err
		}
		_, writeErr := file.WriteString(branch + "\n")
		closeErr := file.Close()
		if writeErr != nil || closeErr != nil {
			return "", errors.Join(writeErr, closeErr)
		}
		return branch, nil
	}
}

func safePart(value string) string {
	var out strings.Builder
	for _, r := range value {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '.' || r == '-' {
			out.WriteRune(r)
		} else {
			out.WriteByte('-')
		}
	}
	if out.Len() == 0 {
		return "ticket"
	}
	return out.String()
}

func validateBranch(branch string) (string, error) {
	if !strings.HasPrefix(branch, "sf/") || strings.Contains(branch, "..") || strings.ContainsAny(branch, " ~^:?*[\\") || strings.HasSuffix(branch, "/") {
		return "", fmt.Errorf("invalid persisted branch %q", branch)
	}
	return branch, nil
}

// Runner executes Git with a private HOME, all inherited GIT_* variables
// removed, system/global configuration disabled, and hooks disabled per call.
// Run is replaceable only for exact-argv tests; production always uses git.
type Runner struct {
	Binary string
	Home   string
	Run    func(context.Context, string, []string, []string) ([]byte, error)
}

func (r Runner) command(ctx context.Context, directory string, args ...string) ([]byte, error) {
	if directory == "" {
		return nil, fmt.Errorf("git directory is required")
	}
	if err := os.MkdirAll(r.home(), 0o700); err != nil {
		return nil, err
	}
	// The authenticated origin may be a local bare repository in hermetic tests
	// and local-only installs. No arbitrary URL is accepted: every operation
	// names origin after identity reauthentication.
	argv := []string{"-C", directory, "-c", "core.hooksPath=/dev/null", "-c", "protocol.file.allow=always"}
	argv = append(argv, args...)
	env, err := r.environment(nil)
	if err != nil {
		return nil, err
	}
	if r.Run != nil {
		return r.Run(ctx, r.binary(), argv, env)
	}
	command := exec.CommandContext(ctx, r.binary(), argv...)
	command.Env = env
	output, err := command.CombinedOutput()
	if err != nil {
		return output, fmt.Errorf("git %s: %w", strings.Join(argv, " "), err)
	}
	return output, nil
}

func (r Runner) commandEnv(ctx context.Context, directory string, extra []string, args ...string) ([]byte, error) {
	if directory == "" {
		return nil, fmt.Errorf("git directory is required")
	}
	argv := []string{"-C", directory, "-c", "core.hooksPath=/dev/null", "-c", "protocol.file.allow=always"}
	argv = append(argv, args...)
	env, err := r.environment(extra)
	if err != nil {
		return nil, err
	}
	if r.Run != nil {
		return r.Run(ctx, r.binary(), argv, env)
	}
	command := exec.CommandContext(ctx, r.binary(), argv...)
	command.Env = env
	output, err := command.CombinedOutput()
	if err != nil {
		return output, fmt.Errorf("git %s: %w", strings.Join(argv, " "), err)
	}
	return output, nil
}

func (r Runner) home() string {
	if r.Home != "" {
		return r.Home
	}
	return filepath.Join(os.TempDir(), "sf-git-home")
}
func (r Runner) binary() string {
	if r.Binary != "" {
		return r.Binary
	}
	return "git"
}

func (r Runner) environment(extra []string) ([]string, error) {
	env := make([]string, 0, len(os.Environ())+6)
	for _, entry := range os.Environ() {
		key, _, _ := strings.Cut(entry, "=")
		if key == "HOME" || strings.HasPrefix(key, "GIT_") {
			continue
		}
		env = append(env, entry)
	}
	env = append(env, "HOME="+r.home(), "GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_GLOBAL=/dev/null", "GIT_TERMINAL_PROMPT=0", "GIT_OPTIONAL_LOCKS=0")
	for _, entry := range extra {
		key, _, _ := strings.Cut(entry, "=")
		if (strings.HasPrefix(key, "GIT_") && !strings.HasPrefix(key, "GIT_AUTHOR_") && !strings.HasPrefix(key, "GIT_COMMITTER_")) || key == "HOME" {
			return nil, fmt.Errorf("credential or git environment override refused")
		}
		env = append(env, entry)
	}
	return env, nil
}

// Identity is recorded before untrusted work and reauthenticated before every
// sf-owned commit, push, cleanup, or takeover boundary.
type Identity struct {
	Repository string
	Worktree   string
	GitFile    string
	CommonDir  string
	Origin     string
	BaseRef    string
	BaseHead   string
	HeadRef    string
	ConfigHash string
	HooksHash  string
}

func (r Runner) Snapshot(ctx context.Context, worktree, baseRef string) (Identity, error) {
	if err := rejectGitEnvironment(os.Environ()); err != nil {
		return Identity{}, err
	}
	repository, err := r.one(ctx, worktree, "rev-parse", "--show-toplevel")
	if err != nil {
		return Identity{}, err
	}
	common, err := r.one(ctx, worktree, "rev-parse", "--path-format=absolute", "--git-common-dir")
	if err != nil {
		return Identity{}, err
	}
	origin, err := r.one(ctx, worktree, "remote", "get-url", "origin")
	if err != nil {
		return Identity{}, err
	}
	origin, err = safeOrigin(origin)
	if err != nil {
		return Identity{}, err
	}
	baseHead, err := r.one(ctx, worktree, "rev-parse", "--verify", baseRef+"^{commit}")
	if err != nil {
		return Identity{}, err
	}
	headRef, err := r.one(ctx, worktree, "symbolic-ref", "--quiet", "--short", "HEAD")
	if err != nil {
		return Identity{}, err
	}
	config, err := r.command(ctx, worktree, "config", "--null", "--list", "--show-origin")
	if err != nil {
		return Identity{}, err
	}
	gitFile, err := os.ReadFile(filepath.Join(worktree, ".git"))
	if err != nil {
		return Identity{}, fmt.Errorf("read worktree git pointer: %w", err)
	}
	if err := r.rejectFeatures(ctx, worktree); err != nil {
		return Identity{}, err
	}
	hooksHash, err := treeDigest(filepath.Join(strings.TrimSpace(common), "hooks"))
	if err != nil {
		return Identity{}, err
	}
	canonicalRepo, err := filepath.EvalSymlinks(strings.TrimSpace(repository))
	if err != nil {
		return Identity{}, err
	}
	canonicalWorktree, err := filepath.EvalSymlinks(worktree)
	if err != nil {
		return Identity{}, err
	}
	return Identity{Repository: canonicalRepo, Worktree: canonicalWorktree, GitFile: string(gitFile), CommonDir: strings.TrimSpace(common), Origin: strings.TrimSpace(origin), BaseRef: baseRef, BaseHead: strings.TrimSpace(baseHead), HeadRef: headRef, ConfigHash: digest(config), HooksHash: hooksHash}, nil
}

func (r Runner) Reauthenticate(ctx context.Context, expected Identity) error {
	actual, err := r.Snapshot(ctx, expected.Worktree, expected.BaseRef)
	if err != nil {
		return err
	}
	if actual != expected {
		return fmt.Errorf("%w: expected=%+v actual=%+v", ErrIdentityMismatch, expected, actual)
	}
	return nil
}

func (r Runner) rejectFeatures(ctx context.Context, worktree string) error {
	if _, err := os.Lstat(filepath.Join(worktree, ".gitmodules")); err == nil {
		return fmt.Errorf("%w: submodules refused", ErrUnsafeWorktree)
	}
	submodules, err := r.command(ctx, worktree, "submodule", "status", "--recursive")
	if err != nil {
		return err
	}
	if strings.TrimSpace(string(submodules)) != "" {
		return fmt.Errorf("%w: submodules refused", ErrUnsafeWorktree)
	}
	replace, err := r.command(ctx, worktree, "for-each-ref", "--format=%(refname)", "refs/replace")
	if err != nil {
		return err
	}
	if strings.TrimSpace(string(replace)) != "" {
		return fmt.Errorf("%w: replace refs refused", ErrUnsafeWorktree)
	}
	return filepath.WalkDir(worktree, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if path == worktree {
			return nil
		}
		rel, _ := filepath.Rel(worktree, path)
		if rel == ".git" {
			return nil // the top-level worktree gitdir pointer is expected.
		}
		if entry.Name() == ".git" {
			return fmt.Errorf("%w: nested repository %s", ErrUnsafeWorktree, rel)
		}
		return nil
	})
}

func (r Runner) one(ctx context.Context, directory string, args ...string) (string, error) {
	output, err := r.command(ctx, directory, args...)
	if err != nil {
		return "", err
	}
	value := strings.TrimSpace(string(output))
	if value == "" {
		return "", fmt.Errorf("git %s returned empty output", strings.Join(args, " "))
	}
	return value, nil
}

func rejectGitEnvironment(env []string) error {
	for _, entry := range env {
		key, _, _ := strings.Cut(entry, "=")
		if key == "GIT_DIR" || key == "GIT_WORK_TREE" || key == "GIT_COMMON_DIR" || key == "GIT_OBJECT_DIRECTORY" || key == "GIT_ALTERNATE_OBJECT_DIRECTORIES" || key == "GIT_REPLACE_REF_BASE" || strings.HasPrefix(key, "GIT_CONFIG_") {
			return fmt.Errorf("%w: inherited %s", ErrIdentityMismatch, key)
		}
	}
	return nil
}

func safeOrigin(raw string) (string, error) {
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Scheme == "" {
		return raw, nil // SCP-like Git syntax has no URL userinfo component.
	}
	if parsed.User != nil {
		if _, hasPassword := parsed.User.Password(); hasPassword {
			return "", fmt.Errorf("%w: credential-bearing origin refused", ErrIdentityMismatch)
		}
		parsed.User = nil
	}
	return parsed.String(), nil
}
func digest(data []byte) string {
	sum := sha256.Sum256(data)
	return "sha256:" + hex.EncodeToString(sum[:])
}

func treeDigest(root string) (string, error) {
	hash := sha256.New()
	if err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		if rel == "." {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		_, _ = hash.Write([]byte(rel))
		_, _ = hash.Write([]byte{0})
		_, _ = hash.Write([]byte(info.Mode().String()))
		_, _ = hash.Write([]byte{0})
		if !entry.IsDir() {
			data, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			_, _ = hash.Write(data)
		}
		return nil
	}); err != nil {
		return "", err
	}
	return "sha256:" + hex.EncodeToString(hash.Sum(nil)), nil
}

type WorktreeState struct{ Active, Taken, Quarantined, Foreign bool }
type Worktree struct {
	Path, Branch string
	Identity     Identity
}

func (r Runner) CreateWorktree(ctx context.Context, repository, path, branch, baseRef string) (Worktree, error) {
	if !strings.HasPrefix(branch, "sf/") || !filepath.IsAbs(path) {
		return Worktree{}, fmt.Errorf("sf branch and absolute worktree path are required")
	}
	if _, err := validateBranch(branch); err != nil {
		return Worktree{}, err
	}
	if _, err := r.command(ctx, repository, "worktree", "add", "-b", branch, path, baseRef); err != nil {
		return Worktree{}, err
	}
	identity, err := r.Snapshot(ctx, path, baseRef)
	if err != nil {
		return Worktree{}, err
	}
	return Worktree{Path: path, Branch: branch, Identity: identity}, nil
}

func (r Runner) InspectWorktree(ctx context.Context, worktree Worktree) error {
	if err := r.Reauthenticate(ctx, worktree.Identity); err != nil {
		return err
	}
	if worktree.Identity.HeadRef != worktree.Branch {
		return fmt.Errorf("%w: worktree branch changed", ErrIdentityMismatch)
	}
	return nil
}

// Retain validates and returns the durable identity without removing anything.
// Takeover, quarantine, and foreign worktrees use this path by design.
func (r Runner) RetainWorktree(ctx context.Context, worktree Worktree) (Identity, error) {
	if err := r.InspectWorktree(ctx, worktree); err != nil {
		return Identity{}, err
	}
	return worktree.Identity, nil
}

func (r Runner) RemoveWorktree(ctx context.Context, repository string, worktree Worktree, state WorktreeState) error {
	if !strings.HasPrefix(worktree.Branch, "sf/") {
		return fmt.Errorf("%w: foreign branch is retained", ErrUnsafeWorktree)
	}
	if state.Active || state.Taken || state.Quarantined || state.Foreign {
		return fmt.Errorf("%w: active/taken/quarantined/foreign worktree is retained", ErrUnsafeWorktree)
	}
	if err := r.InspectWorktree(ctx, worktree); err != nil {
		return err
	}
	// status returns empty output for clean, so do not use one() here.
	if output, statusErr := r.command(ctx, worktree.Path, "status", "--porcelain=v1"); statusErr != nil || strings.TrimSpace(string(output)) != "" {
		return fmt.Errorf("%w: worktree status is not clean", ErrUnsafeWorktree)
	}
	_, err := r.command(ctx, repository, "worktree", "remove", worktree.Path)
	return err
}

type DiffPolicy struct {
	AllowedPaths    []string
	AllowExecutable bool
	ExpectedHead    string
}

func (r Runner) ValidateDiff(ctx context.Context, worktree, baseRef string, policy DiffPolicy) error {
	if len(policy.AllowedPaths) == 0 {
		return fmt.Errorf("allowed paths are required")
	}
	if policy.ExpectedHead != "" {
		head, err := r.one(ctx, worktree, "rev-parse", "HEAD")
		if err != nil {
			return err
		}
		if head != policy.ExpectedHead {
			return fmt.Errorf("%w: candidate history changed", ErrUnsafeWorktree)
		}
	}
	if err := r.rejectFeatures(ctx, worktree); err != nil {
		return err
	}
	if _, err := r.command(ctx, worktree, "merge-base", "--is-ancestor", baseRef, "HEAD"); err != nil {
		return fmt.Errorf("%w: history does not descend from base", ErrUnsafeWorktree)
	}
	changed, err := r.command(ctx, worktree, "diff", "--name-only", "-z", baseRef+"..HEAD")
	if err != nil {
		return err
	}
	unstaged, err := r.command(ctx, worktree, "diff", "--name-only", "-z")
	if err != nil {
		return err
	}
	staged, err := r.command(ctx, worktree, "diff", "--cached", "--name-only", "-z")
	if err != nil {
		return err
	}
	untracked, err := r.command(ctx, worktree, "ls-files", "-o", "--exclude-standard", "-z")
	if err != nil {
		return err
	}
	paths := append(splitNUL(changed), splitNUL(unstaged)...)
	paths = append(paths, splitNUL(staged)...)
	paths = append(paths, splitNUL(untracked)...)
	for _, path := range paths {
		if !allowed(path, policy.AllowedPaths) {
			return fmt.Errorf("%w: changed path %s is not allowed", ErrUnsafeWorktree, path)
		}
	}
	if err := validateFiles(worktree, policy, paths); err != nil {
		return err
	}
	return validateTree(worktree, policy)
}

func validateFiles(worktree string, policy DiffPolicy, changed []string) error {
	seenCase := map[string]string{}
	for _, path := range changed {
		lower := strings.ToLower(path)
		if previous, ok := seenCase[lower]; ok && previous != path {
			return fmt.Errorf("%w: case collision %s and %s", ErrUnsafeWorktree, previous, path)
		}
		seenCase[lower] = path
		full := filepath.Join(worktree, filepath.FromSlash(path))
		info, err := os.Lstat(full)
		if errors.Is(err, os.ErrNotExist) {
			continue
		} // deletion
		if err != nil {
			return err
		}
		if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
			return fmt.Errorf("%w: unsafe file type %s", ErrUnsafeWorktree, path)
		}
		if !policy.AllowExecutable && info.Mode().Perm()&0o111 != 0 {
			return fmt.Errorf("%w: executable mode %s", ErrUnsafeWorktree, path)
		}
		if stat, ok := info.Sys().(*syscall.Stat_t); ok && stat.Nlink > 1 {
			return fmt.Errorf("%w: hardlink %s", ErrUnsafeWorktree, path)
		}
	}
	return nil
}

func validateTree(worktree string, policy DiffPolicy) error {
	seenCase := map[string]string{}
	return filepath.WalkDir(worktree, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if path == worktree {
			return nil
		}
		rel, err := filepath.Rel(worktree, path)
		if err != nil {
			return err
		}
		if rel == ".git" {
			return nil
		}
		lower := strings.ToLower(filepath.ToSlash(rel))
		if previous, ok := seenCase[lower]; ok && previous != rel {
			return fmt.Errorf("%w: case collision %s and %s", ErrUnsafeWorktree, previous, rel)
		}
		seenCase[lower] = rel
		info, err := os.Lstat(path)
		if err != nil {
			return err
		}
		if info.Mode()&os.ModeSymlink != 0 || (!info.IsDir() && !info.Mode().IsRegular()) {
			return fmt.Errorf("%w: unsafe file type %s", ErrUnsafeWorktree, rel)
		}
		if !info.IsDir() && !policy.AllowExecutable && info.Mode().Perm()&0o111 != 0 {
			return fmt.Errorf("%w: executable mode %s", ErrUnsafeWorktree, rel)
		}
		if !info.IsDir() {
			if stat, ok := info.Sys().(*syscall.Stat_t); ok && stat.Nlink > 1 {
				return fmt.Errorf("%w: hardlink %s", ErrUnsafeWorktree, rel)
			}
		}
		return nil
	})
}

func splitNUL(data []byte) []string {
	return strings.FieldsFunc(string(data), func(r rune) bool { return r == 0 })
}
func allowed(path string, prefixes []string) bool {
	for _, prefix := range prefixes {
		prefix = strings.Trim(prefix, "/")
		if path == prefix || strings.HasPrefix(path, prefix+"/") {
			return true
		}
	}
	return false
}

type CommitRequest struct {
	EvidenceDigest, Message string
	Timestamp               time.Time
	BaseRef                 string
	Policy                  DiffPolicy
}

func (r Runner) Commit(ctx context.Context, worktree Worktree, request CommitRequest) (string, error) {
	if request.EvidenceDigest == "" || request.Timestamp.IsZero() {
		return "", fmt.Errorf("candidate evidence digest and timestamp are required")
	}
	if err := r.InspectWorktree(ctx, worktree); err != nil {
		return "", err
	}
	if err := r.ValidateDiff(ctx, worktree.Path, request.BaseRef, request.Policy); err != nil {
		return "", err
	}
	if _, err := r.command(ctx, worktree.Path, "add", "-A", "--"); err != nil {
		return "", err
	}
	message := "sf candidate " + request.EvidenceDigest
	if request.Message != "" {
		message += "\n\n" + request.Message
	}
	timestamp := request.Timestamp.UTC().Format(time.RFC3339)
	if _, err := r.commandEnv(ctx, worktree.Path, []string{"GIT_AUTHOR_NAME=sf", "GIT_AUTHOR_EMAIL=sf@localhost", "GIT_COMMITTER_NAME=sf", "GIT_COMMITTER_EMAIL=sf@localhost", "GIT_AUTHOR_DATE=" + timestamp, "GIT_COMMITTER_DATE=" + timestamp}, "commit", "--no-verify", "-m", message); err != nil {
		return "", err
	}
	return r.one(ctx, worktree.Path, "rev-parse", "HEAD")
}

func (r Runner) Push(ctx context.Context, worktree Worktree) (string, error) {
	if err := r.InspectWorktree(ctx, worktree); err != nil {
		return "", err
	}
	local, err := r.one(ctx, worktree.Path, "rev-parse", "--verify", worktree.Branch+"^{commit}")
	if err != nil {
		return "", err
	}
	remote, err := r.remoteHead(ctx, worktree.Path, worktree.Branch)
	if err != nil {
		return "", err
	}
	if remote == local {
		return local, nil
	}
	if remote != "" {
		observationRef := "refs/sf/observed/" + digest([]byte(worktree.Branch + remote))[7:]
		if _, err := r.command(ctx, worktree.Path, "fetch", "--no-tags", "origin", "refs/heads/"+worktree.Branch+":"+observationRef); err != nil {
			return "", fmt.Errorf("%w: cannot observe remote %s", ErrUnexpectedRemote, remote)
		}
		if _, err := r.command(ctx, worktree.Path, "merge-base", "--is-ancestor", remote, local); err != nil {
			return "", fmt.Errorf("%w: %s is not ancestor of %s", ErrUnexpectedRemote, remote, local)
		}
	}
	refspec := local + ":refs/heads/" + worktree.Branch
	if _, err := r.command(ctx, worktree.Path, "push", "origin", refspec); err != nil {
		observed, observeErr := r.remoteHead(ctx, worktree.Path, worktree.Branch)
		if observeErr == nil && observed == local {
			return local, nil
		}
		return "", err
	}
	observed, err := r.remoteHead(ctx, worktree.Path, worktree.Branch)
	if err != nil {
		return "", err
	}
	if observed != local {
		return "", fmt.Errorf("%w: push did not converge", ErrUnexpectedRemote)
	}
	return local, nil
}

func (r Runner) remoteHead(ctx context.Context, directory, branch string) (string, error) {
	output, err := r.command(ctx, directory, "ls-remote", "--heads", "origin", "refs/heads/"+branch)
	if err != nil {
		return "", err
	}
	fields := strings.Fields(string(output))
	if len(fields) == 0 {
		return "", nil
	}
	if len(fields) != 2 || fields[1] != "refs/heads/"+branch {
		return "", fmt.Errorf("%w: ambiguous remote observation", ErrUnexpectedRemote)
	}
	return fields[0], nil
}

var allocatorMu sync.Mutex

func (a Allocator) LockedAllocate(channel domain.Channel, project domain.ProjectID, ticket domain.TicketID) (string, error) {
	allocatorMu.Lock()
	defer allocatorMu.Unlock()
	return a.Allocate(channel, project, ticket)
}

func sorted(values []string) []string {
	result := append([]string(nil), values...)
	sort.Strings(result)
	return result
}
