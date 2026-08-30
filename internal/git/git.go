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
	"io"
	"io/fs"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/nysa-company/sf/internal/domain"
)

var (
	ErrIdentityMismatch    = errors.New("git repository identity mismatch")
	ErrUnsafeWorktree      = errors.New("git worktree is unsafe")
	ErrUnexpectedRemote    = errors.New("remote branch head is unexpected")
	ErrOutputBound         = errors.New("git output exceeded bound")
	ErrWorktreeQuarantined = errors.New("created worktree could not be safely cleaned up")
)

const maxGitOutput = 1 << 20

// BranchAuthority is implemented by the daemon's SQLite-backed store. Git
// never creates a second persistence authority for ticket branch identity.
type BranchAuthority interface {
	LoadOrStoreBranch(context.Context, string, string) (string, error)
}

// PersistedBranchAuthority is the stronger form implemented by the SQLite
// store. Looking up an allocation first is important: random generation must
// never happen before we have established that this ticket has no durable
// branch identity to replay.
type PersistedBranchAuthority interface {
	LoadBranch(context.Context, string) (string, error)
}

type Allocator struct {
	Authority BranchAuthority
	Random    io.Reader
}

func (a Allocator) Allocate(ctx context.Context, channel domain.Channel, project domain.ProjectID, ticket domain.TicketID) (string, error) {
	if !channel.Valid() || project == "" || ticket == "" || a.Authority == nil {
		return "", fmt.Errorf("allocator requires channel, project, ticket, and SQLite branch authority")
	}
	key := string(channel) + "\x00" + string(project) + "\x00" + string(ticket)
	if lookup, ok := a.Authority.(PersistedBranchAuthority); ok {
		stored, err := lookup.LoadBranch(ctx, key)
		if err != nil {
			return "", err
		}
		if stored != "" {
			return validateAllocatedBranch(channel, stored)
		}
	}
	random := a.Random
	if random == nil {
		random = rand.Reader
	}
	var suffix [16]byte
	if _, err := io.ReadFull(random, suffix[:]); err != nil {
		return "", fmt.Errorf("random branch suffix: %w", err)
	}
	proposed := fmt.Sprintf("sf/%s/%s/%s-%s", channel, digestPart(string(project)), digestPart(string(ticket)), hex.EncodeToString(suffix[:]))
	stored, err := a.Authority.LoadOrStoreBranch(ctx, key, proposed)
	if err != nil {
		return "", err
	}
	return validateAllocatedBranch(channel, stored)
}

func digestPart(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:8])
}

func validateBranch(branch string) (string, error) {
	if !validRef(branch) || (len(strings.Split(branch, "/")) < 3) || (strings.HasPrefix(branch, "sf/stable/") == false && strings.HasPrefix(branch, "sf/dev/") == false) {
		return "", fmt.Errorf("invalid persisted branch %q", branch)
	}
	return branch, nil
}

func validateAllocatedBranch(channel domain.Channel, branch string) (string, error) {
	if !channel.Valid() || !strings.HasPrefix(branch, "sf/"+string(channel)+"/") {
		return "", fmt.Errorf("invalid persisted branch %q", branch)
	}
	return validateBranch(branch)
}

func validRef(ref string) bool {
	if ref == "" || len(ref) > 1024 || ref == "@" || strings.HasPrefix(ref, "/") || strings.HasSuffix(ref, "/") ||
		strings.HasSuffix(ref, ".") || strings.Contains(ref, "..") || strings.Contains(ref, "@{") || strings.Contains(ref, "//") ||
		strings.ContainsAny(ref, " ~^:?*[\\\x00\r\n\t") {
		return false
	}
	for _, value := range []byte(ref) {
		if value < 0x20 || value == 0x7f {
			return false
		}
	}
	for _, component := range strings.Split(ref, "/") {
		if component == "" || component == "." || component == ".." || strings.HasPrefix(component, ".") || strings.HasSuffix(component, ".") || strings.HasSuffix(component, ".lock") {
			return false
		}
	}
	return true
}

func validOID(value string) bool {
	if len(value) != 40 && len(value) != 64 {
		return false
	}
	if strings.ToLower(value) != value {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

func validAbsolutePath(path string) bool {
	return filepath.IsAbs(path) && filepath.Clean(path) == path && path != string(filepath.Separator)
}

// credential.helper values beginning with ! are executed by Git through a
// shell. Keep the configured executable path deliberately boring so an
// otherwise absolute path cannot add shell syntax to that fixed helper.
func validHelperPath(path string) bool {
	if !validAbsolutePath(path) {
		return false
	}
	for _, value := range path {
		if (value >= 'a' && value <= 'z') || (value >= 'A' && value <= 'Z') ||
			(value >= '0' && value <= '9') || strings.ContainsRune("/._+-", value) {
			continue
		}
		return false
	}
	return true
}

func validRepoPath(path string) bool {
	if path == "" || strings.Contains(path, "\\") || strings.HasPrefix(path, "/") || strings.Contains(path, "\x00") {
		return false
	}
	converted := filepath.FromSlash(path)
	return filepath.IsLocal(converted) && filepath.Clean(converted) == converted
}

// Runner executes Git with a private HOME, all inherited GIT_* variables
// removed, system/global configuration disabled, and hooks disabled per call.
// Run is replaceable only for exact-argv tests; production always uses git.
type Runner struct {
	Binary      string
	Home        string
	GHBinary    string // absolute gh used only as Git's HTTPS credential helper
	GHConfigDir string // explicit existing gh auth/config authority; never copied into HOME
	Run         func(context.Context, string, []string, []string) ([]byte, error)
}

func (r Runner) command(ctx context.Context, directory string, args ...string) ([]byte, error) {
	if directory == "" {
		return nil, fmt.Errorf("git directory is required")
	}
	// The authenticated origin may be a local bare repository in hermetic tests
	// and local-only installs. No arbitrary URL is accepted: every operation
	// names origin after identity reauthentication.
	ctx, cancel := boundedGitContext(ctx)
	defer cancel()
	argv := []string{"-C", directory, "-c", "core.hooksPath=/dev/null", "-c", "protocol.file.allow=always"}
	if r.GHBinary != "" {
		argv = append(argv, "-c", "credential.helper=!"+r.GHBinary+" auth git-credential")
	}
	argv = append(argv, args...)
	env, err := r.environment(nil)
	if err != nil {
		return nil, err
	}
	if r.Run != nil {
		output, err := r.Run(ctx, r.binary(), argv, env)
		if len(output) > maxGitOutput {
			return output[:maxGitOutput], ErrOutputBound
		}
		return output, err
	}
	output, err := runBounded(ctx, r.binary(), argv, env)
	if err != nil {
		return output, fmt.Errorf("git command failed: %w", err)
	}
	return output, nil
}

func (r Runner) commandEnv(ctx context.Context, directory string, extra []string, args ...string) ([]byte, error) {
	if directory == "" {
		return nil, fmt.Errorf("git directory is required")
	}
	ctx, cancel := boundedGitContext(ctx)
	defer cancel()
	argv := []string{"-C", directory, "-c", "core.hooksPath=/dev/null", "-c", "protocol.file.allow=always"}
	if r.GHBinary != "" {
		argv = append(argv, "-c", "credential.helper=!"+r.GHBinary+" auth git-credential")
	}
	argv = append(argv, args...)
	env, err := r.environment(extra)
	if err != nil {
		return nil, err
	}
	if r.Run != nil {
		output, err := r.Run(ctx, r.binary(), argv, env)
		if len(output) > maxGitOutput {
			return output[:maxGitOutput], ErrOutputBound
		}
		return output, err
	}
	output, err := runBounded(ctx, r.binary(), argv, env)
	if err != nil {
		return output, fmt.Errorf("git command failed: %w", err)
	}
	return output, nil
}

func (r Runner) home() string {
	return r.Home
}
func (r Runner) binary() string {
	if r.Binary != "" {
		return r.Binary
	}
	return "/usr/bin/git"
}

func (r Runner) environment(extra []string) ([]string, error) {
	if !validAbsolutePath(r.binary()) {
		return nil, fmt.Errorf("runner requires an explicit absolute git binary")
	}
	if r.Home == "" || !validAbsolutePath(r.Home) {
		return nil, fmt.Errorf("runner requires an explicit absolute isolated HOME")
	}
	if (r.GHBinary == "") != (r.GHConfigDir == "") || (r.GHBinary != "" && (!validHelperPath(r.GHBinary) || !validAbsolutePath(r.GHConfigDir))) {
		return nil, fmt.Errorf("HTTPS auth requires absolute gh binary and explicit gh config directory")
	}
	info, err := os.Lstat(r.Home)
	if err == nil {
		if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
			return nil, fmt.Errorf("isolated HOME must be a real directory")
		}
		if info.Mode().Perm()&0o077 != 0 {
			return nil, fmt.Errorf("isolated HOME permissions are too broad")
		}
	}
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	if err := os.MkdirAll(r.Home, 0o700); err != nil {
		return nil, err
	}
	if r.GHBinary != "" {
		binaryInfo, binaryErr := os.Stat(r.GHBinary)
		configInfo, configErr := os.Stat(r.GHConfigDir)
		if binaryErr != nil || !binaryInfo.Mode().IsRegular() || binaryInfo.Mode().Perm()&0o111 == 0 {
			return nil, fmt.Errorf("HTTPS auth gh binary is unavailable or not executable")
		}
		if configErr != nil || !configInfo.IsDir() {
			return nil, fmt.Errorf("HTTPS auth config directory is unavailable")
		}
	}
	env := []string{"PATH=/usr/bin:/bin:/usr/sbin:/sbin", "LANG=C", "HOME=" + r.Home, "GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_GLOBAL=/dev/null", "GIT_TERMINAL_PROMPT=0", "GIT_OPTIONAL_LOCKS=0"}
	if r.GHConfigDir != "" {
		// HTTPS auth is delegated to the explicitly selected gh auth store. The
		// private Git HOME remains credential-free; no token is copied into env.
		env = append(env, "GH_CONFIG_DIR="+r.GHConfigDir, "GIT_ASKPASS_REQUIRE=force")
	}
	for _, entry := range extra {
		key, _, _ := strings.Cut(entry, "=")
		if (strings.HasPrefix(key, "GIT_") && !strings.HasPrefix(key, "GIT_AUTHOR_") && !strings.HasPrefix(key, "GIT_COMMITTER_")) || key == "HOME" {
			return nil, fmt.Errorf("credential or git environment override refused")
		}
		env = append(env, entry)
	}
	return env, nil
}

func boundedGitContext(parent context.Context) (context.Context, context.CancelFunc) {
	const maximum = 2 * time.Minute
	if deadline, ok := parent.Deadline(); ok && time.Until(deadline) <= maximum {
		return context.WithCancel(parent)
	}
	return context.WithTimeout(parent, maximum)
}

func runBounded(ctx context.Context, binary string, argv, env []string) ([]byte, error) {
	command := exec.CommandContext(ctx, binary, argv...)
	command.Env = env
	buffer := &boundedBuffer{limit: maxGitOutput}
	command.Stdout, command.Stderr = buffer, buffer
	err := command.Run()
	if buffer.exceeded {
		return buffer.data, ErrOutputBound
	}
	return buffer.data, err
}

type boundedBuffer struct {
	data     []byte
	limit    int
	exceeded bool
}

func (b *boundedBuffer) Write(value []byte) (int, error) {
	if len(b.data)+len(value) > b.limit {
		remaining := b.limit - len(b.data)
		if remaining > 0 {
			b.data = append(b.data, value[:remaining]...)
		}
		b.exceeded = true
		return len(value), nil
	}
	b.data = append(b.data, value...)
	return len(value), nil
}

// Identity is recorded before untrusted work and reauthenticated before every
// sf-owned commit, push, cleanup, or takeover boundary.
type Identity struct {
	Repository string
	Worktree   string
	GitFile    string
	GitFileDev uint64
	GitFileIno uint64
	CommonDir  string
	Origin     string
	BaseRef    string
	BaseHead   string
	HeadRef    string
	ConfigHash string
	HooksHash  string
}

func canonicalExistingWorktree(path string) (string, error) {
	if !validAbsolutePath(path) {
		return "", fmt.Errorf("%w: absolute clean worktree path required", ErrIdentityMismatch)
	}
	info, err := os.Lstat(path)
	if err != nil {
		return "", err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return "", fmt.Errorf("%w: worktree must be a real directory", ErrIdentityMismatch)
	}
	canonical, err := filepath.EvalSymlinks(path)
	if err != nil {
		return "", fmt.Errorf("%w: worktree path cannot be canonicalized", ErrIdentityMismatch)
	}
	return canonical, nil
}

func realSingleLinkFile(path string) (os.FileInfo, uint64, uint64, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, 0, 0, err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return nil, 0, 0, fmt.Errorf("%w: %s must be a regular file", ErrIdentityMismatch, path)
	}
	if stat, ok := info.Sys().(*syscall.Stat_t); ok {
		if stat.Nlink != 1 {
			return nil, 0, 0, fmt.Errorf("%w: %s has multiple links", ErrIdentityMismatch, path)
		}
		return info, uint64(stat.Dev), uint64(stat.Ino), nil
	}
	return info, 0, 0, nil
}

func sameFileIdentity(a, b os.FileInfo) bool {
	if a == nil || b == nil || a.Mode() != b.Mode() || a.Size() != b.Size() {
		return false
	}
	aStat, aOK := a.Sys().(*syscall.Stat_t)
	bStat, bOK := b.Sys().(*syscall.Stat_t)
	return aOK && bOK && aStat.Dev == bStat.Dev && aStat.Ino == bStat.Ino && aStat.Nlink == bStat.Nlink
}

func realHooksRoot(path string) error {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return fmt.Errorf("%w: hooks root must be a real directory", ErrIdentityMismatch)
	}
	canonical, err := filepath.EvalSymlinks(path)
	if err != nil || canonical != path {
		return fmt.Errorf("%w: hooks root is not canonical", ErrIdentityMismatch)
	}
	return nil
}

func (r Runner) Snapshot(ctx context.Context, worktree, baseRef string) (Identity, error) {
	if err := rejectGitEnvironment(os.Environ()); err != nil {
		return Identity{}, err
	}
	if !validAbsolutePath(worktree) || !validRef(baseRef) {
		return Identity{}, fmt.Errorf("%w: invalid worktree path or base ref", ErrIdentityMismatch)
	}
	canonicalWorktree, err := canonicalExistingWorktree(worktree)
	if err != nil {
		return Identity{}, err
	}
	gitPointer := filepath.Join(canonicalWorktree, ".git")
	gitInfo, gitDev, gitIno, err := realSingleLinkFile(gitPointer)
	if err != nil {
		return Identity{}, err
	}
	_, err = r.one(ctx, worktree, "rev-parse", "--show-toplevel")
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
	file, err := os.Open(gitPointer)
	if err != nil {
		return Identity{}, fmt.Errorf("read worktree git pointer: %w", err)
	}
	defer file.Close()
	openedInfo, err := file.Stat()
	if err != nil || !sameFileIdentity(gitInfo, openedInfo) {
		return Identity{}, fmt.Errorf("%w: worktree git pointer changed while opening", ErrIdentityMismatch)
	}
	gitFile, err := io.ReadAll(file)
	if err != nil {
		return Identity{}, fmt.Errorf("read worktree git pointer: %w", err)
	}
	finalInfo, finalErr := os.Lstat(gitPointer)
	if finalErr != nil || !sameFileIdentity(gitInfo, finalInfo) || gitInfo.Size() != int64(len(gitFile)) {
		return Identity{}, fmt.Errorf("%w: worktree git pointer changed while reading", ErrIdentityMismatch)
	}
	if err := r.rejectFeatures(ctx, worktree); err != nil {
		return Identity{}, err
	}
	common = strings.TrimSpace(common)
	common, err = filepath.EvalSymlinks(common)
	if err != nil || !validAbsolutePath(common) {
		return Identity{}, fmt.Errorf("%w: noncanonical git common directory", ErrIdentityMismatch)
	}
	if err := realHooksRoot(filepath.Join(common, "hooks")); err != nil {
		return Identity{}, err
	}
	hooksHash, err := treeDigest(filepath.Join(common, "hooks"))
	if err != nil {
		return Identity{}, err
	}
	// A linked worktree's --show-toplevel is the worktree itself. The primary
	// repository identity is the parent of the authenticated common .git dir.
	canonicalRepo, err := filepath.EvalSymlinks(filepath.Dir(strings.TrimSpace(common)))
	if err != nil {
		return Identity{}, err
	}
	// Ensure the pointer names precisely the linked worktree git directory.
	pointerText := strings.TrimSpace(string(gitFile))
	if !strings.HasPrefix(pointerText, "gitdir:") || strings.TrimSpace(strings.TrimPrefix(pointerText, "gitdir:")) == "" || strings.Contains(strings.TrimPrefix(pointerText, "gitdir:"), "\n") {
		return Identity{}, fmt.Errorf("%w: malformed worktree git pointer", ErrIdentityMismatch)
	}
	pointerTarget := strings.TrimSpace(strings.TrimPrefix(pointerText, "gitdir:"))
	if !validAbsolutePath(pointerTarget) {
		return Identity{}, fmt.Errorf("%w: noncanonical worktree git pointer", ErrIdentityMismatch)
	}
	pointerTarget, err = filepath.EvalSymlinks(pointerTarget)
	if err != nil || pointerTarget != filepath.Join(common, "worktrees", filepath.Base(pointerTarget)) {
		return Identity{}, fmt.Errorf("%w: worktree git pointer escapes common directory", ErrIdentityMismatch)
	}
	baseHead = strings.TrimSpace(baseHead)
	headRef = strings.TrimSpace(headRef)
	if !validOID(baseHead) || !validBranchOrHeadRef(headRef) || !validAbsolutePath(common) || !validAbsolutePath(canonicalRepo) || !validAbsolutePath(canonicalWorktree) {
		return Identity{}, fmt.Errorf("%w: invalid git identity value", ErrIdentityMismatch)
	}
	return Identity{Repository: canonicalRepo, Worktree: canonicalWorktree, GitFile: string(gitFile), GitFileDev: gitDev, GitFileIno: gitIno, CommonDir: common, Origin: strings.TrimSpace(origin), BaseRef: baseRef, BaseHead: baseHead, HeadRef: headRef, ConfigHash: digest(config), HooksHash: hooksHash}, nil
}

func validBranchOrHeadRef(value string) bool {
	return validRef(value) && !strings.HasPrefix(value, "refs/")
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
	if validAbsolutePath(raw) {
		resolved, err := filepath.EvalSymlinks(raw)
		if err != nil {
			return "", err
		}
		if !validAbsolutePath(resolved) {
			return "", fmt.Errorf("%w: noncanonical local origin refused", ErrIdentityMismatch)
		}
		return resolved, nil
	}
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Scheme != "https" || parsed.Host == "" || parsed.RawQuery != "" || parsed.Fragment != "" || parsed.Opaque != "" {
		return "", fmt.Errorf("%w: noncanonical origin refused", ErrIdentityMismatch)
	}
	if parsed.User != nil {
		if _, hasPassword := parsed.User.Password(); hasPassword {
			return "", fmt.Errorf("%w: credential-bearing origin refused", ErrIdentityMismatch)
		}
		return "", fmt.Errorf("%w: credential-bearing origin refused", ErrIdentityMismatch)
	}
	return parsed.String(), nil
}
func digest(data []byte) string {
	sum := sha256.Sum256(data)
	return "sha256:" + hex.EncodeToString(sum[:])
}

func treeDigest(root string) (string, error) {
	if info, err := os.Lstat(root); err == nil {
		if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
			return "", fmt.Errorf("%w: hook root must be a real directory", ErrIdentityMismatch)
		}
		if canonical, evalErr := filepath.EvalSymlinks(root); evalErr != nil || canonical != root {
			return "", fmt.Errorf("%w: hook root is not canonical", ErrIdentityMismatch)
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", err
	}
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
		if info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("%w: hook symlink refused", ErrIdentityMismatch)
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

// PreflightRepository proves that repository is the primary checkout. Snapshot
// intentionally requires a linked-worktree .git file; callers creating a
// worktree must use this separate primary-checkout preflight first.
func (r Runner) PreflightRepository(ctx context.Context, repository, baseRef string) error {
	if !validAbsolutePath(repository) || !validRef(baseRef) {
		return fmt.Errorf("canonical repository path and base ref are required")
	}
	actual, err := r.one(ctx, repository, "rev-parse", "--show-toplevel")
	if err != nil {
		return err
	}
	actual, err = filepath.EvalSymlinks(actual)
	canonicalRepository, canonicalErr := filepath.EvalSymlinks(repository)
	if err != nil || canonicalErr != nil || actual != canonicalRepository {
		return fmt.Errorf("%w: primary repository path changed", ErrIdentityMismatch)
	}
	info, err := os.Lstat(filepath.Join(repository, ".git"))
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("%w: primary repository .git directory required", ErrIdentityMismatch)
	}
	bare, err := r.one(ctx, repository, "rev-parse", "--is-bare-repository")
	if err != nil || bare != "false" {
		return fmt.Errorf("%w: bare repository refused", ErrIdentityMismatch)
	}
	_, err = r.one(ctx, repository, "rev-parse", "--verify", baseRef+"^{commit}")
	return err
}

func (r Runner) CreateWorktree(ctx context.Context, repository, path, branch, baseRef string) (Worktree, error) {
	if !validAbsolutePath(repository) || !validAbsolutePath(path) || !validRef(baseRef) {
		return Worktree{}, fmt.Errorf("canonical repository/worktree paths and base ref are required")
	}
	if !strings.HasPrefix(branch, "sf/") {
		return Worktree{}, fmt.Errorf("sf branch and absolute worktree path are required")
	}
	if _, err := validateBranch(branch); err != nil {
		return Worktree{}, err
	}
	if _, err := os.Lstat(path); err == nil {
		return Worktree{}, fmt.Errorf("%w: worktree path already exists", ErrIdentityMismatch)
	} else if !errors.Is(err, os.ErrNotExist) {
		return Worktree{}, err
	}
	parent := filepath.Dir(path)
	canonicalParent, err := filepath.EvalSymlinks(parent)
	if err != nil {
		return Worktree{}, fmt.Errorf("%w: worktree parent is unavailable", ErrIdentityMismatch)
	}
	// Parent aliases are harmless once resolved before Git creates anything;
	// the durable Worktree.Path is always this canonical spelling. A symlink at
	// the worktree leaf itself was rejected above and is never followed.
	path = filepath.Join(canonicalParent, filepath.Base(path))
	if _, err := os.Lstat(path); err == nil {
		return Worktree{}, fmt.Errorf("%w: canonical worktree path already exists", ErrIdentityMismatch)
	} else if !errors.Is(err, os.ErrNotExist) {
		return Worktree{}, err
	}
	if err := r.PreflightRepository(ctx, repository, baseRef); err != nil {
		return Worktree{}, err
	}
	if _, err := r.command(ctx, repository, "worktree", "add", "-b", branch, path, baseRef); err != nil {
		return Worktree{}, err
	}
	identity, err := r.Snapshot(ctx, path, baseRef)
	if err != nil {
		if cleanupErr := r.cleanupCreatedWorktree(ctx, repository, path, branch); cleanupErr != nil {
			return Worktree{}, fmt.Errorf("%w: snapshot failed: %v; cleanup failed: %w", ErrWorktreeQuarantined, err, cleanupErr)
		}
		return Worktree{}, err
	}
	return Worktree{Path: path, Branch: branch, Identity: identity}, nil
}

func (r Runner) cleanupCreatedWorktree(ctx context.Context, repository, path, branch string) error {
	// Never recursively remove a path after identity authentication failed. Git
	// owns the creation record and can remove it without following a replaced
	// path. If the path was replaced by a symlink, leave it quarantined rather
	// than risking deletion outside the requested worktree.
	if info, err := os.Lstat(path); err == nil && (info.Mode()&os.ModeSymlink != 0 || !info.IsDir()) {
		return fmt.Errorf("created worktree path was replaced")
	}
	if _, err := r.command(ctx, repository, "worktree", "remove", "--force", "--", path); err != nil {
		// A malformed or swapped .git pointer can make Git unable to remove its
		// own checkout. Remove only a tree that is proven to contain no links or
		// special files, then prune Git's stale worktree registration.
		if safeErr := safeRemoveTree(path); safeErr != nil {
			return fmt.Errorf("git removal: %v; safe filesystem cleanup: %w", err, safeErr)
		}
		if _, pruneErr := r.command(ctx, repository, "worktree", "prune"); pruneErr != nil {
			return pruneErr
		}
	}
	if _, err := r.command(ctx, repository, "branch", "-D", "--", branch); err != nil {
		return err
	}
	if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
		if err == nil {
			return fmt.Errorf("worktree path remains after cleanup")
		}
		return err
	}
	return nil
}

func safeRemoveTree(root string) error {
	if info, err := os.Lstat(root); errors.Is(err, os.ErrNotExist) {
		return nil
	} else if err != nil {
		return err
	} else if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return fmt.Errorf("created worktree path is not a real directory")
	}
	var paths []string
	if err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if path == root {
			return nil
		}
		info, err := os.Lstat(path)
		if err != nil {
			return err
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("created worktree contains a symlink")
		}
		// Removing a hardlink removes only this directory entry and cannot alter
		// the other inode owner, so it is safe during cleanup. Hardlinks remain
		// prohibited for candidate validation and authenticated .git pointers.
		paths = append(paths, path)
		return nil
	}); err != nil {
		return err
	}
	for index := len(paths) - 1; index >= 0; index-- {
		if err := os.Remove(paths[index]); err != nil {
			return err
		}
	}
	return os.Remove(root)
}

func (r Runner) InspectWorktree(ctx context.Context, worktree Worktree) error {
	if worktree.Path == "" || worktree.Path != worktree.Identity.Worktree {
		return fmt.Errorf("%w: worktree path is not bound to authenticated identity", ErrIdentityMismatch)
	}
	if _, err := canonicalExistingWorktree(worktree.Path); err != nil {
		return err
	}
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
	if _, err := validateBranch(worktree.Branch); err != nil {
		return fmt.Errorf("%w: foreign branch is retained", ErrUnsafeWorktree)
	}
	if state.Active || state.Taken || state.Quarantined || state.Foreign {
		return fmt.Errorf("%w: active/taken/quarantined/foreign worktree is retained", ErrUnsafeWorktree)
	}
	canonicalRepository, err := filepath.EvalSymlinks(repository)
	if err != nil || canonicalRepository != worktree.Identity.Repository {
		return fmt.Errorf("%w: removal repository does not match authenticated identity", ErrIdentityMismatch)
	}
	if err := r.PreflightRepository(ctx, canonicalRepository, worktree.Identity.BaseRef); err != nil {
		return err
	}
	if err := r.InspectWorktree(ctx, worktree); err != nil {
		return err
	}
	// status returns empty output for clean, so do not use one() here.
	if output, statusErr := r.command(ctx, worktree.Path, "status", "--porcelain=v1"); statusErr != nil || strings.TrimSpace(string(output)) != "" {
		return fmt.Errorf("%w: worktree status is not clean", ErrUnsafeWorktree)
	}
	// Status is only an observation. Reauthenticate again immediately before
	// the removal effect so a path, pointer, hook, or config swap cannot turn
	// this command into deletion of an unrelated checkout.
	if err := r.InspectWorktree(ctx, worktree); err != nil {
		return err
	}
	_, err = r.command(ctx, canonicalRepository, "worktree", "remove", worktree.Path)
	return err
}

type DiffPolicy struct {
	AllowedPaths    []string
	AllowExecutable bool
	ExpectedHead    string
}

func (r Runner) ValidateDiff(ctx context.Context, worktree, baseRef string, policy DiffPolicy) error {
	if !validAbsolutePath(worktree) || !validRef(baseRef) || len(policy.AllowedPaths) == 0 {
		return fmt.Errorf("allowed paths are required")
	}
	if _, err := canonicalExistingWorktree(worktree); err != nil {
		return err
	}
	for _, allowedPath := range policy.AllowedPaths {
		if !validRepoPath(strings.Trim(allowedPath, "/")) {
			return fmt.Errorf("%w: invalid allowed path", ErrUnsafeWorktree)
		}
	}
	if policy.ExpectedHead != "" {
		if !validOID(policy.ExpectedHead) {
			return fmt.Errorf("%w: invalid expected head", ErrUnsafeWorktree)
		}
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
	changed, err := r.command(ctx, worktree, "diff", "--no-renames", "--name-only", "-z", baseRef+"..HEAD")
	if err != nil {
		return err
	}
	unstaged, err := r.command(ctx, worktree, "diff", "--no-renames", "--name-only", "-z")
	if err != nil {
		return err
	}
	staged, err := r.command(ctx, worktree, "diff", "--no-renames", "--cached", "--name-only", "-z")
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
		if !validRepoPath(path) {
			return fmt.Errorf("%w: invalid changed path", ErrUnsafeWorktree)
		}
		if !allowed(path, policy.AllowedPaths) {
			return fmt.Errorf("%w: changed path %s is not allowed", ErrUnsafeWorktree, path)
		}
	}
	if err := validateFiles(worktree, policy, paths); err != nil {
		return err
	}
	return validateSpecialFiles(worktree)
}

func validateFiles(worktree string, policy DiffPolicy, changed []string) error {
	seenCase := map[string]string{}
	for _, path := range changed {
		if !validRepoPath(path) {
			return fmt.Errorf("%w: invalid changed path", ErrUnsafeWorktree)
		}
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

func validateSpecialFiles(worktree string) error {
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
		info, err := os.Lstat(path)
		if err != nil {
			return err
		}
		if !info.IsDir() && info.Mode()&os.ModeSymlink == 0 && !info.Mode().IsRegular() {
			return fmt.Errorf("%w: unsafe file type %s", ErrUnsafeWorktree, rel)
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
	if !validEvidenceDigest(request.EvidenceDigest) || (request.Message != "" && !boundedCommitText(request.Message, 4_000)) || request.Timestamp.IsZero() || !validRef(request.BaseRef) {
		return "", fmt.Errorf("candidate evidence digest and timestamp are required")
	}
	if request.BaseRef != worktree.Identity.BaseRef {
		return "", fmt.Errorf("%w: commit base ref does not match authenticated base", ErrIdentityMismatch)
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
	// The index is mutable control-plane state. Reauthenticate and validate the
	// staged view after add so a racing change cannot bypass the first check.
	if err := r.InspectWorktree(ctx, worktree); err != nil {
		return "", err
	}
	if err := r.ValidateDiff(ctx, worktree.Path, request.BaseRef, request.Policy); err != nil {
		return "", err
	}
	// Reauthenticate immediately before the commit effect after all staging and
	// validation, binding the final Git command to the durable identity.
	if err := r.InspectWorktree(ctx, worktree); err != nil {
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
	head, err := r.one(ctx, worktree.Path, "rev-parse", "HEAD")
	if err != nil {
		return "", err
	}
	if !validOID(head) {
		return "", fmt.Errorf("%w: invalid committed object id", ErrIdentityMismatch)
	}
	if err := r.InspectWorktree(ctx, worktree); err != nil {
		return "", err
	}
	postCommitPolicy := request.Policy
	postCommitPolicy.ExpectedHead = head
	if err := r.ValidateDiff(ctx, worktree.Path, request.BaseRef, postCommitPolicy); err != nil {
		return "", fmt.Errorf("%w: post-commit candidate validation failed: %v", ErrUnsafeWorktree, err)
	}
	if output, err := r.command(ctx, worktree.Path, "status", "--porcelain=v1"); err != nil || strings.TrimSpace(string(output)) != "" {
		return "", fmt.Errorf("%w: post-commit worktree is not clean", ErrUnsafeWorktree)
	}
	return head, nil
}

func boundedCommitText(value string, maximum int) bool {
	return value != "" && len(value) <= maximum && strings.TrimSpace(value) == value && !strings.ContainsRune(value, '\x00')
}

func validEvidenceDigest(value string) bool {
	if len(value) != len("sha256:")+sha256.Size*2 || !strings.HasPrefix(value, "sha256:") {
		return false
	}
	encoded := strings.TrimPrefix(value, "sha256:")
	decoded, err := hex.DecodeString(encoded)
	return err == nil && len(decoded) == sha256.Size && strings.ToLower(encoded) == encoded
}

// Push publishes exactly expectedHead. Callers must carry the candidate SHA
// from the durable commit/effect record; Push never chooses a moving local
// head on their behalf.
func (r Runner) Push(ctx context.Context, worktree Worktree, expectedHead string) (string, error) {
	if _, err := validateBranch(worktree.Branch); err != nil {
		return "", err
	}
	if !validOID(expectedHead) {
		return "", fmt.Errorf("%w: invalid expected candidate head", ErrUnexpectedRemote)
	}
	_, err := r.provePushHead(ctx, worktree, expectedHead)
	if err != nil {
		return "", err
	}
	remote, err := r.remoteHead(ctx, worktree.Path, worktree.Branch)
	if err != nil {
		return "", err
	}
	if remote == expectedHead {
		return expectedHead, nil
	}
	if remote != "" {
		if !validOID(remote) {
			return "", fmt.Errorf("%w: invalid remote object id", ErrUnexpectedRemote)
		}
		observationRef := "refs/sf/observed/" + digest([]byte(worktree.Branch + remote))[7:]
		if _, err := r.provePushHead(ctx, worktree, expectedHead); err != nil {
			return "", err
		}
		if _, err := r.command(ctx, worktree.Path, "fetch", "--no-tags", "origin", "refs/heads/"+worktree.Branch+":"+observationRef); err != nil {
			return "", fmt.Errorf("%w: cannot observe remote %s", ErrUnexpectedRemote, remote)
		}
		if _, err := r.command(ctx, worktree.Path, "merge-base", "--is-ancestor", remote, expectedHead); err != nil {
			return "", fmt.Errorf("%w: %s is not ancestor of %s", ErrUnexpectedRemote, remote, expectedHead)
		}
	}
	// Reauthenticate and prove the exact candidate immediately before push.
	if _, err := r.provePushHead(ctx, worktree, expectedHead); err != nil {
		return "", err
	}
	refspec := expectedHead + ":refs/heads/" + worktree.Branch
	if _, err := r.command(ctx, worktree.Path, "push", "origin", refspec); err != nil {
		// The server may have accepted the ref while the response was lost. Only
		// reconcile success when the exact expected candidate is observed.
		if _, proveErr := r.provePushHead(ctx, worktree, expectedHead); proveErr != nil {
			return "", err
		}
		observed, observeErr := r.remoteHead(ctx, worktree.Path, worktree.Branch)
		if observeErr == nil && observed == expectedHead {
			return expectedHead, nil
		}
		return "", err
	}
	observed, err := r.remoteHead(ctx, worktree.Path, worktree.Branch)
	if err != nil {
		return "", err
	}
	if observed != expectedHead {
		return "", fmt.Errorf("%w: push did not converge", ErrUnexpectedRemote)
	}
	return expectedHead, nil
}

func (r Runner) provePushHead(ctx context.Context, worktree Worktree, expectedHead string) (string, error) {
	if err := r.InspectWorktree(ctx, worktree); err != nil {
		return "", err
	}
	head, err := r.one(ctx, worktree.Path, "rev-parse", "--verify", "HEAD^{commit}")
	if err != nil {
		return "", err
	}
	branchHead, err := r.one(ctx, worktree.Path, "rev-parse", "--verify", "refs/heads/"+worktree.Branch+"^{commit}")
	if err != nil {
		return "", err
	}
	if !validOID(head) || head != expectedHead || branchHead != expectedHead {
		return "", fmt.Errorf("%w: local candidate head changed", ErrUnexpectedRemote)
	}
	return head, nil
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
	if !validOID(fields[0]) {
		return "", fmt.Errorf("%w: invalid remote object id", ErrUnexpectedRemote)
	}
	return fields[0], nil
}
