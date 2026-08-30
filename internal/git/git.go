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
	"sync"
	"syscall"
	"time"

	"github.com/nysa-company/sf/internal/domain"
	"golang.org/x/sys/unix"
)

var (
	ErrIdentityMismatch    = errors.New("git repository identity mismatch")
	ErrUnsafeWorktree      = errors.New("git worktree is unsafe")
	ErrUnexpectedRemote    = errors.New("remote branch head is unexpected")
	ErrOutputBound         = errors.New("git output exceeded bound")
	ErrWorktreeQuarantined = errors.New("created worktree could not be safely cleaned up")
	// ErrHTTPSCredentialBoundary deliberately keeps HTTPS publication disabled
	// until it has an argv-only credential bridge. Git credential.helper is a
	// shell command interface, including its absolute-path form, so it cannot
	// implement the repository command policy by itself.
	ErrHTTPSCredentialBoundary = errors.New("HTTPS git publication requires an argv-only credential boundary")
	// ErrGitHubRefCASUnavailable is returned before any gh command starts. The
	// GitHub Git Data ref APIs expose create and force/fast-forward update, but
	// no expected-old-SHA precondition. A read followed by either mutation would
	// therefore violate the exact remote-base fence required for v1.
	ErrGitHubRefCASUnavailable = errors.New("github git-data ref API lacks exact expected-old-sha CAS")
)

const maxGitOutput = 1 << 20

// BranchAuthority is implemented by the daemon's SQLite-backed store. Git
// never creates a second persistence authority for ticket branch identity;
// loading must be available as a separate operation so allocation can replay
// a durable identity before consuming randomness.
type BranchAuthority interface {
	LoadBranch(context.Context, string) (string, error)
	LoadOrStoreBranch(context.Context, string, string) (string, error)
}

// PersistedBranchAuthority is retained as a compatibility name for callers
// that documented the stronger lookup contract separately.
type PersistedBranchAuthority = BranchAuthority

type Allocator struct {
	Authority BranchAuthority
	Random    io.Reader
}

func (a Allocator) Allocate(ctx context.Context, channel domain.Channel, project domain.ProjectID, ticket domain.TicketID) (string, error) {
	if !channel.Valid() || project == "" || ticket == "" || a.Authority == nil {
		return "", fmt.Errorf("allocator requires channel, project, ticket, and SQLite branch authority")
	}
	key := string(channel) + "\x00" + string(project) + "\x00" + string(ticket)
	stored, err := a.Authority.LoadBranch(ctx, key)
	if err != nil {
		return "", err
	}
	if stored != "" {
		return validateAllocatedBranch(channel, stored)
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
	stored, err = a.Authority.LoadOrStoreBranch(ctx, key, proposed)
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
	parts := strings.Split(branch, "/")
	if !validRef(branch) || len(parts) != 4 || parts[0] != "sf" || (parts[1] != "stable" && parts[1] != "dev") ||
		!validHexPart(parts[2], 8) {
		return "", fmt.Errorf("invalid persisted branch %q", branch)
	}
	name := strings.Split(parts[3], "-")
	if len(name) != 2 || !validHexPart(name[0], 8) || !validHexPart(name[1], 16) {
		return "", fmt.Errorf("invalid persisted branch %q", branch)
	}
	return branch, nil
}

func validHexPart(value string, bytes int) bool {
	if len(value) != bytes*2 {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil && strings.ToLower(value) == value
}

func validateAllocatedBranch(channel domain.Channel, branch string) (string, error) {
	if !channel.Valid() || !strings.HasPrefix(branch, "sf/"+string(channel)+"/") {
		return "", fmt.Errorf("invalid persisted branch %q", branch)
	}
	return validateBranch(branch)
}

func validRef(ref string) bool {
	if ref == "" || len(ref) > 1024 || ref == "@" || strings.HasPrefix(ref, "-") || strings.HasPrefix(ref, "/") || strings.HasSuffix(ref, "/") ||
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
	Binary string
	Home   string
	// GHBinary and GHConfigDir are retained only to reject old configuration.
	// Git credential.helper executes an effective shell command, so this
	// boundary intentionally has no HTTPS credential-helper integration.
	GHBinary    string
	GHConfigDir string
	// SSH fields enable only the fixed sf-ssh helper for the port-443 GitHub
	// SSH URL. They are not passed to ordinary repository commands.
	SSHHelper     string
	SSHBinary     string
	SSHKnownHosts string
	SSHAgentSock  string
	Run           func(context.Context, string, []string, []string) ([]byte, error)
}

// commandConfigKeys are repository-local settings that can cause Git to run
// another program or redirect an effect. The factory owns the command surface;
// these settings therefore fail closed even when they were present before a
// worktree was created.
func commandConfigKey(key string) bool {
	key = strings.ToLower(key)
	for _, exact := range []string{
		"alias.*", "credential.helper", "core.askpass", "core.editor", "core.fsmonitor",
		"core.fsmonitorhookpath", "core.sshcommand", "core.gitproxy", "core.pager",
		"commit.gpgsign", "interactive.difffilter", "sequence.editor", "merge.tool",
		"difftool.prompt", "gpg.program", "gpg.ssh.program", "merge.guitool", "pager.branch",
		"pager.config", "pager.diff", "pager.grep", "pager.log", "pager.show", "pager.tag",
		"tag.gpgsign", "user.signingkey",
	} {
		if key == exact || (exact == "alias.*" && strings.HasPrefix(key, "alias.")) {
			return true
		}
	}
	for _, prefix := range []string{
		"filter.", "diff.", "difftool.", "merge.", "mergetool.", "include", "url.",
		"http.", "credential.", "ssh.", "submodule.", "pager.",
	} {
		if strings.HasPrefix(key, prefix) {
			// pushurl is authenticated separately and is not itself executable.
			if strings.HasSuffix(key, ".pushurl") && key == "remote.origin.pushurl" {
				continue
			}
			if strings.HasSuffix(key, ".url") && key == "remote.origin.url" {
				continue
			}
			return true
		}
	}
	if strings.HasPrefix(key, "remote.") {
		for _, safe := range []string{"remote.origin.url", "remote.origin.fetch", "remote.origin.pushurl"} {
			if key == safe {
				return false
			}
		}
		return true
	}
	return false
}

func validateCommandConfig(data []byte) error {
	for _, field := range splitNUL(data) {
		// --name-only --show-origin emits origin and key as alternating NUL
		// fields; accepting an origin as a key is harmless, while command keys
		// must be rejected.
		if commandConfigKey(field) {
			return fmt.Errorf("%w: command-bearing repository config %q", ErrIdentityMismatch, field)
		}
	}
	return nil
}

func (r Runner) command(ctx context.Context, directory string, args ...string) ([]byte, error) {
	return r.commandExpected(ctx, directory, 0, 0, args...)
}

func (r Runner) commandExpected(ctx context.Context, directory string, expectedDev, expectedIno uint64, args ...string) ([]byte, error) {
	if directory == "" {
		return nil, fmt.Errorf("git directory is required")
	}
	// The authenticated origin may be a local bare repository in hermetic tests
	// and local-only installs. No arbitrary URL is accepted: every operation
	// names origin after identity reauthentication.
	ctx, cancel := boundedGitContext(ctx)
	defer cancel()
	env, err := r.environment(nil)
	if err != nil {
		return nil, err
	}
	pinned, err := openPinnedDirectory(directory)
	if err != nil {
		return nil, err
	}
	defer pinned.Close()
	if expectedDev != 0 && (pinned.dev() != expectedDev || pinned.ino() != expectedIno) {
		return nil, fmt.Errorf("%w: command directory identity changed", ErrIdentityMismatch)
	}
	gitDirectory := directory
	if r.Run == nil {
		gitDirectory = "."
	}
	argv := r.commandArgs(gitDirectory, args...)
	argv = append(argv, args...)
	if r.Run != nil {
		output, err := r.Run(ctx, r.binary(), argv, env)
		if verifyErr := pinned.verify(); verifyErr != nil {
			return output, verifyErr
		}
		if len(output) > maxGitOutput {
			return output[:maxGitOutput], ErrOutputBound
		}
		return output, err
	}
	output, err := runBounded(ctx, r.binary(), argv, env, directory)
	if verifyErr := pinned.verify(); verifyErr != nil {
		return output, verifyErr
	}
	if err != nil {
		return output, fmt.Errorf("git command failed: %w", err)
	}
	return output, nil
}

func (r Runner) commandEnv(ctx context.Context, directory string, extra []string, args ...string) ([]byte, error) {
	return r.commandEnvExpected(ctx, directory, 0, 0, extra, args...)
}

func (r Runner) commandEnvExpected(ctx context.Context, directory string, expectedDev, expectedIno uint64, extra []string, args ...string) ([]byte, error) {
	if directory == "" {
		return nil, fmt.Errorf("git directory is required")
	}
	ctx, cancel := boundedGitContext(ctx)
	defer cancel()
	env, err := r.environment(extra)
	if err != nil {
		return nil, err
	}
	pinned, err := openPinnedDirectory(directory)
	if err != nil {
		return nil, err
	}
	defer pinned.Close()
	if expectedDev != 0 && (pinned.dev() != expectedDev || pinned.ino() != expectedIno) {
		return nil, fmt.Errorf("%w: command directory identity changed", ErrIdentityMismatch)
	}
	gitDirectory := directory
	if r.Run == nil {
		gitDirectory = "."
	}
	argv := r.commandArgs(gitDirectory, args...)
	argv = append(argv, args...)
	if r.Run != nil {
		output, err := r.Run(ctx, r.binary(), argv, env)
		if verifyErr := pinned.verify(); verifyErr != nil {
			return output, verifyErr
		}
		if len(output) > maxGitOutput {
			return output[:maxGitOutput], ErrOutputBound
		}
		return output, err
	}
	output, err := runBounded(ctx, r.binary(), argv, env, directory)
	if verifyErr := pinned.verify(); verifyErr != nil {
		return output, verifyErr
	}
	if err != nil {
		return output, fmt.Errorf("git command failed: %w", err)
	}
	return output, nil
}

func (r Runner) commandArgs(gitDirectory string, args ...string) []string {
	argv := []string{"-C", gitDirectory, "-c", "core.hooksPath=/dev/null", "-c", "protocol.file.allow=always"}
	// Config inspection must see the repository's unmodified effective keys so
	// command-bearing settings cannot be hidden by these safety overrides.
	inspectConfig := len(args) > 0 && args[0] == "config"
	if !inspectConfig {
		argv = append(argv,
			"-c", "credential.helper=",
			"-c", "core.fsmonitor=false",
			"-c", "core.sshCommand=",
			"-c", "core.askPass=",
			"-c", "core.pager=",
			"-c", "commit.gpgsign=false",
			"-c", "tag.gpgsign=false",
			"-c", "interactive.diffFilter=",
		)
	}
	return argv
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
	if r.GHBinary != "" || r.GHConfigDir != "" {
		return nil, ErrHTTPSCredentialBoundary
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
	env := []string{"PATH=/usr/bin:/bin:/usr/sbin:/sbin", "LANG=C", "HOME=" + r.Home, "GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_GLOBAL=/dev/null", "GIT_TERMINAL_PROMPT=0", "GIT_OPTIONAL_LOCKS=0"}
	seen := map[string]bool{}
	for _, entry := range extra {
		key, value, found := strings.Cut(entry, "=")
		if !found || seen[key] || !validExtraEnvironment(r, key, value) {
			return nil, fmt.Errorf("credential or git environment override refused")
		}
		seen[key] = true
		env = append(env, entry)
	}
	return env, nil
}

// validExtraEnvironment is deliberately positive-only. In particular, an
// arbitrary non-GIT name is not harmless: loader, proxy and shell-startup
// variables all alter a child process before Git gets a chance to apply its
// own configuration hardening.
func validExtraEnvironment(r Runner, key, value string) bool {
	switch key {
	case "GIT_SSH":
		return value == r.SSHHelper
	case "GIT_SSH_VARIANT":
		return value == "ssh"
	case "SF_GIT_SSH_BINARY":
		return value == r.SSHBinary
	case "SF_GIT_SSH_KNOWN_HOSTS":
		return value == r.SSHKnownHosts
	case "SSH_AUTH_SOCK":
		return value == r.SSHAgentSock
	case "SF_GIT_SSH_REPOSITORY":
		return repoNameForSSH(value)
	case "GIT_AUTHOR_NAME", "GIT_COMMITTER_NAME":
		return value == "sf"
	case "GIT_AUTHOR_EMAIL", "GIT_COMMITTER_EMAIL":
		return value == "sf@localhost"
	case "GIT_AUTHOR_DATE", "GIT_COMMITTER_DATE":
		_, err := time.Parse(time.RFC3339, value)
		return err == nil && strings.HasSuffix(value, "Z")
	default:
		return false
	}
}

func repoNameForSSH(value string) bool {
	parts := strings.Split(value, "/")
	if len(parts) != 2 {
		return false
	}
	for _, part := range parts {
		if part == "" || len(part) > 100 {
			return false
		}
		for _, ch := range part {
			if !(ch >= 'a' && ch <= 'z' || ch >= 'A' && ch <= 'Z' || ch >= '0' && ch <= '9' || ch == '.' || ch == '_' || ch == '-') {
				return false
			}
		}
	}
	return true
}

func boundedGitContext(parent context.Context) (context.Context, context.CancelFunc) {
	const maximum = 2 * time.Minute
	if deadline, ok := parent.Deadline(); ok && time.Until(deadline) <= maximum {
		return context.WithCancel(parent)
	}
	return context.WithTimeout(parent, maximum)
}

type pinnedDirectory struct {
	path string
	file *os.File
	info os.FileInfo
}

func openPinnedDirectory(path string) (*pinnedDirectory, error) {
	if !validAbsolutePath(path) {
		return nil, fmt.Errorf("%w: absolute command directory required", ErrIdentityMismatch)
	}
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return nil, fmt.Errorf("%w: command directory must be a real directory", ErrIdentityMismatch)
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	opened, err := file.Stat()
	if err != nil || !sameFileIdentity(info, opened) {
		file.Close()
		return nil, fmt.Errorf("%w: command directory changed while opening", ErrIdentityMismatch)
	}
	return &pinnedDirectory{path: path, file: file, info: info}, nil
}

func (p *pinnedDirectory) verify() error {
	current, err := os.Lstat(p.path)
	if err != nil || !samePathIdentity(p.info, current) {
		return fmt.Errorf("%w: command directory changed during effect", ErrIdentityMismatch)
	}
	return nil
}

func samePathIdentity(a, b os.FileInfo) bool {
	if a == nil || b == nil || a.Mode() != b.Mode() {
		return false
	}
	aStat, aOK := a.Sys().(*syscall.Stat_t)
	bStat, bOK := b.Sys().(*syscall.Stat_t)
	return aOK && bOK && aStat.Dev == bStat.Dev && aStat.Ino == bStat.Ino
}

func (p *pinnedDirectory) Close() error { return p.file.Close() }

func (p *pinnedDirectory) dev() uint64 {
	if stat, ok := p.info.Sys().(*syscall.Stat_t); ok {
		return uint64(stat.Dev)
	}
	return 0
}

func (p *pinnedDirectory) ino() uint64 {
	if stat, ok := p.info.Sys().(*syscall.Stat_t); ok {
		return uint64(stat.Ino)
	}
	return 0
}

func runBounded(ctx context.Context, binary string, argv, env []string, directory string) ([]byte, error) {
	command := exec.CommandContext(ctx, binary, argv...)
	command.Dir = directory
	command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	command.WaitDelay = 750 * time.Millisecond
	command.Env = env
	runDone := make(chan struct{})
	defer close(runDone)
	killGroup := func(signal syscall.Signal) {
		if command.Process != nil {
			_ = syscall.Kill(-command.Process.Pid, signal)
		}
	}
	command.Cancel = func() error {
		killGroup(syscall.SIGTERM)
		go func() {
			timer := time.NewTimer(200 * time.Millisecond)
			defer timer.Stop()
			select {
			case <-timer.C:
				select {
				case <-runDone:
				default:
					killGroup(syscall.SIGKILL)
				}
			case <-runDone:
			}
		}()
		return nil
	}
	buffer := &boundedBuffer{limit: maxGitOutput, stop: func() { killGroup(syscall.SIGKILL) }}
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
	stop     func()
	once     sync.Once
	mu       sync.Mutex
}

func (b *boundedBuffer) Write(value []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.data)+len(value) > b.limit {
		remaining := b.limit - len(b.data)
		if remaining > 0 {
			b.data = append(b.data, value[:remaining]...)
		}
		b.exceeded = true
		b.once.Do(func() {
			if b.stop != nil {
				b.stop()
			}
		})
		return len(value), nil
	}
	b.data = append(b.data, value...)
	return len(value), nil
}

// Identity is recorded before untrusted work and reauthenticated before every
// sf-owned commit, push, cleanup, or takeover boundary.
type Identity struct {
	Repository    string
	RepositoryDev uint64
	RepositoryIno uint64
	Worktree      string
	WorktreeDev   uint64
	WorktreeIno   uint64
	GitFile       string
	GitFileDev    uint64
	GitFileIno    uint64
	CommonDir     string
	CommonDirDev  uint64
	CommonDirIno  uint64
	Origin        string
	PushOrigin    string
	PushOriginDev uint64
	PushOriginIno uint64
	BaseRef       string
	BaseHead      string
	HeadRef       string
	ConfigHash    string
	HooksHash     string
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

func directoryIdentity(path string) (uint64, uint64, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return 0, 0, err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return 0, 0, fmt.Errorf("%w: directory must be a real directory", ErrIdentityMismatch)
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return 0, 0, fmt.Errorf("%w: directory identity unavailable", ErrIdentityMismatch)
	}
	return uint64(stat.Dev), uint64(stat.Ino), nil
}

func localOriginIdentity(path string) (uint64, uint64, error) {
	if !validAbsolutePath(path) {
		return 0, 0, nil
	}
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil {
		return 0, 0, err
	}
	info, err := os.Stat(resolved)
	if err != nil {
		return 0, 0, err
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return 0, 0, fmt.Errorf("%w: local origin identity unavailable", ErrIdentityMismatch)
	}
	return uint64(stat.Dev), uint64(stat.Ino), nil
}

func verifyIdentityDirectories(identity Identity) error {
	for _, item := range []struct {
		path     string
		dev, ino uint64
	}{
		{identity.Repository, identity.RepositoryDev, identity.RepositoryIno},
		{identity.Worktree, identity.WorktreeDev, identity.WorktreeIno},
		{identity.CommonDir, identity.CommonDirDev, identity.CommonDirIno},
	} {
		dev, ino, err := directoryIdentity(item.path)
		if err != nil || dev != item.dev || ino != item.ino {
			return fmt.Errorf("%w: authenticated directory identity changed", ErrIdentityMismatch)
		}
	}
	if identity.PushOriginDev != 0 {
		dev, ino, err := localOriginIdentity(identity.PushOrigin)
		if err != nil || dev != identity.PushOriginDev || ino != identity.PushOriginIno {
			return fmt.Errorf("%w: authenticated local origin identity changed", ErrIdentityMismatch)
		}
	}
	return nil
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
	return r.snapshotExpected(ctx, "", worktree, baseRef)
}

func (r Runner) snapshotExpected(ctx context.Context, expectedRepository, worktree, baseRef string) (Identity, error) {
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
	worktreeDev, worktreeIno, err := directoryIdentity(canonicalWorktree)
	if err != nil {
		return Identity{}, err
	}
	gitPointer := filepath.Join(canonicalWorktree, ".git")
	gitInfo, gitDev, gitIno, err := realSingleLinkFile(gitPointer)
	if err != nil {
		return Identity{}, err
	}
	top, err := r.one(ctx, worktree, "rev-parse", "--show-toplevel")
	if err != nil {
		return Identity{}, err
	}
	top, err = filepath.EvalSymlinks(top)
	if err != nil || top != canonicalWorktree {
		return Identity{}, fmt.Errorf("%w: worktree top-level does not match requested path", ErrIdentityMismatch)
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
	pushURLs, err := r.command(ctx, worktree, "remote", "get-url", "--all", "--push", "origin")
	if err != nil {
		return Identity{}, err
	}
	pushLines := nonEmptyLines(pushURLs)
	if len(pushLines) != 1 {
		return Identity{}, fmt.Errorf("%w: origin must have exactly one push URL", ErrIdentityMismatch)
	}
	pushOrigin, err := safeOrigin(pushLines[0])
	if err != nil {
		return Identity{}, err
	}
	pushOriginDev, pushOriginIno, err := localOriginIdentity(pushOrigin)
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
	configKeys, err := r.command(ctx, worktree, "config", "--null", "--name-only", "--list", "--show-origin")
	if err != nil {
		return Identity{}, err
	}
	if err := validateCommandConfig(configKeys); err != nil {
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
	commonDev, commonIno, err := directoryIdentity(common)
	if err != nil {
		return Identity{}, err
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
	if expectedRepository != "" && canonicalRepo != expectedRepository {
		return Identity{}, fmt.Errorf("%w: common directory belongs to a different repository", ErrIdentityMismatch)
	}
	repositoryDev, repositoryIno, err := directoryIdentity(canonicalRepo)
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
	return Identity{Repository: canonicalRepo, RepositoryDev: repositoryDev, RepositoryIno: repositoryIno, Worktree: canonicalWorktree, WorktreeDev: worktreeDev, WorktreeIno: worktreeIno, GitFile: string(gitFile), GitFileDev: gitDev, GitFileIno: gitIno, CommonDir: common, CommonDirDev: commonDev, CommonDirIno: commonIno, Origin: strings.TrimSpace(origin), PushOrigin: pushOrigin, PushOriginDev: pushOriginDev, PushOriginIno: pushOriginIno, BaseRef: baseRef, BaseHead: baseHead, HeadRef: headRef, ConfigHash: digest(config), HooksHash: hooksHash}, nil
}

func nonEmptyLines(data []byte) []string {
	var lines []string
	for _, line := range strings.Split(string(data), "\n") {
		if line = strings.TrimSpace(line); line != "" {
			lines = append(lines, line)
		}
	}
	return lines
}

func validBranchOrHeadRef(value string) bool {
	return validRef(value) && !strings.HasPrefix(value, "refs/")
}

func (r Runner) Reauthenticate(ctx context.Context, expected Identity) error {
	actual, err := r.snapshotExpected(ctx, expected.Repository, expected.Worktree, expected.BaseRef)
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

func (r Runner) oneExpected(ctx context.Context, directory string, expectedDev, expectedIno uint64, args ...string) (string, error) {
	output, err := r.commandExpected(ctx, directory, expectedDev, expectedIno, args...)
	if err != nil {
		return "", err
	}
	value := strings.TrimSpace(string(output))
	if value == "" {
		return "", fmt.Errorf("git %s returned empty output", strings.Join(args, " "))
	}
	return value, nil
}

func (r Runner) oneEnvExpected(ctx context.Context, directory string, expectedDev, expectedIno uint64, extra []string, args ...string) (string, error) {
	output, err := r.commandEnvExpected(ctx, directory, expectedDev, expectedIno, extra, args...)
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
	if err != nil || (parsed.Scheme != "https" && parsed.Scheme != "ssh") || parsed.Host == "" || parsed.RawQuery != "" || parsed.Fragment != "" || parsed.Opaque != "" {
		return "", fmt.Errorf("%w: noncanonical origin refused", ErrIdentityMismatch)
	}
	if parsed.Scheme == "ssh" {
		if parsed.User == nil || parsed.User.Username() != "git" || parsed.Hostname() != "ssh.github.com" || parsed.Port() != "443" || !validGitHubRepoPath(strings.TrimPrefix(parsed.Path, "/")) {
			return "", fmt.Errorf("%w: noncanonical github ssh origin refused", ErrIdentityMismatch)
		}
		return parsed.String(), nil
	}
	if parsed.User != nil {
		if _, hasPassword := parsed.User.Password(); hasPassword {
			return "", fmt.Errorf("%w: credential-bearing origin refused", ErrIdentityMismatch)
		}
		return "", fmt.Errorf("%w: credential-bearing origin refused", ErrIdentityMismatch)
	}
	return parsed.String(), nil
}

func validGitHubRepoPath(value string) bool {
	parts := strings.Split(value, "/")
	if len(parts) != 2 || !strings.HasSuffix(parts[1], ".git") {
		return false
	}
	for _, item := range []string{parts[0], strings.TrimSuffix(parts[1], ".git")} {
		if item == "" || len(item) > 100 {
			return false
		}
		for _, ch := range item {
			if !(ch >= 'a' && ch <= 'z' || ch >= 'A' && ch <= 'Z' || ch >= '0' && ch <= '9' || ch == '.' || ch == '_' || ch == '-') {
				return false
			}
		}
	}
	return true
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

// GitHubPublicationClaim is the adapter-facing subset of a durable external
// effect claim. The SQLite owner validates it immediately before a future
// publisher starts any external command; Git deliberately never owns that
// persistence or substitutes an in-memory authorization check.
type GitHubPublicationClaim struct {
	SemanticKey   string
	RequestDigest string
	Fence         domain.Fence
}

// GitHubPublicationAuthority is supplied by the SQLite effect owner. It is
// intentionally narrow so this package only coordinates through the durable
// candidate/ref identity, not a second workflow authority.
type GitHubPublicationAuthority interface {
	ValidateGitHubPublication(context.Context, GitHubPublicationClaim) error
}

// GitHubPublicationRequest binds the local candidate to the remote base that
// was observed by the effect owner. A future implementation may publish only
// this exact identity and must durably record a different published SHA before
// any PR, review, or approval gate can use it.
type GitHubPublicationRequest struct {
	Worktree           Worktree
	ExpectedRemoteBase string
	ExpectedHead       string
	ExpectedTree       string
	Policy             DiffPolicy
	Claim              GitHubPublicationClaim
}

// PreflightRepository proves that repository is the primary checkout. Snapshot
// intentionally requires a linked-worktree .git file; callers creating a
// worktree must use this separate primary-checkout preflight first.
func (r Runner) PreflightRepository(ctx context.Context, repository, baseRef string) error {
	if !validAbsolutePath(repository) || !validRef(baseRef) {
		return fmt.Errorf("canonical repository path and base ref are required")
	}
	canonicalRepository, err := canonicalExistingRepository(repository)
	if err != nil {
		return err
	}
	repository = canonicalRepository
	actual, err := r.one(ctx, repository, "rev-parse", "--show-toplevel")
	if err != nil {
		return err
	}
	actual, err = filepath.EvalSymlinks(actual)
	if err != nil || actual != canonicalRepository {
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

func canonicalExistingRepository(path string) (string, error) {
	if !validAbsolutePath(path) {
		return "", fmt.Errorf("%w: absolute clean repository path required", ErrIdentityMismatch)
	}
	info, err := os.Lstat(path)
	if err != nil {
		return "", err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return "", fmt.Errorf("%w: repository must be a real directory", ErrIdentityMismatch)
	}
	canonical, err := filepath.EvalSymlinks(path)
	if err != nil {
		return "", err
	}
	return canonical, nil
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
	canonicalRepository, err := canonicalExistingRepository(repository)
	if err != nil {
		return Worktree{}, err
	}
	repository = canonicalRepository
	repositoryDev, repositoryIno, err := directoryIdentity(repository)
	if err != nil {
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
	if info, err := os.Lstat(path); err == nil {
		if info.Mode()&os.ModeSymlink == 0 && info.IsDir() {
			identity, snapErr := r.snapshotExpected(ctx, repository, path, baseRef)
			if snapErr == nil && identity.HeadRef == branch {
				return Worktree{Path: path, Branch: branch, Identity: identity}, nil
			}
		}
		return Worktree{}, fmt.Errorf("%w: canonical worktree path already exists", ErrIdentityMismatch)
	} else if !errors.Is(err, os.ErrNotExist) {
		return Worktree{}, err
	}
	if err := r.PreflightRepository(ctx, repository, baseRef); err != nil {
		return Worktree{}, err
	}
	if dev, ino, identityErr := directoryIdentity(repository); identityErr != nil || dev != repositoryDev || ino != repositoryIno {
		return Worktree{}, fmt.Errorf("%w: primary repository changed before worktree creation", ErrIdentityMismatch)
	}
	if _, err := r.commandExpected(ctx, repository, repositoryDev, repositoryIno, "worktree", "add", "-b", branch, path, "--", baseRef); err != nil {
		return Worktree{}, err
	}
	createdPath, err := openPinnedDirectory(path)
	if err != nil {
		return Worktree{}, fmt.Errorf("%w: created worktree could not be pinned: %v", ErrWorktreeQuarantined, err)
	}
	identity, err := r.snapshotExpected(ctx, repository, path, baseRef)
	if err != nil {
		cleanupErr := r.cleanupCreatedWorktree(ctx, repository, path, branch, baseRef, createdPath)
		_ = createdPath.Close()
		if cleanupErr != nil {
			return Worktree{}, fmt.Errorf("%w: snapshot failed: %v; cleanup failed: %w", ErrWorktreeQuarantined, err, cleanupErr)
		}
		return Worktree{}, err
	}
	_ = createdPath.Close()
	return Worktree{Path: path, Branch: branch, Identity: identity}, nil
}

func (r Runner) cleanupCreatedWorktree(ctx context.Context, repository, path, branch, baseRef string, createdPath *pinnedDirectory) error {
	// Never recursively remove a path after identity authentication failed. The
	// primary checkout is rechecked immediately before Git's ownership-aware
	// cleanup; if either check fails, leave the path quarantined rather than
	// mutating an unauthenticated checkout.
	if createdPath == nil {
		return fmt.Errorf("created worktree was not pinned")
	}
	cleanupCtx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if err := createdPath.verify(); err != nil {
		return err
	}
	if info, err := os.Lstat(path); err == nil && (info.Mode()&os.ModeSymlink != 0 || !info.IsDir()) {
		return fmt.Errorf("created worktree path was replaced")
	}
	if err := r.PreflightRepository(cleanupCtx, repository, baseRef); err != nil {
		return fmt.Errorf("primary repository changed: %w", err)
	}
	repositoryDev, repositoryIno, err := directoryIdentity(repository)
	if err != nil {
		return err
	}
	if _, err := r.commandExpected(cleanupCtx, repository, repositoryDev, repositoryIno, "worktree", "remove", "--force", "--", path); err != nil {
		if safeErr := safeRemoveTree(path, createdPath); safeErr != nil {
			return fmt.Errorf("git removal left worktree quarantined: %v; pinned cleanup: %w", err, safeErr)
		}
		_, pruneErr := r.commandExpected(cleanupCtx, repository, repositoryDev, repositoryIno, "worktree", "prune")
		if pruneErr != nil {
			err = errors.Join(err, pruneErr)
		}
	}
	if _, branchErr := r.commandExpected(cleanupCtx, repository, repositoryDev, repositoryIno, "branch", "-D", "--", branch); branchErr != nil {
		return errors.Join(err, branchErr)
	}
	if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
		if err == nil {
			return fmt.Errorf("worktree path remains after cleanup")
		}
		return err
	}
	return nil
}

// safeRemoveTree removes only entries reached through an already-open file
// descriptor. It is used solely for a just-created worktree whose Git pointer
// made authentication fail; it never follows a replacement path or symlink.
func safeRemoveTree(root string, pinned *pinnedDirectory) error {
	if pinned == nil {
		return fmt.Errorf("missing pinned worktree")
	}
	if err := pinned.verify(); err != nil {
		return err
	}
	rootFD, err := unix.Open(root, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW, 0)
	if err != nil {
		return err
	}
	rootFile := os.NewFile(uintptr(rootFD), root)
	defer rootFile.Close()
	rootInfo, err := rootFile.Stat()
	if err != nil || !samePathIdentity(pinned.info, rootInfo) {
		return fmt.Errorf("created worktree changed before cleanup")
	}
	if err := removeDirectoryEntries(rootFD, rootFile); err != nil {
		return err
	}
	parentFile, err := os.Open(filepath.Dir(root))
	if err != nil {
		return err
	}
	defer parentFile.Close()
	if err := pinned.verify(); err != nil {
		return err
	}
	var current unix.Stat_t
	rootStat, ok := rootInfo.Sys().(*syscall.Stat_t)
	if !ok || unix.Fstatat(int(parentFile.Fd()), filepath.Base(root), &current, unix.AT_SYMLINK_NOFOLLOW) != nil || current.Dev != rootStat.Dev || current.Ino != rootStat.Ino {
		return fmt.Errorf("created worktree path was replaced before unlink")
	}
	return unix.Unlinkat(int(parentFile.Fd()), filepath.Base(root), unix.AT_REMOVEDIR)
}

func removeDirectoryEntries(fd int, directory *os.File) error {
	names, err := directory.Readdirnames(-1)
	if err != nil {
		return err
	}
	for _, name := range names {
		childFD, openErr := unix.Openat(fd, name, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW, 0)
		if openErr == nil {
			var opened unix.Stat_t
			if err := unix.Fstat(childFD, &opened); err != nil {
				_ = unix.Close(childFD)
				return err
			}
			child := os.NewFile(uintptr(childFD), name)
			removeErr := removeDirectoryEntries(childFD, child)
			_ = child.Close()
			if removeErr != nil {
				return removeErr
			}
			var current unix.Stat_t
			if err := unix.Fstatat(fd, name, &current, unix.AT_SYMLINK_NOFOLLOW); err != nil || current.Dev != opened.Dev || current.Ino != opened.Ino {
				return fmt.Errorf("directory entry %s was replaced", name)
			}
			if err := unix.Unlinkat(fd, name, unix.AT_REMOVEDIR); err != nil {
				return err
			}
			continue
		}
		var current unix.Stat_t
		if err := unix.Fstatat(fd, name, &current, unix.AT_SYMLINK_NOFOLLOW); err != nil {
			return err
		}
		if err := unix.Unlinkat(fd, name, 0); err != nil {
			return err
		}
	}
	return nil
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
	if err := verifyIdentityDirectories(worktree.Identity); err != nil {
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
	repositoryDev, repositoryIno, err := directoryIdentity(canonicalRepository)
	if err != nil || repositoryDev != worktree.Identity.RepositoryDev || repositoryIno != worktree.Identity.RepositoryIno {
		return fmt.Errorf("%w: removal repository identity changed", ErrIdentityMismatch)
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
	pinnedWorktree, err := openPinnedDirectory(worktree.Path)
	if err != nil {
		return err
	}
	defer pinnedWorktree.Close()
	if pinnedWorktree.dev() != worktree.Identity.WorktreeDev || pinnedWorktree.ino() != worktree.Identity.WorktreeIno {
		return fmt.Errorf("%w: removal worktree identity changed", ErrIdentityMismatch)
	}
	if err := pinnedWorktree.verify(); err != nil {
		return err
	}
	_, err = r.commandExpected(ctx, canonicalRepository, repositoryDev, repositoryIno, "worktree", "remove", "--", worktree.Path)
	if err != nil {
		if verifyErr := pinnedWorktree.verify(); verifyErr != nil {
			return verifyErr
		}
		return err
	}
	// A successful Git removal is expected to unlink the authenticated path.
	// If anything remains, it must still be the pinned inode; never accept a
	// replacement as proof that the requested worktree was removed.
	if _, statErr := os.Lstat(worktree.Path); statErr == nil {
		return pinnedWorktree.verify()
	} else if !errors.Is(statErr, os.ErrNotExist) {
		return statErr
	}
	return nil
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
	if err := validateChangedPaths(policy, paths); err != nil {
		return err
	}
	if err := validateFiles(worktree, policy, paths); err != nil {
		return err
	}
	return validateSpecialFiles(worktree)
}

func validateChangedPaths(policy DiffPolicy, paths []string) error {
	seenCase := map[string]string{}
	for _, path := range paths {
		if !validRepoPath(path) {
			return fmt.Errorf("%w: invalid changed path", ErrUnsafeWorktree)
		}
		if !allowed(path, policy.AllowedPaths) {
			return fmt.Errorf("%w: changed path %s is not allowed", ErrUnsafeWorktree, path)
		}
		lower := strings.ToLower(path)
		if previous, ok := seenCase[lower]; ok && previous != path {
			return fmt.Errorf("%w: case collision %s and %s", ErrUnsafeWorktree, previous, path)
		}
		seenCase[lower] = path
	}
	return nil
}

func validateFiles(worktree string, policy DiffPolicy, changed []string) error {
	for _, path := range changed {
		if err := rejectSymlinkComponents(worktree, path); err != nil {
			return err
		}
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

// rejectSymlinkComponents prevents a clean-looking final lstat from silently
// traversing a pre-existing symlinked parent. The caller subsequently checks
// the leaf's type/link count; together the two checks keep candidate bytes in
// the authenticated worktree under the guarded/supervisor threat model.
func rejectSymlinkComponents(worktree, candidate string) error {
	current := worktree
	parts := strings.Split(filepath.FromSlash(candidate), string(filepath.Separator))
	for i, part := range parts {
		current = filepath.Join(current, part)
		info, err := os.Lstat(current)
		if errors.Is(err, os.ErrNotExist) {
			return nil
		} // deletion
		if err != nil {
			return err
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("%w: symlink component %s", ErrUnsafeWorktree, candidate)
		}
		if i < len(parts)-1 && !info.IsDir() {
			return fmt.Errorf("%w: non-directory component %s", ErrUnsafeWorktree, candidate)
		}
	}
	return nil
}

// validateImmutableTree applies the candidate policy to the exact tree that
// commit-tree will consume. The mutable worktree and index are intentionally
// not consulted after write-tree: a concurrent writer may change them, but it
// cannot change this object. This prevents an index race from advancing the
// sf branch before a later, merely diagnostic validation can notice it.
func (r Runner) validateImmutableTree(ctx context.Context, worktree, baseRef, tree string, policy DiffPolicy) error {
	if !validOID(tree) || !validRef(baseRef) {
		return fmt.Errorf("%w: invalid candidate tree or base", ErrUnsafeWorktree)
	}
	if _, err := r.command(ctx, worktree, "cat-file", "-e", tree+"^{tree}"); err != nil {
		return fmt.Errorf("%w: candidate tree is unavailable", ErrUnsafeWorktree)
	}
	if _, err := r.command(ctx, worktree, "merge-base", "--is-ancestor", baseRef, "HEAD"); err != nil {
		return fmt.Errorf("%w: history does not descend from base", ErrUnsafeWorktree)
	}
	changed, err := r.command(ctx, worktree, "diff", "--no-renames", "--name-only", "-z", baseRef, tree)
	if err != nil {
		return err
	}
	paths := splitNUL(changed)
	if err := validateChangedPaths(policy, paths); err != nil {
		return err
	}
	if len(paths) == 0 {
		return nil
	}
	args := append([]string{"ls-tree", "-r", "-z", tree, "--"}, paths...)
	entries, err := r.command(ctx, worktree, args...)
	if err != nil {
		return err
	}
	return validateImmutableTreeEntries(entries, policy)
}

func validateImmutableTreeEntries(data []byte, policy DiffPolicy) error {
	for _, entry := range splitNUL(data) {
		metadata, path, found := strings.Cut(entry, "\t")
		fields := strings.Fields(metadata)
		if !found || len(fields) != 3 || !validRepoPath(path) || !validOID(fields[2]) {
			return fmt.Errorf("%w: malformed candidate tree entry", ErrUnsafeWorktree)
		}
		if !allowed(path, policy.AllowedPaths) {
			return fmt.Errorf("%w: candidate tree path %s is not allowed", ErrUnsafeWorktree, path)
		}
		if fields[1] != "blob" || (fields[0] != "100644" && (fields[0] != "100755" || !policy.AllowExecutable)) {
			return fmt.Errorf("%w: unsafe candidate tree entry %s", ErrUnsafeWorktree, path)
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
	ExpectedParent          string
	Policy                  DiffPolicy
}

func (r Runner) Commit(ctx context.Context, worktree Worktree, request CommitRequest) (string, error) {
	if !validEvidenceDigest(request.EvidenceDigest) || (request.Message != "" && !boundedCommitText(request.Message, 4_000)) || request.Timestamp.IsZero() || !validRef(request.BaseRef) || !validOID(request.ExpectedParent) {
		return "", fmt.Errorf("candidate evidence digest and timestamp are required")
	}
	if request.BaseRef != worktree.Identity.BaseRef {
		return "", fmt.Errorf("%w: commit base ref does not match authenticated base", ErrIdentityMismatch)
	}
	if err := r.InspectWorktree(ctx, worktree); err != nil {
		return "", err
	}
	if head, matched, err := r.reconcileCommit(ctx, worktree, request); err != nil {
		return "", err
	} else if matched {
		postPolicy := request.Policy
		postPolicy.ExpectedHead = head
		if err := r.ValidateDiff(ctx, worktree.Path, request.BaseRef, postPolicy); err != nil {
			return "", fmt.Errorf("%w: reconciled commit failed validation: %v", ErrUnsafeWorktree, err)
		}
		if output, statusErr := r.commandExpected(ctx, worktree.Path, worktree.Identity.WorktreeDev, worktree.Identity.WorktreeIno, "status", "--porcelain=v1"); statusErr != nil || strings.TrimSpace(string(output)) != "" {
			return "", fmt.Errorf("%w: reconciled commit worktree is not clean", ErrUnsafeWorktree)
		}
		return head, nil
	} else if head != request.ExpectedParent {
		return "", fmt.Errorf("%w: candidate parent changed", ErrUnsafeWorktree)
	}
	prePolicy := request.Policy
	prePolicy.ExpectedHead = request.ExpectedParent
	if err := r.ValidateDiff(ctx, worktree.Path, request.BaseRef, prePolicy); err != nil {
		return "", err
	}
	if _, err := r.commandExpected(ctx, worktree.Path, worktree.Identity.WorktreeDev, worktree.Identity.WorktreeIno, "add", "-A", "--"); err != nil {
		return "", err
	}
	// The index is mutable control-plane state. Reauthenticate and validate the
	// staged view after add so a racing change cannot bypass the first check.
	if err := r.InspectWorktree(ctx, worktree); err != nil {
		return "", err
	}
	head, err := r.oneExpected(ctx, worktree.Path, worktree.Identity.WorktreeDev, worktree.Identity.WorktreeIno, "rev-parse", "HEAD")
	if err != nil || head != request.ExpectedParent {
		return "", fmt.Errorf("%w: candidate parent changed before commit", ErrUnsafeWorktree)
	}
	stagedPolicy := request.Policy
	stagedPolicy.ExpectedHead = request.ExpectedParent
	if err := r.ValidateDiff(ctx, worktree.Path, request.BaseRef, stagedPolicy); err != nil {
		return "", err
	}
	// Reauthenticate immediately before the commit effect after all staging and
	// validation, binding the final Git command to the durable identity.
	if err := r.InspectWorktree(ctx, worktree); err != nil {
		return "", err
	}
	if head, err := r.oneExpected(ctx, worktree.Path, worktree.Identity.WorktreeDev, worktree.Identity.WorktreeIno, "rev-parse", "HEAD"); err != nil || head != request.ExpectedParent {
		return "", fmt.Errorf("%w: candidate parent changed immediately before commit", ErrUnsafeWorktree)
	}
	message := candidateMessage(request)
	timestamp := request.Timestamp.UTC().Format(time.RFC3339)
	tree, err := r.oneExpected(ctx, worktree.Path, worktree.Identity.WorktreeDev, worktree.Identity.WorktreeIno, "write-tree")
	if err != nil || !validOID(tree) {
		return "", fmt.Errorf("%w: staged tree could not be persisted", ErrUnsafeWorktree)
	}
	if err := r.validateImmutableTree(ctx, worktree.Path, request.BaseRef, tree, request.Policy); err != nil {
		return "", err
	}
	// The tree is immutable, but the control plane and parent are not. Prove
	// both again immediately before commit-tree; later index changes cannot
	// affect the already-validated tree object.
	if err := r.InspectWorktree(ctx, worktree); err != nil {
		return "", err
	}
	if head, err := r.oneExpected(ctx, worktree.Path, worktree.Identity.WorktreeDev, worktree.Identity.WorktreeIno, "rev-parse", "HEAD"); err != nil || head != request.ExpectedParent {
		return "", fmt.Errorf("%w: candidate parent changed before commit-tree", ErrUnsafeWorktree)
	}
	newHead, err := r.oneEnvExpected(ctx, worktree.Path, worktree.Identity.WorktreeDev, worktree.Identity.WorktreeIno, deterministicCommitEnv(timestamp), "commit-tree", tree, "-p", request.ExpectedParent, "-m", message)
	if err != nil || !validOID(newHead) {
		return "", fmt.Errorf("%w: candidate commit could not be created", ErrUnsafeWorktree)
	}
	// update-ref's old-value argument is the concurrency boundary: even if a
	// second writer advances the branch after the final observation, this CAS
	// refuses to rewrite or append to the unexpected parent.
	if _, err := r.commandExpected(ctx, worktree.Path, worktree.Identity.WorktreeDev, worktree.Identity.WorktreeIno, "update-ref", "--no-deref", "refs/heads/"+worktree.Branch, newHead, request.ExpectedParent); err != nil {
		return "", err
	}
	head, err = r.oneExpected(ctx, worktree.Path, worktree.Identity.WorktreeDev, worktree.Identity.WorktreeIno, "rev-parse", "HEAD")
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
	if output, err := r.commandExpected(ctx, worktree.Path, worktree.Identity.WorktreeDev, worktree.Identity.WorktreeIno, "status", "--porcelain=v1"); err != nil || strings.TrimSpace(string(output)) != "" {
		return "", fmt.Errorf("%w: post-commit worktree is not clean", ErrUnsafeWorktree)
	}
	return head, nil
}

func (r Runner) reconcileCommit(ctx context.Context, worktree Worktree, request CommitRequest) (string, bool, error) {
	head, err := r.oneExpected(ctx, worktree.Path, worktree.Identity.WorktreeDev, worktree.Identity.WorktreeIno, "rev-parse", "HEAD")
	if err != nil {
		return "", false, err
	}
	if head == request.ExpectedParent {
		return head, false, nil
	}
	parents, err := r.oneExpected(ctx, worktree.Path, worktree.Identity.WorktreeDev, worktree.Identity.WorktreeIno, "rev-list", "--parents", "-n", "1", "HEAD")
	if err != nil {
		return "", false, err
	}
	fields := strings.Fields(parents)
	if len(fields) != 2 || fields[0] != head || fields[1] != request.ExpectedParent {
		return head, false, nil
	}
	tree, err := r.oneExpected(ctx, worktree.Path, worktree.Identity.WorktreeDev, worktree.Identity.WorktreeIno, "rev-parse", "HEAD^{tree}")
	if err != nil || !validOID(tree) {
		return "", false, err
	}
	// Reconstruct the object, rather than comparing a parent and subject. A Git
	// commit OID binds its hash algorithm, tree, all parents, complete message,
	// author/committer identity and timestamps. This also makes a lost-response
	// adoption deterministic and rejects a visually equivalent spoof.
	expected, err := r.oneEnvExpected(ctx, worktree.Path, worktree.Identity.WorktreeDev, worktree.Identity.WorktreeIno,
		deterministicCommitEnv(request.Timestamp.UTC().Format(time.RFC3339)), "commit-tree", tree, "-p", request.ExpectedParent, "-m", candidateMessage(request))
	if err != nil {
		return "", false, err
	}
	return head, expected == head, nil
}

func candidateMessage(request CommitRequest) string {
	message := "sf candidate " + request.EvidenceDigest
	if request.Message != "" {
		message += "\n\n" + request.Message
	}
	return message
}

func deterministicCommitEnv(timestamp string) []string {
	return []string{"GIT_AUTHOR_NAME=sf", "GIT_AUTHOR_EMAIL=sf@localhost", "GIT_COMMITTER_NAME=sf", "GIT_COMMITTER_EMAIL=sf@localhost", "GIT_AUTHOR_DATE=" + timestamp, "GIT_COMMITTER_DATE=" + timestamp}
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

// PushRequest carries both durable sides of the publication fence. Empty
// ExpectedPriorHead means the ticket branch must not exist (except an exact
// retry already at ExpectedHead); callers that have observed a prior candidate
// branch must carry it exactly rather than accepting a fast-forwardable ref.
type PushRequest struct{ ExpectedHead, ExpectedPriorHead string }

// Push is retained for existing callers; new effect owners should use
// PushWithRequest to persist the candidate-branch observation explicitly.
func (r Runner) Push(ctx context.Context, worktree Worktree, expectedHead string) (string, error) {
	return r.PushWithRequest(ctx, worktree, PushRequest{ExpectedHead: expectedHead})
}

func (r Runner) PushWithRequest(ctx context.Context, worktree Worktree, request PushRequest) (string, error) {
	if _, err := validateBranch(worktree.Branch); err != nil {
		return "", err
	}
	if !validOID(request.ExpectedHead) || (request.ExpectedPriorHead != "" && !validOID(request.ExpectedPriorHead)) {
		return "", fmt.Errorf("%w: invalid expected candidate head", ErrUnexpectedRemote)
	}
	if strings.HasPrefix(worktree.Identity.PushOrigin, "https://") {
		return "", ErrHTTPSCredentialBoundary
	}
	sshEnv, _, err := r.githubSSHPushEnvironment(worktree.Identity.PushOrigin)
	if err != nil {
		return "", err
	}
	_, err = r.provePushHead(ctx, worktree, request.ExpectedHead)
	if err != nil {
		return "", err
	}
	// Every publication starts from a fresh remote observation. The local
	// snapshot alone is insufficient: another actor may have moved BaseRef
	// after the worktree was made but before this irreversible effect.
	baseEnv, _, err := r.githubSSHPushEnvironment(worktree.Identity.Origin)
	if err != nil {
		return "", err
	}
	remoteBase, err := r.remoteHeadEnv(ctx, worktree.Path, worktree.Identity.WorktreeDev, worktree.Identity.WorktreeIno, worktree.Identity.Origin, worktree.Identity.BaseRef, baseEnv)
	if err != nil || remoteBase != worktree.Identity.BaseHead {
		if err != nil {
			return "", err
		}
		return "", fmt.Errorf("%w: remote base moved", ErrUnexpectedRemote)
	}
	remote, err := r.remoteHeadEnv(ctx, worktree.Path, worktree.Identity.WorktreeDev, worktree.Identity.WorktreeIno, worktree.Identity.PushOrigin, worktree.Branch, sshEnv)
	if err != nil {
		return "", err
	}
	if remote == request.ExpectedHead {
		return request.ExpectedHead, nil
	}
	if remote != request.ExpectedPriorHead {
		return "", fmt.Errorf("%w: candidate branch does not match durable observation", ErrUnexpectedRemote)
	}
	// Reauthenticate and prove the exact candidate immediately before push.
	if _, err := r.provePushHead(ctx, worktree, request.ExpectedHead); err != nil {
		return "", err
	}
	refspec := request.ExpectedHead + ":refs/heads/" + worktree.Branch
	if worktree.Identity.PushOrigin == "" {
		return "", fmt.Errorf("%w: authenticated push URL is missing", ErrIdentityMismatch)
	}
	if _, err := r.commandEnvExpected(ctx, worktree.Path, worktree.Identity.WorktreeDev, worktree.Identity.WorktreeIno, sshEnv, "push", worktree.Identity.PushOrigin, refspec); err != nil {
		// The server may have accepted the ref while the response was lost. Only
		// reconcile success when the exact expected candidate is observed.
		if _, proveErr := r.provePushHead(ctx, worktree, request.ExpectedHead); proveErr != nil {
			return "", err
		}
		observed, observeErr := r.remoteHeadEnv(ctx, worktree.Path, worktree.Identity.WorktreeDev, worktree.Identity.WorktreeIno, worktree.Identity.PushOrigin, worktree.Branch, sshEnv)
		if observeErr == nil && observed == request.ExpectedHead {
			return request.ExpectedHead, nil
		}
		return "", err
	}
	observed, err := r.remoteHeadEnv(ctx, worktree.Path, worktree.Identity.WorktreeDev, worktree.Identity.WorktreeIno, worktree.Identity.PushOrigin, worktree.Branch, sshEnv)
	if err != nil {
		return "", err
	}
	if observed != request.ExpectedHead {
		return "", fmt.Errorf("%w: push did not converge", ErrUnexpectedRemote)
	}
	return request.ExpectedHead, nil
}

func (r Runner) githubSSHPushEnvironment(origin string) ([]string, bool, error) {
	if validAbsolutePath(origin) {
		return nil, false, nil
	}
	parsed, err := url.Parse(origin)
	if err != nil || parsed.Scheme != "ssh" || parsed.User == nil || parsed.User.Username() != "git" || parsed.Hostname() != "ssh.github.com" || parsed.Port() != "443" || !validGitHubRepoPath(strings.TrimPrefix(parsed.Path, "/")) {
		return nil, false, fmt.Errorf("%w: only canonical GitHub SSH publication is supported", ErrIdentityMismatch)
	}
	for _, item := range []struct{ path, name string }{{r.SSHHelper, "ssh helper"}, {r.SSHBinary, "ssh binary"}, {r.SSHKnownHosts, "known hosts"}, {r.SSHAgentSock, "agent socket"}} {
		if !validAbsolutePath(item.path) {
			return nil, false, fmt.Errorf("%w: %s is required", ErrHTTPSCredentialBoundary, item.name)
		}
	}
	return []string{"GIT_SSH=" + r.SSHHelper, "GIT_SSH_VARIANT=ssh", "SF_GIT_SSH_BINARY=" + r.SSHBinary, "SF_GIT_SSH_KNOWN_HOSTS=" + r.SSHKnownHosts, "SF_GIT_SSH_REPOSITORY=" + strings.TrimSuffix(strings.TrimPrefix(parsed.Path, "/"), ".git"), "SSH_AUTH_SOCK=" + r.SSHAgentSock}, true, nil
}

// PublishGitHub validates the exact local candidate and the durable mutation
// claim, then fails before launching gh. GitHub's documented Git Data API has
// no compare-and-swap field for an expected prior ref SHA: POST creates only an
// absent ref and PATCH offers force or ordinary fast-forward behavior. Neither
// can atomically prove the remote base observed for this candidate. Publishing
// through a gh api read-then-write sequence would create the very reconcile
// gap this boundary is intended to close.
//
// A future implementation needs a forge endpoint with expected-old-SHA CAS, or
// a separately qualified transport whose server-side atomic ref transaction
// returns and durably binds the published candidate SHA. Until then this method
// makes no network call and is safe to retry after a lost response because no
// response can have represented a started mutation.
func (r Runner) PublishGitHub(ctx context.Context, request GitHubPublicationRequest, authority GitHubPublicationAuthority) (string, error) {
	if authority == nil || request.Claim.SemanticKey == "" || !validEvidenceDigest(request.Claim.RequestDigest) ||
		request.Claim.Fence.LeaderEpoch == 0 || request.Claim.Fence.RunnerEpoch == 0 || request.Claim.Fence.ClaimEpoch == 0 ||
		!validOID(request.ExpectedRemoteBase) || !validOID(request.ExpectedHead) || !validOID(request.ExpectedTree) || len(request.Policy.AllowedPaths) == 0 {
		return "", fmt.Errorf("%w: invalid durable github publication request", ErrUnexpectedRemote)
	}
	if _, err := validateBranch(request.Worktree.Branch); err != nil {
		return "", err
	}
	if request.ExpectedRemoteBase != request.Worktree.Identity.BaseHead {
		return "", fmt.Errorf("%w: remote base does not match authenticated base", ErrUnexpectedRemote)
	}
	if err := r.InspectWorktree(ctx, request.Worktree); err != nil {
		return "", err
	}
	head, err := r.oneExpected(ctx, request.Worktree.Path, request.Worktree.Identity.WorktreeDev, request.Worktree.Identity.WorktreeIno, "rev-parse", "HEAD^{commit}")
	if err != nil || head != request.ExpectedHead {
		return "", fmt.Errorf("%w: local candidate head changed", ErrUnexpectedRemote)
	}
	parent, err := r.oneExpected(ctx, request.Worktree.Path, request.Worktree.Identity.WorktreeDev, request.Worktree.Identity.WorktreeIno, "rev-parse", "HEAD^1")
	if err != nil || parent != request.ExpectedRemoteBase {
		return "", fmt.Errorf("%w: local candidate parent is not expected remote base", ErrUnexpectedRemote)
	}
	tree, err := r.oneExpected(ctx, request.Worktree.Path, request.Worktree.Identity.WorktreeDev, request.Worktree.Identity.WorktreeIno, "rev-parse", "HEAD^{tree}")
	if err != nil || tree != request.ExpectedTree {
		return "", fmt.Errorf("%w: local candidate tree changed", ErrUnexpectedRemote)
	}
	policy := request.Policy
	policy.ExpectedHead = request.ExpectedHead
	if err := r.validateImmutableTree(ctx, request.Worktree.Path, request.Worktree.Identity.BaseRef, tree, policy); err != nil {
		return "", err
	}
	if err := authority.ValidateGitHubPublication(ctx, request.Claim); err != nil {
		return "", err
	}
	return "", ErrGitHubRefCASUnavailable
}

func (r Runner) provePushHead(ctx context.Context, worktree Worktree, expectedHead string) (string, error) {
	if err := r.InspectWorktree(ctx, worktree); err != nil {
		return "", err
	}
	head, err := r.oneExpected(ctx, worktree.Path, worktree.Identity.WorktreeDev, worktree.Identity.WorktreeIno, "rev-parse", "--verify", "HEAD^{commit}")
	if err != nil {
		return "", err
	}
	branchHead, err := r.oneExpected(ctx, worktree.Path, worktree.Identity.WorktreeDev, worktree.Identity.WorktreeIno, "rev-parse", "--verify", "refs/heads/"+worktree.Branch+"^{commit}")
	if err != nil {
		return "", err
	}
	if !validOID(head) || head != expectedHead || branchHead != expectedHead {
		return "", fmt.Errorf("%w: local candidate head changed", ErrUnexpectedRemote)
	}
	return head, nil
}

func (r Runner) remoteHead(ctx context.Context, directory string, expectedDev, expectedIno uint64, origin, branch string) (string, error) {
	return r.remoteHeadEnv(ctx, directory, expectedDev, expectedIno, origin, branch, nil)
}
func (r Runner) remoteHeadEnv(ctx context.Context, directory string, expectedDev, expectedIno uint64, origin, branch string, extra []string) (string, error) {
	output, err := r.commandEnvExpected(ctx, directory, expectedDev, expectedIno, extra, "ls-remote", "--heads", origin, "refs/heads/"+branch)
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
