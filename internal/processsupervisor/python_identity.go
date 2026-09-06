package processsupervisor

import (
	"context"
	"os"
	"path/filepath"

	"github.com/nysa-company/sf/internal/pythonclosure"
	"golang.org/x/sys/unix"
)

// CommandExecutableIdentity shares the explicitly composed runtime view with
// command execution. Existing Go/Node resolution is unchanged. Prepared Python
// identity is content evidence only: execution also requires exact policy,
// worktree, Store lease and sandbox authentication. Empty composition refuses.
func (s RepositoryCommandSupervisor) CommandExecutableIdentity(ctx context.Context, argv []string) (string, string, error) {
	if ctx == nil {
		return "", "", ErrUnclear
	}
	if err := ctx.Err(); err != nil {
		return "", "", err
	}
	if len(argv) == 0 || argv[0] != "python3" {
		return RepositoryCommandExecutableIdentity(argv)
	}
	prepared, executable, digest, err := s.openPythonRuntime(ctx, argv)
	if err != nil {
		return "", "", err
	}
	defer prepared.Close()
	return executable, digest, nil
}

// openPythonRuntime is the shared resolution boundary. A launch caller owns
// the returned descriptors until proven drain; identity-only callers close
// them immediately. Opening never deletes or repairs prepared cache content.
func (s RepositoryCommandSupervisor) openPythonRuntime(ctx context.Context, argv []string) (*pythonclosure.Prepared, string, string, error) {
	if ctx == nil {
		return nil, "", "", ErrUnclear
	}
	if err := ctx.Err(); err != nil {
		return nil, "", "", err
	}
	recipe, err := pythonclosure.ParseRecipe(argv)
	if err != nil || !cleanAbsolute(s.PythonSnapshots) {
		return nil, "", "", ErrUnclear
	}
	canonical, err := filepath.EvalSymlinks(s.PythonSnapshots)
	if err != nil || canonical != s.PythonSnapshots {
		return nil, "", "", ErrUnclear
	}
	before, err := os.Lstat(canonical)
	if err != nil || !before.IsDir() || before.Mode()&os.ModeSymlink != 0 {
		return nil, "", "", ErrUnclear
	}
	fd, err := unix.Open(canonical, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, "", "", ErrUnclear
	}
	root := os.NewFile(uintptr(fd), "python-channel-snapshots")
	defer root.Close()
	opened, err := root.Stat()
	if err != nil || !os.SameFile(before, opened) {
		return nil, "", "", ErrUnclear
	}
	prepared, err := pythonclosure.OpenPreparedDirectoryFD(ctx, fd, recipe.EnvironmentDigest, recipe.LockDigest, pythonclosure.BootstrapDigest())
	if err != nil {
		return nil, "", "", ErrUnclear
	}
	executable := filepath.Join(canonical, recipe.EnvironmentDigest[7:], "runtime", filepath.FromSlash(prepared.Interpreter()))
	return prepared, executable, recipe.EnvironmentDigest, nil
}
