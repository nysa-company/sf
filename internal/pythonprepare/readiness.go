package pythonprepare

import (
	"context"
	"os"
	"path/filepath"
	"runtime"

	"github.com/nysa-company/sf/internal/pythonclosure"
	"golang.org/x/sys/unix"
)

// CheckRecipe is read-only readiness for this code-owned runtime, not execution
// authority. It never prepares missing content or discovers another channel.
func CheckRecipe(ctx context.Context, root string, argv []string) error {
	catalog, err := DefaultCatalog(runtime.GOOS, runtime.GOARCH)
	if err != nil {
		return err
	}
	recipe, err := pythonclosure.ParseRecipe(argv)
	if err != nil || recipe.EnvironmentDigest != catalog.EnvironmentDigest || recipe.LockDigest != LockDigest() || !filepath.IsAbs(root) || filepath.Clean(root) != root || root == "/" {
		return ErrArtifact
	}
	canonical, err := filepath.EvalSymlinks(root)
	if err != nil || canonical != root {
		return ErrArtifact
	}
	fd, err := unix.Open(root, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return ErrArtifact
	}
	file := os.NewFile(uintptr(fd), "python-readiness-cache")
	defer file.Close()
	p, err := pythonclosure.OpenPreparedDirectoryFD(ctx, fd, recipe.EnvironmentDigest, recipe.LockDigest, pythonclosure.BootstrapDigest())
	if err != nil {
		return err
	}
	return p.Close()
}

// RecipeArgv constructs the sole pinned profile without making it executable.
// Runtime readiness is separately platform-checked by CheckRecipe.
func RecipeArgv(testPath string) ([]string, error) {
	if !pythonclosure.ValidTestPath(testPath) {
		return nil, ErrArtifact
	}
	catalog, err := DefaultCatalog("darwin", "arm64")
	if err != nil {
		return nil, err
	}
	return []string{"python3", pythonclosure.RecipeFlag, catalog.EnvironmentDigest, LockDigest(), testPath}, nil
}
