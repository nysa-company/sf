package pythonclosure

import (
	"crypto/sha256"
	"encoding/hex"
	"path"
	"strings"
)

const RecipeFlag = "--sf-pytest-v1"

// Recipe is a prepared-environment recipe, never arbitrary Python flags. The
// environment and lock are frozen into argv before provider work starts.
// Parsing alone does not enable this recipe in executionpolicy.
type Recipe struct {
	EnvironmentDigest string
	LockDigest        string
	TestPath          string
}

func ParseRecipe(argv []string) (Recipe, error) {
	if len(argv) != 5 || argv[0] != "python3" || argv[1] != RecipeFlag || !validDigest(argv[2]) || !validDigest(argv[3]) || !ValidTestPath(argv[4]) {
		return Recipe{}, ErrInvalid
	}
	return Recipe{argv[2], argv[3], argv[4]}, nil
}

// ValidTestPath admits one relative Python file or tests directory, not flags
// or glob expansion. The supervisor separately authenticates the worktree.
func ValidTestPath(value string) bool {
	return validPath(value) && !strings.HasPrefix(value, "-") && !strings.ContainsAny(value, "*?[]") && strings.Count(value, "/") <= maximum.depth && (strings.HasSuffix(value, ".py") || path.Base(value) == "tests")
}

// BootstrapSource is factory-owned. The initial recipe deliberately excludes
// project pytest configuration, plugin auto-discovery and bytecode rewriting.
// Project conftest/tests still execute, but only inside the supervisor's OS
// boundary. Interpreter flags are supplied by the supervisor, not this script.
// Changing this source changes BootstrapDigest and invalidates prepared state.
const BootstrapSource = `"""SF prepared pytest entrypoint v1."""
import os
import pathlib
import sys

if len(sys.argv) != 4:
    raise SystemExit(125)
dependencies, worktree, test = sys.argv[1:]
root = pathlib.Path(worktree)
target = root / test
if not root.is_absolute() or not pathlib.Path(dependencies).is_absolute():
    raise SystemExit(125)
if pathlib.Path(test).is_absolute() or ".." in pathlib.Path(test).parts:
    raise SystemExit(125)
os.environ.pop("PYTEST_ADDOPTS", None)
os.environ.pop("PYTEST_PLUGINS", None)
os.environ["PYTEST_DISABLE_PLUGIN_AUTOLOAD"] = "1"
sys.path[:0] = [dependencies, worktree]
import pytest
raise SystemExit(pytest.main([
    "-q", "-p", "no:cacheprovider", "--assert=plain", "--color=no",
    "-c", "/dev/null", "--rootdir", worktree, "--confcutdir", worktree,
    str(target),
]))
`

func BootstrapDigest() string {
	sum := sha256.Sum256([]byte(BootstrapSource))
	return "sha256:" + hex.EncodeToString(sum[:])
}
