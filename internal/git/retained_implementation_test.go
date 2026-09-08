package git

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/nysa-company/sf/internal/domain"
)

func TestRetainedImplementationSurvivesProtectedChangesAndCheckpointCommit(t *testing.T) {
	ctx, runner, repository, _ := fixture(t)
	if err := os.Mkdir(filepath.Join(repository, "tests"), 0700); err != nil {
		t.Fatal(err)
	}
	for path, value := range map[string]string{"tests/proof.txt": "old proof\n", "src/untouched.txt": "unchanged source\n", "tests-other.txt": "outside protected directory\n"} {
		if err := os.WriteFile(filepath.Join(repository, path), []byte(value), 0600); err != nil {
			t.Fatal(err)
		}
	}
	rawGit(t, repository, "add", ".")
	rawGit(t, repository, "commit", "-m", "original checkpoint")
	branch, err := allocatorForTest().Allocate(ctx, domain.ChannelDev, "project", "SF-implementation")
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "worktree")
	worktree, err := runner.CreateWorktree(ctx, repository, path, branch, "main", createClaim(t, repository, path, branch, "main"))
	if err != nil {
		t.Fatal(err)
	}
	checkpoint := worktree.Identity.BaseHead
	write := func(name, contents string) {
		t.Helper()
		if err := os.WriteFile(filepath.Join(path, name), []byte(contents), 0600); err != nil {
			t.Fatal(err)
		}
	}
	inspect := func() string {
		t.Helper()
		head := rawGit(t, path, "rev-parse", "HEAD")
		status := rawGit(t, path, "status", "--porcelain=v1", "--untracked-files=all")
		index := rawGit(t, path, "ls-files", "--stage")
		digest, err := runner.InspectRetainedImplementation(ctx, worktree, checkpoint, []string{"tests"})
		if err != nil || !strings.HasPrefix(digest, "sha256:") || len(digest) != 71 {
			t.Fatalf("digest=%q error=%v", digest, err)
		}
		if rawGit(t, path, "rev-parse", "HEAD") != head || rawGit(t, path, "status", "--porcelain=v1", "--untracked-files=all") != status || rawGit(t, path, "ls-files", "--stage") != index {
			t.Fatal("inspection changed Git state")
		}
		return digest
	}
	write("src/main.txt", "retained implementation\n")
	original := inspect()
	write("tests/proof.txt", "amended proof\n")
	if got := inspect(); got != original {
		t.Fatal("protected unstaged edit changed implementation")
	}
	rawGit(t, path, "add", "tests/proof.txt")
	if got := inspect(); got != original {
		t.Fatal("protected staged edit changed implementation")
	}
	rawGit(t, path, "commit", "--only", "-m", "amended checkpoint", "--", "tests/proof.txt")
	if got := inspect(); got != original {
		t.Fatal("protected checkpoint commit changed implementation")
	}
	if rawGit(t, path, "rev-parse", "HEAD") == checkpoint {
		t.Fatal("fixture did not advance HEAD")
	}
	last := original
	changed := func() {
		t.Helper()
		next := inspect()
		if next == last {
			t.Fatal("nonprotected change did not alter implementation digest")
		}
		last = next
	}
	write("src/main.txt", "different implementation\n")
	changed()
	rawGit(t, path, "add", "src/main.txt")
	changed() // identical source bytes, different index partition
	write("new.txt", "untracked\x00source")
	changed()
	info, err := os.Stat(filepath.Join(path, "src/untouched.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(filepath.Join(path, "src/untouched.txt"), info.Mode().Perm()^0040); err != nil {
		t.Fatal(err)
	}
	// Non-executable permission changes on otherwise unchanged tracked files
	// must not disappear from the implementation inventory.
	changed()
	write("tests-other.txt", "must not be excluded by prefix\n")
	changed()
	if err := os.Remove(filepath.Join(path, "src/main.txt")); err != nil {
		t.Fatal(err)
	}
	changed()
}

func TestRetainedImplementationRejectsUnsafeInventoryAndBaseline(t *testing.T) {
	for _, mode := range []string{"symbolic baseline", "blob baseline", "invalid protected", "root exclusion", "empty exclusion", "duplicate exclusion", "unchanged tracked symlink", "protected symlink", "ignored", "too many files", "oversized"} {
		t.Run(mode, func(t *testing.T) {
			ctx, runner, repository, _ := fixture(t)
			if mode == "unchanged tracked symlink" {
				if err := os.Symlink("main.txt", filepath.Join(repository, "src/link")); err != nil {
					t.Fatal(err)
				}
				rawGit(t, repository, "add", ".")
				rawGit(t, repository, "commit", "-m", "tracked symlink")
			}
			branch, err := allocatorForTest().Allocate(ctx, domain.ChannelDev, "project", "SF-implementation-refusal")
			if err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(t.TempDir(), "worktree")
			worktree, err := runner.CreateWorktree(ctx, repository, path, branch, "main", createClaim(t, repository, path, branch, "main"))
			if err != nil {
				t.Fatal(err)
			}
			checkpoint, protected := worktree.Identity.BaseHead, []string{"tests"}
			switch mode {
			case "symbolic baseline":
				checkpoint = "main"
			case "blob baseline":
				checkpoint = rawGit(t, path, "rev-parse", "HEAD:src/main.txt")
			case "invalid protected":
				protected = []string{"../src"}
			case "root exclusion":
				protected = []string{"."}
			case "empty exclusion":
				protected = nil
			case "duplicate exclusion":
				protected = []string{"tests", "tests"}
			case "protected symlink":
				if err := os.Symlink("src/main.txt", filepath.Join(path, "tests")); err != nil {
					t.Fatal(err)
				}
			case "ignored":
				for name, text := range map[string]string{".gitignore": "hidden\n", "hidden": "ignored bytes"} {
					if err := os.WriteFile(filepath.Join(path, name), []byte(text), 0600); err != nil {
						t.Fatal(err)
					}
				}
			case "too many files":
				for i := 0; i < 256; i++ {
					if err := os.WriteFile(filepath.Join(path, fmt.Sprintf("file-%03d", i)), nil, 0600); err != nil {
						t.Fatal(err)
					}
				}
			case "oversized":
				if err := os.WriteFile(filepath.Join(path, "large"), make([]byte, retainedWorktreeMaxFileBytes+1), 0600); err != nil {
					t.Fatal(err)
				}
			}
			if digest, err := runner.InspectRetainedImplementation(ctx, worktree, checkpoint, protected); err == nil || digest != "" {
				t.Fatalf("unsafe inventory admitted: %q %v", digest, err)
			}
		})
	}
}
