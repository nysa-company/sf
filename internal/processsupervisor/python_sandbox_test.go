package processsupervisor

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/nysa-company/sf/internal/pythonclosure"
)

func pythonSandboxFixture(t *testing.T) PythonSandboxPaths {
	t.Helper()
	if runtime.GOOS != "darwin" {
		t.Skip("macOS sandbox profile")
	}
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	p := PythonSandboxPaths{Worktree: filepath.Join(root, "worktree"), Runtime: filepath.Join(root, "runtime"), Dependencies: filepath.Join(root, "dependencies"), Scratch: filepath.Join(root, "scratch"), Bootstrap: filepath.Join(root, "bootstrap.py"), Executable: filepath.Join(root, "runtime/python")}
	for _, dir := range []string{p.Worktree, p.Runtime, p.Dependencies, p.Scratch} {
		if err := os.Mkdir(dir, 0700); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(p.Executable, []byte("never executed fixture"), 0500); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p.Bootstrap, []byte(pythonclosure.BootstrapSource), 0400); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestPreparedPythonSandboxRestrictsWritableRoot(t *testing.T) {
	p := pythonSandboxFixture(t)
	profile, err := RepositoryPythonSandboxProfile(p)
	if err != nil {
		t.Fatal(err)
	}
	for _, rule := range []string{"(deny default)", "(deny network*)", "(deny process-fork)", "(deny process-exec)", "(allow process-exec (literal " + seatbeltString(p.Executable) + "))", "(allow file-read* file-write* (subpath " + seatbeltString(p.Scratch) + "))"} {
		if !strings.Contains(profile, rule) {
			t.Fatal("missing required boundary", rule)
		}
	}
	for _, root := range []string{p.Worktree, p.Runtime, p.Dependencies} {
		if strings.Contains(profile, "(allow file-read* file-write* (subpath "+seatbeltString(root)+"))") {
			t.Fatal("writable read-only root")
		}
	}
	for _, which := range []string{"project scratch", "runtime scratch", "dependency scratch", "ancestor scratch", "executable outside runtime", "symlink", "public scratch", "changed bootstrap"} {
		t.Run(which, func(t *testing.T) {
			p := pythonSandboxFixture(t)
			switch which {
			case "project scratch":
				p.Scratch = p.Worktree
			case "runtime scratch":
				p.Scratch = p.Runtime
			case "dependency scratch":
				p.Scratch = p.Dependencies
			case "ancestor scratch":
				p.Scratch = filepath.Dir(p.Worktree)
			case "executable outside runtime":
				p.Executable = p.Bootstrap
			case "symlink":
				link := p.Runtime + "-link"
				if err := os.Symlink(p.Runtime, link); err != nil {
					t.Fatal(err)
				}
				p.Runtime = link
			case "public scratch":
				if err := os.Chmod(p.Scratch, 0755); err != nil {
					t.Fatal(err)
				}
			case "changed bootstrap":
				if err := os.Chmod(p.Bootstrap, 0600); err != nil {
					t.Fatal(err)
				}
				data := []byte(pythonclosure.BootstrapSource)
				data[0] = 'X'
				if err := os.WriteFile(p.Bootstrap, data, 0600); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := RepositoryPythonSandboxProfile(p); err == nil {
				t.Fatal("accepted unsafe layout")
			}
		})
	}
}
