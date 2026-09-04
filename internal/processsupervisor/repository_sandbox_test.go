package processsupervisor

import (
	"errors"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"
)

// This is intentionally an OS-backed fixture, not a string-only profile
// test. It proves the two properties the tracked test gate relies on: its
// target is a process-group leader (setsid fails), and Seatbelt denies a
// hostile double-fork before any child can retain inherited pipes.
func TestRepositoryStrictSandboxBlocksDoubleForkAndSetsid(t *testing.T) {
	if runtime.GOOS != "darwin" {
		t.Skip("repository command product boundary is macOS")
	}
	self, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	self, err = filepath.EvalSymlinks(self)
	if err != nil {
		t.Fatal(err)
	}
	if os.Getenv("SF_REPOSITORY_STRICT_HELPER") == "1" {
		pgid, err := syscall.Getpgid(0)
		if err != nil {
			t.Fatal(err)
		}
		if pgid != os.Getpid() {
			if err := syscall.Setpgid(0, 0); err != nil {
				t.Fatal(err)
			}
		}
		if pgid, err = syscall.Getpgid(0); err != nil || pgid != os.Getpid() {
			t.Fatalf("sandbox helper is not a process-group leader: pgid=%d pid=%d err=%v", pgid, os.Getpid(), err)
		}
		profile, err := repositoryStrictSandboxProfile(os.Getenv("SF_REPOSITORY_STRICT_ROOT"), self)
		if err != nil {
			t.Fatal(err)
		}
		if err := ApplyRepositoryTestSandbox(profile); err != nil {
			t.Fatal(err)
		}
		if _, err := syscall.Setsid(); err == nil {
			_, _ = os.Stdout.WriteString("setsid-ok\n")
		} else if errors.Is(err, syscall.EPERM) {
			_, _ = os.Stdout.WriteString("setsid-denied\n")
		} else {
			t.Fatalf("setsid failed for an unexpected reason: %v", err)
		}
		// Execute the already allowlisted test binary so the only remaining
		// forbidden operation is process-fork itself. Using /usr/bin/true here
		// would let process-exec denial mask a missing process-fork rule.
		pid, err := syscall.ForkExec(self, []string{self, "-test.run=^$"}, &syscall.ProcAttr{
			Env:   os.Environ(),
			Files: []uintptr{0, 1, 2},
		})
		if err == nil {
			var status syscall.WaitStatus
			_, _ = syscall.Wait4(pid, &status, 0, nil)
			_, _ = os.Stdout.WriteString("fork-ok\n")
		} else if errors.Is(err, syscall.EPERM) {
			_, _ = os.Stdout.WriteString("fork-denied\n")
		} else {
			t.Fatalf("fork failed for an unexpected reason: %v", err)
		}
		connection, err := net.DialTimeout("tcp", os.Getenv("SF_REPOSITORY_STRICT_ADDR"), time.Second)
		if err == nil {
			_ = connection.Close()
			_, _ = os.Stdout.WriteString("network-ok\n")
		} else if errors.Is(err, syscall.EPERM) {
			_, _ = os.Stdout.WriteString("network-denied\n")
		} else {
			t.Fatalf("network probe failed for an unexpected reason: %v", err)
		}
		return
	}
	profile, err := repositoryStrictSandboxProfile(t.TempDir(), self)
	if err != nil {
		t.Fatal(err)
	}
	_ = profile // Constructing this first also proves the fixture host supports the strict profile.
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	cmd := exec.Command(os.Args[0], "-test.run=^TestRepositoryStrictSandboxBlocksDoubleForkAndSetsid$")
	cmd.Env = append(os.Environ(), "SF_REPOSITORY_STRICT_HELPER=1", "SF_REPOSITORY_STRICT_ROOT="+t.TempDir(), "SF_REPOSITORY_STRICT_ADDR="+listener.Addr().String())
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("sandbox fixture failed: %v: %s", err, out)
	}
	text := string(out)
	if !strings.Contains(text, "setsid-denied") || !strings.Contains(text, "fork-denied") || !strings.Contains(text, "network-denied") || strings.Contains(text, "fork-ok") || strings.Contains(text, "network-ok") {
		t.Fatalf("hostile fixture escaped expected restrictions: %q", text)
	}
}

func TestRepositoryStrictSandboxAllowsReadOnlyGoRepository(t *testing.T) {
	if runtime.GOOS != "darwin" {
		t.Skip("repository command product boundary is macOS")
	}
	if os.Getenv("SF_REPOSITORY_STRICT_GO_HELPER") == "1" {
		repository := os.Getenv("SF_REPOSITORY_STRICT_GO_REPOSITORY")
		contents, err := os.ReadFile(filepath.Join(repository, "go.mod"))
		if err != nil || !strings.Contains(string(contents), "module ") {
			t.Fatalf("confined Go helper could not read repository module: %v", err)
		}
		_, _ = os.Stdout.WriteString("go-repository-read-ok\n")
		return
	}
	repository, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	self, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	self, err = filepath.EvalSymlinks(self)
	if err != nil {
		t.Fatal(err)
	}
	private := t.TempDir()
	profile, err := repositoryStrictSandboxProfileFor(repositorySandboxPaths{Repository: repository, Worktree: repository, GitFile: filepath.Join(repository, ".git"), CommonDir: filepath.Join(repository, ".git"), Home: private, Temporary: private, Executable: self})
	if err != nil {
		t.Fatal(err)
	}
	// The helper is this already-compiled Go test binary, the same shape that
	// the production gate executes after Go has compiled a test. It needs no
	// Go driver, GOROOT, module cache, network, or process-fork allowance.
	cmd := exec.Command(repositorySandboxExec, "-p", profile, self, "-test.run=^TestRepositoryStrictSandboxAllowsReadOnlyGoRepository$")
	cmd.Dir = "/"
	cmd.Env = []string{"HOME=" + private, "TMPDIR=" + private, "SF_REPOSITORY_STRICT_GO_HELPER=1", "SF_REPOSITORY_STRICT_GO_REPOSITORY=" + repository}
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("read-only confined Go helper did not run inside strict sandbox: %v: %s", err, out)
	}
	if !strings.Contains(string(out), "go-repository-read-ok") {
		t.Fatalf("strict sandbox Go helper did not prove repository read: %q", out)
	}
}

func TestRepositoryStrictSandboxDeniesSeparateWorktreeAndHostWrites(t *testing.T) {
	if runtime.GOOS != "darwin" {
		t.Skip("repository command product boundary is macOS")
	}
	self, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	self, err = filepath.EvalSymlinks(self)
	if err != nil {
		t.Fatal(err)
	}
	if os.Getenv("SF_REPOSITORY_STRICT_WRITE_HELPER") == "1" {
		repository := os.Getenv("SF_REPOSITORY_STRICT_WRITE_REPOSITORY")
		worktree := os.Getenv("SF_REPOSITORY_STRICT_WRITE_WORKTREE")
		host := os.Getenv("SF_REPOSITORY_STRICT_WRITE_HOST")
		sentinel := os.Getenv("SF_REPOSITORY_STRICT_WRITE_SENTINEL")
		private := os.Getenv("SF_REPOSITORY_STRICT_WRITE_PRIVATE")
		profile, err := repositoryStrictSandboxProfileFor(repositorySandboxPaths{Repository: repository, Worktree: worktree, GitFile: worktree + "/.git", CommonDir: repository + "/.git", Home: private, Temporary: private, Executable: self})
		if err != nil || ApplyRepositoryTestSandbox(profile) != nil {
			t.Fatal("initialize strict write sandbox")
		}
		probeDeniedWrite := func(name, path string) {
			if err := os.WriteFile(path, []byte("probe"), 0o600); err == nil {
				_, _ = os.Stdout.WriteString(name + "-ok\n")
			} else if errors.Is(err, syscall.EPERM) {
				_, _ = os.Stdout.WriteString(name + "-denied\n")
			} else {
				t.Fatalf("%s failed for an unexpected reason: %v", name, err)
			}
		}
		probeDeniedWrite("source-write", repository+"/source")
		probeDeniedWrite("worktree-write", worktree+"/source")
		probeDeniedWrite("git-write", worktree+"/.git/config")
		probeDeniedWrite("outside-write", host+"/outside")
		if err := os.WriteFile(private+"/allowed", []byte("probe"), 0o600); err != nil {
			t.Fatalf("private write denied: %v", err)
		}
		_, _ = os.Stdout.WriteString("private-write-ok\n")
		if _, err := os.ReadFile(sentinel); err == nil {
			_, _ = os.Stdout.WriteString("credential-read-ok\n")
		} else if errors.Is(err, syscall.EPERM) {
			_, _ = os.Stdout.WriteString("credential-read-denied\n")
		} else {
			t.Fatalf("credential read failed for an unexpected reason: %v", err)
		}
		return
	}
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	repository := root + "/repository"
	worktree := root + "/worktree"
	private := root + "/private"
	host := root + "/host"
	for _, path := range []string{repository, worktree, worktree + "/.git", private, host} {
		if err := os.MkdirAll(path, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	sentinel := host + "/credential-sentinel"
	if err := os.WriteFile(sentinel, []byte("non-secret sentinel"), 0o600); err != nil {
		t.Fatal(err)
	}
	profile, err := repositoryStrictSandboxProfileFor(repositorySandboxPaths{Repository: repository, Worktree: worktree, GitFile: worktree + "/.git", CommonDir: repository + "/.git", Home: private, Temporary: private, Executable: self})
	if err != nil {
		t.Fatal(err)
	}
	_ = profile // Constructing this first also proves the fixture host supports the strict profile.
	cmd := exec.Command(os.Args[0], "-test.run=^TestRepositoryStrictSandboxDeniesSeparateWorktreeAndHostWrites$")
	cmd.Env = append(os.Environ(),
		"SF_REPOSITORY_STRICT_WRITE_HELPER=1",
		"SF_REPOSITORY_STRICT_WRITE_REPOSITORY="+repository,
		"SF_REPOSITORY_STRICT_WRITE_WORKTREE="+worktree,
		"SF_REPOSITORY_STRICT_WRITE_HOST="+host,
		"SF_REPOSITORY_STRICT_WRITE_SENTINEL="+sentinel,
		"SF_REPOSITORY_STRICT_WRITE_PRIVATE="+private,
	)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("strict fixture failed: %v: %s", err, out)
	}
	text := string(out)
	for _, want := range []string{"source-write-denied", "worktree-write-denied", "git-write-denied", "outside-write-denied", "credential-read-denied", "private-write-ok"} {
		if !strings.Contains(text, want) {
			t.Fatalf("strict profile allowed prohibited operation %q: %s", want, text)
		}
	}
}

func TestRepositoryNodeSandboxUsesPrivateStagedClosure(t *testing.T) {
	if runtime.GOOS != "darwin" {
		t.Skip("repository command product boundary is macOS")
	}
	if os.Getenv("SF_REPOSITORY_NODE_SANDBOX_HELPER") == "1" {
		staged, worktree, closure := os.Getenv("SF_REPOSITORY_NODE_STAGED"), os.Getenv("SF_REPOSITORY_NODE_WORKTREE_TEST"), os.Getenv("SF_REPOSITORY_NODE_CLOSURE_TEST")
		profile, err := RepositoryNodeSandboxProfile(worktree, closure, staged)
		if err != nil || ApplyRepositoryTestSandbox(profile) != nil {
			t.Fatal("initialize Node Seatbelt")
		}
		if err := syscall.Exec(staged, []string{staged, "--version"}, os.Environ()); err != nil {
			t.Fatal(err)
		}
		return
	}
	if _, err := os.Stat("/opt/homebrew/bin/node"); err != nil {
		t.Skip("Homebrew Node is unavailable")
	}
	resolved, err := resolveFixedNodeExecutable("node")
	if err != nil {
		t.Fatal(err)
	}
	closure, err := nodeClosureFor(resolved)
	if err != nil {
		t.Fatal(err)
	}
	identity, err := nodeClosureIdentity(closure)
	if err != nil {
		t.Fatal(err)
	}
	staged, library, cleanup, err := stageNodeClosure(closure, identity)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	worktree, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestRepositoryNodeSandboxUsesPrivateStagedClosure$")
	cmd.Env = append(os.Environ(), "PATH=/usr/bin:/bin", "DYLD_LIBRARY_PATH="+library, "HOME=/var/empty", "TMPDIR=/var/empty", "SF_REPOSITORY_NODE_SANDBOX_HELPER=1", "SF_REPOSITORY_NODE_STAGED="+staged, "SF_REPOSITORY_NODE_WORKTREE_TEST="+worktree, "SF_REPOSITORY_NODE_CLOSURE_TEST="+filepath.Dir(library))
	out, err := cmd.CombinedOutput()
	if err != nil || !strings.HasPrefix(strings.TrimSpace(string(out)), "v22.") {
		t.Fatalf("staged closure could not start without original Homebrew paths: %v: %s", err, out)
	}
}
