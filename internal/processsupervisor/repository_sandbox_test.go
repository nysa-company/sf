package processsupervisor

import (
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"testing"
)

// This is intentionally an OS-backed fixture, not a string-only profile
// test. It proves the two properties the tracked test gate relies on: its
// target is a process-group leader (setsid fails), and Seatbelt denies a
// hostile double-fork before any child can retain inherited pipes.
func TestRepositoryStrictSandboxBlocksDoubleForkAndSetsid(t *testing.T) {
	if runtime.GOOS != "darwin" {
		t.Skip("repository command product boundary is macOS")
	}
	perl, err := exec.LookPath("perl")
	if err != nil {
		t.Skip("perl fixture unavailable")
	}
	if os.Getenv("SF_REPOSITORY_STRICT_HELPER") == "1" {
		if err := syscall.Setpgid(0, 0); err != nil {
			t.Fatal(err)
		}
		profile, err := repositoryStrictSandboxProfile(os.Getenv("SF_REPOSITORY_STRICT_ROOT"), perl)
		if err != nil {
			t.Fatal(err)
		}
		if err := ApplyRepositoryTestSandbox(profile); err != nil {
			t.Fatal(err)
		}
		fixture := `use POSIX qw(setsid); use Socket qw(AF_INET SOCK_STREAM sockaddr_in inet_aton); my $session=setsid(); print defined($session) && $session >= 0 ? "setsid-ok\n" : "setsid-denied\n"; my $pid=fork(); print defined($pid) ? "fork-ok\n" : "fork-denied\n"; socket(my $sock, AF_INET, SOCK_STREAM, 0) or die; my $network=connect($sock, sockaddr_in($ENV{SF_REPOSITORY_STRICT_PORT}, inet_aton("127.0.0.1"))); print $network ? "network-ok\n" : "network-denied\n"; exit 0;`
		if err := syscall.Exec(perl, []string{perl, "-e", fixture}, os.Environ()); err != nil {
			t.Fatal(err)
		}
		return
	}
	profile, err := repositoryStrictSandboxProfile(t.TempDir(), perl)
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
	cmd.Env = append(os.Environ(), "SF_REPOSITORY_STRICT_HELPER=1", "SF_REPOSITORY_STRICT_ROOT="+t.TempDir(), "SF_REPOSITORY_STRICT_PORT="+strconv.Itoa(listener.Addr().(*net.TCPAddr).Port))
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
	perl, err := exec.LookPath("perl")
	if err != nil {
		t.Skip("perl fixture unavailable")
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
	profile, err := repositoryStrictSandboxProfileFor(repositorySandboxPaths{Repository: repository, Worktree: worktree, GitFile: worktree + "/.git", CommonDir: repository + "/.git", Home: private, Temporary: private, Executable: perl})
	if err != nil {
		t.Fatal(err)
	}
	fixture := `sub probe { my ($name,$mode,$path)=@_; if (open(my $f,$mode,$path)) { close($f); print "$name-ok\n" } else { print "$name-denied\n" } } probe("source-write",">",$ARGV[0]."/source"); probe("worktree-write",">",$ARGV[1]."/source"); probe("git-write",">",$ARGV[1]."/.git/config"); probe("outside-write",">",$ARGV[2]."/outside"); probe("credential-read","<",$ARGV[3]);`
	cmd := exec.Command(repositorySandboxExec, "-p", profile, perl, "-e", fixture, repository, worktree, host, sentinel)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("strict fixture failed: %v: %s", err, out)
	}
	text := string(out)
	for _, want := range []string{"source-write-denied", "worktree-write-denied", "git-write-denied", "outside-write-denied", "credential-read-denied"} {
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
