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

func TestRepositoryStrictSandboxAllowsReadOnlyGit(t *testing.T) {
	if runtime.GOOS != "darwin" {
		t.Skip("repository command product boundary is macOS")
	}
	gitPath, err := resolveFixedExecutable("git")
	if err != nil {
		t.Skipf("approved Git unavailable: %v", err)
	}
	repository, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	gitFile := filepath.Join(repository, ".git")
	gitFileRaw, err := os.ReadFile(gitFile)
	if err != nil {
		t.Fatal(err)
	}
	commonDir := filepath.Join(repository, ".git")
	if target, ok := strings.CutPrefix(strings.TrimSpace(string(gitFileRaw)), "gitdir: "); ok {
		commonDir = filepath.Dir(filepath.Dir(strings.TrimSpace(target)))
	}
	profile, err := repositoryStrictSandboxProfileFor(repositorySandboxPaths{Repository: repository, Worktree: repository, GitFile: gitFile, CommonDir: commonDir, Executable: gitPath})
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(repositorySandboxExec, "-p", profile, gitPath, "-C", repository, "status", "--porcelain=v1")
	cmd.Dir = "/"
	gitHome := t.TempDir()
	cmd.Env = append(os.Environ(), "HOME="+gitHome, "GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_GLOBAL=/dev/null", "GIT_ATTR_NOSYSTEM=1", "GIT_OPTIONAL_LOCKS=0", "GIT_TERMINAL_PROMPT=0")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("read-only Git did not run inside strict sandbox: %v: %s", err, out)
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
	root := t.TempDir()
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
