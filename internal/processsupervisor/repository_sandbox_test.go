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

func TestRepositoryStrictSandboxAllowsReadOnlyGoToolchainRepository(t *testing.T) {
	if runtime.GOOS != "darwin" {
		t.Skip("repository command product boundary is macOS")
	}
	goPath, err := resolveFixedExecutable("go")
	if err != nil {
		t.Skipf("approved Go unavailable: %v", err)
	}
	repository, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	goRoot := filepath.Dir(filepath.Dir(goPath))
	profile, err := repositoryStrictSandboxProfileFor(repositorySandboxPaths{Repository: repository, Worktree: repository, GitFile: filepath.Join(repository, ".git"), CommonDir: filepath.Join(repository, ".git"), Executable: goPath, Toolchain: goRoot})
	if err != nil {
		t.Fatal(err)
	}
	// `go env GOMOD` is a no-fork, read-only toolchain operation which must
	// resolve the checked-in module marker from the confined repository. It is
	// the narrow strict-profile capability used by the Go-only v1 boundary; the
	// actual `go test` driver remains outside Seatbelt only while compiling,
	// before each generated test binary crosses the durable sandbox gate.
	cmd := exec.Command(repositorySandboxExec, "-p", profile, goPath, "env", "GOMOD")
	cmd.Dir = repository
	cmd.Env = []string{"HOME=" + t.TempDir(), "PATH=/usr/bin:/bin", "GOROOT=" + goRoot, "GOTOOLCHAIN=local", "GOENV=off", "GOWORK=off", "GOPROXY=off", "GOSUMDB=off"}
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("read-only Go toolchain did not run inside strict sandbox: %v: %s", err, out)
	}
	if got := strings.TrimSpace(string(out)); got != filepath.Join(repository, "go.mod") {
		t.Fatalf("strict sandbox Go repository read=%q want %q", got, filepath.Join(repository, "go.mod"))
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
