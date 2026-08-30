package processsupervisor

import (
	"net"
	"os"
	"os/exec"
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
	repository, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	profile, err := repositoryStrictSandboxProfile(repository, gitPath)
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(repositorySandboxExec, "-p", profile, gitPath, "-C", repository, "status", "--porcelain=v1")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("read-only Git did not run inside strict sandbox: %v: %s", err, out)
	}
}
