//go:build darwin

package processsupervisor

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestAuthoringSandboxNativeHelperBoundary(t *testing.T) {
	if os.Getenv("SF_AUTHORING_SANDBOX_HELPER") == "1" {
		runAuthoringSandboxHelper(t)
		return
	}

	self, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	self, err = filepath.EvalSymlinks(self)
	if err != nil {
		t.Fatal(err)
	}
	stage := filepath.Dir(self)
	canonical := func(path string) string {
		path, err := filepath.EvalSymlinks(path)
		if err != nil {
			t.Fatal(err)
		}
		return path
	}
	home := canonical(t.TempDir())
	temporary := canonical(t.TempDir())
	outside := canonical(t.TempDir())
	privateFile := filepath.Join(home, "approved.txt")
	repositoryFile := filepath.Join(outside, "repository.txt")
	if err := os.WriteFile(privateFile, []byte("authoring-private-ok"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(repositoryFile, []byte("must-not-read"), 0o600); err != nil {
		t.Fatal(err)
	}
	profile, err := authoringSandbox(stage, self, home, temporary)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(profile, `(allow file-read* (literal "/"))`) || strings.Contains(profile, `(subpath "/")`) || strings.Contains(profile, "(allow process-fork)") {
		t.Fatal("native-loader baseline must grant only literal root, never descendants or child creation")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "/usr/bin/sandbox-exec", "-p", profile, self, "-test.run", "^TestAuthoringSandboxNativeHelperBoundary$")
	cmd.Dir = "/"
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.WaitDelay = 2 * time.Second
	cmd.Cancel = func() error {
		if cmd.Process == nil {
			return nil
		}
		return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
	}
	cmd.Env = []string{
		"HOME=" + home,
		"TMPDIR=" + temporary,
		"SF_AUTHORING_SANDBOX_HELPER=1",
		"SF_AUTHORING_SANDBOX_PRIVATE=" + privateFile,
		"SF_AUTHORING_SANDBOX_REPOSITORY=" + repositoryFile,
	}
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("native authoring sandbox helper failed: %v: %s", err, out)
	}
	if !strings.Contains(string(out), "authoring-sandbox-ok") {
		t.Fatalf("native authoring sandbox helper did not prove all boundaries: %q", out)
	}
}

func runAuthoringSandboxHelper(t *testing.T) {
	privateFile := os.Getenv("SF_AUTHORING_SANDBOX_PRIVATE")
	repositoryFile := os.Getenv("SF_AUTHORING_SANDBOX_REPOSITORY")
	contents, err := os.ReadFile(privateFile)
	if err != nil || string(contents) != "authoring-private-ok" {
		t.Fatalf("approved private read failed: %v %q", err, contents)
	}
	if contents, err := os.ReadFile(repositoryFile); err == nil || string(contents) == "must-not-read" {
		t.Fatalf("repository read unexpectedly succeeded: %v %q", err, contents)
	}
	if output, err := exec.Command("/bin/echo", "child").CombinedOutput(); err == nil {
		t.Fatalf("child exec unexpectedly succeeded: %q", output)
	}
	fmt.Fprintln(os.Stdout, "authoring-sandbox-ok")
}
