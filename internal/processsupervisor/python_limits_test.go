package processsupervisor

import (
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"syscall"
	"testing"
	"time"
)

func TestPreparedPythonFileLimitIsEnforcedByOS(t *testing.T) {
	if runtime.GOOS != "darwin" {
		t.Skip("macOS gate limits")
	}
	if os.Getenv("SF_PYTHON_LIMIT_TEST_CHILD") == "1" {
		if err := ApplyRepositoryPythonResourceLimits(); err != nil {
			t.Fatal(err)
		}
		for _, item := range []struct {
			resource int
			maximum  uint64
		}{{syscall.RLIMIT_CORE, 0}, {syscall.RLIMIT_FSIZE, RepositoryPythonFileSizeLimit}, {syscall.RLIMIT_NOFILE, RepositoryPythonOpenFileLimit}} {
			var got syscall.Rlimit
			if syscall.Getrlimit(item.resource, &got) != nil || got.Cur > item.maximum || got.Max > item.maximum {
				t.Fatal("hard resource limit absent")
			}
		}
		// Ignore the signal only in this fixture to assert the kernel's EFBIG
		// result deterministically instead of relying on a runtime signal handler.
		signal.Ignore(syscall.SIGXFSZ)
		file, err := os.OpenFile(os.Getenv("SF_PYTHON_LIMIT_TEST_FILE"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if err != nil {
			t.Fatal(err)
		}
		defer file.Close()
		chunk := bytes.Repeat([]byte("x"), 1<<20)
		for i := 0; i < 17; i++ {
			_, err = file.Write(chunk)
			if err != nil {
				break
			}
		}
		if !errors.Is(err, syscall.EFBIG) {
			t.Fatalf("expected EFBIG, got %v", err)
		}
		return
	}
	self, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "bounded-output")
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, self, "-test.run=^TestPreparedPythonFileLimitIsEnforcedByOS$", "-test.count=1")
	cmd.Env = []string{"PATH=/usr/bin:/bin", "SF_PYTHON_LIMIT_TEST_CHILD=1", "SF_PYTHON_LIMIT_TEST_FILE=" + path}
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("child: %v %s", err, out)
	}
	info, err := os.Stat(path)
	if err != nil || info.Size() > RepositoryPythonFileSizeLimit {
		t.Fatalf("file exceeded hard bound: %v", err)
	}
}
