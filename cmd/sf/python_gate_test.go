package main

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/processsupervisor"
)

func TestPythonGateRejectsMalformedInputBeforeDescriptorUse(t *testing.T) {
	if handled, _ := repositoryPythonGate([]string{"sf", "status"}); handled {
		t.Fatal("captured ordinary CLI command")
	}
	for _, argv := range [][]string{
		{"sf", "__repository_python_command_gate"},
		{"sf", "__repository_python_command_gate", "{}", "../test.py"},
		{"sf", "__repository_python_command_gate", "{}", "tests", "--extra"},
		{"sf", "__repository_python_command_gate", "null", "tests"},
		{"sf", "__repository_python_command_gate", `{"unexpected":1}`, "tests"},
		{"sf", "__repository_python_command_gate", "{} ", "tests"},
	} {
		handled, code := repositoryPythonGate(argv)
		if !handled || code != 125 {
			t.Fatalf("malformed gate result: %v %d", handled, code)
		}
	}
}

func TestPythonGateWaitsForEOFWithoutLaunching(t *testing.T) {
	if os.Getenv("SF_PYTHON_EOF_TEST_CHILD") == "1" {
		data, err := json.Marshal(processsupervisor.PythonSandboxPaths{})
		if err != nil {
			t.Fatal(err)
		}
		handled, code := repositoryPythonGate([]string{"sf", "__repository_python_command_gate", string(data), "tests"})
		if !handled || code != 125 {
			t.Fatalf("EOF result: %v %d", handled, code)
		}
		return
	}
	self, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	defer w.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, self, "-test.run=^TestPythonGateWaitsForEOFWithoutLaunching$", "-test.count=1")
	cmd.Env = []string{"PATH=/usr/bin:/bin", "SF_PYTHON_EOF_TEST_CHILD=1"}
	cmd.ExtraFiles = []*os.File{r}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	r.Close()
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	select {
	case err := <-done:
		t.Fatalf("gate exited before EOF: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	w.Close()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}
