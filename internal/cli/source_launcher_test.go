package cli

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestTicketEntrySourceLauncher(t *testing.T) {
	source, err := os.ReadFile("../../scripts/sf")
	if err != nil {
		t.Fatal(err)
	}
	root := filepath.Join(t.TempDir(), "source checkout")
	if err := os.MkdirAll(filepath.Join(root, "scripts"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(root, "bin"), 0700); err != nil {
		t.Fatal(err)
	}
	launcher := filepath.Join(root, "scripts", "sf")
	if err := os.WriteFile(launcher, source, 0700); err != nil {
		t.Fatal(err)
	}
	command := func(args ...string) *exec.Cmd {
		cmd := exec.Command(launcher, args...)
		cmd.Dir = t.TempDir()
		return cmd
	}
	output, err := command("version").CombinedOutput()
	if err == nil || !strings.Contains(string(output), "make build-dev") {
		t.Fatalf("missing bundle: %s %v", output, err)
	}
	// A stable binary must never be selected as a fallback.
	if err := os.WriteFile(filepath.Join(root, "bin", "sf"), []byte("#!/bin/sh\necho WRONG-STABLE\n"), 0700); err != nil {
		t.Fatal(err)
	}
	output, err = command("version").CombinedOutput()
	if err == nil || strings.Contains(string(output), "WRONG-STABLE") {
		t.Fatalf("stable fallback: %s %v", output, err)
	}
	fake := "#!/bin/sh\nprintf '<%s>\\n' \"$@\"\nexit 23\n"
	if err := os.WriteFile(filepath.Join(root, "bin", "sf-dev"), []byte(fake), 0700); err != nil {
		t.Fatal(err)
	}
	output, err = command("ticket", "new", "file with spaces.md", "", "$(no-execution)").CombinedOutput()
	exit, ok := err.(*exec.ExitError)
	if !ok || exit.ExitCode() != 23 || string(output) != "<ticket>\n<new>\n<file with spaces.md>\n<>\n<$(no-execution)>\n" {
		t.Fatalf("argv/exit: %s %v", output, err)
	}
}
