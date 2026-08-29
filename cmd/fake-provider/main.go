// fake-provider is a small process-boundary fixture. It is deliberately
// boring: all behavior is selected by argv/environment and its output is
// bounded, so supervisor tests can use it without a real model or credentials.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

const (
	defaultFixtureDuration = 500 * time.Millisecond
	maximumFixtureDuration = 2 * time.Second
)

type result struct {
	Outcome string `json:"outcome"`
	Fixture string `json:"fixture"`
}

type script struct {
	Steps map[string]struct {
		Behavior string `json:"behavior"`
		Outcome  string `json:"outcome"`
	} `json:"steps"`
}

func main() {
	behavior := flag.String("behavior", "valid", "valid,partial,malformed,hang,progress,secret,forbidden")
	scriptPath := flag.String("script", os.Getenv("SF_FAKE_PROVIDER_SCRIPT"), "optional JSON script")
	phase := flag.String("phase", os.Getenv("SF_FAKE_PROVIDER_PHASE"), "script step to run")
	scenario := flag.String("scenario", "normal", "normal,ignore-term,setsid,double-fork")
	duration := flag.Duration("duration", defaultFixtureDuration, "bounded fixture lifetime (maximum 2s)")
	write := flag.String("write", "", "optional worktree-relative file to write")
	flag.Parse()
	if *duration <= 0 || *duration > maximumFixtureDuration {
		fmt.Fprintf(os.Stderr, "fake-provider: duration must be between 1ns and %s\n", maximumFixtureDuration)
		os.Exit(2)
	}
	if *scriptPath != "" {
		var fixture script
		data, err := os.ReadFile(*scriptPath)
		if err != nil || json.Unmarshal(data, &fixture) != nil {
			fmt.Fprintln(os.Stderr, "fake-provider: invalid script")
			os.Exit(2)
		}
		step, ok := fixture.Steps[*phase]
		if !ok {
			fmt.Fprintln(os.Stderr, "fake-provider: missing script phase")
			os.Exit(2)
		}
		if step.Behavior != "" {
			*behavior = step.Behavior
		}
	}

	if os.Getenv("SF_FAKE_PROVIDER_CHILD") == "1" {
		if os.Getenv("SF_FAKE_PROVIDER_FORK_STAGE") == "1" {
			if err := forkGrandchild(*duration); err != nil {
				fmt.Fprintln(os.Stderr, "fake-provider:", err)
				os.Exit(2)
			}
			return
		}
		runChild(*duration)
		return
	}
	switch *scenario {
	case "ignore-term":
		ignoreTERM()
	case "setsid":
		// Setsid is best-effort: an already detached test process may not be
		// able to create a new session. The fixture still remains bounded.
		_, _ = syscall.Setsid()
	case "double-fork":
		if err := forkChild(*duration); err != nil {
			fmt.Fprintln(os.Stderr, "fake-provider:", err)
			os.Exit(2)
		}
		return
	case "normal":
	default:
		fmt.Fprintln(os.Stderr, "fake-provider: unknown scenario")
		os.Exit(2)
	}

	if *write != "" {
		if err := writeRelative(*write, []byte("partial fixture\n")); err != nil {
			fmt.Fprintln(os.Stderr, "fake-provider:", err)
			os.Exit(2)
		}
	}

	switch *behavior {
	case "valid":
		writeJSON(result{Outcome: "completed", Fixture: "fake-provider"})
	case "partial":
		writeJSON(result{Outcome: "partial", Fixture: "fake-provider"})
		os.Exit(3)
	case "malformed":
		fmt.Print("not-json\n")
		os.Exit(3)
	case "secret":
		fmt.Print("token=fixture-secret-must-redact\n")
		os.Exit(3)
	case "forbidden":
		fmt.Print("forbidden-action=attempted\n")
		os.Exit(3)
	case "hang", "progress":
		if *behavior == "progress" {
			ticker := time.NewTicker(25 * time.Millisecond)
			defer ticker.Stop()
			deadline := time.NewTimer(*duration)
			defer deadline.Stop()
			for {
				select {
				case <-ticker.C:
					fmt.Fprintln(os.Stderr, "progress")
				case <-deadline.C:
					return
				}
			}
		}
		time.Sleep(*duration)
	default:
		fmt.Fprintln(os.Stderr, "fake-provider: unknown behavior")
		os.Exit(2)
	}
}

func writeJSON(value result) {
	_ = json.NewEncoder(os.Stdout).Encode(value)
}

func writeRelative(name string, content []byte) error {
	clean := filepath.Clean(name)
	if filepath.IsAbs(name) || clean == "." || clean == ".." || strings.HasPrefix(clean, ".."+string(filepath.Separator)) {
		return fmt.Errorf("write path must be relative")
	}
	root, err := os.Getwd()
	if err != nil {
		return err
	}
	resolvedRoot, err := filepath.EvalSymlinks(root)
	if err != nil {
		return err
	}
	path := filepath.Join(root, clean)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	resolvedParent, err := filepath.EvalSymlinks(filepath.Dir(path))
	if err != nil {
		return err
	}
	if !within(resolvedRoot, resolvedParent) {
		return fmt.Errorf("write path escapes worktree through symlink")
	}
	if info, err := os.Lstat(path); err == nil && info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("write path is a symlink")
	} else if err != nil && !os.IsNotExist(err) {
		return err
	}
	return os.WriteFile(path, content, 0o644)
}

func within(root, path string) bool {
	rel, err := filepath.Rel(root, path)
	return err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))
}

func ignoreTERM() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGTERM)
	go func() {
		for range signals {
			fmt.Fprintln(os.Stderr, "TERM ignored by fixture")
		}
	}()
}

func forkChild(duration time.Duration) error {
	self, err := os.Executable()
	if err != nil {
		return err
	}
	child := exec.Command(self, "--behavior=hang", "--duration="+duration.String())
	child.Env = childEnvironment("1")
	child.Stdout = os.Stdout
	child.Stderr = os.Stderr
	if err := child.Start(); err != nil {
		return err
	}
	return nil
}

func forkGrandchild(duration time.Duration) error {
	self, err := os.Executable()
	if err != nil {
		return err
	}
	child := exec.Command(self, "--behavior=hang", "--duration="+duration.String())
	child.Env = childEnvironment("2")
	child.Stdout = os.Stdout
	child.Stderr = os.Stderr
	if err := child.Start(); err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "fake-provider: escaped-child-pid=%d lifetime=%s\n", child.Process.Pid, duration)
	return nil
}

func childEnvironment(stage string) []string {
	return []string{
		"SF_FAKE_PROVIDER_CHILD=1",
		"SF_FAKE_PROVIDER_FORK_STAGE=" + stage,
		"SF_FAKE_PROVIDER_SETSID=1",
	}
}

func runChild(duration time.Duration) {
	// Keep the escaped child alive long enough for a supervisor to observe it,
	// but never beyond the caller-selected bound.
	if os.Getenv("SF_FAKE_PROVIDER_SETSID") == "1" {
		_, _ = syscall.Setsid()
	}
	fmt.Fprintf(os.Stderr, "fake-provider: escaped-child-start pid=%d lifetime=%s\n", os.Getpid(), duration)
	time.Sleep(duration)
	fmt.Fprintf(os.Stderr, "fake-provider: escaped-child-exit pid=%d\n", os.Getpid())
}
