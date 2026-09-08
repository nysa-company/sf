package auth

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCodexLoginAndStatusHonorHomeOverride(t *testing.T) {
	manager, runner := managerFixture(t)
	home, _ := manager.Home()
	selected := filepath.Join(home, "other account")
	manager.Getenv = func(key string) string {
		if key == "CODEX_HOME" {
			return selected
		}
		return ""
	}
	status, attempted, err := manager.Login(context.Background(), Codex, Terminal{In: strings.NewReader(""), Out: &bytes.Buffer{}, Err: &bytes.Buffer{}})
	if err != nil || !attempted || !status.Authenticated {
		t.Fatalf("login: %+v %v %v", status, attempted, err)
	}
	for _, call := range runner.calls {
		assertCodexHome(t, call.environment, selected)
	}
	assertCodexHome(t, runner.interactiveEnvironment, selected)
	if _, err := os.Stat(selected); !os.IsNotExist(err) {
		t.Fatalf("SF created authentication directory: %v", err)
	}
}

func assertCodexHome(t *testing.T, environment []string, selected string) {
	t.Helper()
	assertSafeEnvironment(t, environment)
	count := 0
	for _, value := range environment {
		if strings.HasPrefix(value, "CODEX_HOME=") {
			count++
			if value != "CODEX_HOME="+selected {
				t.Fatal("wrong Codex account")
			}
		}
	}
	if count != 1 {
		t.Fatalf("Codex home count=%d", count)
	}
}

func TestCodexConfigRejectsUnsafeOverrideBeforeProbe(t *testing.T) {
	manager, runner := managerFixture(t)
	manager.Getenv = func(key string) string {
		if key == "CODEX_HOME" {
			return "relative"
		}
		return ""
	}
	status := manager.Status(context.Background(), Codex)
	if status.State != StateProbeFailed || len(runner.calls) != 0 {
		t.Fatalf("unsafe override reached probe: %+v", status)
	}
}

func TestCodexOverrideDoesNotReachGitHub(t *testing.T) {
	manager, runner := managerFixture(t)
	manager.Getenv = func(key string) string {
		if key == "CODEX_HOME" {
			return "relative"
		}
		return ""
	}
	manager.Status(context.Background(), GitHub)
	if len(runner.calls) == 0 {
		t.Fatal("GitHub probe did not run")
	}
	for _, call := range runner.calls {
		for _, value := range call.environment {
			if strings.HasPrefix(value, "CODEX_HOME=") {
				t.Fatal("Codex configuration reached GitHub")
			}
		}
	}
}
