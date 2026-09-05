package auth

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestGitHubLoginAndStatusUseSameSelectedConfig(t *testing.T) {
	for _, useXDG := range []bool{false, true} {
		manager, runner := managerFixture(t)
		home, _ := manager.Home()
		selected := filepath.Join(home, "selected account", "gh")
		previous := manager.Getenv
		manager.Getenv = func(key string) string {
			if key == "GH_CONFIG_DIR" && !useXDG {
				return selected
			}
			if key == "XDG_CONFIG_HOME" {
				return filepath.Dir(selected)
			}
			return previous(key)
		}
		terminal := Terminal{In: strings.NewReader(""), Out: &bytes.Buffer{}, Err: &bytes.Buffer{}}
		status, attempted, err := manager.Login(context.Background(), GitHub, terminal)
		if err != nil || !attempted || !status.Authenticated {
			t.Fatalf("login: %+v %v %v", status, attempted, err)
		}
		for _, call := range runner.calls {
			assertSelectedConfig(t, call.environment, selected)
		}
		assertSelectedConfig(t, runner.interactiveEnvironment, selected)
		if _, err := os.Stat(selected); !os.IsNotExist(err) {
			t.Fatalf("SF created configuration: %v", err)
		}
	}
}

func assertSelectedConfig(t *testing.T, environment []string, selected string) {
	t.Helper()
	assertSafeEnvironment(t, environment)
	count := 0
	for _, entry := range environment {
		if strings.HasPrefix(entry, "GH_CONFIG_DIR=") {
			count++
			if entry != "GH_CONFIG_DIR="+selected {
				t.Fatal("wrong selected account")
			}
		}
	}
	if count != 1 {
		t.Fatalf("configuration count=%d", count)
	}
}

func TestGitHubUnsafeConfigRefusesBeforeProbeOrLogin(t *testing.T) {
	manager, runner := managerFixture(t)
	manager.Getenv = func(key string) string {
		if key == "GH_CONFIG_DIR" {
			return "relative"
		}
		return ""
	}
	status, attempted, err := manager.Login(context.Background(), GitHub, Terminal{})
	if err == nil || attempted || status.State != StateProbeFailed || len(runner.calls) != 0 || runner.interactive != 0 {
		t.Fatalf("unsafe config reached subprocess: %+v %v %v", status, attempted, err)
	}
}

func TestGitHubConfigIsNotForwardedToOtherProviders(t *testing.T) {
	manager, runner := managerFixture(t)
	manager.Getenv = func(key string) string {
		if key == "GH_CONFIG_DIR" {
			return "relative"
		}
		return ""
	}
	manager.Status(context.Background(), Codex)
	if len(runner.calls) == 0 {
		t.Fatal("Codex probe did not run")
	}
	for _, call := range runner.calls {
		for _, entry := range call.environment {
			if strings.HasPrefix(entry, "GH_CONFIG_DIR=") {
				t.Fatal("GitHub config leaked into another provider")
			}
		}
	}
}
