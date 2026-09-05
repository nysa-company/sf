package cli

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"testing"

	"github.com/nysa-company/sf/internal/domain"
)

func TestInferredProjectNameIsBoundedAndUsable(t *testing.T) {
	for _, test := range []struct{ input, want string }{
		{"My App", "my-app"}, {"123_app", "project-123-app"},
		{"___", "project"}, {"Hello---World", "hello-world"},
		{strings.Repeat("a", 100), strings.Repeat("a", 48)},
	} {
		got := inferredProjectName(test.input)
		if got != test.want || !regexp.MustCompile(`^[a-z][a-z0-9-]{0,47}$`).MatchString(got) {
			t.Fatalf("%q: got %q, want %q", test.input, got, test.want)
		}
	}
}

func TestResolveInitDefaultsToCurrentRepository(t *testing.T) {
	repository := initializedRepository(t)
	t.Chdir(repository)
	request, err := resolveInitRequest(context.Background(), InitRequest{})
	if err != nil || request.Repo != repository || request.Project != inferredProjectName(filepath.Base(repository)) {
		t.Fatalf("request=%+v err=%v", request, err)
	}
	request, err = resolveInitRequest(context.Background(), InitRequest{Repo: ".", Project: "explicit-name"})
	if err != nil || request.Project != "explicit-name" {
		t.Fatalf("explicit request=%+v err=%v", request, err)
	}
}

func TestInitCheckIsReadOnlyAndDoesNotClaimFullReadiness(t *testing.T) {
	repository, home := initializedRepository(t), t.TempDir()
	response := RunInitCheck(context.Background(), InitRequest{Channel: domain.ChannelDev, Repo: repository, Home: home})
	if response.OK != (runtime.GOOS == "darwin") || response.Mutation.Attempted {
		t.Fatalf("response=%+v", response)
	}
	var data struct {
		Setup initPreview `json:"setup"`
	}
	if err := json.Unmarshal(response.Data, &data); err != nil {
		t.Fatal(err)
	}
	if data.Setup.LocalRecipe != "supported" || data.Setup.Providers != "not_checked" || data.Setup.Publication != "not_checked" {
		t.Fatalf("preview=%+v", data.Setup)
	}
	entries, err := os.ReadDir(home)
	if err != nil || len(entries) != 0 {
		t.Fatalf("preview created channel state: %v %v", entries, err)
	}
	if _, err := os.Lstat(filepath.Join(repository, ".sf")); !os.IsNotExist(err) {
		t.Fatalf("preview created configuration/locks: %v", err)
	}
}

func TestInitCheckRefusesInvalidConfigWithoutWritingState(t *testing.T) {
	repository, home := initializedRepository(t), t.TempDir()
	if err := os.Mkdir(filepath.Join(repository, ".sf"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(repository, ".sf", "config.toml"), []byte("unrestricted = true\n"), 0600); err != nil {
		t.Fatal(err)
	}
	response := RunInitCheck(context.Background(), InitRequest{Channel: domain.ChannelDev, Repo: repository, Home: home})
	if response.OK || response.Mutation.Attempted {
		t.Fatalf("response=%+v", response)
	}
	entries, err := os.ReadDir(home)
	if err != nil || len(entries) != 0 {
		t.Fatalf("preview wrote state: %v %v", entries, err)
	}
}

func TestInitCheckExplainsUnsupportedStacksWithoutRunningThem(t *testing.T) {
	for _, test := range []struct{ marker, content, reason string }{
		{"pyproject.toml", "[project]\nname='example'\n", "Python local execution is not supported"},
		{"Gemfile", "raise 'must never execute'\n", "Ruby/Rails local execution is not supported"},
		{"package.json", `{"dependencies":{"typescript":"5.0.0"}}`, "dependency-free Node"},
	} {
		t.Run(test.marker, func(t *testing.T) {
			repository, home := initializedRepository(t), t.TempDir()
			if err := os.Remove(filepath.Join(repository, "go.mod")); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(repository, test.marker), []byte(test.content), 0600); err != nil {
				t.Fatal(err)
			}
			response := RunInitCheck(context.Background(), InitRequest{Channel: domain.ChannelDev, Repo: repository, Home: home})
			if response.OK || response.Mutation.Attempted || response.Error == nil || !strings.Contains(response.Error.Message, test.reason) {
				t.Fatalf("response=%+v", response)
			}
			entries, err := os.ReadDir(home)
			if err != nil || len(entries) != 0 {
				t.Fatalf("preview wrote state: %v %v", entries, err)
			}
		})
	}
}
