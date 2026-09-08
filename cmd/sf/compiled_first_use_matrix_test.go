package main

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/api"
	"github.com/nysa-company/sf/internal/config"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/store"
)

// This is compiled compatibility/registration evidence, not a human usability
// trial or proof of model, test-runtime, publication or merge readiness.
func TestCompiledDevFirstUseStackMatrixIsLocalAndHonest(t *testing.T) {
	if runtime.GOOS != "darwin" {
		t.Skip("macOS first-use acceptance")
	}
	binary := buildDevRuntimeBundle(t)
	for _, test := range []struct {
		name, reason string
		files        map[string]string
		accepted     bool
	}{
		{"go", "", map[string]string{"go.mod": "module example.test/firstuse\n\ngo 1.25\n"}, true},
		{"node", "", map[string]string{"package.json": `{"name":"first-use","private":true,"type":"module"}`, "smoke.test.js": "import test from 'node:test'; test('baseline', () => {});\n"}, true},
		{"node-without-tests", "existing discoverable JavaScript test", map[string]string{"package.json": `{"name":"first-use","private":true}`}, false},
		{"typescript-dependencies", "dependency-free Node", map[string]string{"package.json": `{"devDependencies":{"typescript":"5.0.0"}}`}, false},
		{"python-unprepared", "Python requires the prepared", map[string]string{"pyproject.toml": "[project]\nname='first-use'\n"}, false},
		{"rails", "Ruby/Rails local execution is not supported", map[string]string{"Gemfile": "raise 'must not execute'\n"}, false},
		{"mixed-rails-node", "", map[string]string{"Gemfile": "raise 'must not execute'\n", "package.json": `{}`}, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			root, err := filepath.EvalSymlinks(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			home, repository := filepath.Join(root, "home"), filepath.Join(root, "project")
			for _, path := range []string{home, repository} {
				if err := os.Mkdir(path, 0700); err != nil {
					t.Fatal(err)
				}
			}
			environment := []string{"HOME=" + home, "PATH=/usr/bin:/bin:/usr/sbin:/sbin", "TMPDIR=" + root, "LANG=C", "CODEX_HOME=" + filepath.Join(root, "codex"), "GH_CONFIG_DIR=" + filepath.Join(root, "gh"), "GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_GLOBAL=/dev/null"}
			run := func(executable string, args ...string) ([]byte, error) {
				ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
				defer cancel()
				command := exec.CommandContext(ctx, executable, args...)
				command.Dir, command.Env = repository, environment
				return command.CombinedOutput()
			}
			for path, contents := range test.files {
				if err := os.WriteFile(filepath.Join(repository, path), []byte(contents), 0600); err != nil {
					t.Fatal(err)
				}
			}
			for _, args := range [][]string{{"init", "-b", "main"}, {"add", "--", "."}, {"-c", "user.name=SF Test", "-c", "user.email=sf@example.invalid", "-c", "commit.gpgsign=false", "commit", "-m", "committed project fixture"}} {
				if output, err := run("/usr/bin/git", args...); err != nil {
					t.Fatalf("git: %v %s", err, output)
				}
			}
			output, runErr := run(binary, "init", "--check", "--project", "first-use", "--json")
			var response api.Response
			if err := json.Unmarshal(output, &response); err != nil || response.OK != test.accepted || (runErr == nil) != response.OK || response.Mutation.Attempted {
				t.Fatalf("preview exit=%v envelope=%+v decode=%v output=%s", runErr, response, err, output)
			}
			var data struct {
				Setup struct {
					Providers, Publication string
				} `json:"setup"`
			}
			if err := json.Unmarshal(response.Data, &data); err != nil || data.Setup.Providers != "not_checked" || data.Setup.Publication != "not_checked" {
				t.Fatalf("preview claimed remote/provider readiness: %s (%v)", response.Data, err)
			}
			if test.reason != "" && (response.Error == nil || !strings.Contains(response.Error.Message, test.reason)) {
				t.Fatalf("missing bounded compatibility reason: %+v", response.Error)
			}
			entries, err := os.ReadDir(home)
			if err != nil || len(entries) != 0 {
				t.Fatalf("read-only preview changed HOME: %v %v", entries, err)
			}
			if _, err := os.Lstat(filepath.Join(repository, ".sf")); !os.IsNotExist(err) {
				t.Fatalf("preview wrote project configuration: %v", err)
			}
			if !test.accepted {
				return
			}
			output, runErr = run(binary, "init", "--project", "first-use", "--json")
			if err := json.Unmarshal(output, &response); err != nil || runErr != nil || !response.OK {
				t.Fatalf("registration: %v %v %s", runErr, err, output)
			}
			paths, err := config.PathsFor(home, domain.ChannelDev)
			if err != nil {
				t.Fatal(err)
			}
			db, err := store.OpenReadOnly(t.Context(), paths.Database)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			project, err := db.Project(t.Context(), domain.ChannelDev, "first-use")
			if err != nil || project.Path != repository || project.ConfigGeneration != 1 {
				t.Fatalf("registered project=%+v err=%v", project, err)
			}
			tickets, err := db.Tickets(t.Context(), domain.ChannelDev, "first-use", 10)
			if err != nil || len(tickets) != 0 {
				t.Fatalf("registration submitted tickets: %v %v", tickets, err)
			}
			if _, err := os.Lstat(paths.Socket); !os.IsNotExist(err) {
				t.Fatalf("registration started runtime: %v", err)
			}
		})
	}
}
