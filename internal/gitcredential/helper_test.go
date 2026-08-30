package gitcredential

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type recordingRunner struct {
	path  string
	args  []string
	env   []string
	stdin string
	runs  int
}

func (runner *recordingRunner) Run(_ context.Context, path string, args, env []string, input io.Reader, output io.Writer) error {
	runner.runs++
	runner.path = path
	runner.args = append([]string(nil), args...)
	runner.env = append([]string(nil), env...)
	raw, _ := io.ReadAll(input)
	runner.stdin = string(raw)
	_, _ = io.WriteString(output, "username=x-access-token\npassword=test-secret\n\n")
	return nil
}

func TestRunDelegatesOnlyCanonicalGet(t *testing.T) {
	root := t.TempDir()
	gh := filepath.Join(root, "gh")
	if err := os.WriteFile(gh, []byte("fixture"), 0o700); err != nil {
		t.Fatal(err)
	}
	config := filepath.Join(root, "config")
	if err := os.Mkdir(config, 0o700); err != nil {
		t.Fatal(err)
	}
	environment := map[string]string{"SF_GIT_HTTPS_REPOSITORY": "nysa-company/nysa-app", "SF_GIT_GH_BINARY": gh, "SF_GIT_GH_CONFIG_DIR": config}
	lookup := func(key string) (string, bool) { value, ok := environment[key]; return value, ok }
	runner := &recordingRunner{}
	var output bytes.Buffer
	request := "protocol=https\nhost=github.com\npath=nysa-company/nysa-app.git\n\n"
	if err := Run(context.Background(), []string{"get"}, strings.NewReader(request), &output, lookup, runner); err != nil {
		t.Fatal(err)
	}
	if runner.runs != 1 || runner.path != gh || strings.Join(runner.args, " ") != "auth git-credential get" || runner.stdin != request || !strings.Contains(output.String(), "password=test-secret") {
		t.Fatalf("runner=%+v output=%q", runner, output.String())
	}
	for _, entry := range runner.env {
		if strings.Contains(entry, "test-secret") {
			t.Fatal("credential entered child environment")
		}
	}
}

func TestRunRefusesOtherHostsRepositoriesAndOversizedInput(t *testing.T) {
	root := t.TempDir()
	gh := filepath.Join(root, "gh")
	if err := os.WriteFile(gh, []byte("fixture"), 0o700); err != nil {
		t.Fatal(err)
	}
	config := filepath.Join(root, "config")
	if err := os.Mkdir(config, 0o700); err != nil {
		t.Fatal(err)
	}
	environment := map[string]string{"SF_GIT_HTTPS_REPOSITORY": "nysa-company/nysa-app", "SF_GIT_GH_BINARY": gh, "SF_GIT_GH_CONFIG_DIR": config}
	lookup := func(key string) (string, bool) { value, ok := environment[key]; return value, ok }
	for _, request := range []string{
		"protocol=https\nhost=evil.example\npath=nysa-company/nysa-app.git\n\n",
		"protocol=https\nhost=github.com\npath=other/repo.git\n\n",
		"protocol=https\nhost=github.com\npath=nysa-company/nysa-app.git\npassword=injected\n\n",
		strings.Repeat("x", maxCredentialBytes+1),
	} {
		runner := &recordingRunner{}
		var output bytes.Buffer
		err := Run(context.Background(), []string{"get"}, strings.NewReader(request), &output, lookup, runner)
		if err == nil || runner.runs != 0 || output.Len() != 0 || strings.Contains(err.Error(), "injected") {
			t.Fatalf("request accepted or leaked: runs=%d output=%q err=%v", runner.runs, output.String(), err)
		}
	}
}

func TestRunStoreAndEraseNeverPersistOrEchoCredential(t *testing.T) {
	environment := map[string]string{"SF_GIT_HTTPS_REPOSITORY": "nysa-company/nysa-app"}
	lookup := func(key string) (string, bool) { value, ok := environment[key]; return value, ok }
	request := "protocol=https\nhost=github.com\npath=nysa-company/nysa-app.git\nusername=x-access-token\npassword=test-secret\n\n"
	for _, operation := range []string{"store", "erase"} {
		runner := &recordingRunner{}
		var output bytes.Buffer
		if err := Run(context.Background(), []string{operation}, strings.NewReader(request), &output, lookup, runner); err != nil {
			t.Fatal(err)
		}
		if runner.runs != 0 || output.Len() != 0 {
			t.Fatalf("%s persisted or echoed a credential", operation)
		}
	}
}
