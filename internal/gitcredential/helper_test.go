package gitcredential

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
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
	home, err := filepath.EvalSymlinks(root)
	if err != nil {
		t.Fatal(err)
	}
	environment := map[string]string{"SF_GIT_HTTPS_REPOSITORY": "nysa-company/nysa-app", "SF_GIT_GH_BINARY": gh, "SF_GIT_GH_BINARY_DIGEST": fileDigest(gh), "SF_GIT_GH_CONFIG_DIR": config, "SF_GIT_GH_HOME": home, "HOME": "/hostile", "GH_TOKEN": "ambient-secret", "PATH": "/hostile/bin"}
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
	wantEnv := []string{"HOME=" + home, "LANG=C", "LC_ALL=C", "GH_CONFIG_DIR=" + config, "PATH=/usr/bin:/bin:/usr/sbin:/sbin", "GH_PROMPT_DISABLED=1", "GIT_TERMINAL_PROMPT=0"}
	if strings.Join(runner.env, "\x00") != strings.Join(wantEnv, "\x00") {
		t.Fatal("unexpected gh child environment")
	}
}

func TestTrustedHomeRefusesUnsafePaths(t *testing.T) {
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if !TrustedHome(root) {
		t.Fatal("private canonical home refused")
	}
	file := filepath.Join(root, "file")
	if err := os.WriteFile(file, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(root, "link")
	if err := os.Symlink(root, link); err != nil {
		t.Fatal(err)
	}
	broad := filepath.Join(root, "broad")
	if err := os.Mkdir(broad, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(broad, 0o777); err != nil {
		t.Fatal(err)
	}
	child := filepath.Join(broad, "child")
	if err := os.Mkdir(child, 0o700); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"", "/", "relative", root + "/../home", root + "\n", filepath.Join(root, "missing"), file, link, broad, child, filepath.Join(link, "missing")} {
		if TrustedHome(path) {
			t.Fatalf("unsafe home accepted: %q", path)
		}
	}
}

func TestRunMissingHomeFailsBeforeCredentialAccess(t *testing.T) {
	root := t.TempDir()
	gh := filepath.Join(root, "gh")
	if err := os.WriteFile(gh, []byte("fixture"), 0o700); err != nil {
		t.Fatal(err)
	}
	environment := map[string]string{"SF_GIT_HTTPS_REPOSITORY": "owner/repo", "SF_GIT_GH_BINARY": gh, "SF_GIT_GH_BINARY_DIGEST": fileDigest(gh), "SF_GIT_GH_CONFIG_DIR": root}
	for _, home := range []string{"", "/", "/missing/home"} {
		environment["SF_GIT_GH_HOME"] = home
		runner := &recordingRunner{}
		var output bytes.Buffer
		err := Run(context.Background(), []string{"get"}, strings.NewReader("protocol=https\nhost=github.com\npath=owner/repo.git\n\n"), &output, func(key string) (string, bool) { value, ok := environment[key]; return value, ok }, runner)
		if err == nil || runner.runs != 0 || output.Len() != 0 {
			t.Fatal("unsafe home reached gh")
		}
	}
}

func TestResponseCheck(t *testing.T) {
	for _, test := range []struct {
		response string
		valid    bool
	}{
		{"username=fixture\npassword=fake-value\n\n", true},
		{"username=fixture\r\npassword=fake-value\r\n\r\n", true},
		{"protocol=https\nhost=github.com\nusername=fixture\npassword=fake-value\n\n", true},
		{"protocol=http\nhost=github.com\nusername=fixture\npassword=fake-value\n\n", false},
		{"protocol=https\nhost=example.test\nusername=fixture\npassword=fake-value\n\n", false},
		{"protocol=https\nprotocol=https\nhost=github.com\nusername=fixture\npassword=fake-value\n\n", false},
		{"protocol=https\nhost=github.com\nhost=github.com\nusername=fixture\npassword=fake-value\n\n", false},
		{"protocol=https\nusername=fixture\npassword=fake-value\n\n", false},
		{"host=github.com\nusername=fixture\npassword=fake-value\n\n", false},
		{"username=fixture\n", false}, {"password=fake-value\n", false},
		{"username=fixture\npassword=\n", false},
		{"username=fixture\npassword=fake-value\npassword=duplicate\n", false},
		{"username=fixture\npassword=fake-value\nother=value\n", false},
		{strings.Repeat("x", maxCredentialBytes+1), false},
	} {
		check := NewResponseCheck()
		_, _ = io.WriteString(check, test.response)
		if check.Valid() != test.valid {
			t.Fatal("incorrect credential response verdict")
		}
		backing := check.data
		check.Clear()
		if check.Valid() {
			t.Fatal("cleared response remains valid")
		}
		for _, value := range backing {
			if value != 0 {
				t.Fatal("response was not erased")
			}
		}
	}
}

func TestRunSimulatedKeychainRequiresAuthenticatedHome(t *testing.T) {
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	gh := filepath.Join(root, "gh")
	// This hermetic stand-in refuses the old /var/empty behavior, without
	// accessing Keychain, authentication files, or the network.
	script := "#!/bin/sh\n[ \"$HOME\" = \"$GH_CONFIG_DIR\" ] || exit 1\n[ \"$1 $2 $3\" = \"auth git-credential get\" ] || exit 1\n[ -z \"$GH_TOKEN$GITHUB_TOKEN$SSH_AUTH_SOCK\" ] || exit 1\nprintf 'username=fixture\\npassword=fake-value\\n\\n'\n"
	if err := os.WriteFile(gh, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("GH_TOKEN", "ambient-fixture")
	t.Setenv("GITHUB_TOKEN", "ambient-fixture")
	t.Setenv("SSH_AUTH_SOCK", "/ambient/socket")
	environment := map[string]string{"SF_GIT_HTTPS_REPOSITORY": "owner/repo", "SF_GIT_GH_BINARY": gh, "SF_GIT_GH_BINARY_DIGEST": fileDigest(gh), "SF_GIT_GH_CONFIG_DIR": root, "SF_GIT_GH_HOME": root}
	check := NewResponseCheck()
	defer check.Clear()
	err = Run(context.Background(), []string{"get"}, strings.NewReader("protocol=https\nhost=github.com\npath=owner/repo.git\n\n"), check, func(key string) (string, bool) { value, ok := environment[key]; return value, ok }, OSRunner{})
	if err != nil || !check.Valid() {
		t.Fatal("authenticated home was not supplied to the isolated gh child")
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
	environment := map[string]string{"SF_GIT_HTTPS_REPOSITORY": "nysa-company/nysa-app", "SF_GIT_GH_BINARY": gh, "SF_GIT_GH_BINARY_DIGEST": fileDigest(gh), "SF_GIT_GH_CONFIG_DIR": config}
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

func fileDigest(path string) string {
	data, _ := os.ReadFile(path)
	sum := sha256.Sum256(data)
	return "sha256:" + hex.EncodeToString(sum[:])
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

func TestRunRefusesUnboundGHSnapshot(t *testing.T) {
	root := t.TempDir()
	gh := filepath.Join(root, "gh")
	if err := os.WriteFile(gh, []byte("fixture"), 0o700); err != nil {
		t.Fatal(err)
	}
	config := filepath.Join(root, "config")
	if err := os.Mkdir(config, 0o700); err != nil {
		t.Fatal(err)
	}
	request := "protocol=https\nhost=github.com\npath=owner/repo.git\n\n"
	for _, digest := range []string{"", "sha256:" + strings.Repeat("0", 64)} {
		environment := map[string]string{"SF_GIT_HTTPS_REPOSITORY": "owner/repo", "SF_GIT_GH_BINARY": gh, "SF_GIT_GH_BINARY_DIGEST": digest, "SF_GIT_GH_CONFIG_DIR": config}
		lookup := func(key string) (string, bool) { value, ok := environment[key]; return value, ok }
		runner := &recordingRunner{}
		var output bytes.Buffer
		if err := Run(context.Background(), []string{"get"}, strings.NewReader(request), &output, lookup, runner); err == nil || runner.runs != 0 {
			t.Fatalf("unbound gh snapshot accepted: digest=%q runs=%d err=%v", digest, runner.runs, err)
		}
	}
}
