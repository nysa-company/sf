// Package gitcredential implements the narrow GitHub HTTPS credential bridge
// used by sf-owned Git commands. It never asks gh for a displayed token and it
// never stores credentials: one canonical Git credential `get` request is
// delegated to `gh auth git-credential` over inherited stdin/stdout pipes.
package gitcredential

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
)

const maxCredentialBytes = 16 << 10

var ErrRefused = errors.New("GitHub credential request refused")

type LookupEnv func(string) (string, bool)

type Runner interface {
	Run(context.Context, string, []string, []string, io.Reader, io.Writer) error
}

type OSRunner struct{}

func (OSRunner) Run(ctx context.Context, path string, args, env []string, stdin io.Reader, stdout io.Writer) error {
	command := exec.CommandContext(ctx, path, args...)
	command.Env = env
	command.Stdin = stdin
	command.Stdout = stdout
	// gh errors can contain paths or account metadata. The parent Git process
	// needs only a failing helper exit; no helper diagnostic is authoritative.
	command.Stderr = io.Discard
	return command.Run()
}

// Run validates one invocation made by Git's credential-helper protocol.
// `store` and `erase` are accepted as no-ops so Git never sends a credential
// back into gh or an sf-owned persistence surface.
func Run(ctx context.Context, args []string, input io.Reader, output io.Writer, lookup LookupEnv, runner Runner) error {
	if len(args) != 1 || (args[0] != "get" && args[0] != "store" && args[0] != "erase") || input == nil || output == nil || lookup == nil || runner == nil {
		return ErrRefused
	}
	raw, err := io.ReadAll(io.LimitReader(input, maxCredentialBytes+1))
	if err != nil || len(raw) > maxCredentialBytes {
		return ErrRefused
	}
	request, err := parseRequest(raw)
	if err != nil {
		return ErrRefused
	}
	repository, ok := lookup("SF_GIT_HTTPS_REPOSITORY")
	if !ok || !validRepository(repository) || request["protocol"] != "https" || request["host"] != "github.com" || request["path"] != repository+".git" {
		return ErrRefused
	}
	if args[0] != "get" {
		return nil
	}
	if request["password"] != "" || request["url"] != "" {
		return ErrRefused
	}
	gh, ok := lookup("SF_GIT_GH_BINARY")
	if !ok || !trustedExecutable(gh) {
		return ErrRefused
	}
	configDir, ok := lookup("SF_GIT_GH_CONFIG_DIR")
	if !ok || !trustedDirectory(configDir) {
		return ErrRefused
	}
	canonical := []byte("protocol=https\nhost=github.com\npath=" + repository + ".git\n\n")
	bounded := &boundedWriter{destination: output, remaining: maxCredentialBytes}
	if err := runner.Run(ctx, gh, []string{"auth", "git-credential", "get"}, []string{"HOME=/var/empty", "LANG=C", "LC_ALL=C", "GH_CONFIG_DIR=" + configDir}, bytes.NewReader(canonical), bounded); err != nil || bounded.exceeded {
		return ErrRefused
	}
	return nil
}

func parseRequest(raw []byte) (map[string]string, error) {
	values := map[string]string{}
	text := strings.ReplaceAll(string(raw), "\r\n", "\n")
	for _, line := range strings.Split(text, "\n") {
		if line == "" {
			continue
		}
		key, value, ok := strings.Cut(line, "=")
		if !ok || value == "" || strings.ContainsAny(key+value, "\x00\r\n") {
			return nil, ErrRefused
		}
		switch key {
		case "protocol", "host", "path", "username", "password", "url":
		default:
			return nil, ErrRefused
		}
		if _, duplicate := values[key]; duplicate {
			return nil, ErrRefused
		}
		values[key] = value
	}
	if values["protocol"] == "" || values["host"] == "" || values["path"] == "" {
		return nil, ErrRefused
	}
	return values, nil
}

func validRepository(value string) bool {
	parts := strings.Split(value, "/")
	if len(parts) != 2 {
		return false
	}
	for _, part := range parts {
		if part == "" || len(part) > 100 {
			return false
		}
		for _, character := range part {
			if !(character >= 'a' && character <= 'z' || character >= 'A' && character <= 'Z' || character >= '0' && character <= '9' || character == '.' || character == '_' || character == '-') {
				return false
			}
		}
	}
	return true
}

func trustedExecutable(path string) bool {
	if !cleanAbsolute(path) {
		return false
	}
	info, err := os.Lstat(path)
	if err != nil || info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() || info.Mode().Perm()&0o022 != 0 || !trustedOwner(info) {
		return false
	}
	return true
}

func trustedDirectory(path string) bool {
	if !cleanAbsolute(path) {
		return false
	}
	info, err := os.Lstat(path)
	return err == nil && info.Mode()&os.ModeSymlink == 0 && info.IsDir() && info.Mode().Perm()&0o022 == 0 && trustedOwner(info)
}

func cleanAbsolute(path string) bool {
	return filepath.IsAbs(path) && filepath.Clean(path) == path && !strings.ContainsAny(path, "\x00\r\n")
}

func trustedOwner(info os.FileInfo) bool {
	stat, ok := info.Sys().(*syscall.Stat_t)
	return ok && (stat.Uid == 0 || stat.Uid == uint32(os.Getuid()))
}

type boundedWriter struct {
	destination io.Writer
	remaining   int
	exceeded    bool
}

func (writer *boundedWriter) Write(value []byte) (int, error) {
	if len(value) > writer.remaining {
		writer.exceeded = true
		return 0, fmt.Errorf("credential response exceeded bound")
	}
	written, err := writer.destination.Write(value)
	writer.remaining -= written
	return written, err
}
