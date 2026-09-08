//go:build darwin

package claudeprovider

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/providerjson"
)

// This optional protocol test makes no paid request. Bare mode intentionally
// uses synthetic API auth and never reads subscription/keychain credentials.
// It is not SF's qualified subscription invocation or authority for a relaunch.
func TestInstalledClaudeLocalRetryProtocol(t *testing.T) {
	if os.Getenv("SF_TEST_CLAUDE_LOCAL_RETRY") != "1" {
		t.Skip("explicit installed-CLI local-only protocol test")
	}
	t.Run("unknown_terminal", func(t *testing.T) { nativeRejectionProtocol(t, 400) })
	t.Run("server_exhaustion", func(t *testing.T) { nativeRejectionProtocol(t, 503) })
}

func nativeRejectionProtocol(t *testing.T, terminalStatus int) {
	t.Helper()
	cli, err := exec.LookPath("claude")
	if err != nil {
		t.Fatal("installed Claude CLI missing")
	}
	home := t.TempDir()
	env := []string{"PATH=/usr/bin:/bin", "HOME=" + home, "TMPDIR=" + home,
		"CLAUDE_CONFIG_DIR=" + home, "LANG=en_US.UTF-8",
		"ANTHROPIC_API_KEY=sf-synthetic-not-a-credential",
		"CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC=1"}
	if terminalStatus == 503 {
		env = append(env, "CLAUDE_CODE_MAX_RETRIES=1")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	version := exec.CommandContext(ctx, cli, "--version")
	version.Env, version.Dir = env, home
	output, err := version.Output()
	if err != nil || string(bytes.TrimSpace(output)) != "2.1.263 (Claude Code)" {
		t.Fatal("installed CLI does not match the protocol fixture version")
	}
	var requests atomic.Int32
	var messageRequests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		count := requests.Add(1)
		if r.Method != http.MethodPost || count > 8 {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if _, err := io.Copy(io.Discard, http.MaxBytesReader(w, r.Body, 1<<20)); err != nil {
			w.WriteHeader(http.StatusRequestEntityTooLarge)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Retry-After", "1")
		status, kind := http.StatusBadRequest, "invalid_request_error"
		if r.URL.Path == "/v1/messages" {
			n := messageRequests.Add(1)
			if terminalStatus == 503 {
				status, kind = http.StatusServiceUnavailable, "api_error"
			} else if n == 1 {
				status, kind = http.StatusTooManyRequests, "rate_limit_error"
			}
		}
		w.WriteHeader(status)
		// No reflection of headers, payloads or account state into diagnostics.
		_, _ = fmt.Fprintf(w, `{"type":"error","error":{"type":%q,"message":"synthetic local fixture rejection"}}`, kind)
	}))
	defer server.Close()
	port := server.Listener.Addr().(*net.TCPAddr).Port
	profile := fmt.Sprintf(`(version 1)(allow default)(deny network*)(allow network-outbound (remote ip "localhost:%d"))`, port)
	// Prove the allowlist isn't an accidental allow-all or ignored profile.
	var forbiddenRequests atomic.Int32
	forbidden := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		forbiddenRequests.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer forbidden.Close()
	probe := exec.CommandContext(ctx, "/usr/bin/sandbox-exec", "-p", profile,
		"/usr/bin/curl", "--silent", "--max-time", "2", forbidden.URL)
	probe.Env, probe.Dir = env, home
	if err := probe.Run(); err == nil || forbiddenRequests.Load() != 0 {
		t.Fatal("network policy did not refuse the unlisted endpoint")
	}
	command := exec.CommandContext(ctx, "/usr/bin/sandbox-exec", "-p", profile, cli,
		"--bare", "--restricted", "--safe-mode", "--print", "--output-format", "stream-json",
		"--verbose", "--model", "claude-sonnet-5", "--tools", "", "--permission-mode", "dontAsk",
		"--no-session-persistence", "--strict-mcp-config", "--mcp-config", `{"mcpServers":{}}`)
	command.Env, command.Dir = append(env, "ANTHROPIC_BASE_URL="+server.URL), home
	command.Stdin = strings.NewReader("Reply OK. No tools.\n")
	command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	command.Cancel = func() error { return syscall.Kill(-command.Process.Pid, syscall.SIGKILL) }
	command.WaitDelay = 2 * time.Second
	var stdout, stderr nativeRetryBuffer
	command.Stdout, command.Stderr = &stdout, &stderr
	err = command.Run()
	if ctx.Err() != nil || err == nil || command.ProcessState == nil || command.ProcessState.ExitCode() != 1 ||
		stdout.truncated || stderr.truncated || requests.Load() > 8 || messageRequests.Load() != 2 {
		t.Fatalf("local rejection did not finish within request/output/process bounds: requests=%d messages=%d", requests.Load(), messageRequests.Load())
	}
	var session string
	var retries, terminals, inits int
	lines := bytes.Split(bytes.TrimSpace(stdout.Bytes()), []byte{'\n'})
	for i, line := range lines {
		fields, err := providerjson.Object(line)
		if err != nil {
			t.Fatal("native stream contains an invalid event")
		}
		var kind, subtype, current string
		_ = json.Unmarshal(fields["type"], &kind)
		_ = json.Unmarshal(fields["subtype"], &subtype)
		_ = json.Unmarshal(fields["session_id"], &current)
		if kind == "system" && subtype == "init" {
			inits++
			var model string
			_ = json.Unmarshal(fields["model"], &model)
			if i != 0 || current == "" || model != "claude-sonnet-5" {
				t.Fatal("native initial identity mismatch")
			}
			session = current
		}
		if session == "" || current != session {
			t.Fatal("native event session mismatch")
		}
		if kind == "system" && subtype == "api_retry" {
			retries++
			var status, attempt, maximum, delay int
			_ = json.Unmarshal(fields["error_status"], &status)
			_ = json.Unmarshal(fields["attempt"], &attempt)
			_ = json.Unmarshal(fields["max_retries"], &maximum)
			_ = json.Unmarshal(fields["retry_delay_ms"], &delay)
			wantStatus, wantMaximum := 429, 10
			if terminalStatus == 503 {
				wantStatus, wantMaximum = 503, 1
			}
			if status != wantStatus || attempt != 1 || maximum != wantMaximum || delay != 1000 {
				t.Fatal("native retry framing changed")
			}
		}
		if kind == "result" {
			terminals++
			var isError bool
			_ = json.Unmarshal(fields["is_error"], &isError)
			if i != len(lines)-1 || !isError {
				t.Fatal("native rejection lacks a final error result")
			}
			result, parseErr := providerjson.Command(ctx, claudeInput(), contracts.CommandResult{ExitCode: 1, Stdout: line}, true)
			if parseErr == nil || result.Outcome != contracts.PhaseResultIndeterminate || len(result.Artifact) != 0 {
				t.Fatal("native rejection acquired completion or artifact-repair authority")
			}
		}
	}
	if inits != 1 || retries != 1 || terminals != 1 {
		t.Fatalf("native rejection stream cardinality changed: init=%d retry=%d terminal=%d", inits, retries, terminals)
	}
	input := claudeInput()
	input.Provider.Model, input.Provider.Version = "claude-sonnet-5", "2.1.263"
	observed, err := ObserveRejection(ctx, input, contracts.CommandResult{ExitCode: 1, Stdout: stdout.Bytes(), Stderr: stderr.Bytes()})
	if terminalStatus == 400 {
		// This pinned CLI emits category unknown for this synthetic 400.
		// Do not infer invalid_request from human text or the earlier 429.
		if !errors.Is(err, ErrRejectionStream) || observed != (RejectionObservation{}) {
			t.Fatal("unknown native terminal was accepted")
		}
	} else if err != nil || observed.Category != "server_error" || observed.InternalRetries != 1 {
		t.Fatal("complete native server rejection was not recognized")
	}
}

type nativeRetryBuffer struct {
	bytes.Buffer
	truncated bool
}

func (b *nativeRetryBuffer) Write(p []byte) (int, error) {
	n := len(p)
	remaining := providerjson.MaxResultBytes - b.Len()
	if len(p) > remaining {
		p, b.truncated = p[:remaining], true
	}
	_, _ = b.Buffer.Write(p)
	return n, nil
}
