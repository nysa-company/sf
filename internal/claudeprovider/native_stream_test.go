//go:build darwin

package claudeprovider

import (
	"bytes"
	"context"
	"encoding/json"
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

// Local protocol evidence only: synthetic API credentials, a disposable home,
// and OS-enforced localhost-only transport. No subscription qualification or
// model quality is asserted by a canned API response.
func TestInstalledClaudeLocalSuccessStream(t *testing.T) {
	if os.Getenv("SF_TEST_CLAUDE_LOCAL_RETRY") != "1" {
		t.Skip("explicit installed-CLI local-only protocol test")
	}
	t.Run("success", func(t *testing.T) { nativeSuccessStream(t, false) })
	t.Run("internal-retry", func(t *testing.T) { nativeSuccessStream(t, true) })
}

func nativeSuccessStream(t *testing.T, rejectFirst bool) {
	t.Helper()
	cli, err := exec.LookPath("claude")
	if err != nil {
		t.Fatal("installed Claude CLI missing")
	}
	home := t.TempDir()
	env := []string{"PATH=/usr/bin:/bin", "HOME=" + home, "TMPDIR=" + home, "CLAUDE_CONFIG_DIR=" + home, "LANG=en_US.UTF-8", "ANTHROPIC_API_KEY=sf-synthetic-not-a-credential", "CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC=1", "CLAUDE_CODE_MAX_RETRIES=1"}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	version := exec.CommandContext(ctx, cli, "--version")
	version.Env, version.Dir = env, home
	output, err := version.Output()
	if err != nil || string(bytes.TrimSpace(output)) != "2.1.263 (Claude Code)" {
		t.Fatal("installed CLI version mismatch")
	}
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/messages" || requests.Add(1) > 4 {
			w.WriteHeader(400)
			return
		}
		raw, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 1<<20))
		if err != nil {
			w.WriteHeader(413)
			return
		}
		if rejectFirst && requests.Load() == 1 {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("Retry-After", "1")
			w.WriteHeader(503)
			_, _ = io.WriteString(w, `{"type":"error","error":{"type":"api_error","message":"synthetic local rejection"}}`)
			return
		}
		var request struct {
			Model  string `json:"model"`
			Stream bool   `json:"stream"`
			Tools  []struct {
				Name string `json:"name"`
			} `json:"tools"`
		}
		if json.Unmarshal(raw, &request) != nil {
			w.WriteHeader(400)
			return
		}
		block := map[string]any{"type": "text", "text": `{"done":true}`}
		stop := "end_turn"
		for _, tool := range request.Tools {
			if tool.Name == "StructuredOutput" {
				block = map[string]any{"type": "tool_use", "id": "toolu_fixture", "name": "StructuredOutput", "input": map[string]bool{"done": true}}
				stop = "tool_use"
				break
			}
		}
		message := map[string]any{"id": "msg_fixture", "type": "message", "role": "assistant", "model": request.Model, "content": []any{block}, "stop_reason": stop, "stop_sequence": nil, "usage": map[string]int{"input_tokens": 1, "output_tokens": 1}}
		if !request.Stream {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(message)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		emit := func(kind string, value any) {
			payload, _ := json.Marshal(value)
			_, _ = fmt.Fprintf(w, "event: %s\ndata: %s\n\n", kind, payload)
		}
		message["content"], message["stop_reason"] = []any{}, nil
		emit("message_start", map[string]any{"type": "message_start", "message": message})
		start := map[string]any{"type": "text", "text": ""}
		delta := map[string]any{"type": "text_delta", "text": `{"done":true}`}
		if stop == "tool_use" {
			start = map[string]any{"type": "tool_use", "id": "toolu_fixture", "name": "StructuredOutput", "input": map[string]any{}}
			delta = map[string]any{"type": "input_json_delta", "partial_json": `{"done":true}`}
		}
		emit("content_block_start", map[string]any{"type": "content_block_start", "index": 0, "content_block": start})
		emit("content_block_delta", map[string]any{"type": "content_block_delta", "index": 0, "delta": delta})
		emit("content_block_stop", map[string]any{"type": "content_block_stop", "index": 0})
		emit("message_delta", map[string]any{"type": "message_delta", "delta": map[string]any{"stop_reason": stop, "stop_sequence": nil}, "usage": map[string]int{"output_tokens": 1}})
		emit("message_stop", map[string]any{"type": "message_stop"})
	}))
	defer server.Close()
	port := server.Listener.Addr().(*net.TCPAddr).Port
	profile := fmt.Sprintf(`(version 1)(allow default)(deny network*)(allow network-outbound (remote ip "localhost:%d"))`, port)
	var forbiddenCalls atomic.Int32
	forbidden := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { forbiddenCalls.Add(1); w.WriteHeader(200) }))
	defer forbidden.Close()
	probe := exec.CommandContext(ctx, "/usr/bin/sandbox-exec", "-p", profile, "/usr/bin/curl", "--silent", "--max-time", "2", forbidden.URL)
	probe.Env, probe.Dir = env, home
	if err := probe.Run(); err == nil || forbiddenCalls.Load() != 0 {
		t.Fatal("local-only policy accepted an unlisted endpoint")
	}
	command := exec.CommandContext(ctx, "/usr/bin/sandbox-exec", "-p", profile, cli, "--bare", "--restricted", "--safe-mode", "--print", "--output-format", "stream-json", "--verbose", "--model", "claude-sonnet-5", "--tools", "", "--permission-mode", "dontAsk", "--no-session-persistence", "--strict-mcp-config", "--mcp-config", `{"mcpServers":{}}`, "--json-schema", `{"type":"object","properties":{"done":{"type":"boolean"}},"required":["done"],"additionalProperties":false}`, "--max-turns", "3")
	command.Env, command.Dir = append(env, "ANTHROPIC_BASE_URL="+server.URL), home
	command.Stdin = strings.NewReader("Return done=true. No tools except structured output.\n")
	command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	command.Cancel = func() error { return syscall.Kill(-command.Process.Pid, syscall.SIGKILL) }
	command.WaitDelay = 2 * time.Second
	var stdout, stderr nativeRetryBuffer
	command.Stdout, command.Stderr = &stdout, &stderr
	err = command.Run()
	if err != nil || ctx.Err() != nil || stdout.truncated || stderr.truncated {
		t.Fatalf("local success fixture failed: requests=%d exit_success=%v bounded=%v", requests.Load(), err == nil, ctx.Err() == nil)
	}
	var inits, assistants, users, terminals, unknown int
	var terminal []byte
	for _, line := range bytes.Split(bytes.TrimSpace(stdout.Bytes()), []byte{'\n'}) {
		fields, err := providerjson.Object(line)
		if err != nil {
			t.Fatal("invalid native event object")
		}
		switch rejectionString(fields, "type") {
		case "system":
			if rejectionString(fields, "subtype") == "init" {
				inits++
			} else if rejectionString(fields, "subtype") != "api_retry" {
				unknown++
			}
		case "assistant":
			assistants++
		case "user":
			users++
		case "result":
			terminals++
			terminal = line
		default:
			unknown++
		}
	}
	artifact, err := providerjson.Artifact(terminal, true)
	if err != nil || !bytes.Equal(artifact, []byte(`{"done":true}`)) || inits != 1 || terminals != 1 || unknown != 0 {
		t.Fatalf("native stream framing failed: init=%d assistant=%d user=%d result=%d unknown=%d artifact_ok=%v", inits, assistants, users, terminals, unknown, err == nil)
	}
	input := claudeInput()
	input.Provider.Model, input.Provider.Version = "claude-sonnet-5", "2.1.263"
	extracted, err := TerminalStreamResult(ctx, input, contracts.CommandResult{ExitCode: 0, Stdout: stdout.Bytes(), Stderr: stderr.Bytes()})
	if err != nil || !bytes.Equal(extracted, terminal) {
		t.Fatal("native complete stream did not authenticate")
	}
}
