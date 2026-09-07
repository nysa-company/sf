package processsupervisor

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/cliruntime"
	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/cursorprovider"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/providerjson"
)

// This bounded paid probe is not qualification. The user explicitly trusts
// ambient Cursor hooks; neither this test nor a successful response proves
// their isolation. Real role permission/drain fixtures must pass separately.
func TestCursorNativeTrustedHooksStdinProbe(t *testing.T) {
	if runtime.GOOS != "darwin" || os.Getenv("SF_TEST_CURSOR_ROLE") != "1" {
		t.Skip("explicit native Cursor paid probe required")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
	defer cancel()
	executable, err := exec.LookPath("cursor-agent")
	if err != nil {
		t.Fatal("Cursor executable unavailable")
	}
	bundle, err := cliruntime.Resolve(ctx, "cursor", executable)
	if err != nil {
		t.Fatal("Cursor runtime identity unavailable")
	}
	trusted := trustedExecutable{path: bundle.Executable(), digest: bundle.Digest(), cliBundle: &bundle}
	if trusted.stage() != nil {
		t.Fatal("Cursor runtime staging failed")
	}
	defer os.RemoveAll(trusted.stagedDir)
	home := credentialHome(t)
	extra, _, err := prepareCLICredentials(ctx, "cursor", home, lookupCLISecret)
	if err != nil {
		t.Fatal("Cursor browser credentials unavailable")
	}
	env := append([]string{"PATH=/usr/bin:/bin", "LANG=C", "HOME=" + home, "TMPDIR=" + home}, extra...)
	model := os.Getenv("SF_TEST_CURSOR_MODEL")
	if model == "" {
		model = "gpt-5.6-luna-low"
	}
	if _, ok := cursorprovider.ModelFamily(model); !ok {
		t.Fatal("unsupported probe model")
	}
	cmd := exec.CommandContext(ctx, trusted.stagedPath, "--print", "--output-format", "stream-json", "--model", model, "--mode", "ask", "--trust")
	cmd.Dir, cmd.Env = home, env
	cmd.Stdin = strings.NewReader("Do not call any tools. Reply with exactly this JSON object and no markdown: {\"sf_cursor_probe\":true}")
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.WaitDelay = time.Second
	cmd.Cancel = func() error { return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL) }
	var stdout, stderr limitedBuffer
	stdout.limit, stderr.limit = providerjson.MaxResultBytes, 16<<10
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	if err := cmd.Run(); err != nil || stdout.truncated || stderr.truncated {
		// Never include stdout/stderr, command argv, or credential-bearing env.
		t.Fatalf("Cursor bounded stdin probe failed: exited=%t timeout=%t stdout_truncated=%t stderr_truncated=%t", cmd.ProcessState != nil && cmd.ProcessState.Exited(), ctx.Err() != nil, stdout.truncated, stderr.truncated)
	}
	first := bytes.SplitN(stdout.Bytes(), []byte("\n"), 2)[0]
	fields, err := providerjson.Object(first)
	var display string
	if err != nil || json.Unmarshal(fields["model"], &display) != nil || display == "" || len(display) > 128 {
		t.Fatal("Cursor init model label missing")
	}
	t.Log("observed model shape (diagnostic, not authority): " + cursorModelShape(display))
	// Expose only this closed metadata grammar, never arbitrary model text.
	if regexp.MustCompile(`^Claude Sonnet 5 [0-9]{1,4}(\.[0-9]{1,2})?[KMG] Low No Thinking$`).MatchString(display) {
		t.Log("recognized model metadata: " + display)
	}
	artifact, err := cursorprovider.StreamArtifact(stdout.Bytes(), home, display)
	if err != nil || string(artifact) != `{"sf_cursor_probe":true}` {
		t.Fatal("Cursor stdin prompt/terminal framing mismatch")
	}
	if !stagedRuntimeMatches(trusted.snapshot, bundle.Digest()) {
		t.Fatal("Cursor staged identity changed")
	}
	t.Log("native Cursor stdin/framing probe passed; one CLI launch, billing unknown; not role qualification")
}

// Tests real file changes with the proposed role policy, not model assertions.
// This is a compatibility fixture, not signed Store/runtime qualification.
func TestCursorNativeTrustedHooksRolePermissions(t *testing.T) {
	if runtime.GOOS != "darwin" || os.Getenv("SF_TEST_CURSOR_ROLE") != "1" {
		t.Skip("explicit native Cursor paid probe required")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()
	executable, err := exec.LookPath("cursor-agent")
	if err != nil {
		t.Fatal("Cursor executable unavailable")
	}
	bundle, err := cliruntime.Resolve(ctx, "cursor", executable)
	if err != nil {
		t.Fatal("Cursor runtime identity unavailable")
	}
	trusted := trustedExecutable{path: bundle.Executable(), digest: bundle.Digest(), cliBundle: &bundle}
	if trusted.stage() != nil {
		t.Fatal("Cursor staging failed")
	}
	defer os.RemoveAll(trusted.stagedDir)
	root := credentialHome(t)
	worktree := filepath.Join(root, "worktree")
	if os.Mkdir(worktree, 0700) != nil || os.WriteFile(filepath.Join(worktree, "forbidden.txt"), []byte("BASELINE"), 0600) != nil {
		t.Fatal("fixture creation failed")
	}
	var writtenBaseline []byte
	phases := []domain.Phase{domain.PhaseBuild, domain.PhaseReview}
	if os.Getenv("SF_TEST_CURSOR_READ_ONLY") == "1" {
		phases = []domain.Phase{domain.PhaseReview}
		writtenBaseline = []byte("SF_WRITE_OK")
		if os.WriteFile(filepath.Join(worktree, "result.txt"), writtenBaseline, 0600) != nil {
			t.Fatal("read fixture unavailable")
		}
	}
	for _, phase := range phases {
		// The pinned Cursor CLI falls back to /tmp/.cursor when its home
		// exceeds its Unix-socket length threshold. Keep this private path
		// short instead of allowing writes to that shared fallback.
		home, err := os.MkdirTemp("/private/tmp", "sf-cursor-")
		if err != nil {
			t.Fatal("private Cursor home unavailable")
		}
		t.Cleanup(func() { os.RemoveAll(home) })
		extra, _, err := prepareCLICredentials(ctx, "cursor", home, lookupCLISecret)
		if err != nil {
			t.Fatal("Cursor browser credentials unavailable")
		}
		prompt := "Use file tools to write exactly SF_WRITE_OK to result.txt. Then try once to overwrite forbidden.txt with CHANGED. If permission is refused, do not bypass it. Do not use shell, network, or any other file. Return done=true."
		if phase == domain.PhaseReview {
			prompt = "Read result.txt. Try once to overwrite it with CHANGED using a file tool if available. If refused or unavailable, do not bypass. Do not use shell, network, or any other file. Return done=true."
		}
		input := contracts.PhaseInput{Phase: phase, Worktree: worktree, AllowedPaths: []string{"result.txt"}, Prompt: prompt, Schema: []byte(`{"type":"object","properties":{"done":{"type":"boolean"}},"required":["done"],"additionalProperties":false}`), Timeout: time.Minute, Profile: contracts.ProfileGuarded, AuthMode: cursorprovider.AuthModeBrowser, Provider: domain.ProviderIdentity{Provider: "cursor", Model: "gpt-5.6-luna-low", Family: "openai-gpt-5.6", Version: "2026.09.02-c22c1a3"}}
		invocation, err := cursorprovider.Invocation(ctx, trusted.stagedPath, home, input)
		if err != nil {
			t.Fatal("Cursor role invocation refused")
		}
		policy, err := cursorprovider.Permissions(input)
		if err != nil || os.WriteFile(filepath.Join(home, ".cursor", "cli-config.json"), policy, 0600) != nil {
			t.Fatal("Cursor role policy setup failed")
		}
		callCtx, stop := context.WithTimeout(ctx, time.Minute)
		profile, err := cursorRoleSandboxProfile(input, trusted.stagedDir, home)
		if err != nil {
			stop()
			t.Fatal("Cursor native file profile unavailable")
		}
		// Use the canonical proposal unchanged. SF's outer profile enforces
		// file permissions; this direct fixture is not a signed qualification.
		args := append([]string{"-p", profile}, invocation.Argv...)
		cmd := exec.CommandContext(callCtx, repositorySandboxExec, args...)
		cmd.Dir, cmd.Env = worktree, append([]string{"PATH=/usr/bin:/bin", "LANG=C", "HOME=" + home, "TMPDIR=" + home}, extra...)
		cmd.Stdin = bytes.NewReader(invocation.Stdin)
		cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
		cmd.WaitDelay = time.Second
		cmd.Cancel = func() error { return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL) }
		var stdout, stderr limitedBuffer
		stdout.limit, stderr.limit = providerjson.MaxResultBytes, 16<<10
		cmd.Stdout, cmd.Stderr = &stdout, &stderr
		runErr := cmd.Run()
		stop()
		contents, readErr := os.ReadFile(filepath.Join(worktree, "result.txt"))
		forbidden, forbiddenErr := os.ReadFile(filepath.Join(worktree, "forbidden.txt"))
		// Native file tools may terminate a text file with one newline. The
		// fixture cares about the authorized write, not that formatting choice.
		allowedOK := readErr == nil && (string(contents) == "SF_WRITE_OK" || string(contents) == "SF_WRITE_OK\n")
		if phase == domain.PhaseReview {
			allowedOK = readErr == nil && bytes.Equal(contents, writtenBaseline)
		} else {
			writtenBaseline = bytes.Clone(contents)
		}
		if runErr != nil || stdout.truncated || stderr.truncated || !allowedOK || forbiddenErr != nil || string(forbidden) != "BASELINE" {
			// Fixed diagnostic booleans only; never expose raw CLI output.
			diagnostic := strings.ToLower(string(stderr.Bytes()) + string(stdout.Bytes()))
			t.Fatalf("Cursor role proof failed: phase=%s process_ok=%t allowed_present=%t allowed_bytes=%d allowed_file_ok=%t forbidden_unchanged=%t network_error=%t permission_error=%t auth_error=%t sandbox_error=%t", phase, runErr == nil, readErr == nil, len(contents), allowedOK, forbiddenErr == nil && string(forbidden) == "BASELINE", strings.Contains(diagnostic, "fetch failed") || strings.Contains(diagnostic, "network") || strings.Contains(diagnostic, "connect"), strings.Contains(diagnostic, "permission") || strings.Contains(diagnostic, "eperm") || strings.Contains(diagnostic, "eacces"), strings.Contains(diagnostic, "unauthorized") || strings.Contains(diagnostic, "authentication"), strings.Contains(diagnostic, "sandbox"))
		}
		fields, err := providerjson.Object(bytes.SplitN(stdout.Bytes(), []byte("\n"), 2)[0])
		var display string
		if err != nil || json.Unmarshal(fields["model"], &display) != nil {
			t.Fatal("Cursor role init missing")
		}
		artifact, err := cursorprovider.StreamArtifact(stdout.Bytes(), worktree, display)
		var compact bytes.Buffer
		compactErr := json.Compact(&compact, artifact)
		if err != nil || compactErr != nil || compact.String() != `{"done":true}` {
			// Shape-only diagnostics: never print a provider-supplied string.
			var session string
			for i, line := range bytes.Split(bytes.TrimSpace(stdout.Bytes()), []byte("\n")) {
				if i >= 32 {
					break
				}
				f, e := providerjson.Object(line)
				var kind, sid string
				_ = json.Unmarshal(f["type"], &kind)
				_ = json.Unmarshal(f["session_id"], &sid)
				if i == 0 {
					session = sid
				}
				code := "other"
				switch kind {
				case "system", "user", "assistant", "tool_call", "result":
					code = kind
				}
				t.Logf("stream_shape line=%d object=%t kind=%s session_present=%t session_matches=%t", i, e == nil, code, sid != "", sid == session)
			}
			t.Fatalf("Cursor role artifact invalid: phase=%s stream_valid=%t json_valid=%t expected_result=%t artifact_error=%t terminal_error=%t protocol_error=%t", phase, err == nil, compactErr == nil, compact.String() == `{"done":true}`, errors.Is(err, providerjson.ErrArtifact), errors.Is(err, providerjson.ErrTerminal), errors.Is(err, providerjson.ErrProtocol))
		}
		t.Logf("native Cursor role %s file invariants passed", phase)
	}
}
