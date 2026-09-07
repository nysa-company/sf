package processsupervisor

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/claudeprovider"
	"github.com/nysa-company/sf/internal/cliruntime"
	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
)

// Real supervisor process/capture/drain with a synthetic CLI, gate helper and
// credential. The store_retry case joins real Store/Git persistence and retry;
// none of these cases exercises installed Claude or the production gate.
func TestServerRejectionCapturedFromSupervisedProcess(t *testing.T) {
	for _, scenario := range []string{"complete", "partial", "success_marker", "stderr_only", "store_retry"} {
		t.Run(scenario, func(t *testing.T) { supervisedRejectionProcess(t, scenario) })
	}
}

func supervisedRejectionProcess(t *testing.T, scenario string) {
	t.Helper()
	if runtime.GOOS != "darwin" {
		t.Skip("native Claude runtime registration")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	stream := `{"type":"system","subtype":"init","model":"claude-sonnet-5","session_id":"11111111-1111-1111-1111-111111111111","uuid":"22222222-2222-2222-2222-222222222222"}
{"type":"system","subtype":"api_retry","error":"server_error","attempt":1,"max_retries":1,"retry_delay_ms":1000,"error_status":503,"session_id":"11111111-1111-1111-1111-111111111111","uuid":"33333333-3333-3333-3333-333333333333"}
{"type":"assistant","error":"server_error","parent_tool_use_id":null,"message":{"content":[{"type":"text","text":"synthetic rejection"}]},"session_id":"11111111-1111-1111-1111-111111111111","uuid":"44444444-4444-4444-4444-444444444444"}
{"type":"result","subtype":"success","is_error":true,"session_id":"11111111-1111-1111-1111-111111111111","uuid":"55555555-5555-5555-5555-555555555555"}`
	exe := filepath.Join(root, "claude")
	redirect := ""
	switch scenario {
	case "partial":
		stream = stream[:strings.LastIndex(stream, "\n")]
	case "success_marker":
		stream = strings.ReplaceAll(stream, `"is_error":true`, `"is_error":false`)
	case "stderr_only":
		redirect = " >&2"
	}
	if err := os.WriteFile(exe, []byte("#!/bin/sh\ncat"+redirect+" <<'SF_REJECTION'\n"+stream+"\nSF_REJECTION\nexit 1\n"), 0700); err != nil {
		t.Fatal(err)
	}
	bundle, err := cliruntime.Resolve(ctx, "claude", exe)
	if err != nil {
		t.Fatal(err)
	}
	lookup := func(context.Context, string, string) ([]byte, error) {
		return json.Marshal(map[string]any{"claudeAiOauth": map[string]any{"accessToken": "synthetic-fixture-only", "expiresAt": time.Now().Add(time.Hour).UnixMilli()}})
	}
	_, authDigest, err := prepareCLICredentials(ctx, "claude", credentialHome(t), lookup)
	if err != nil {
		t.Fatal(err)
	}
	recorded := false
	s, err := New(recordingLaunches(func(_ context.Context, _ contracts.DrainRequest, identity Identity, _ string) error {
		if identity.PID <= 0 || identity.PGID != identity.PID || identity.ProcessStartIdentity == "" {
			return ErrUnclear
		}
		recorded = true
		return nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	s.Executable = testProviderGate(t)
	binding := contracts.RuntimeBinding{Identity: domain.ProviderIdentity{Provider: "claude", Model: "claude-sonnet-5", Family: "anthropic-claude", Version: "2.1.263"}, BinaryDigest: bundle.Digest(), PolicyDigest: s.ProviderPolicyDigest("claude"), FixtureDigest: strings.Repeat("f", 64), AuthDigest: authDigest, AuthMode: claudeprovider.AuthModeSubscription}
	if _, err := s.RegisterRuntime(binding, exe, root); err != nil {
		t.Fatal(err)
	}
	request, _, input := codexRunFixture(t, exe, binding, root)
	input.BaseSHA = strings.Repeat("b", 40)
	input.WorktreeIdentity = `{"fixture":true}`
	_, input.RequestDigest = mustCanonicalPhaseInput(t, input)
	request.BaseSHA, request.RequestDigest = input.BaseSHA, input.RequestDigest
	request.WorktreeIdentity = input.WorktreeIdentity
	request.BindingDigest = strings.Repeat("d", 64)
	var storeFixture *rejectionProcessStore
	if scenario == "store_retry" {
		storeFixture = setupRejectionProcessStore(t, ctx, s, binding)
		request, input = processClaimRequest(storeFixture.claim), storeFixture.claim.Input
	}
	invocation, err := claudeprovider.Invocation(ctx, exe, root, input)
	if err != nil {
		t.Fatal(err)
	}
	inspected := false
	if err := s.ConfigureRejectionCheckpoint(rejectionInspectorFunc(func(ctx context.Context, request contracts.DrainRequest) (string, string, error) {
		inspected = true
		if storeFixture != nil {
			return storeFixture.physical.InspectRejectionCheckpoint(ctx, request)
		}
		return strings.Repeat("b", 40), strings.Repeat("c", 64), nil
	})); err != nil {
		t.Fatal(err)
	}
	result, runErr := s.runWithCLISecrets(ctx, request, invocation, input, lookup)
	if runErr == nil || result.ExitCode != -1 || ctx.Err() != nil || !recorded {
		t.Fatalf("synthetic process outcome: exit=%d err=%v", result.ExitCode, runErr)
	}
	drain, receipt, err := s.DrainServerRejection(ctx, request)
	if scenario != "complete" && scenario != "store_retry" {
		if err == nil || inspected || len(receipt.Signature) != 0 {
			t.Fatal("ambiguous process output acquired rejection authority")
		}
		if _, err := s.Drain(ctx, request); err != nil {
			t.Fatal("ordinary drain failed", err)
		}
		return
	}
	if err != nil || !inspected || !contracts.VerifyDrainProof(s.PublicKey(), request, drain) || !contracts.VerifyServerRejection(s.PublicKey(), request, receipt) {
		t.Fatalf("synthetic receipt refused: inspected=%t stdout_bytes=%d stderr_bytes=%d err=%v", inspected, len(result.Stdout), len(result.Stderr), err)
	}
	if _, _, err := s.DrainServerRejection(ctx, request); err == nil {
		t.Fatal("receipt replayed")
	}
	if storeFixture != nil {
		storeFixture.finishAndRetry(t, ctx, drain, receipt)
		request, input = processClaimRequest(storeFixture.claim), storeFixture.claim.Input
		invocation, err = claudeprovider.Invocation(ctx, exe, root, input)
		if err != nil {
			t.Fatal(err)
		}
		recorded = false
		result, runErr = s.runWithCLISecrets(ctx, request, invocation, input, lookup)
		if runErr == nil || result.ExitCode != -1 || !recorded {
			t.Fatal("second synthetic process did not record its rejection")
		}
		drain, receipt, err = s.DrainServerRejection(ctx, request)
		if err != nil {
			t.Fatal("second process rejection receipt", err)
		}
		if err := storeFixture.db.FinishProviderAttemptWithServerRejection(ctx, storeFixture.claim, drain, receipt, time.Now().UTC()); err != nil {
			t.Fatal("persist second process rejection", err)
		}
	}
}
