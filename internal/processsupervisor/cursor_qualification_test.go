package processsupervisor

import (
	"bytes"
	"encoding/json"
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
)

func TestCursorReadDiagnosticsNeverExposeProviderText(t *testing.T) {
	for _, raw := range []string{`{"error":{"path":"SECRET_PATH","error":"SECRET_TOKEN permission denied"}}`, `{"fileNotFound":{"path":"/private/outside"}}`, `{"SECRET_VARIANT":{"error":"SECRET_TOKEN"}}`} {
		var fields map[string]json.RawMessage
		if json.Unmarshal([]byte(raw), &fields) != nil {
			t.Fatal("fixture")
		}
		shape := cursorReadOutcomeShape(fields, "/private/worktree", "/private/outside")
		if strings.Contains(shape, "SECRET") || strings.Contains(shape, "private") {
			t.Fatal("diagnostic exposed untrusted data")
		}
	}
}

func TestCursorWriteDiagnosticsAreCountsOnly(t *testing.T) {
	stream := []byte(`{"type":"tool_call","subtype":"started","tool_call":{"writeToolCall":{"args":{"path":"result.txt","contents":"SECRET_CONTENT"}}}}
{"type":"tool_call","subtype":"completed","tool_call":{"writeToolCall":{"result":{"error":{"path":"SECRET_PATH","error":"SECRET_TOKEN EPERM"}}}}}`)
	want := "write_started=1 write_completed=1 result_target=1 write_success=0 write_failure=1 write_os_denied=1"
	if got := cursorFixtureWriteShape(stream, "/private/worktree"); got != want {
		t.Fatalf("fixed counts changed: %s", got)
	}
	for _, raw := range [][]byte{[]byte(`{"SECRET_KEY":"SECRET_TOKEN"}`), []byte("SECRET_TOKEN"), bytes.Repeat([]byte("SECRET_TOKEN"), 100000)} {
		if got := cursorFixtureWriteShape(raw, "/private/worktree"); strings.Contains(got, "SECRET") || strings.Contains(got, "private") {
			t.Fatal("diagnostic leaked provider data")
		}
	}
}

func TestCursorQualificationPinnedReadToolError(t *testing.T) {
	stream := `{"type":"system","subtype":"init","apiKeySource":"login","cwd":"/private/worktree","model":"label","session_id":"s"}
{"type":"tool_call","subtype":"started","call_id":"c","session_id":"s","tool_call":{"readToolCall":{"args":{"path":"/private/outside"}}}}
{"type":"tool_call","subtype":"completed","call_id":"c","session_id":"s","tool_call":{"readToolCall":{"result":{"error":{"errorMessage":"EPERM: operation not permitted"}}}}}
{"type":"result","subtype":"success","is_error":false,"session_id":"s","result":"{\"done\":true}"}`
	if err := cursorQualificationEvidence(contracts.CommandResult{Stdout: []byte(stream)}, "/private/worktree", "label", "/private/outside", []byte("PRIVATE_CANARY"), true); err != nil {
		t.Fatal("pinned ReadToolResult.error.errorMessage rejected", err)
	}
}

func TestCursorQualificationRejectsInvalidAuthorityWithoutCalls(t *testing.T) {
	s, err := New(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for _, leader := range []uint64{0, 1} {
		_, proof, err := s.QualifyCursor(t.Context(), "/missing", "unknown", domain.ChannelDev, leader)
		if err == nil || len(proof.Signature) != 0 {
			t.Fatal("invalid qualification signed")
		}
	}
}

func TestCursorQualificationRequiresPairedDeniedRead(t *testing.T) {
	init := `{"type":"system","subtype":"init","apiKeySource":"login","cwd":"/private/worktree","model":"label","session_id":"s"}` + "\n"
	start := `{"type":"tool_call","subtype":"started","call_id":"c","session_id":"s","tool_call":{"readToolCall":{"args":{"path":"/private/outside"}}}}` + "\n"
	denial := `"error":{"errorMessage":"EPERM"}`
	end := `{"type":"tool_call","subtype":"completed","call_id":"c","session_id":"s","tool_call":{"readToolCall":{"args":{"path":"/private/outside"},"result":{` + denial + `}}}}` + "\n"
	result := `{"type":"result","subtype":"success","is_error":false,"session_id":"s","result":"{\"done\":true}"}`
	valid := init + start + end + result
	check := func(raw string) error {
		return cursorQualificationEvidence(contracts.CommandResult{Stdout: []byte(raw)}, "/private/worktree", "label", "/private/outside", []byte("PRIVATE_CANARY"), true)
	}
	if err := check(valid); err != nil {
		t.Fatal(err)
	}
	if err := check(strings.ReplaceAll(valid, "/private/outside", "../outside")); err != nil {
		t.Fatal("same worktree-relative denied target rejected", err)
	}
	// Completion may omit args; it must inherit them only from its paired start.
	withoutArgs := strings.Replace(end, `"args":{"path":"/private/outside"},`, "", 1)
	if err := check(init + start + withoutArgs + result); err != nil {
		t.Fatal("paired result-only completion rejected", err)
	}
	if check(init+start+strings.Replace(end, "/private/outside", "/private/other", 1)+result) == nil {
		t.Fatal("completion retargeted read")
	}
	for _, message := range []string{"EACCES", "EPERM", "Operation not permitted", "Permission denied"} {
		deniedEnd := strings.Replace(end, denial, `"error":{"errorMessage":"`+message+`"}`, 1)
		if err := check(init + start + deniedEnd + result); err != nil {
			t.Fatal("matching OS read denial rejected", err)
		}
	}
	for _, failure := range []string{`"error":{"path":"/private/other","error":"EPERM"}`, `"error":{"errorMessage":"file not found"}`, `"permissionDenied":{}`, `"rejected":{}`, `"error":{"errorMessage":null}`, `"error":{"errorMessage":"EPERM","extra":"unknown"}`, `"error":{"errorMessage":"EPERM","errorMessage":"EPERM"}`, `"success":{},"error":{}`} {
		if check(init+start+strings.Replace(end, denial, failure, 1)+result) == nil {
			t.Fatal("unproven read denial accepted")
		}
	}
	for _, raw := range []string{init + result, init + end + result, init + start + result, init + start + start + end + result, strings.Replace(valid, `"error":`, `"success":`, 1), strings.ReplaceAll(valid, "readToolCall", "shellToolCall"), strings.ReplaceAll(valid, "/private/outside", "/private/other"), strings.Replace(valid, `"session_id":"s"`, `"session_id":"other"`, 1), strings.Replace(valid, denial, `"error":{"errorMessage":"PRIVATE_CANARY EPERM"}`, 1)} {
		if check(raw) == nil {
			t.Fatal("incomplete or unsafe evidence signed")
		}
	}
}

func TestCursorModelDiagnosticsExposeOnlyFixedVocabulary(t *testing.T) {
	if got := cursorModelShape("GPT-5.6 Luna (Low)"); got != "gpt/5.6/luna/low" {
		t.Fatal("fixed model classifier changed")
	}
	for _, secret := range []string{"PRIVATE_CANARY", "https://secret.example/token", strings.Repeat("x", 129)} {
		if strings.Contains(cursorModelShape(secret), secret) {
			t.Fatal("untrusted diagnostic leaked")
		}
	}
}

func TestCursorQualificationToolMetadataIsNotAnotherTool(t *testing.T) {
	valid := `{"readToolCall":{},"toolCallId":"c","startedAtMs":"100","completedAtMs":"101","hookAdditionalContexts":[]}`
	fields, err := cursorQualificationToolFields([]byte(valid), "c")
	if err != nil || len(fields) != 1 || fields["readToolCall"] == nil {
		t.Fatal("pinned tool metadata rejected")
	}
	for _, raw := range []string{strings.Replace(valid, `"c"`, `"other"`, 1), strings.Replace(valid, `"100"`, `100`, 1), strings.Replace(valid, `"101"`, `"-1"`, 1), strings.Replace(valid, `[]`, `null`, 1), strings.Replace(valid, `"readToolCall":{}`, `"readToolCall":{},"shellToolCall":{}`, 1), `{"toolCallId":"c"}`} {
		if _, err := cursorQualificationToolFields([]byte(raw), "c"); err == nil {
			t.Fatal("ambiguous tool metadata accepted")
		}
	}
}

func TestInstalledCursorQualificationSignsFreshFixtureVerdict(t *testing.T) {
	if os.Getenv("SF_TEST_CURSOR_QUALIFY") != "1" {
		t.Skip("explicit paid native qualification")
	}
	s, err := New(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	s.Executable = testProviderGate(t)
	exe, err := exec.LookPath("cursor-agent")
	if err != nil {
		t.Fatal("Cursor unavailable")
	}
	model := os.Getenv("SF_TEST_CURSOR_MODEL")
	if model == "" {
		model = "gpt-5.6-luna-low"
	}
	b, proof, err := s.QualifyCursor(t.Context(), exe, model, domain.ChannelDev, 1)
	if err != nil {
		t.Fatal(err)
	}
	if proof.Identity != b.Identity || proof.BinaryDigest != b.BinaryDigest || !contracts.VerifyQualificationAttestation(s.PublicKey(), proof) {
		t.Fatal("qualification signature mismatch")
	}
}
