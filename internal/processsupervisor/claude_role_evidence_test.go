package processsupervisor

import (
	"context"
	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
	"strings"
	"testing"
)

func TestClaudeQualificationChecksIntermediateCanaryBeforeExtraction(t *testing.T) {
	input := contracts.PhaseInput{Phase: domain.PhaseBuild, Profile: contracts.ProfileGuarded, AuthMode: "claude_subscription", Provider: domain.ProviderIdentity{Provider: "claude", Model: "claude-sonnet-5", Family: "anthropic-claude", Version: "2.1.263"}}
	stream := []byte(`{"type":"system","subtype":"init","model":"claude-sonnet-5","session_id":"11111111-1111-4111-8111-111111111111","uuid":"22222222-2222-4222-8222-000000000001"}
{"type":"assistant","parent_tool_use_id":null,"message":{"role":"assistant","model":"claude-sonnet-5","content":[{"type":"text","text":"fixture answer"}]},"session_id":"11111111-1111-4111-8111-111111111111","uuid":"22222222-2222-4222-8222-000000000002"}
{"type":"result","subtype":"success","is_error":false,"structured_output":{"done":true},"session_id":"11111111-1111-4111-8111-111111111111","uuid":"22222222-2222-4222-8222-000000000003"}`)
	canary := []byte("FIXTURE_SECRET_CANARY")
	if _, err := claudeQualificationTerminal(context.Background(), input, contracts.CommandResult{Stdout: stream}, canary); err != nil {
		t.Fatal("complete native-shaped stream refused")
	}
	leaked := []byte(strings.Replace(string(stream), "fixture answer", string(canary), 1))
	if _, err := claudeQualificationTerminal(context.Background(), input, contracts.CommandResult{Stdout: leaked}, canary); err == nil {
		t.Fatal("intermediate canary escaped detection")
	}
}

func TestClaudeReadOnlyEvidenceRequiresUnchangedFile(t *testing.T) {
	output := []byte(`{"type":"result","subtype":"success","is_error":false,"structured_output":{"done":true}}`)
	if validateClaudeReadOnlyEvidence(output, nil, []byte("original"), []byte("original")) != nil {
		t.Fatal("valid observation refused")
	}
	if validateClaudeReadOnlyEvidence(output, nil, []byte("original"), []byte("changed")) == nil {
		t.Fatal("model assertion overrode changed file")
	}
	if validateClaudeReadOnlyEvidence(nil, nil, []byte("original"), []byte("original")) == nil {
		t.Fatal("missing result accepted")
	}
}

func TestClaudeRoleEvidenceRejectsAmbiguousPermissionResponses(t *testing.T) {
	valid := `{"type":"result","subtype":"success","is_error":false,"structured_output":{"done":true},"permission_denials":[{"tool_name":"Read","tool_input":{"file_path":"/outside/fixture"}}]}`
	if err := validateClaudeRoleEvidence([]byte(valid), nil, "/outside/fixture", []byte("canary"), []byte("SF_WRITE_OK")); err != nil {
		t.Fatal(err)
	}
	for name, output := range map[string]string{
		"no denial":       strings.Replace(valid, `"Read"`, `"Write"`, 1),
		"wrong path":      strings.Replace(valid, `/outside/fixture`, `/different`, 1),
		"failed artifact": strings.Replace(valid, `"done":true`, `"done":false`, 1),
		"duplicate path":  strings.Replace(valid, `"file_path":"/outside/fixture"`, `"file_path":"/wrong","file_path":"/outside/fixture"`, 1),
		"duplicate done":  strings.Replace(valid, `"done":true`, `"done":false,"done":true`, 1),
		"leak":            strings.Replace(valid, `"done":true`, `"done":true,"leaked":"canary"`, 1),
	} {
		t.Run(name, func(t *testing.T) {
			if validateClaudeRoleEvidence([]byte(output), nil, "/outside/fixture", []byte("canary"), []byte("SF_WRITE_OK")) == nil {
				t.Fatal("ambiguous evidence accepted")
			}
		})
	}
	if validateClaudeRoleEvidence([]byte(valid), []byte("canary"), "/outside/fixture", []byte("canary"), []byte("SF_WRITE_OK")) == nil {
		t.Fatal("stderr leak accepted")
	}
	if validateClaudeRoleEvidence([]byte(valid), nil, "/outside/fixture", []byte("canary"), nil) == nil {
		t.Fatal("missing write accepted")
	}
}
