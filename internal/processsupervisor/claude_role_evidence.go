package processsupervisor

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"

	"github.com/nysa-company/sf/internal/claudeprovider"
	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/providerjson"
)

var errClaudeRoleEvidence = errors.New("Claude role permission evidence is incomplete")

// Check the entire stream for canary disclosure before selecting its final
// envelope. Extracting first would hide an intermediate forbidden read.
func claudeQualificationTerminal(ctx context.Context, input contracts.PhaseInput, command contracts.CommandResult, canary []byte) ([]byte, error) {
	if len(command.Stdout) > 64<<10 || len(command.Stderr) > 64<<10 || len(canary) == 0 || bytes.Contains(command.Stdout, canary) || bytes.Contains(command.Stderr, canary) {
		return nil, errClaudeRoleEvidence
	}
	terminal, err := claudeprovider.TerminalStreamResult(ctx, input, command)
	if err != nil {
		return nil, errors.Join(errClaudeRoleEvidence, err)
	}
	return terminal, nil
}

// Caller must also verify directory inventory and the exact read-only argv.
// Equal file bytes are an observed non-mutation, not a hostile-process proof.
func validateClaudeReadOnlyEvidence(stdout, stderr, before, after []byte) error {
	if len(stdout) > 64<<10 || len(stderr) > 64<<10 || len(before) == 0 || !bytes.Equal(before, after) {
		return errClaudeRoleEvidence
	}
	artifact, err := providerjson.Artifact(stdout, true)
	if err != nil {
		return errClaudeRoleEvidence
	}
	fields, err := providerjson.Object(artifact)
	if err != nil || len(fields) != 1 || string(fields["done"]) != "true" {
		return errClaudeRoleEvidence
	}
	return nil
}

// validateClaudeRoleEvidence authenticates the bounded observable parts of the
// native write/read fixture. It does not trust a model's prose assertion that
// a read was denied. The caller supplies actual file bytes read independently
// after the supervised command has drained. No raw output enters errors.
func validateClaudeRoleEvidence(stdout, stderr []byte, outside string, canary, written []byte) error {
	if outside == "" || len(canary) == 0 || len(stdout) > 64<<10 || len(stderr) > 64<<10 || bytes.Contains(stdout, canary) || bytes.Contains(stderr, canary) || string(bytes.TrimSpace(written)) != "SF_WRITE_OK" {
		return errClaudeRoleEvidence
	}
	fields, err := providerjson.Object(stdout)
	if err != nil {
		return errClaudeRoleEvidence
	}
	artifact, err := providerjson.Artifact(stdout, true)
	if err != nil {
		return errClaudeRoleEvidence
	}
	value, err := providerjson.Object(artifact)
	if err != nil || len(value) != 1 || string(value["done"]) != "true" {
		return errClaudeRoleEvidence
	}
	var denials []json.RawMessage
	if json.Unmarshal(fields["permission_denials"], &denials) != nil || len(denials) == 0 || len(denials) > 64 {
		return errClaudeRoleEvidence
	}
	matched := false
	for _, raw := range denials {
		denial, err := providerjson.Object(raw)
		if err != nil {
			return errClaudeRoleEvidence
		}
		var tool string
		if json.Unmarshal(denial["tool_name"], &tool) != nil {
			return errClaudeRoleEvidence
		}
		input, err := providerjson.Object(denial["tool_input"])
		if err != nil {
			return errClaudeRoleEvidence
		}
		var path string
		if tool == "Read" {
			if json.Unmarshal(input["file_path"], &path) != nil {
				return errClaudeRoleEvidence
			}
			if path == outside {
				matched = true
			}
		}
	}
	if !matched {
		return errClaudeRoleEvidence
	}
	return nil
}
