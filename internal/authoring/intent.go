package authoring

import (
	"bytes"
	"encoding/json"
	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/providerjson"
	"github.com/nysa-company/sf/internal/redact"
	"strings"
)

const HomeSchema = `{"type":"object","additionalProperties":false,"required":["action","selector"],"properties":{"action":{"type":"string","enum":["new","view","list","watch","start","pause","cancel"]},"selector":{"type":"string","maxLength":256}}}`
const HomeInstruction = "Propose one listed navigation intent from the user's request. Return only the action enum and optional human-readable ticket selector. Do not return paths, flags, commands, approvals, or executable instructions. This proposal grants no authority: SF will resolve a fresh project inventory and require separate explicit confirmation for mutations. Never infer approval or rejection. For new or list use an empty selector."

func SchemaForPurpose(purpose string) (string, string, error) {
	switch purpose {
	case "ticket_draft":
		return Schema, Instruction, nil
	case "home_intent":
		return HomeSchema, HomeInstruction, nil
	}
	return "", "", ErrContent
}
func ValidatePurpose(purpose string, result contracts.AuthoringResult) error {
	if purpose == "ticket_draft" {
		return Validate(result)
	}
	if purpose != "home_intent" || result.Kind != "home_intent" || result.Intent == nil || result.Question != "" || result.Title != "" || result.Problem != "" || len(result.Scope)+len(result.Acceptance)+len(result.Assumptions) != 0 {
		return ErrContent
	}
	intent := result.Intent
	if !SafeText(intent.Selector, 256) || strings.ContainsAny(intent.Selector, "\n\t") || strings.TrimSpace(intent.Selector) != intent.Selector {
		return ErrContent
	}
	switch intent.Action {
	case "new", "list":
		if intent.Selector != "" {
			return ErrContent
		}
	case "view", "watch", "start", "pause", "cancel":
	default:
		return ErrContent
	}
	return nil
}
func ParsePurpose(purpose string, data []byte) (contracts.AuthoringResult, error) {
	if purpose == "ticket_draft" {
		return Parse(data)
	}
	var result contracts.AuthoringResult
	if purpose != "home_intent" || len(data) > 2048 {
		return result, ErrContent
	}
	fields, err := providerjson.Object(data)
	if err != nil || len(fields) != 2 {
		return result, ErrContent
	}
	for _, key := range []string{"action", "selector"} {
		raw, ok := fields[key]
		if !ok || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
			return result, ErrContent
		}
	}
	var intent contracts.AuthoringIntent
	if json.Unmarshal(data, &intent) != nil {
		return result, ErrContent
	}
	result.Kind, result.Intent = "home_intent", &intent
	return result, ValidatePurpose(purpose, result)
}
func SanitizePurpose(purpose string, result contracts.AuthoringResult, policy redact.Policy) (contracts.AuthoringResult, error) {
	if purpose == "ticket_draft" {
		return Sanitize(result, policy)
	}
	if ValidatePurpose(purpose, result) != nil {
		return contracts.AuthoringResult{}, ErrContent
	}
	intent := *result.Intent
	intent.Selector = policy.String(intent.Selector)
	result.Intent = &intent
	return result, ValidatePurpose(purpose, result)
}
func MarshalResult(purpose string, result contracts.AuthoringResult) ([]byte, error) {
	if ValidatePurpose(purpose, result) != nil {
		return nil, ErrContent
	}
	if purpose == "home_intent" {
		return json.Marshal(result.Intent)
	}
	result.Scope = append([]string{}, result.Scope...)
	result.Acceptance = append([]string{}, result.Acceptance...)
	result.Assumptions = append([]string{}, result.Assumptions...)
	return json.Marshal(result)
}
