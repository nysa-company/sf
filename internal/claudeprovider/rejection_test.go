package claudeprovider

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/providerjson"
)

func rejectionFixture() (contracts.PhaseInput, contracts.CommandResult) {
	input := claudeInput()
	input.Provider.Model, input.Provider.Version = "claude-sonnet-5", "2.1.263"
	output := `{"type":"system","subtype":"init","model":"claude-sonnet-5","session_id":"11111111-1111-1111-1111-111111111111","uuid":"22222222-2222-2222-2222-222222222222"}
{"type":"system","subtype":"api_retry","error":"server_error","attempt":1,"max_retries":1,"retry_delay_ms":1000,"error_status":503,"session_id":"11111111-1111-1111-1111-111111111111","uuid":"33333333-3333-3333-3333-333333333333"}
{"type":"assistant","error":"server_error","parent_tool_use_id":null,"message":{"content":[{"type":"text","text":"do not expose synthetic diagnostic"}]},"session_id":"11111111-1111-1111-1111-111111111111","uuid":"44444444-4444-4444-4444-444444444444"}
{"type":"result","subtype":"success","is_error":true,"session_id":"11111111-1111-1111-1111-111111111111","uuid":"55555555-5555-5555-5555-555555555555"}`
	return input, contracts.CommandResult{ExitCode: 1, Stdout: []byte(output)}
}

func TestObserveRejectionCompleteStream(t *testing.T) {
	input, command := rejectionFixture()
	for _, category := range []string{"server_error", "rate_limit", "billing_error", "authentication_failed", "invalid_request", "model_not_found"} {
		t.Run(category, func(t *testing.T) {
			// Retry history and final category need not match: e.g. 503 then 401.
			copy := command
			copy.Stdout = []byte(strings.Replace(string(command.Stdout), `"type":"assistant","error":"server_error"`, `"type":"assistant","error":"`+category+`"`, 1))
			got, err := ObserveRejection(context.Background(), input, copy)
			if err != nil || got.Category != category || got.InternalRetries != 1 || got.LastRetryDelayMS != 1000 || len(got.StreamDigest) != 64 || got.AllFailuresServerErrors != (category == "server_error") {
				t.Fatal("complete rejection observation failed")
			}
			// Observation does not change the existing terminal command policy.
			result, err := providerjson.Command(context.Background(), input, copy, true)
			if err == nil || result.Outcome != contracts.PhaseResultIndeterminate || len(result.Artifact) != 0 {
				t.Fatal("observation acquired retry or completion authority")
			}
		})
	}
	mixed := command
	mixed.Stdout = []byte(strings.Replace(strings.Replace(string(command.Stdout), `"error":"server_error"`, `"error":"rate_limit"`, 1), `"error_status":503`, `"error_status":429`, 1))
	if got, err := ObserveRejection(context.Background(), input, mixed); err != nil || got.Category != "server_error" || got.AllFailuresServerErrors {
		t.Fatal("earlier non-server rejection was hidden by last error")
	}
	lines := strings.Split(string(command.Stdout), "\n")
	command.Stdout = []byte(strings.Join([]string{lines[0], lines[2], lines[3]}, "\n"))
	if got, err := ObserveRejection(context.Background(), input, command); err != nil || got.InternalRetries != 0 {
		t.Fatal("complete immediate rejection failed")
	}
}

func TestObserveRejectionRefusesAmbiguousOrChangedStreams(t *testing.T) {
	input, command := rejectionFixture()
	base := string(command.Stdout)
	lines := strings.Split(base, "\n")
	cases := map[string]string{
		"missing final":      strings.Join(lines[:3], "\n"),
		"missing init":       strings.Join(lines[1:], "\n"),
		"normal assistant":   strings.Replace(base, `"error":"server_error","parent_tool_use_id"`, `"parent_tool_use_id"`, 1),
		"extra assistant":    strings.Join([]string{lines[0], lines[2], lines[1], lines[2], lines[3]}, "\n"),
		"user event":         strings.Replace(base, `"type":"assistant"`, `"type":"user"`, 1),
		"tool effect":        strings.Replace(base, `"type":"text"`, `"type":"tool_use"`, 1),
		"hook":               strings.Replace(base, `"subtype":"api_retry"`, `"subtype":"hook_started"`, 1),
		"partial":            strings.Replace(base, `"type":"assistant"`, `"type":"stream_event"`, 1),
		"unknown category":   strings.ReplaceAll(base, `"server_error"`, `"unknown"`),
		"http mismatch":      strings.Replace(base, `"error_status":503`, `"error_status":429`, 1),
		"connection error":   strings.Replace(base, `"error_status":503`, `"error_status":null`, 1),
		"attempt gap":        strings.Replace(base, `"attempt":1`, `"attempt":2`, 1),
		"oversized limit":    strings.Replace(base, `"max_retries":1`, `"max_retries":16`, 1),
		"delay negative":     strings.Replace(base, `"retry_delay_ms":1000`, `"retry_delay_ms":-1`, 1),
		"delay oversized":    strings.Replace(base, `"retry_delay_ms":1000`, `"retry_delay_ms":60001`, 1),
		"fractional attempt": strings.Replace(base, `"attempt":1`, `"attempt":1.5`, 1),
		"model drift":        strings.Replace(base, `"model":"claude-sonnet-5"`, `"model":"auto"`, 1),
		"session drift":      strings.Replace(base, `"session_id":"11111111-1111-1111-1111-111111111111"`, `"session_id":"aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"`, 1),
		"duplicate id":       strings.Replace(base, "44444444-4444-4444-4444-444444444444", "33333333-3333-3333-3333-333333333333", 1),
		"zero id":            strings.Replace(base, "44444444-4444-4444-4444-444444444444", "00000000-0000-0000-0000-000000000000", 1),
		"child":              strings.Replace(base, `"parent_tool_use_id":null`, `"parent_tool_use_id":"child"`, 1),
		"success":            strings.Replace(base, `"is_error":true`, `"is_error":false`, 1),
		"artifact":           strings.Replace(base, `"is_error":true`, `"is_error":true,"structured_output":{}`, 1),
		"terminal subtype":   strings.Replace(base, `"subtype":"success"`, `"subtype":"error_max_turns"`, 1),
		"duplicate key":      strings.Replace(base, `"is_error":true`, `"is_error":true,"is_error":false`, 1),
		"nested duplicate":   strings.Replace(base, `"content":[`, `"content":[],"content":[`, 1),
		"null text":          strings.Replace(base, `"text":"do not expose synthetic diagnostic"`, `"text":null`, 1),
		"trailing":           base + "\n{}",
		"blank event":        strings.Replace(base, "\n", "\n\n", 1),
		"too many events":    strings.Repeat(lines[0]+"\n", 19),
		"too large":          strings.Repeat("x", providerjson.MaxResultBytes+1),
	}
	for name, value := range cases {
		t.Run(name, func(t *testing.T) {
			copy := command
			copy.Stdout = []byte(value)
			got, err := ObserveRejection(context.Background(), input, copy)
			if !errors.Is(err, ErrRejectionStream) || got != (RejectionObservation{}) {
				t.Fatal("ambiguous stream accepted or partial observation escaped")
			}
		})
	}
	for _, mutate := range []func(*contracts.CommandResult){
		func(c *contracts.CommandResult) { c.ExitCode = 0 },
		func(c *contracts.CommandResult) { c.ExitCode = -1 },
		func(c *contracts.CommandResult) { c.StdoutTruncated = true },
		func(c *contracts.CommandResult) { c.StderrTruncated = true },
		func(c *contracts.CommandResult) { c.Stderr = bytes.Repeat([]byte{'x'}, providerjson.MaxResultBytes+1) },
	} {
		copy := command
		mutate(&copy)
		if _, err := ObserveRejection(context.Background(), input, copy); err == nil {
			t.Fatal("uncertain command accepted")
		}
	}
	input.Provider.Version = "changed"
	if _, err := ObserveRejection(context.Background(), input, command); err == nil {
		t.Fatal("changed runtime accepted")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := ObserveRejection(ctx, input, command); !errors.Is(err, context.Canceled) {
		t.Fatal("cancel ignored")
	}
}

func FuzzObserveRejection(f *testing.F) {
	input, command := rejectionFixture()
	f.Add(command.Stdout)
	f.Add([]byte("{}"))
	f.Fuzz(func(t *testing.T, output []byte) {
		if len(output) > providerjson.MaxResultBytes+1 {
			return
		}
		copy := command
		copy.Stdout = output
		got, err := ObserveRejection(context.Background(), input, copy)
		if err != nil {
			if got != (RejectionObservation{}) {
				t.Fatal("partial observation escaped")
			}
			return
		}
		if !rejectionCategory(got.Category) || len(got.StreamDigest) != 64 || got.InternalRetries < 0 || got.InternalRetries > 15 || got.LastRetryDelayMS < 0 || got.LastRetryDelayMS > 60000 {
			t.Fatal("accepted observation violates bounds")
		}
	})
}
