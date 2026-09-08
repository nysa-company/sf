package claudeprovider

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/providerjson"
)

var ErrRejectionStream = errors.New("Claude rejection stream is incomplete or inconsistent")

// RejectionObservation describes only a complete, bounded CLI protocol shape.
// It is not signed, does not authenticate the process or filesystem, and grants
// no admission, billing, repair or retry authority. In particular rate_limit
// can include exhausted quota. Callers must not turn this into Retryable=true.
type RejectionObservation struct {
	Category         string
	StreamDigest     string
	InternalRetries  int
	LastRetryDelayMS int
	// AllFailuresServerErrors is a transcript classification, not permission
	// to retry. It prevents losing an earlier auth/quota error behind a final503.
	AllFailuresServerErrors bool
}

// ObserveRejection accepts exactly init, zero or more internal retry notices,
// one typed API-error assistant message, and a final error result. Any normal
// model response, user/tool message, hook, partial event or trailing data
// refuses. The streaming invocation is policy-versioned; this observation
// alone still requires supervisor signing, physical proof and Store admission.
func ObserveRejection(ctx context.Context, input contracts.PhaseInput, command contracts.CommandResult) (RejectionObservation, error) {
	fail := func() (RejectionObservation, error) { return RejectionObservation{}, ErrRejectionStream }
	if err := ctx.Err(); err != nil {
		return RejectionObservation{}, err
	}
	if family, ok := ModelFamily(input.Provider.Model); !ok || input.Provider.Family != family || input.Provider.Provider != "claude" || input.Provider.Version != "2.1.263" ||
		input.AuthMode != AuthModeSubscription || input.Profile != contracts.ProfileGuarded ||
		command.ExitCode != 1 || command.StdoutTruncated || command.StderrTruncated || len(command.Stdout) == 0 ||
		len(command.Stdout) > providerjson.MaxResultBytes || len(command.Stderr) > providerjson.MaxResultBytes {
		return fail()
	}
	lines := bytes.Split(bytes.TrimSpace(command.Stdout), []byte{'\n'})
	if len(lines) < 3 || len(lines) > 18 { // init + <=15 retries + API error + result
		return fail()
	}
	var result RejectionObservation
	allServer := true
	var session string
	var maximum int
	seen := make(map[string]bool, len(lines))
	for i, line := range lines {
		fields, err := providerjson.Object(line)
		if err != nil {
			return fail()
		}
		kind, current, id := rejectionString(fields, "type"), rejectionString(fields, "session_id"), rejectionString(fields, "uuid")
		if !rejectionID(current) || !rejectionID(id) || seen[id] {
			return fail()
		}
		seen[id] = true
		if i == 0 {
			if kind != "system" || rejectionString(fields, "subtype") != "init" || rejectionString(fields, "model") != input.Provider.Model {
				return fail()
			}
			session = current
			continue
		}
		if current != session {
			return fail()
		}
		switch {
		case i == len(lines)-1:
			var isError *bool
			subtype := rejectionString(fields, "subtype")
			if kind != "result" || result.Category == "" || json.Unmarshal(fields["is_error"], &isError) != nil || isError == nil || !*isError ||
				(subtype != "success" && subtype != "error_during_execution") {
				return fail()
			}
			if raw, ok := fields["structured_output"]; ok && !bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
				return fail()
			}
		case i == len(lines)-2:
			if kind != "assistant" || !bytes.Equal(bytes.TrimSpace(fields["parent_tool_use_id"]), []byte("null")) {
				return fail()
			}
			result.Category = rejectionString(fields, "error")
			allServer = allServer && result.Category == "server_error"
			if !rejectionCategory(result.Category) {
				return fail()
			}
			message, err := providerjson.Object(fields["message"])
			if err != nil {
				return fail()
			}
			var content []json.RawMessage
			if json.Unmarshal(message["content"], &content) != nil || len(content) != 1 {
				return fail()
			}
			block, err := providerjson.Object(content[0])
			var text *string
			if err != nil || len(block) != 2 || rejectionString(block, "type") != "text" || json.Unmarshal(block["text"], &text) != nil || text == nil || *text == "" {
				return fail()
			}
		default:
			allServer = allServer && rejectionString(fields, "error") == "server_error"
			if kind != "system" || rejectionString(fields, "subtype") != "api_retry" || !rejectionCategory(rejectionString(fields, "error")) {
				return fail()
			}
			var attempt, limit, delay, status *int
			if json.Unmarshal(fields["attempt"], &attempt) != nil || json.Unmarshal(fields["max_retries"], &limit) != nil ||
				json.Unmarshal(fields["retry_delay_ms"], &delay) != nil || json.Unmarshal(fields["error_status"], &status) != nil ||
				attempt == nil || limit == nil || delay == nil || status == nil || *attempt != result.InternalRetries+1 ||
				*limit < 1 || *limit > 15 || *attempt > *limit || *delay < 0 || *delay > 60000 || *status < 400 || *status > 599 ||
				(maximum != 0 && maximum != *limit) || !rejectionHTTPMatches(rejectionString(fields, "error"), *status) {
				return fail()
			}
			maximum, result.InternalRetries, result.LastRetryDelayMS = *limit, *attempt, *delay
		}
	}
	if err := ctx.Err(); err != nil {
		return RejectionObservation{}, err
	}
	sum := sha256.Sum256(command.Stdout)
	result.StreamDigest = hex.EncodeToString(sum[:])
	result.AllFailuresServerErrors = allServer
	return result, nil
}

func rejectionString(fields map[string]json.RawMessage, key string) string {
	var value string
	if json.Unmarshal(fields[key], &value) != nil {
		return ""
	}
	return value
}

func rejectionCategory(value string) bool {
	switch value {
	case "authentication_failed", "oauth_org_not_allowed", "billing_error", "rate_limit", "invalid_request", "model_not_found", "server_error", "max_output_tokens":
		return true
	default:
		return false
	}
}

func rejectionID(value string) bool {
	if len(value) != 36 || value == "00000000-0000-0000-0000-000000000000" {
		return false
	}
	for i, c := range value {
		if i == 8 || i == 13 || i == 18 || i == 23 {
			if c != '-' {
				return false
			}
		} else if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}

func rejectionHTTPMatches(category string, status int) bool {
	switch category {
	case "server_error":
		return status >= 500 && status <= 599
	case "rate_limit":
		return status == 429
	case "authentication_failed":
		return status == 401 || status == 403
	case "oauth_org_not_allowed":
		return status == 403
	case "billing_error":
		return status == 402 || status == 429
	case "invalid_request", "max_output_tokens":
		return status == 400
	case "model_not_found":
		return status == 404
	default:
		return false
	}
}
