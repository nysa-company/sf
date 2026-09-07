// Package providerjson decodes the bounded terminal JSON envelope used by
// headless Claude Code and Cursor. It grants no execution or billing authority.
package providerjson

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
)

const MaxResultBytes = 1 << 20

var (
	ErrProtocol = errors.New("provider terminal JSON is invalid")
	ErrTerminal = errors.New("provider reported terminal failure")
	ErrArtifact = errors.New("provider final artifact is invalid")
)

// Artifact accepts one complete successful envelope, never a partial stream.
// schemaOutput selects Claude's structured_output field; otherwise result is
// a JSON-encoded artifact string, as required by Cursor's print-mode contract.
// Unknown envelope metadata is tolerated; duplicate keys and mistyped consumed
// fields are not. The phase-specific artifact schema is validated downstream.
// Callers must independently require successful, untruncated process output,
// exact runtime identity, safe drain, and authenticated billing evidence.
func Artifact(output []byte, schemaOutput bool) ([]byte, error) {
	fields, err := Object(output)
	if err != nil {
		return nil, err
	}
	return artifactFields(fields, schemaOutput)
}

// Object reads a single bounded object while rejecting duplicate top-level
// keys. Callers may impose a smaller limit and must validate consumed fields.
func Object(output []byte) (map[string]json.RawMessage, error) {
	if len(output) == 0 || len(output) > MaxResultBytes {
		return nil, ErrProtocol
	}
	decoder := json.NewDecoder(bytes.NewReader(output))
	first, err := decoder.Token()
	if err != nil || first != json.Delim('{') {
		return nil, ErrProtocol
	}
	fields := make(map[string]json.RawMessage)
	for decoder.More() {
		token, err := decoder.Token()
		key, ok := token.(string)
		if err != nil || !ok || len(fields) >= 128 {
			return nil, ErrProtocol
		}
		if _, exists := fields[key]; exists {
			return nil, ErrProtocol
		}
		var value json.RawMessage
		if decoder.Decode(&value) != nil {
			return nil, ErrProtocol
		}
		fields[key] = value
	}
	if last, err := decoder.Token(); err != nil || last != json.Delim('}') {
		return nil, ErrProtocol
	}
	if _, err := decoder.Token(); err != io.EOF {
		return nil, ErrProtocol
	}
	return fields, nil
}

func artifactFields(fields map[string]json.RawMessage, schemaOutput bool) ([]byte, error) {
	var kind, subtype string
	var isError *bool
	if json.Unmarshal(fields["type"], &kind) != nil || kind != "result" ||
		json.Unmarshal(fields["subtype"], &subtype) != nil || subtype == "" ||
		json.Unmarshal(fields["is_error"], &isError) != nil || isError == nil {
		return nil, ErrProtocol
	}
	if *isError || subtype != "success" {
		return nil, ErrTerminal
	}
	var artifact []byte
	if schemaOutput {
		artifact = fields["structured_output"]
	} else {
		var text string
		if json.Unmarshal(fields["result"], &text) != nil {
			return nil, ErrArtifact
		}
		artifact = []byte(text)
	}
	artifact = bytes.TrimSpace(artifact)
	if len(artifact) == 0 || artifact[0] != '{' || !json.Valid(artifact) {
		return nil, ErrArtifact
	}
	return append([]byte(nil), artifact...), nil
}
