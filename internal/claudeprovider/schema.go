package claudeprovider

import "encoding/json"

// cliSchema projects only the dialect-common subset used by SF onto Claude's
// draft-07 validator. The authenticated PhaseInput remains unchanged, and SF
// still validates the returned artifact independently. Never drop validation
// keywords or silently translate newer dialect-specific semantics.
func cliSchema(raw []byte) ([]byte, error) {
	var schema map[string]json.RawMessage
	if json.Unmarshal(raw, &schema) != nil || schema == nil {
		return nil, ErrInvocation
	}
	dialect, exists := schema["$schema"]
	if !exists {
		return raw, nil
	}
	var name string
	if json.Unmarshal(dialect, &name) != nil || name != "https://json-schema.org/draft/2020-12/schema" || !commonSchema(schema, 0) {
		return nil, ErrInvocation
	}
	schema["$schema"] = json.RawMessage(`"http://json-schema.org/draft-07/schema#"`)
	return json.Marshal(schema)
}

func commonSchema(schema map[string]json.RawMessage, depth int) bool {
	if schema == nil || depth > 16 {
		return false
	}
	for key, value := range schema {
		switch key {
		case "$schema", "$id":
			if depth != 0 {
				return false
			}
		case "type", "required", "enum", "const", "minItems", "maxItems", "minLength", "maxLength", "pattern", "description", "title":
		case "additionalProperties":
			var flag bool
			if json.Unmarshal(value, &flag) != nil {
				return false
			}
		case "properties":
			var properties map[string]map[string]json.RawMessage
			if json.Unmarshal(value, &properties) != nil || properties == nil {
				return false
			}
			for _, child := range properties {
				if !commonSchema(child, depth+1) {
					return false
				}
			}
		case "items":
			var child map[string]json.RawMessage
			if json.Unmarshal(value, &child) != nil || !commonSchema(child, depth+1) {
				return false
			}
		default:
			return false
		}
	}
	return true
}
