package claudeprovider

import (
	"encoding/json"
	"reflect"
	"testing"
)

func TestSchemaProjectionPreservesEveryConstraint(t *testing.T) {
	raw := []byte(`{"$schema":"https://json-schema.org/draft/2020-12/schema","type":"object","additionalProperties":false,"required":["paths"],"properties":{"paths":{"type":"array","minItems":1,"items":{"type":"string","pattern":"^[a-z]+$"}}}}`)
	before := string(raw)
	projected, err := cliSchema(raw)
	if err != nil || string(raw) != before {
		t.Fatal("schema projection failed or mutated input", err)
	}
	var original, actual map[string]any
	_ = json.Unmarshal(raw, &original)
	_ = json.Unmarshal(projected, &actual)
	original["$schema"] = "http://json-schema.org/draft-07/schema#"
	if !reflect.DeepEqual(original, actual) {
		t.Fatal("constraint changed")
	}
	for _, extra := range []string{`"unevaluatedProperties":false`, `"prefixItems":[]`, `"$ref":"other"`, `"items":[]`} {
		bad := []byte(`{"$schema":"https://json-schema.org/draft/2020-12/schema",` + extra + `}`)
		if _, err := cliSchema(bad); err == nil {
			t.Fatalf("unsupported keyword accepted: %s", extra)
		}
	}
}
