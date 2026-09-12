package authoring

import (
	"encoding/json"
	"github.com/nysa-company/sf/internal/contracts"
	"strings"
)

// EncodeRequest is shared by reservation preflight and the final process
// boundary. Reference data is never executable authority; no bytes are clipped.
func EncodeRequest(input contracts.AuthoringInput) ([]byte, error) {
	schema, instruction, err := SchemaForPurpose(input.Purpose)
	if err != nil || input.Purpose == "home_intent" && input.Context != "" || !SafeText(input.Prompt, 16<<10) || strings.TrimSpace(input.Prompt) == "" || !SafeText(input.Context, 64<<10) {
		return nil, ErrContent
	}
	data, err := json.Marshal(struct{ Instruction, User, Reference string }{instruction, input.Prompt, input.Context})
	if err != nil || len(data)+len(schema) > 64<<10 {
		return nil, ErrContent
	}
	return data, nil
}
