package api

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/nysa-company/sf/internal/domain"
)

func TestRequestRoundTrip(t *testing.T) {
	request := Request{
		Version:    Version,
		RequestID:  "request-1",
		Method:     "ticket.status",
		Ticket:     "SF-1",
		Parameters: json.RawMessage(`{"watch":false}`),
	}
	var buffer bytes.Buffer
	if err := Encode(&buffer, request); err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeRequest(&buffer)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.Method != request.Method || decoded.RequestID != request.RequestID {
		t.Fatalf("decoded=%+v", decoded)
	}
}

func TestResponseRequiresConsistentOutcome(t *testing.T) {
	responses := []Response{
		{Version: Version, RequestID: "1", OK: true, Error: &Error{Code: "bad"}},
		{Version: Version, RequestID: "1", OK: false},
		{Version: Version, RequestID: "1", OK: false, Error: &Error{Code: "bad"}, NextAction: &domain.NextAction{}},
	}
	for _, response := range responses {
		if err := response.Validate(); err == nil {
			t.Fatalf("expected invalid response: %+v", response)
		}
	}
}

func TestDecoderRejectsUnknownAndOversizedInput(t *testing.T) {
	unknown := `{"version":"sf.local/v1","request_id":"1","method":"x","unexpected":true}`
	if _, err := DecodeRequest(strings.NewReader(unknown)); err == nil {
		t.Fatal("expected unknown field rejection")
	}
	oversized := strings.Repeat("x", MaxMessageBytes+1)
	if _, err := DecodeRequest(strings.NewReader(oversized)); err == nil {
		t.Fatal("expected size rejection")
	}
}

func TestDecoderRejectsMultipleJSONValues(t *testing.T) {
	input := `{"version":"sf.local/v1","request_id":"1","method":"x"} {}`
	if _, err := DecodeRequest(strings.NewReader(input)); err == nil {
		t.Fatal("expected trailing value rejection")
	}
}
