package authoring

import (
	"github.com/nysa-company/sf/internal/contracts"
	"testing"
)

func TestHomeIntentClosedSchemaAndPurposeSeparation(t *testing.T) {
	for _, bad := range []string{
		`{"action":"approve","selector":"SF-id"}`,
		`{"action":"pause","selector":"SF-id","argv":["sf","cancel"]}`,
		`{"action":"pause","action":"cancel","selector":"SF-id"}`,
		`{"action":"pause","selector":null}`,
		`{"action":"new","selector":"/tmp/out.md"}`,
		`{"action":"pause","selector":"a\u001bb"}`,
	} {
		if _, err := ParsePurpose("home_intent", []byte(bad)); err == nil {
			t.Fatal("unsafe intent accepted", bad)
		}
	}
	result, err := ParsePurpose("home_intent", []byte(`{"action":"pause","selector":"count jobs"}`))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Markdown(result); err == nil {
		t.Fatal("home intent reached draft renderer")
	}
	if ValidatePurpose("ticket_draft", result) == nil {
		t.Fatal("intent became draft authority")
	}
	data, err := MarshalResult("home_intent", result)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ParsePurpose("home_intent", data); err != nil {
		t.Fatal("home stored result not roundtrippable", err)
	}
	if _, err := ParsePurpose("ticket_draft", data); err == nil {
		t.Fatal("home output parsed as draft")
	}
	draft := contracts.AuthoringResult{Kind: "draft", Title: "Draft", Problem: "Problem", Acceptance: []string{"Observable"}}
	if ValidatePurpose("home_intent", draft) == nil {
		t.Fatal("draft became home command")
	}
	if _, err := EncodeRequest(contracts.AuthoringInput{Purpose: "home_intent", Prompt: "list", Context: "private"}); err == nil {
		t.Fatal("home received repository context")
	}
}
