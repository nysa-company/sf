package cli

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/nysa-company/sf/internal/api"
)

func TestRecoveryStatusHumanAndJSONKeepTheSameNextAction(t *testing.T) {
	data, err := json.Marshal(map[string]any{
		"ticket":      map[string]any{"ticket": "SF-retained", "state": "blocked"},
		"recovery":    map[string]any{"cause": "Proof failed; cause unknown", "work_disposition": "Registered, not inspected", "writer_safety": "not_checked", "safety_note": "Use a supported control action"},
		"next_action": map[string]any{"code": "postbuild_command_failed", "argv": []string{"sf-dev", "cancel", "SF-retained"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	response := api.Response{Version: api.Version, RequestID: "recovery-view", OK: true, Data: data}
	var human, machine bytes.Buffer
	if err := Render(&human, response, false); err != nil {
		t.Fatal(err)
	}
	if err := Render(&machine, response, true); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"What happened: Proof failed; cause unknown", "Retained work: Registered, not inspected", "Writer safety: not_checked", "Next: sf-dev cancel SF-retained"} {
		if !strings.Contains(human.String(), want) {
			t.Errorf("human output missing %q: %s", want, human.String())
		}
	}
	if strings.Count(human.String(), "Next:") != 1 || strings.Contains(human.String(), "sf-dev retry") {
		t.Fatal("renderer invented or duplicated recovery action")
	}
	var decoded api.Response
	if err := json.Unmarshal(machine.Bytes(), &decoded); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(decoded.Data, data) {
		t.Fatal("JSON rendering changed status semantics")
	}
}
