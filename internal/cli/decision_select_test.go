package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/nysa-company/sf/internal/api"
	"github.com/nysa-company/sf/internal/domain"
)

func TestDecisionPickerRequiresExactHeadConfirmation(t *testing.T) {
	for _, tc := range []struct {
		name, command, answer string
		stale, badView, json  bool
		wantCalls             int
	}{
		{"approve", "approve", "1\napprove\n", false, false, false, 3},
		{"reject", "reject", "1\nreject\n", false, false, false, 3},
		{"cancel picker", "approve", "q\n", false, false, false, 1},
		{"cancel confirmation", "approve", "1\nq\n", false, false, false, 2},
		{"no generic yes", "approve", "1\nyes\n", false, false, false, 2},
		{"missing confirmation", "approve", "1\n", false, false, false, 2},
		{"changed candidate", "approve", "1\napprove\n", true, false, false, 3},
		{"invalid view", "approve", "1\napprove\n", false, true, false, 2},
		{"JSON never prompts", "approve", "1\napprove\n", false, false, true, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var output, prompt bytes.Buffer
			calls := 0
			head := strings.Repeat("a", 40)
			client := fakeClient(func(_ context.Context, r api.Request) (api.Response, error) {
				calls++
				switch calls {
				case 1:
					return selectionInventory(selectionItem(selectedID, "app")), nil
				case 2:
					if r.Method != "ticket.status" || r.Ticket != selectedID {
						t.Fatalf("unexpected read: %+v", r)
					}
					item := selectionItem(selectedID, "app")
					item.State = domain.StateWaitingApproval
					if tc.badView {
						item.State = domain.StateBuilding
					}
					data, _ := json.Marshal(map[string]any{"ticket": item, "evidence": map[string]any{"candidate": map[string]any{"head_sha": head}}})
					response := responseOK()
					response.Data = data
					return response, nil
				default:
					var values map[string]any
					if json.Unmarshal(r.Parameters, &values) != nil || values["reviewed_head"] != head || r.Ticket != selectedID || r.Method != "ticket."+tc.command {
						t.Fatalf("unbound decision: %+v", r)
					}
					if tc.stale {
						return failure("approval_head_changed", "changed", []string{"sf", "status", selectedID}), nil
					}
					return responseOK(), nil
				}
			})
			a := newApp(client, &output, &prompt)
			a.input = strings.NewReader(tc.answer)
			a.interactive = func() bool { return true }
			cmd := a.command()
			args := []string{tc.command, "--project", "app"}
			if tc.command == "reject" {
				args = append(args, "--reason", "needs tests")
			}
			if tc.json {
				args = append(args, "--json")
			}
			cmd.SetArgs(args)
			_ = cmd.ExecuteContext(context.Background())
			if calls != tc.wantCalls {
				t.Fatalf("calls=%d want=%d output=%s", calls, tc.wantCalls, output.String())
			}
			if tc.json && prompt.Len() != 0 {
				t.Fatalf("JSON prompted: %s", prompt.String())
			}
			if calls == 3 && !strings.Contains(prompt.String(), head) {
				t.Fatal("head not displayed")
			}
			if tc.stale && (a.last == nil || a.last.OK || a.last.Error.Code != "approval_head_changed") {
				t.Fatalf("stale refusal lost: %s", output.String())
			}
		})
	}
}
