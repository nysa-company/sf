package daemon

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/nysa-company/sf/internal/api"
	"github.com/nysa-company/sf/internal/domain"
)

func TestOperatorDecisionRequiresRequestedReviewedHead(t *testing.T) {
	for _, decision := range []string{"approved", "rejected"} {
		t.Run(decision, func(t *testing.T) {
			daemon, _, stop := testDaemon(t)
			defer stop()
			ticket := prepareDaemonGuardedLifecycle(t, daemon, "SF-head-binding", domain.StateWaitingApproval)
			candidate, err := daemon.store.RecoverableCandidate(t.Context(), ticket.Ref)
			if err != nil {
				t.Fatal(err)
			}
			request := func(head any) api.Response {
				values := map[string]any{"channel": daemon.channel, "reviewed_head": head}
				if decision == "rejected" {
					values["reason"] = "needs revision"
				}
				data, _ := json.Marshal(values)
				return daemon.operatorDecision(t.Context(), api.Request{Ticket: string(ticket.Ref.Ticket), Parameters: data}, domain.OperatorIdentity{UID: 501}, decision)
			}
			for _, head := range []any{nil, "", "abcdef", strings.Repeat("E", 40), 42, strings.Repeat("a", 40)} {
				response := request(head)
				if response.OK || response.Mutation.Attempted {
					t.Fatalf("accepted %v: %+v", head, response)
				}
				current, err := daemon.store.Ticket(t.Context(), ticket.Ref)
				if err != nil || current.Version != ticket.Version || current.State != domain.StateWaitingApproval {
					t.Fatalf("refusal mutated ticket: %+v %v", current, err)
				}
			}
			response := request(candidate.Snapshot.HeadSHA)
			if !response.OK || !response.Mutation.Attempted {
				t.Fatalf("exact head refused: %+v", response)
			}
		})
	}
}
