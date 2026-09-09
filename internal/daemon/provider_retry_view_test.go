package daemon

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"

	"github.com/nysa-company/sf/internal/api"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/store"
	"github.com/nysa-company/sf/internal/transport"
)

func TestProviderRetryViewOnlyProjectsAuthenticatedActions(t *testing.T) {
	for _, tc := range []struct {
		name        string
		disposition store.ProviderRetryDisposition
		err         error
		command     string
	}{
		{"eligible", store.ProviderRetryEligible, nil, "retry"},
		{"exhausted", store.ProviderRetryExhausted, nil, "cancel"},
		{"resubmit", store.ProviderRetryResubmissionRequired, nil, "cancel"},
		{"ordinary", store.ProviderRetryNotProvider, nil, ""},
		{"malformed", store.ProviderRetryEligible, errors.New("private-secret-marker"), ""},
		{"unknown", store.ProviderRetryDisposition(255), nil, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ticket := store.Ticket{Ref: domain.TicketRef{Channel: domain.ChannelDev, Project: "demo", Ticket: "SF-retry-view"}, State: domain.StatePaused}
			view := map[string]any{"recovery": recoveryView(ticket, nil)}
			if tc.err != nil {
				view["next_action"] = domain.NextAction{Code: "guessed_resume", Argv: []string{"sf-dev", "resume", string(ticket.Ref.Ticket)}}
			}
			projectProviderRetry(t.Context(), ticket, view, "sf-dev", func(context.Context, store.Ticket) (store.ProviderRetryDisposition, error) {
				return tc.disposition, tc.err
			})
			action, found := view["next_action"].(domain.NextAction)
			if tc.command == "" {
				if found {
					t.Fatal("invented action")
				}
			} else if !found || strings.Join(action.Argv, " ") != "sf-dev "+tc.command+" SF-retry-view" {
				t.Fatalf("action=%+v", action)
			}
			raw, err := json.Marshal(view)
			if err != nil || strings.Contains(string(raw), "private-secret-marker") || view["recovery"].(map[string]any)["writer_safety"] != "not_checked" {
				t.Fatalf("unsafe projection: %s %v", raw, err)
			}
		})
	}
}

func TestProviderRetryViewStatusShowAndList(t *testing.T) {
	for _, mode := range []string{"eligible", "exhausted", "malformed", "wrong_schema", "missing_schema"} {
		t.Run(mode, func(t *testing.T) {
			malformed := mode == "malformed" || mode == "wrong_schema" || mode == "missing_schema"
			d, paths, _ := testDaemon(t)
			started := createAndStartControlTicket(t, d, "SF-retry-view")
			if mode == "exhausted" {
				seedDaemonProviderRetryEpoch(t, d, started, true)
			} else {
				seedDaemonProviderExhaustion(t, d, started)
			}
			before, err := d.store.Ticket(t.Context(), started.Ref)
			if err != nil {
				t.Fatal(err)
			}
			if malformed {
				writer, err := sql.Open("sqlite", paths.Database)
				if err != nil {
					t.Fatal(err)
				}
				payload := "json_set(payload,'$.phase','build')"
				switch mode {
				case "wrong_schema":
					payload = "json_set(payload,'$.schema','unsupported-schema')"
				case "missing_schema":
					payload = "json_remove(payload,'$.schema')"
				}
				result, err := writer.ExecContext(t.Context(), `UPDATE events SET payload=`+payload+` WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version=? AND trigger='retry_or_correction_exhausted'`, before.Ref.Channel, before.Ref.Project, before.Ref.Ticket, before.Version)
				closeErr := writer.Close()
				if err != nil || closeErr != nil {
					t.Fatalf("damage fixture: %v %v", err, closeErr)
				}
				if rows, err := result.RowsAffected(); err != nil || rows != 1 {
					t.Fatalf("damage fixture rows=%d err=%v", rows, err)
				}
			}
			for _, kind := range []string{"show", "status", "list"} {
				method, selected := "ticket.status", string(before.Ref.Ticket)
				parameters := json.RawMessage(`{"channel":"stable","project":"demo","watch":false}`)
				if kind == "show" {
					method = "ticket.show"
					parameters = json.RawMessage(`{"channel":"stable","project":"demo","ticket":"SF-retry-view"}`)
				}
				if kind == "list" {
					selected = ""
				}
				response := d.Handle(t.Context(), transport.Peer{UID: uint32(os.Getuid())}, api.Request{Version: api.Version, RequestID: "retry-view-" + kind, Method: method, Ticket: selected, OperatorLabel: "operator", Parameters: parameters})
				if !response.OK || response.Mutation.Attempted {
					t.Fatalf("%s response=%+v", kind, response)
				}
				data := string(response.Data)
				if malformed {
					if strings.Contains(data, `"next_action"`) || !strings.Contains(data, "eligibility could not be authenticated") {
						t.Fatalf("%s malformed projection=%s", kind, data)
					}
				} else {
					code, command := "provider_retry", "retry"
					if mode == "exhausted" {
						code, command = "provider_retry_exhausted", "cancel"
					}
					want := `"next_action":{"code":"` + code + `","argv":["sf","` + command + `","SF-retry-view"]}`
					if !strings.Contains(data, want) || !strings.Contains(data, `"writer_safety":"not_checked"`) {
						t.Fatalf("%s projection=%s", kind, data)
					}
				}
			}
			after, err := d.store.Ticket(t.Context(), before.Ref)
			if err != nil || after.Version != before.Version || after.RunnerEpoch != before.RunnerEpoch || after.State != before.State {
				t.Fatal("status mutated ticket")
			}
		})
	}
}
