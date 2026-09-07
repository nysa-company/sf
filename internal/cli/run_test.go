package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/api"
	"github.com/nysa-company/sf/internal/domain"
)

const runTestID = "SF-0123456789abcdef0123456789abcdef"

func runTestResponse(state domain.State, kind string, observed bool) api.Response {
	data, _ := json.Marshal(selectableTicket{ID: runTestID, Channel: domain.ChannelStable, Project: "app", State: state, Title: "Ticket"})
	identity := runTestID
	if kind == "ticket_start" {
		identity = "stable/app/" + runTestID + "/planning"
	}
	return api.Response{Version: api.Version, RequestID: "response", OK: true, Mutation: api.Mutation{Attempted: true, Kind: kind, Identity: identity, Observed: observed}, Data: data}
}

func executeRunTest(t *testing.T, client Client, watch bool, extra ...string) (api.Response, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "ticket.md")
	if err := os.WriteFile(path, []byte(ticketTemplate), 0600); err != nil {
		t.Fatal(err)
	}
	var output, errors bytes.Buffer
	a := newApp(client, &output, &errors)
	a.channel = domain.ChannelStable
	command := a.command()
	args := []string{"run", path, "--project", "app", "--json"}
	args = append(args, extra...)
	if watch {
		args = append(args, "--watch")
	}
	command.SetArgs(args)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := command.ExecuteContext(ctx); err != nil {
		t.Fatal(err)
	}
	if a.last == nil {
		t.Fatal("no result")
	}
	return *a.last, output.String()
}

func TestRunEstimateConsentIsExplicitAndOnlySentToQueuedStart(t *testing.T) {
	for _, state := range []domain.State{domain.StateQueued, domain.StatePlanning} {
		for _, consent := range []bool{false, true} {
			calls := 0
			var extra []string
			if consent {
				extra = []string{"--accept-cost-estimates"}
			}
			response, _ := executeRunTest(t, fakeClient(func(_ context.Context, request api.Request) (api.Response, error) {
				calls++
				var parameters map[string]any
				if err := json.Unmarshal(request.Parameters, &parameters); err != nil {
					t.Fatal(err)
				}
				value, exists := parameters["accept_cost_estimates"]
				if request.Method == "ticket.submit" {
					if exists {
						t.Fatal("consent applied at submission")
					}
					return runTestResponse(state, "ticket_submit", state != domain.StateQueued), nil
				}
				if request.Method != "ticket.start" || state != domain.StateQueued || request.Ticket != runTestID || exists != consent || consent && value != true {
					t.Fatal("start consent or target mismatch")
				}
				return runTestResponse(domain.StatePlanning, "ticket_start", false), nil
			}), false, extra...)
			wantCalls := 1
			if state == domain.StateQueued {
				wantCalls = 2
			}
			if !response.OK || calls != wantCalls {
				t.Fatalf("consent=%t state=%s calls=%d", consent, state, calls)
			}
		}
	}
}

func TestRunComposesExactSubmitStartWatch(t *testing.T) {
	calls := []string{}
	response, output := executeRunTest(t, fakeClient(func(_ context.Context, request api.Request) (api.Response, error) {
		calls = append(calls, request.Method)
		switch request.Method {
		case "ticket.submit":
			var parameters struct {
				Source  string `json:"source"`
				New     bool   `json:"new"`
				Project string `json:"project"`
			}
			if err := json.Unmarshal(request.Parameters, &parameters); err != nil || parameters.New || parameters.Project != "app" || parameters.Source != ticketTemplate {
				t.Fatalf("parameters=%+v err=%v", parameters, err)
			}
			return runTestResponse(domain.StateQueued, "ticket_submit", false), nil
		case "ticket.start":
			if request.Ticket != runTestID {
				t.Fatal("wrong start target")
			}
			return runTestResponse(domain.StatePlanning, "ticket_start", false), nil
		case "ticket.status":
			if request.Ticket != runTestID {
				t.Fatal("wrong watch target")
			}
			response := runTestResponse(domain.StateDone, "", false)
			response.Data, _ = json.Marshal(map[string]any{"ticket": json.RawMessage(response.Data)})
			return response, nil
		default:
			t.Fatalf("unexpected method %s", request.Method)
			return api.Response{}, nil
		}
	}), true)
	if !response.OK || len(calls) != 3 || bytes.Count([]byte(output), []byte("\n")) != 2 {
		t.Fatalf("response=%+v calls=%v output=%s", response, calls, output)
	}
}

func TestRunNeverRetriesUncertainMutation(t *testing.T) {
	for _, failAt := range []string{"ticket.submit", "ticket.start"} {
		t.Run(failAt, func(t *testing.T) {
			calls := 0
			response, _ := executeRunTest(t, fakeClient(func(_ context.Context, request api.Request) (api.Response, error) {
				calls++
				if request.Method == failAt {
					return api.Response{}, errors.New("lost response")
				}
				return runTestResponse(domain.StateQueued, "ticket_submit", false), nil
			}), false)
			want := 1
			if failAt == "ticket.start" {
				want = 2
			}
			if calls != want || response.OK || !response.Mutation.Attempted || response.NextAction == nil {
				t.Fatalf("calls=%d response=%+v", calls, response)
			}
		})
	}
}

func TestRunReplaysExistingTicketWithoutImplicitResume(t *testing.T) {
	for _, state := range []domain.State{domain.StatePlanning, domain.StatePaused, domain.StateBlocked, domain.StateDone} {
		calls := 0
		response, _ := executeRunTest(t, fakeClient(func(_ context.Context, request api.Request) (api.Response, error) {
			calls++
			if request.Method != "ticket.submit" {
				t.Fatal("replayed a mutation")
			}
			return runTestResponse(state, "ticket_submit", true), nil
		}), false)
		wantOK := state != domain.StatePaused && state != domain.StateBlocked
		if calls != 1 || response.OK != wantOK || !response.Mutation.Observed {
			t.Fatalf("state=%s response=%+v", state, response)
		}
	}
}

func TestRunRefusesMismatchedSubmissionBeforeStart(t *testing.T) {
	calls := 0
	response, _ := executeRunTest(t, fakeClient(func(_ context.Context, request api.Request) (api.Response, error) {
		calls++
		response := runTestResponse(domain.StateQueued, "ticket_submit", false)
		response.Mutation.Identity = "SF-other"
		return response, nil
	}), false)
	if calls != 1 || response.OK || response.Error.Code != "invalid_response" {
		t.Fatalf("calls=%d response=%+v", calls, response)
	}
}
