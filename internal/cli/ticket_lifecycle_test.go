package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/nysa-company/sf/internal/api"
	"github.com/nysa-company/sf/internal/domain"
)

func TestTicketStartFilePreservesSubmissionComposition(t *testing.T) {
	for _, outcome := range []string{"queued", "paused", "uncertain_submit", "uncertain_start", "changed_preview"} {
		t.Run(outcome, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "ticket.md")
			if err := os.WriteFile(path, []byte(ticketTemplate), 0600); err != nil {
				t.Fatal(err)
			}
			calls := []string{}
			a := newApp(fakeClient(func(_ context.Context, request api.Request) (api.Response, error) {
				calls = append(calls, request.Method)
				var parameters map[string]any
				if json.Unmarshal(request.Parameters, &parameters) != nil {
					t.Fatal("invalid parameters")
				}
				switch request.Method {
				case "ticket.submit":
					if parameters["project"] != "app" || parameters["source"] != ticketTemplate || parameters["new"] != false {
						t.Fatalf("submission parameters=%s", request.Parameters)
					}
					if outcome == "uncertain_submit" {
						return api.Response{}, errors.New("lost response")
					}
					if outcome == "paused" {
						return runTestResponse(domain.StatePaused, "ticket_submit", true), nil
					}
					return runTestResponse(domain.StateQueued, "ticket_submit", false), nil
				case "ticket.start":
					if request.Ticket != runTestID || parameters["accept_cost_estimates"] != true {
						t.Fatalf("start=%+v", request)
					}
					if outcome == "uncertain_start" {
						return api.Response{}, errors.New("lost response")
					}
					return runTestResponse(domain.StatePlanning, "ticket_start", false), nil
				default:
					t.Fatalf("file path invoked picker or extra mutation: %s", request.Method)
					return api.Response{}, nil
				}
			}), &bytes.Buffer{}, &bytes.Buffer{})
			if outcome == "changed_preview" {
				a.expectedDraftDigest = "different-preview-digest"
			}
			command := a.command()
			command.SetArgs([]string{"ticket", "start", "--file", path, "--project", "app", "--accept-cost-estimates", "--json"})
			if err := command.ExecuteContext(context.Background()); err != nil {
				t.Fatal(err)
			}
			if a.last == nil {
				t.Fatal("missing response")
			}
			wantCalls := 2
			if outcome == "paused" || outcome == "uncertain_submit" {
				wantCalls = 1
			} else if outcome == "changed_preview" {
				wantCalls = 0
			}
			if len(calls) != wantCalls || a.last.OK != (outcome == "queued") {
				t.Fatalf("calls=%v response=%+v", calls, a.last)
			}
			if outcome == "uncertain_start" && (a.last.Mutation.Identity != runTestID || a.last.Mutation.Kind != "ticket_run" || strings.Join(a.last.NextAction.Argv[1:], " ") != "ticket view "+runTestID) {
				t.Fatalf("partial submission identity lost: %+v", a.last)
			}
		})
	}
}

func TestTicketStartFileRequiresNonemptyFileAndProjectBeforeRequests(t *testing.T) {
	for _, args := range [][]string{{"--file", "draft.md"}, {"--file", "", "--project", "app"}, {"SF-1", "--file", "draft.md", "--project", "app"}} {
		calls := 0
		client := fakeClient(func(context.Context, api.Request) (api.Response, error) { calls++; return responseOK(), nil })
		argv := append([]string{"ticket", "start", "--json"}, args...)
		if code := Execute(context.Background(), argv, &bytes.Buffer{}, &bytes.Buffer{}, client); code != int(ExitInput) || calls != 0 {
			t.Fatalf("args=%v exit=%d calls=%d", args, code, calls)
		}
	}
}
