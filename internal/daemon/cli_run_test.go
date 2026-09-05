package daemon

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/api"
	"github.com/nysa-company/sf/internal/cli"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/store"
)

type lostRunResponseClient struct {
	client  cli.Client
	method  string
	dropped bool
}

func (c *lostRunResponseClient) Call(ctx context.Context, request api.Request) (api.Response, error) {
	response, err := c.client.Call(ctx, request)
	if err == nil && response.OK && request.Method == c.method && !c.dropped {
		c.dropped = true
		return api.Response{}, errors.New("fixture lost committed response")
	}
	return response, err
}

func TestCLIRunRealDaemonLostResponseDoesNotDuplicateWork(t *testing.T) {
	for _, method := range []string{"ticket.submit", "ticket.start"} {
		t.Run(method, func(t *testing.T) {
			d, paths, _ := testDaemon(t)
			path := writeTicket(t, t.TempDir(), "Lost run response")
			client := &lostRunResponseClient{client: cli.SocketClient{Path: paths.Socket, Timeout: 5 * time.Second}, method: method}
			var output, errorOutput strings.Builder
			code := cli.Execute(context.Background(), []string{"run", path, "--project", "demo", "--json"}, &output, &errorOutput, client)
			if code == 0 || !client.dropped {
				t.Fatalf("code=%d output=%s", code, output.String())
			}
			tickets, err := d.store.Tickets(context.Background(), domain.ChannelStable, "demo", 10)
			if err != nil || len(tickets) != 1 {
				t.Fatalf("tickets=%+v err=%v", tickets, err)
			}
			want := domain.StateQueued
			if method == "ticket.start" {
				want = domain.StatePlanning
			}
			if tickets[0].State != want {
				t.Fatalf("state=%s want=%s", tickets[0].State, want)
			}
			code, result, _ := executeCLI(t, context.Background(), paths, "run", path, "--project", "demo", "--json")
			if code != 0 {
				t.Fatalf("replay code=%d output=%s", code, result)
			}
			after, err := d.store.Tickets(context.Background(), domain.ChannelStable, "demo", 10)
			if err != nil || len(after) != 1 || after[0].Ref != tickets[0].Ref || after[0].Version != 2 || after[0].State != domain.StatePlanning {
				t.Fatalf("after=%+v err=%v", after, err)
			}
			events, err := d.store.Events(context.Background(), domain.ChannelStable, 0, 100)
			if err != nil {
				t.Fatal(err)
			}
			starts := 0
			for _, event := range events {
				if event.Trigger == "operator_start" {
					starts++
				}
			}
			if starts != 1 {
				t.Fatalf("starts=%d", starts)
			}
		})
	}
}

func TestCLIRunRealDaemonReusesExactTicketAndStart(t *testing.T) {
	d, paths, _ := testDaemon(t)
	path := writeTicket(t, t.TempDir(), "Run once")
	for i := 0; i < 2; i++ {
		code, output, errorOutput := executeCLI(t, context.Background(), paths, "run", path, "--project", "demo", "--json")
		var response api.Response
		if code != 0 || errorOutput != "" || json.Unmarshal([]byte(output), &response) != nil || !response.OK {
			t.Fatalf("iteration=%d code=%d output=%s error=%s", i, code, output, errorOutput)
		}
		if i == 1 && !response.Mutation.Observed {
			t.Fatal("second invocation was not a replay observation")
		}
	}
	tickets, err := d.store.Tickets(context.Background(), domain.ChannelStable, "demo", 10)
	if err != nil || len(tickets) != 1 || tickets[0].State != domain.StatePlanning || tickets[0].Version != 2 {
		t.Fatalf("tickets=%+v err=%v", tickets, err)
	}
	events, err := d.store.Events(context.Background(), domain.ChannelStable, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	starts := 0
	for _, event := range events {
		if event.Trigger == "operator_start" {
			starts++
		}
	}
	if starts != 1 {
		t.Fatalf("starts=%d", starts)
	}
}

func TestCLIRunRealDaemonPreservesSubmissionOnStartRefusal(t *testing.T) {
	d, paths, _ := testDaemon(t)
	d.doctor = func(context.Context, store.Project) error { return errors.New("fixture prerequisite unavailable") }
	path := writeTicket(t, t.TempDir(), "Preflight refusal")
	code, output, errorOutput := executeCLI(t, context.Background(), paths, "run", path, "--project", "demo", "--json")
	var response api.Response
	if code == 0 || errorOutput != "" || json.Unmarshal([]byte(output), &response) != nil || response.OK || response.Error.Code != "doctor_required" || !response.Mutation.Attempted || response.Mutation.Identity == "" {
		t.Fatalf("code=%d output=%s error=%s", code, output, errorOutput)
	}
	tickets, err := d.store.Tickets(context.Background(), domain.ChannelStable, "demo", 10)
	if err != nil || len(tickets) != 1 || tickets[0].State != domain.StateQueued || tickets[0].Version != 1 {
		t.Fatalf("tickets=%+v err=%v", tickets, err)
	}
	// Restoring readiness and repeating the same source must start that row,
	// not invent another identity after the earlier partial operation.
	d.doctor = nil
	code, output, _ = executeCLI(t, context.Background(), paths, "run", path, "--project", "demo", "--json")
	if code != 0 {
		t.Fatalf("retry code=%d output=%s", code, output)
	}
	after, err := d.store.Tickets(context.Background(), domain.ChannelStable, "demo", 10)
	if err != nil || len(after) != 1 || after[0].Ref != tickets[0].Ref || after[0].State != domain.StatePlanning {
		t.Fatalf("after=%+v err=%v", after, err)
	}
}
