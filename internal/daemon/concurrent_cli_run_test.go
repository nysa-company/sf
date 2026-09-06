package daemon

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/api"
	"github.com/nysa-company/sf/internal/cli"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/store"
	"github.com/nysa-company/sf/internal/transport"
)

// Real socket/CLI/Store admission, not provider-delivery evidence. Concurrent
// run requests must preserve the refused submission and exact replay identities.
func TestConcurrentCLIRunQueuesThirdWithoutDuplicatingStarts(t *testing.T) {
	d, paths, _ := testDaemon(t)
	files := []string{
		writeTicket(t, t.TempDir(), "Concurrent alpha"),
		writeTicket(t, t.TempDir(), "Concurrent beta"),
		writeTicket(t, t.TempDir(), "Concurrent gamma"),
	}
	type result struct {
		code   int
		output string
	}
	start := make(chan struct{})
	results := make(chan result, len(files))
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	for _, file := range files {
		go func() {
			<-start
			var out, errOut strings.Builder
			code := cli.Execute(ctx, []string{"run", file, "--project", "demo", "--json"}, &out, &errOut, cli.SocketClient{Path: paths.Socket, Timeout: 5 * time.Second})
			results <- result{code, out.String() + errOut.String()}
		}()
	}
	close(start)
	success, refused := 0, 0
	for range files {
		select {
		case got := <-results:
			if got.code == 0 {
				success++
			} else if strings.Contains(got.output, "capacity_unavailable") {
				refused++
			} else {
				t.Fatalf("unexpected run result: %+v", got)
			}
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	if success != 2 || refused != 1 {
		t.Fatalf("success=%d refused=%d", success, refused)
	}
	before, err := d.store.Tickets(ctx, domain.ChannelStable, "demo", 10)
	if err != nil || len(before) != 3 {
		t.Fatalf("tickets=%+v err=%v", before, err)
	}
	planning, queued := 0, 0
	for _, ticket := range before {
		switch ticket.State {
		case domain.StatePlanning:
			planning++
		case domain.StateQueued:
			queued++
		default:
			t.Fatalf("unexpected ticket: %+v", ticket)
		}
	}
	if planning != 2 || queued != 1 {
		t.Fatalf("planning=%d queued=%d", planning, queued)
	}
	for _, file := range files {
		// Replay every original input, including the capacity-refused one.
		executeCLI(t, ctx, paths, "run", file, "--project", "demo", "--json")
	}
	after, err := d.store.Tickets(ctx, domain.ChannelStable, "demo", 10)
	if err != nil || len(after) != 3 {
		t.Fatalf("replay tickets=%+v err=%v", after, err)
	}
	for _, ticket := range before {
		current, err := d.store.Ticket(ctx, ticket.Ref)
		if err != nil || current.Version != ticket.Version || current.RunnerEpoch != ticket.RunnerEpoch || current.State != ticket.State {
			t.Fatalf("replay changed ticket: before=%+v after=%+v err=%v", ticket, current, err)
		}
	}
	events, err := d.store.Events(ctx, domain.ChannelStable, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	starts := 0
	for _, event := range events {
		if event.Trigger == "operator_start" {
			starts++
		}
	}
	if starts != 2 {
		t.Fatalf("start events=%d, want exactly two", starts)
	}
	leases, err := d.store.Leases(ctx, domain.ChannelStable)
	if err != nil || len(leases) != 4 {
		t.Fatalf("leases=%+v err=%v", leases, err)
	}
	// Hold the drain boundary closed: a cancellation request must not free
	// capacity while the prior runtime might still be writing.
	var target, sibling, waiting domain.TicketRef
	for _, ticket := range before {
		if ticket.State == domain.StateQueued {
			waiting = ticket.Ref
		} else if target.Ticket == "" {
			target = ticket.Ref
		} else {
			sibling = ticket.Ref
		}
	}
	drained := false
	d.control = testRuntimeController{drain: func(_ context.Context, ref domain.TicketRef) (bool, error) {
		if ref != target {
			t.Errorf("drained wrong ticket: %v", ref)
		}
		return drained, nil
	}}
	if response := daemonControl(d, target.Ticket, "cancel"); response.OK {
		t.Fatal("cancel succeeded before drain")
	}
	if code, _, _ := executeCLI(t, ctx, paths, "start", string(waiting.Ticket), "--json"); code == 0 {
		t.Fatal("third started before drain")
	}
	leases, err = d.store.Leases(ctx, domain.ChannelStable)
	if err != nil || len(leases) != 4 {
		t.Fatalf("undrained leases=%+v err=%v", leases, err)
	}
	drained = true
	if response := daemonControl(d, target.Ticket, "cancel"); !response.OK {
		t.Fatalf("cancel after drain: %+v", response)
	}
	if code, out, _ := executeCLI(t, ctx, paths, "start", string(waiting.Ticket), "--json"); code != 0 {
		t.Fatalf("queued start after drain: %s", out)
	}
	for _, previous := range before {
		if previous.Ref != sibling {
			continue
		}
		current, err := d.store.Ticket(ctx, sibling)
		if err != nil || current.State != previous.State || current.Version != previous.Version || current.RunnerEpoch != previous.RunnerEpoch {
			t.Fatalf("cancel altered sibling: %+v err=%v", current, err)
		}
	}
	leases, err = d.store.Leases(ctx, domain.ChannelStable)
	if err != nil || len(leases) != 4 {
		t.Fatalf("reassigned leases=%+v err=%v", leases, err)
	}
	for _, lease := range leases {
		if lease.Ref == target {
			t.Fatalf("cancelled target retained lease: %+v", lease)
		}
	}
}

func TestConcurrentTicketCapacitySurvivesTwoDaemonRestarts(t *testing.T) {
	d, paths, stop := testDaemon(t)
	ctx := t.Context()
	first := createAndStartControlTicket(t, d, "SF-restart-concurrent-a")
	second := createAndStartControlTicket(t, d, "SF-restart-concurrent-b")
	waiting := domain.TicketRef{Channel: d.channel, Project: "demo", Ticket: "SF-restart-concurrent-c"}
	if err := d.store.CreateTicket(ctx, store.Ticket{Ref: waiting, SourceDigest: "restart-concurrent-c", Type: domain.TicketBug, MergeMode: domain.MergeGuarded}); err != nil {
		t.Fatal(err)
	}
	original, err := d.store.Leases(ctx, d.channel)
	if err != nil || len(original) != 4 {
		t.Fatalf("leases=%+v err=%v", original, err)
	}
	stop()
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	previous := []store.Ticket{first, second}
	for restart := 0; restart < 2; restart++ {
		next, err := Start(ctx, Config{Channel: domain.ChannelStable, Paths: paths, DaemonIdentity: "concurrent-restart"})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = next.Close() })
		for index, before := range previous {
			after, err := next.store.Ticket(ctx, before.Ref)
			if err != nil || after.Version != before.Version+1 || after.RunnerEpoch != before.RunnerEpoch+1 || after.State != before.State {
				t.Fatalf("restart=%d before=%+v after=%+v err=%v", restart, before, after, err)
			}
			previous[index] = after
		}
		leases, err := next.store.Leases(ctx, domain.ChannelStable)
		if err != nil || len(leases) != 4 {
			t.Fatalf("restart leases=%+v err=%v", leases, err)
		}
		for _, lease := range leases {
			matched := false
			for _, old := range original {
				if lease.Ref == old.Ref && lease.Scope == old.Scope && lease.ScopeKey == old.ScopeKey && lease.AcquiredAt.Equal(old.AcquiredAt) && lease.RunnerEpoch == old.RunnerEpoch+uint64(restart)+1 {
					matched = true
				}
			}
			if !matched {
				t.Fatalf("unrecognized recovered lease: %+v", lease)
			}
		}
		response := next.Handle(ctx, transport.Peer{UID: uint32(os.Getuid())}, api.Request{Version: api.Version, RequestID: "restart-third", Method: "ticket.start", Ticket: string(waiting.Ticket), Parameters: []byte(`{"channel":"stable","project":"demo"}`)})
		if response.OK || response.Error == nil || response.Error.Code != "capacity_unavailable" {
			t.Fatalf("third admission after restart: %+v error=%+v", response, response.Error)
		}
		queued, err := next.store.Ticket(ctx, waiting)
		if err != nil || queued.State != domain.StateQueued || queued.Version != 1 {
			t.Fatalf("queued mutated: %+v err=%v", queued, err)
		}
		if err := next.Close(); err != nil {
			t.Fatal(err)
		}
	}
}
