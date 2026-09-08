package daemon

import (
	"strings"
	"testing"

	"github.com/nysa-company/sf/internal/api"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/store"
)

func TestPostbuildCommandFailureRequiresCancelAndFreshSubmission(t *testing.T) {
	for _, test := range []struct {
		channel domain.Channel
		binary  string
	}{
		{channel: domain.ChannelStable, binary: "sf"},
		{channel: domain.ChannelDev, binary: "sf-dev"},
	} {
		t.Run(string(test.channel), func(t *testing.T) {
			daemon := &Daemon{channel: test.channel}
			if !nonRecoverableTicketBlocker("postbuild_command_failed") {
				t.Fatal("post-build blocker is recoverable")
			}
			ticket := store.Ticket{Ref: domain.TicketRef{Channel: test.channel, Project: "nysa", Ticket: "SF-postbuild"}, State: domain.StateBlocked, ResumeState: domain.StateBuilding, BlockedCode: "postbuild_command_failed"}
			action, ok := daemon.ticketBlockedNextAction(ticket)
			if !ok || strings.Join(action.Argv, " ") != test.binary+" cancel SF-postbuild" {
				t.Fatalf("blocked next action=%+v ok=%t", action, ok)
			}
			response := daemon.failure(api.Request{Ticket: "SF-postbuild"}, "postbuild_command_failed", "cancel this ticket, then submit a fresh ticket", false)
			if response.Error == nil || response.Error.Retryable || response.NextAction == nil || strings.Join(response.NextAction.Argv, " ") != test.binary+" cancel SF-postbuild" {
				t.Fatalf("failure response=%+v", response)
			}
		})
	}
}
