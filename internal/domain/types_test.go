package domain

import "testing"

func TestTerminalStates(t *testing.T) {
	for _, state := range []State{StateDone, StateExternalMerged, StateCancelled} {
		if !state.Terminal() {
			t.Fatalf("%s must be terminal", state)
		}
	}
	for _, state := range []State{StateQueued, StatePlanning, StatePaused, StateBlocked} {
		if state.Terminal() {
			t.Fatalf("%s must not be terminal", state)
		}
	}
}

func TestTicketRefValidation(t *testing.T) {
	valid := TicketRef{Channel: ChannelDev, Project: "nysa", Ticket: "SF-1"}
	if err := valid.Validate(); err != nil {
		t.Fatalf("valid ticket reference: %v", err)
	}

	for name, ref := range map[string]TicketRef{
		"channel": {Channel: "other", Project: "nysa", Ticket: "SF-1"},
		"project": {Channel: ChannelDev, Ticket: "SF-1"},
		"ticket":  {Channel: ChannelDev, Project: "nysa"},
	} {
		t.Run(name, func(t *testing.T) {
			if err := ref.Validate(); err == nil {
				t.Fatal("expected validation error")
			}
		})
	}
}
