package cli

import (
	"testing"

	"github.com/nysa-company/sf/internal/api"
	"github.com/nysa-company/sf/internal/domain"
)

func TestPostbuildCommandFailureIsAnOperatorAction(t *testing.T) {
	response := api.Response{
		Version:   api.Version,
		RequestID: "postbuild-command-failure",
		Error:     &api.Error{Code: "postbuild_command_failed", Message: "cancel this ticket, then submit a fresh ticket"},
		NextAction: &domain.NextAction{
			Code: "postbuild_command_failed",
			Argv: []string{"sf-dev", "cancel", "SF-postbuild"},
		},
	}
	if err := validateCLIResponse(response); err != nil {
		t.Fatalf("response validation: %v", err)
	}
	if got := exitCode(response); got != ExitAction {
		t.Fatalf("exit=%d, want %d", got, ExitAction)
	}
}
