package claudeprovider_test

import (
	"context"
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/claudeprovider"
	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/workflowprompt"
)

func TestAllWorkflowSchemasHaveClaudeProjection(t *testing.T) {
	for _, schema := range [][]byte{workflowprompt.PlannerSchema(), workflowprompt.VerificationSchema(), workflowprompt.BuilderSchema(), workflowprompt.ReviewerSchema()} {
		input := contracts.PhaseInput{Phase: domain.PhasePlanning, Worktree: "/private/fixture", Prompt: "fixture", Schema: schema, Timeout: time.Minute, Profile: contracts.ProfileGuarded, AuthMode: claudeprovider.AuthModeSubscription, Provider: domain.ProviderIdentity{Provider: "claude", Model: "claude-sonnet-5", Family: "anthropic-claude", Version: "2.1.263"}}
		if _, err := claudeprovider.Invocation(context.Background(), "/private/claude", "/private/auth", input); err != nil {
			t.Fatal(err)
		}
	}
}
