package contracts

import (
	"context"
	"time"

	"github.com/nysa-company/sf/internal/domain"
)

type ExecutionProfile string

const (
	ProfileGuarded    ExecutionProfile = "qualified_guarded"
	ProfileAutonomous ExecutionProfile = "autonomous_eligible"
)

type PhaseInput struct {
	Ticket       domain.TicketRef
	Phase        domain.Phase
	Prompt       string
	Repository   string
	Worktree     string
	AllowedPaths []string
	Provider     domain.ProviderIdentity
	Timeout      time.Duration
	Profile      ExecutionProfile
	Schema       []byte
}

type PhaseResult struct {
	Outcome      string
	Artifact     []byte
	Transcript   string
	Provider     domain.ProviderIdentity
	ChangedFiles []string
	UsageTrusted bool
	UsageUnits   int64
}

type Provider interface {
	Name() string
	Probe(context.Context) (domain.ProviderIdentity, error)
	Run(context.Context, PhaseInput) (PhaseResult, error)
}
