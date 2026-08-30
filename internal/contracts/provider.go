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

// RuntimeBinding is re-probed immediately before a paid invocation. Its
// digests are opaque SHA-256 values; credentials themselves never cross this
// interface or enter SQLite.
type RuntimeBinding struct {
	Identity      domain.ProviderIdentity
	BinaryDigest  string
	PolicyDigest  string
	FixtureDigest string
	AuthDigest    string
}

// DrainResult is supplied by the process supervisor after cancellation or
// recovery. A false value keeps the durable claim quarantined.
type DrainResult struct{ Drained bool }

type Provider interface {
	Name() string
	Probe(context.Context) (domain.ProviderIdentity, error)
	Binding(context.Context) (RuntimeBinding, error)
	Run(context.Context, PhaseInput) (PhaseResult, error)
	Drain(context.Context) (DrainResult, error)
}
