// Package domain contains the implementation-neutral identities and states
// shared by the CLI, daemon, workflow engine, providers, Git, and GitHub.
package domain

import "fmt"

type Channel string

const (
	ChannelStable Channel = "stable"
	ChannelDev    Channel = "dev"
)

func (c Channel) Valid() bool { return c == ChannelStable || c == ChannelDev }

type TicketID string
type ProjectID string

type TicketType string

const (
	TicketBug            TicketType = "bug"
	TicketFeature        TicketType = "feature"
	TicketRefactor       TicketType = "refactor"
	TicketInfrastructure TicketType = "infrastructure"
	TicketDocumentation  TicketType = "documentation"
	TicketSpike          TicketType = "spike"
)

func (t TicketType) Valid() bool {
	switch t {
	case TicketBug, TicketFeature, TicketRefactor, TicketInfrastructure, TicketDocumentation, TicketSpike:
		return true
	default:
		return false
	}
}

type MergeMode string

const (
	MergeManual     MergeMode = "manual"
	MergeGuarded    MergeMode = "guarded"
	MergeAutonomous MergeMode = "autonomous"
)

func (m MergeMode) Valid() bool {
	return m == MergeManual || m == MergeGuarded || m == MergeAutonomous
}

type State string

const (
	StateQueued             State = "queued"
	StatePlanning           State = "planning"
	StateVerifying          State = "verifying"
	StateBuilding           State = "building"
	StatePublishing         State = "publishing"
	StateWaitingCI          State = "waiting_ci"
	StateReviewing          State = "reviewing"
	StateWaitingApproval    State = "waiting_approval"
	StateWaitingManualMerge State = "waiting_manual_merge"
	StateMerging            State = "merging"
	StateReconciling        State = "reconciling"
	StateStopping           State = "stopping"
	StateCancelling         State = "cancelling"
	StatePaused             State = "paused"
	StateBlocked            State = "blocked"
	StateDone               State = "done"
	StateExternalMerged     State = "external_merged"
	StateCancelled          State = "cancelled"
)

func (s State) Terminal() bool {
	return s == StateDone || s == StateExternalMerged || s == StateCancelled
}

func AllStates() []State {
	return []State{
		StateQueued,
		StatePlanning,
		StateVerifying,
		StateBuilding,
		StatePublishing,
		StateWaitingCI,
		StateReviewing,
		StateWaitingApproval,
		StateWaitingManualMerge,
		StateMerging,
		StateReconciling,
		StateStopping,
		StateCancelling,
		StatePaused,
		StateBlocked,
		StateDone,
		StateExternalMerged,
		StateCancelled,
	}
}

func (s State) Valid() bool {
	for _, candidate := range AllStates() {
		if s == candidate {
			return true
		}
	}
	return false
}

type Phase string

const (
	PhasePlanning     Phase = "planning"
	PhaseVerification Phase = "verification"
	PhaseBuild        Phase = "build"
	PhasePublish      Phase = "publish"
	PhaseReview       Phase = "review"
	PhaseMerge        Phase = "merge"
	PhaseReconcile    Phase = "reconcile"
)

type TicketRef struct {
	Channel Channel
	Project ProjectID
	Ticket  TicketID
}

func (r TicketRef) Validate() error {
	if !r.Channel.Valid() {
		return fmt.Errorf("invalid channel %q", r.Channel)
	}
	if r.Project == "" {
		return fmt.Errorf("project is required")
	}
	if r.Ticket == "" {
		return fmt.Errorf("ticket is required")
	}
	return nil
}

type CandidateSnapshot struct {
	Generation               uint64
	BaseSHA                  string
	HeadSHA                  string
	TreeSHA                  string
	SourceDigest             string
	VerificationIntentDigest string
	ProofDigest              string
	CommandPolicyDigest      string
}

type Fence struct {
	LeaderEpoch uint64
	RunnerEpoch uint64
	ClaimEpoch  uint64
}

// ExternalEffectClaim is the opaque-to-adapters proof that SQLite granted one
// current executor permission to cross an external mutation boundary.
type ExternalEffectClaim struct {
	SemanticKey   string
	Ref           TicketRef
	Kind          string
	RequestDigest string
	TicketVersion uint64
	LeaderEpoch   uint64
	RunnerEpoch   uint64
	ClaimEpoch    uint64
}

type MergeAuthorization struct {
	ReviewedHead string
	CurrentHead  string
	Approved     bool
	GatesGreen   bool
}

type ProviderIdentity struct {
	Provider string
	Model    string
	Family   string
	Version  string
}

type OperatorIdentity struct {
	UID      uint32
	Username string
	Label    string
}

type NextAction struct {
	Code string
	Argv []string
}
