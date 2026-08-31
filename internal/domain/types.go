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
	// CommandPolicyDigest remains an asserted candidate identity. v31 does not
	// invent a second authority for it: the later proof-result admission path
	// is responsible for binding command execution to this digest.
	CommandPolicyDigest string
	// BuilderEvidenceDigest is the canonical digest of the authenticated
	// Builder artifact which produced this exact candidate.
	BuilderEvidenceDigest string
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
	ReviewedHead        string
	CurrentHead         string
	ReviewedBaseSHA     string
	CurrentBaseSHA      string
	ReviewedBaseHeadOID string
	CurrentBaseHeadOID  string
	Approved            bool
	GatesGreen          bool
}

// MergeIntent is durable reconciliation evidence, not merely a hash of a
// request.  It records the original protected-base witness selected at review
// time so a restart can distinguish an observed manual merge from a different
// branch history.
type MergeIntent struct {
	Ref               TicketRef
	SemanticKey       string
	RequestDigest     string
	TicketVersion     uint64
	LeaderEpoch       uint64
	RunnerEpoch       uint64
	ClaimEpoch        uint64
	RepositoryHost    string
	RepositoryOwner   string
	RepositoryName    string
	PullRequestNumber int
	// HeadOwner, HeadRepository, and HeadRef bind the complete source identity
	// that GitHub reports for the reviewed pull request. A PR number and head
	// object alone are not a restart-safe source witness: a repository can
	// retain a number while its source metadata changes.
	HeadOwner        string
	HeadRepository   string
	HeadRef          string
	HeadOID          string
	BaseRef          string
	OriginalBaseOID  string
	ProtectionRuleID string
	// ProtectionKind records whether the witnessed merge gate was classic
	// branch protection or an exact repository ruleset. Empty is retained only
	// for pre-v30 classic rows.
	ProtectionKind         string
	ProtectionChecksDigest string
	StrictStatusChecks     bool
	AdminEnforced          bool
	ActiveRulesetCount     uint32
	Method                 string
}

// ValidateProtectionWitness keeps the persisted protection shape explicit.
// The detailed repository and merge fields are validated at their boundaries;
// this method is deliberately limited to the witness that distinguishes a
// classic rule from a repository ruleset.
func (intent MergeIntent) ValidateProtectionWitness() error {
	if intent.ActiveRulesetCount == 0 {
		if (intent.ProtectionKind == "" || intent.ProtectionKind == "classic") && intent.ProtectionChecksDigest == "" {
			return nil
		}
		return fmt.Errorf("invalid classic protection witness")
	}
	if intent.ActiveRulesetCount != 1 || intent.ProtectionKind != "ruleset" || len(intent.ProtectionChecksDigest) != 64 {
		return fmt.Errorf("invalid ruleset protection witness")
	}
	for _, value := range intent.ProtectionChecksDigest {
		if !(value >= '0' && value <= '9' || value >= 'a' && value <= 'f') {
			return fmt.Errorf("invalid ruleset protection checks digest")
		}
	}
	return nil
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
	Code string   `json:"code"`
	Argv []string `json:"argv"`
}
