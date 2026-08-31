package contracts

import (
	"context"
	"errors"

	"github.com/nysa-company/sf/internal/domain"
)

// ErrExternalCleanupUncertain means a supervised external process may still
// have descendants or output writers. Mutation guards must quarantine their
// gate until a supervisor supplies an unambiguous drained (not quarantined)
// proof.
var ErrExternalCleanupUncertain = errors.New("external process cleanup is uncertain")
var ErrExternalCleanupQuarantineFatal = errors.New("external cleanup quarantine could not be persisted")

type RepositoryIdentity struct {
	Host  string
	Owner string
	Name  string
}

type PullRequestIdentity struct {
	Repository     RepositoryIdentity
	Number         int
	HeadOwner      string
	HeadRepository string
	HeadRef        string
	HeadOID        string
	BaseRef        string
	// BaseOID is the exact remote protected-branch tip observed on the PR.
	// It is optional for non-merge identity lookups for compatibility, but a
	// guarded merge requires it and binds it to the reviewed authorization.
	BaseOID      string
	FactoryOwned bool
}

type RequiredCheck struct {
	Name       string
	ExternalID string
	State      string
}

// MergeBranchVerifier is implemented by a merge-proof coordinator after it
// arranges a freshly authorized protected-ref fetch and proves that the
// observed merge is reachable from a protected branch whose merge started at
// originalBaseOID. It is intentionally distinct from the claim-bound Git
// ProtectedBranchVerifier contract. The branch tip is expected to change as
// part of a successful merge, so BaseHeadOID is post-merge evidence, not a
// request to keep the old tip.
type MergeBranchVerifier interface {
	VerifyProtectedBranch(context.Context, RepositoryIdentity, string, string, string) (ProtectedBranchObservation, error)
}

type ProtectedBranchObservation struct {
	Repository  RepositoryIdentity
	BaseRef     string
	MergeCommit string
	// OriginalBaseOID is the sealed base witness carried from approval through
	// post-merge reconciliation.
	OriginalBaseOID string
	BaseHeadOID     string
	Contains        bool
}

// ExternalMutationGuard owns the final handoff from a durable effect claim to
// process start. Implementations must hold authorization through start and
// drain/invalidate it before pause, cancellation, takeover, or leader change.
// Adapters never substitute a second best-effort SQLite read for this guard.
type ExternalMutationGuard interface {
	RunExternalMutation(context.Context, domain.ExternalEffectClaim, func(context.Context) ([]byte, error)) ([]byte, error)
}

// MergeIntentRecorder persists structured merge evidence before any merge
// intent can be reconciled after a crash or a lost response.
type MergeIntentRecorder interface {
	RecordMergeIntent(context.Context, domain.MergeIntent) error
}

// MergeIntentObserver performs restart reconciliation from the complete
// durable witness and returns the observed immutable merge identity.
type MergeIntentObserver interface {
	ObserveMergeIntent(context.Context, domain.MergeIntent) (string, error)
}

// ExternalMutationQuarantiner durably blocks later launches after any
// supervised command reports uncertain cleanup, including read observations
// that occur before a durable mutation handoff.
type ExternalMutationQuarantiner interface {
	QuarantineExternalMutations(context.Context) error
}

type ExternalMutationQuarantineStatus interface {
	ExternalMutationsQuarantined(context.Context) (bool, error)
}

// ExternalMutationQuarantineAuthority is the complete durable quarantine
// capability. A write-only quarantine cannot make a restarted client fail
// closed, so production GitHub clients must require both operations together.
type ExternalMutationQuarantineAuthority interface {
	ExternalMutationQuarantiner
	ExternalMutationQuarantineStatus
}

type GitHub interface {
	AuthStatus(context.Context) error
	Repository(context.Context, RepositoryIdentity) (RepositoryIdentity, error)
	FindPullRequest(context.Context, PullRequestIdentity) (PullRequestIdentity, bool, error)
	CreateDraftPullRequest(context.Context, domain.ExternalEffectClaim, PullRequestIdentity, string, string) (PullRequestIdentity, error)
	UpdatePullRequest(context.Context, domain.ExternalEffectClaim, PullRequestIdentity, string, string) error
	RequiredChecks(context.Context, PullRequestIdentity) ([]RequiredCheck, error)
	MarkReady(context.Context, domain.ExternalEffectClaim, PullRequestIdentity) error
	MergeExactHead(context.Context, domain.ExternalEffectClaim, PullRequestIdentity, string, string, domain.MergeAuthorization) error
}

// DraftPullRequestObserver is the recovery-only publication lookup.  It
// inventories the exact source/base identity (including the factory ownership
// marker) and returns the current open/draft facts needed to settle a lost PR
// creation response.  A false result is a proven absence; implementations
// must return an error for foreign or ambiguous candidates.
type DraftPullRequestObserver interface {
	ObserveDraftPullRequest(context.Context, PullRequestIdentity) (identity PullRequestIdentity, state string, draft bool, found bool, err error)
}

// DraftPullRequestOutputObserver proves that the live factory PR still carries
// the exact title and marker-bearing body bound into the durable effect
// request. It is required before publication evidence or a state transition;
// identity/state/draft alone cannot witness an edit's requested output.
type DraftPullRequestOutputObserver interface {
	ObserveFactoryPullRequestOutput(context.Context, PullRequestIdentity, string, string) (identity PullRequestIdentity, state string, draft bool, applied bool, err error)
}

// DraftPullRequestRefresher is the correction-only continuity boundary. It
// identifies the already-owned PR by its durable number/source and returns
// the same PR after its branch head has advanced to expected. It must refuse
// foreign, missing, closed, or ambiguous rows.
type DraftPullRequestRefresher interface {
	RefreshFactoryPullRequestIdentity(context.Context, PullRequestIdentity, PullRequestIdentity) (PullRequestIdentity, error)
}

// DraftPullRequestCorrector is the correction-only mutation/recovery boundary.
// It retains the prior marker as an ownership witness while requiring the exact
// replacement identity and output before an edit effect may be confirmed.
// A non-applied result is a proven absence of the requested output, never an
// authorization to adopt a different pull request.
type DraftPullRequestCorrector interface {
	UpdateFactoryPullRequest(context.Context, domain.ExternalEffectClaim, PullRequestIdentity, PullRequestIdentity, string, string) error
	ObserveFactoryPullRequestUpdate(context.Context, PullRequestIdentity, PullRequestIdentity, string, string) (identity PullRequestIdentity, state string, draft bool, applied bool, err error)
}
