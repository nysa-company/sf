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

// ProtectedBranchVerifier is implemented by the Git boundary after it freshly
// fetches the protected ref and proves that the observed merge is reachable
// from a protected branch whose merge started at originalBaseOID.  The branch
// tip is expected to change as part of a successful merge, so BaseHeadOID is
// evidence of the post-merge observation, not a request to keep the old tip.
type ProtectedBranchVerifier interface {
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
