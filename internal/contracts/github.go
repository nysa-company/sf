package contracts

import (
	"context"
	"errors"

	"github.com/nysa-company/sf/internal/domain"
)

// ErrExternalCleanupUncertain means a supervised external process may still
// have descendants or output writers. Mutation guards must quarantine their
// gate until a supervisor supplies a drain or quarantine proof.
var ErrExternalCleanupUncertain = errors.New("external process cleanup is uncertain")

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
	FactoryOwned   bool
}

type RequiredCheck struct {
	Name       string
	ExternalID string
	State      string
}

// ProtectedBranchVerifier is implemented by the Git boundary after it freshly
// fetches the protected ref and proves merge-commit ancestry.
type ProtectedBranchVerifier interface {
	VerifyProtectedBranch(context.Context, RepositoryIdentity, string, string) (ProtectedBranchObservation, error)
}

type ProtectedBranchObservation struct {
	Repository  RepositoryIdentity
	BaseRef     string
	MergeCommit string
	BaseHeadOID string
	Contains    bool
}

// ExternalMutationGuard owns the final handoff from a durable effect claim to
// process start. Implementations must hold authorization through start and
// drain/invalidate it before pause, cancellation, takeover, or leader change.
// Adapters never substitute a second best-effort SQLite read for this guard.
type ExternalMutationGuard interface {
	RunExternalMutation(context.Context, domain.ExternalEffectClaim, func(context.Context) ([]byte, error)) ([]byte, error)
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
