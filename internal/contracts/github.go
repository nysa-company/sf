package contracts

import (
	"context"

	"github.com/nysa-company/sf/internal/domain"
)

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

type GitHub interface {
	AuthStatus(context.Context) error
	Repository(context.Context, RepositoryIdentity) (RepositoryIdentity, error)
	FindPullRequest(context.Context, PullRequestIdentity) (PullRequestIdentity, bool, error)
	CreateDraftPullRequest(context.Context, PullRequestIdentity, string, string, string) (PullRequestIdentity, error)
	UpdatePullRequest(context.Context, PullRequestIdentity, string, string) error
	RequiredChecks(context.Context, PullRequestIdentity) ([]RequiredCheck, error)
	MarkReady(context.Context, PullRequestIdentity, domain.Fence) error
	MergeExactHead(context.Context, PullRequestIdentity, string, string, domain.Fence) error
}
