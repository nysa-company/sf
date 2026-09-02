package workflowruntime

import (
	"context"
	"errors"
	"testing"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/phaseartifact"
	"github.com/nysa-company/sf/internal/store"
	"github.com/nysa-company/sf/internal/workflowprompt"
	"github.com/nysa-company/sf/internal/workflowworker"
)

func TestRepositoryMaterializerMissingStoreFailsClosed(t *testing.T) {
	var materializer RepositoryMaterializer
	request := workflowworker.PhaseRequest{Phase: "verification"}
	if _, err := materializer.MaterializeVerificationCheckpoint(context.Background(), request, phaseartifact.Verification{}, store.ProviderAttemptResultKey{}); !errors.Is(err, ErrRepositoryMaterialization) {
		t.Fatalf("verification materializer error=%v", err)
	}
	if err := materializer.AuthenticateVerificationCheckpoint(context.Background(), request, phaseartifact.Verification{}, workflowworker.VerificationCheckpoint{}); !errors.Is(err, ErrRepositoryMaterialization) {
		t.Fatalf("verification authenticator error=%v", err)
	}
	request.Phase = "build"
	if _, err := materializer.MaterializeCandidate(context.Background(), request, workflowprompt.PlanIdentity{}, workflowprompt.VerificationIdentity{}, phaseartifact.Builder{}, store.ProviderAttemptResultKey{}); !errors.Is(err, ErrRepositoryMaterialization) {
		t.Fatalf("candidate materializer error=%v", err)
	}
	if err := materializer.AuthenticateCandidate(context.Background(), request, workflowprompt.PlanIdentity{}, workflowprompt.VerificationIdentity{}, phaseartifact.Builder{}, workflowworker.CandidateWitness{}); !errors.Is(err, ErrRepositoryMaterialization) {
		t.Fatalf("candidate authenticator error=%v", err)
	}
}

func TestVerificationAmendmentCheckpointDigestIgnoresRecoveryFence(t *testing.T) {
	amendment := &store.VerificationAmendment{
		TransitionTicketVersion: 7,
		ConsumedVersion:         6,
		Fence:                   domain.Fence{LeaderEpoch: 11, RunnerEpoch: 4},
		Prior:                   store.VerificationRevision{Revision: 2, IntentDigest: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", ProofDigest: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", CheckpointID: "cccccccccccccccccccccccccccccccccccccccc"},
		BuilderResult:           store.ProviderAttemptResultKey{AttemptID: 3, Ref: domain.TicketRef{Channel: domain.ChannelDev, Project: "p", Ticket: "SF-1"}, Phase: domain.PhaseBuild, Attempt: 1},
		BuilderTypedSHA256:      "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
		ProposedDigest:          "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
		ProposedCommand:         []string{"go", "test", "./..."},
		Reason:                  "replace protected proof",
		Requester:               "builder",
		BudgetRequestID:         "verification-amendment/3/digest",
	}
	request := workflowworker.PhaseRequest{Ticket: store.Ticket{Ref: amendment.BuilderResult.Ref, Version: 7, RunnerEpoch: 4}, Fence: amendment.Fence, Worktree: store.StoredWorktree{Path: "/worktree", Branch: "dev/p/SF-1", IdentityJSON: []byte(`{"repository":"/repo"}`), BaseSHA: "ffffffffffffffffffffffffffffffffffffffff"}, Amendment: amendment}
	provider := store.ProviderAttemptResultKey{AttemptID: 4, Ref: request.Ticket.Ref, Phase: domain.PhaseVerification, Attempt: 2}
	command := contracts.RepositoryCommandResultKey{SemanticKey: "command", ClaimEpoch: 1}
	artifact := phaseartifact.Verification{Schema: "sf.verification/v1", AcceptanceDigest: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", ProofKind: phaseartifact.ProofAcceptance, OwnedFiles: []string{"internal"}, Command: []string{"go", "test", "./..."}, PrebuildOutcome: "red", EvidenceDigest: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"}
	stable := verificationAmendmentCheckpointCommitDigest(request, provider, command, "result", artifact)
	request.Ticket.Version, request.Ticket.RunnerEpoch = 9, 6
	request.Fence = domain.Fence{LeaderEpoch: 13, RunnerEpoch: 6}
	if recovered := verificationAmendmentCheckpointCommitDigest(request, provider, command, "result", artifact); recovered != stable {
		t.Fatalf("recovery changed amendment checkpoint digest: %q != %q", recovered, stable)
	}
}

func TestCandidatePlanScopeRequiresExactNonEmptyPersistedScope(t *testing.T) {
	base := phaseartifact.Planner{Schema: "sf.planner/v1", Acceptance: []string{"accept"}, Proof: phaseartifact.ProofPlan{Kind: phaseartifact.ProofAcceptance, Command: []string{"go", "test", "./..."}, Details: "details"}, Paths: []string{"src"}, Commands: [][]string{{"go", "test", "./..."}}, Risks: []string{"none"}}
	supplied, err := workflowprompt.NewPlanIdentity(base)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := candidatePlanScope(base, supplied); err != nil || len(got) != 1 || got[0] != "src" {
		t.Fatalf("valid scope=%v err=%v", got, err)
	}
	for name, stored := range map[string]phaseartifact.Planner{
		"empty":   {Schema: base.Schema, Acceptance: base.Acceptance, Proof: base.Proof, Commands: base.Commands, Risks: base.Risks},
		"dot":     {Schema: base.Schema, Acceptance: base.Acceptance, Proof: base.Proof, Paths: []string{"."}, Commands: base.Commands, Risks: base.Risks},
		"invalid": {Schema: base.Schema, Acceptance: base.Acceptance, Proof: base.Proof, Paths: []string{"../outside"}, Commands: base.Commands, Risks: base.Risks},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := candidatePlanScope(stored, supplied); !errors.Is(err, ErrRepositoryMaterialization) {
				t.Fatalf("scope=%+v err=%v", stored.Paths, err)
			}
		})
	}
	tampered := supplied
	tampered.Plan.Paths = []string{"docs"}
	if _, err := candidatePlanScope(base, tampered); !errors.Is(err, ErrRepositoryMaterialization) {
		t.Fatalf("tampered plan accepted: %v", err)
	}
}

func TestCandidateChangedFilesMustBeSubsetOfPlanScope(t *testing.T) {
	if err := candidateChangedFilesWithinScope([]string{"src/main.go"}, []string{"src"}); err != nil {
		t.Fatalf("valid subset rejected: %v", err)
	}
	if err := candidateChangedFilesWithinScope([]string{"docs/readme.md"}, []string{"src"}); !errors.Is(err, ErrRepositoryMaterialization) {
		t.Fatalf("path escalation accepted: %v", err)
	}
	if err := candidateChangedFilesWithinScope([]string{"src/main.go"}, nil); !errors.Is(err, ErrRepositoryMaterialization) {
		t.Fatalf("empty scope accepted: %v", err)
	}
}
