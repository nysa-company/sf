package store

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/phaseartifact"
)

// This fixture uses the normal initial lifecycle (despite the older helper's
// name): Planner, independent verification command/checkpoint, then Building.
// It does not insert synthetic provider results or rewrite ticket counters.
func postbuildFailureFixture(t *testing.T, exit int) (*Store, context.Context, PostbuildFailureRequest) {
	t.Helper()
	db, ctx, leader, ticket := operatorSourceResumeBuildingFixture(t)
	fence := domain.Fence{LeaderEpoch: leader, RunnerEpoch: ticket.RunnerEpoch}
	binding, _ := setupProviderPair(t, db, ctx)
	verification, err := db.CurrentVerification(ctx, ticket.Ref)
	if err != nil {
		t.Fatal(err)
	}
	claim, err := db.BeginProviderAttempt(ctx, supervisedOperatorSource(t, db, ctx, ProviderAttemptRequest{Ref: ticket.Ref, ExpectedVersion: ticket.Version, Fence: fence, Phase: domain.PhaseBuild, Role: "builder", Binding: runtime(binding), ConfigDigest: ticket.ConfigDigest, Capacity: 1, At: time.Now().UTC()}))
	if err != nil {
		t.Fatal(err)
	}
	if err := db.RecordProviderLaunch(ctx, claim, contracts.ProviderLaunch{PID: int(claim.ID), PGID: int(claim.ID), BootIdentity: "postbuild-failure", ProcessStartIdentity: fmt.Sprintf("postbuild-%d", claim.ID), Worktree: claim.Worktree}); err != nil {
		t.Fatal(err)
	}
	raw := []byte(`{"schema":"sf.builder/v1","summary":"implementation","changed_files":["src/fix.go"],"commands":[["go","test","./..."]]}`)
	if _, err := db.CompleteProviderAttemptSuccess(ctx, claim, proof(t, claim), ticket.Version, fence, contracts.PhaseResult{Provider: claim.Binding.Identity, Artifact: raw, UsageTrusted: true, UsageUnits: 1}, phaseartifact.Validation{TicketType: ticket.Type, ProtectedVerification: verification.Revision.OwnedFiles}, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	key := ProviderAttemptResultKey{Ref: ticket.Ref, Phase: domain.PhaseBuild, AttemptID: claim.ID, Attempt: claim.Attempt}
	command := completeEvidenceRepositoryCommand(t, db, ctx, RepositoryCommandPurposePostbuildCandidate, ticket.Ref, ticket.Version, fence, key, verification.Revision.IntentDigest, verification.Revision.ProofDigest, verification.Checkpoint.CommitOID, "", exit)
	return db, ctx, PostbuildFailureRequest{Ref: ticket.Ref, ExpectedVersion: ticket.Version, Fence: fence, BuilderResult: key, CommandResult: command}
}

func TestAuthenticatePostbuildFailureExactReadOnlyProof(t *testing.T) {
	db, ctx, request := postbuildFailureFixture(t, 1)
	before, err := db.Ticket(ctx, request.Ref)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		got, err := db.AuthenticatePostbuildFailure(ctx, request)
		if err != nil {
			t.Fatal(err)
		}
		if got.Ref != request.Ref || got.TicketVersion != request.ExpectedVersion || got.Fence != request.Fence || got.BuilderResult != request.BuilderResult || got.CommandResult.Key != request.CommandResult || got.CommandResult.Result.ExitCode != 1 || got.BuilderTypedDigest == "" || got.Verification.Checkpoint.CommitOID == "" {
			t.Fatal("proof lost an exact immutable binding")
		}
	}
	after, err := db.Ticket(ctx, request.Ref)
	if err != nil || after.State != before.State || after.Version != before.Version || after.RunnerEpoch != before.RunnerEpoch {
		t.Fatal("proof reader changed ticket authority")
	}
}

func TestAuthenticatePostbuildFailureRefusals(t *testing.T) {
	for _, name := range []string{"successful_command", "wrong_ticket", "wrong_builder", "missing_command", "stale_version", "stale_leader", "stale_runner", "forged_exit"} {
		t.Run(name, func(t *testing.T) {
			exit := 1
			if name == "successful_command" {
				exit = 0
			}
			db, ctx, request := postbuildFailureFixture(t, exit)
			want := ErrEvidenceConflict
			switch name {
			case "wrong_ticket":
				request.Ref.Ticket = "SF-other"
			case "wrong_builder":
				request.BuilderResult.AttemptID++
			case "missing_command":
				request.CommandResult.SemanticKey += "/forged"
			case "stale_version":
				request.ExpectedVersion++
				want = ErrStaleFence
			case "stale_leader":
				request.Fence.LeaderEpoch++
				want = ErrStaleFence
			case "stale_runner":
				request.Fence.RunnerEpoch++
				want = ErrStaleFence
			case "forged_exit":
				// Simulate disk tampering beneath the immutable write API.
				if _, err := db.db.ExecContext(ctx, `DROP TRIGGER repository_command_results_immutable_update`); err != nil {
					t.Fatal(err)
				}
				if _, err := db.db.ExecContext(ctx, `UPDATE repository_command_results SET exit_code=255 WHERE semantic_key=? AND claim_epoch=?`, request.CommandResult.SemanticKey, request.CommandResult.ClaimEpoch); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := db.AuthenticatePostbuildFailure(ctx, request); !errors.Is(err, want) {
				t.Fatalf("got %v, want %v", err, want)
			}
		})
	}
}

func TestAuthenticatePostbuildFailureRejectsUnresolvedEffect(t *testing.T) {
	db, ctx, request := postbuildFailureFixture(t, 255)
	if _, err := db.AuthenticatePostbuildFailure(ctx, request); err != nil {
		t.Fatal(err)
	}
	if _, err := db.PlanEffect(ctx, EffectPlan{SemanticKey: "postbuild-failure/unresolved", Ref: request.Ref, Kind: "repository_command", TicketVersion: request.ExpectedVersion, Fence: request.Fence, RequestDigest: repositoryCommandDigest("9")}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.AuthenticatePostbuildFailure(ctx, request); !errors.Is(err, ErrControlNotDrained) {
		t.Fatalf("unresolved effect admitted: %v", err)
	}
}
