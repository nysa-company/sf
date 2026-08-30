package daemon

import (
	"context"
	"errors"
	"time"

	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/store"
)

const maxStatusHistory = 100

// evidenceView returns bounded, authenticated workflow checkpoints. Raw model
// transcripts, credentials, worktree identity bytes, and proof bodies are
// deliberately excluded from the operator/status response.
func (daemon *Daemon) evidenceView(ctx context.Context, ref domain.TicketRef) (map[string]any, error) {
	view := map[string]any{}
	plan, err := daemon.store.Plan(ctx, ref)
	if err == nil {
		view["plan"] = map[string]any{
			"digest": plan.Digest, "ticket_version": plan.TicketVersion,
			"created_at": timeView(plan.CreatedAt), "acceptance_count": len(plan.Document.Acceptance),
			"proof_kind": plan.Document.ProofKind, "path_count": len(plan.Document.Paths),
			"command_count": len(plan.Document.Commands), "risk_count": len(plan.Document.Risks),
		}
	} else if !errors.Is(err, store.ErrNotFound) {
		return nil, err
	}

	verification, err := daemon.store.CurrentVerification(ctx, ref)
	if err == nil {
		view["verification"] = map[string]any{
			"revision": verification.Revision.Revision, "intent_digest": verification.Revision.IntentDigest,
			"proof_digest": verification.Revision.ProofDigest, "owned_files": append([]string(nil), verification.Revision.OwnedFiles...),
			"checkpoint_id": verification.Revision.CheckpointID, "amends_revision": verification.Revision.Amends,
			"ticket_version": verification.TicketVersion, "created_at": timeView(verification.CreatedAt),
		}
	} else if !errors.Is(err, store.ErrNotFound) {
		return nil, err
	}

	candidate, err := daemon.store.LatestCandidate(ctx, ref)
	if err == nil {
		view["candidate"] = map[string]any{
			"generation": candidate.Snapshot.Generation, "base_sha": candidate.Snapshot.BaseSHA,
			"head_sha": candidate.Snapshot.HeadSHA, "tree_sha": candidate.Snapshot.TreeSHA,
			"source_digest": candidate.Snapshot.SourceDigest, "verification_intent_digest": candidate.Snapshot.VerificationIntentDigest,
			"proof_digest": candidate.Snapshot.ProofDigest, "command_policy_digest": candidate.Snapshot.CommandPolicyDigest,
			"ticket_version": candidate.TicketVersion, "created_at": timeView(candidate.CreatedAt),
		}
	} else if !errors.Is(err, store.ErrNotFound) {
		return nil, err
	}

	worktree, err := daemon.store.Worktree(ctx, ref)
	if err == nil {
		view["worktree"] = map[string]any{
			"path": worktree.Path, "branch": worktree.Branch, "state": worktree.State,
			"base_sha": worktree.BaseSHA, "head_sha": worktree.HeadSHA, "ticket_version": worktree.TicketVersion,
		}
	} else if !errors.Is(err, store.ErrNotFound) {
		return nil, err
	}

	attempts, err := daemon.store.PhaseAttempts(ctx, ref)
	if err != nil {
		return nil, err
	}
	truncatedAttempts := len(attempts) > maxStatusHistory
	if truncatedAttempts {
		attempts = attempts[len(attempts)-maxStatusHistory:]
	}
	attemptViews := make([]map[string]any, 0, len(attempts))
	for _, attempt := range attempts {
		attemptViews = append(attemptViews, map[string]any{
			"phase": attempt.Phase, "attempt": attempt.Attempt, "state": attempt.State,
			"provider": attempt.Provider.Provider, "model": attempt.Provider.Model, "family": attempt.Provider.Family,
			"provider_version": attempt.Provider.Version, "base_sha": attempt.BaseSHA, "outcome": attempt.Outcome,
			"started_at": timeView(attempt.StartedAt), "finished_at": timeView(attempt.FinishedAt),
		})
	}
	view["phase_attempts"] = attemptViews
	view["phase_attempts_truncated"] = truncatedAttempts

	decisions, err := daemon.store.OperatorDecisions(ctx, ref)
	if err != nil {
		return nil, err
	}
	truncatedDecisions := len(decisions) > maxStatusHistory
	if truncatedDecisions {
		decisions = decisions[len(decisions)-maxStatusHistory:]
	}
	decisionViews := make([]map[string]any, 0, len(decisions))
	for _, decision := range decisions {
		decisionViews = append(decisionViews, map[string]any{
			"id": decision.ID, "reviewed_head": decision.ReviewedHead, "operator_uid": decision.OperatorUID,
			"decision": decision.Decision, "invalidated": decision.Invalidated,
			"ticket_version": decision.TicketVersion, "created_at": timeView(decision.CreatedAt),
		})
	}
	view["operator_decisions"] = decisionViews
	view["operator_decisions_truncated"] = truncatedDecisions
	return view, nil
}

func timeView(value time.Time) string {
	if value.IsZero() {
		return ""
	}
	return value.UTC().Format(time.RFC3339Nano)
}

func evidenceErrorCode(err error) string {
	if errors.Is(err, store.ErrBusy) {
		return "store_busy"
	}
	if errors.Is(err, store.ErrEvidenceConflict) {
		return "evidence_conflict"
	}
	return "evidence_unavailable"
}
