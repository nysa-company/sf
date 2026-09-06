package daemon

import (
	"context"
	"errors"
	"strings"
	"time"
	"unicode"

	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/redact"
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

	verification, err := daemon.store.HistoricalVerification(ctx, ref)
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

	candidate, err := daemon.store.HistoricalCandidate(ctx, ref)
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
	review, err := daemon.store.LatestReviewDiagnostic(ctx, ref)
	if err == nil {
		view["review_diagnostic"] = reviewDiagnosticView(review, daemon.projector.Policy)
	} else if !errors.Is(err, store.ErrNotFound) {
		// Unavailable diagnostics must not hide the durable ticket state or
		// display an older verdict as if it authenticated successfully.
		view["review_diagnostic"] = map[string]any{"available": false, "error_code": evidenceErrorCode(err)}
	}

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

func reviewDiagnosticView(value store.HistoricalReviewDiagnostic, policy redact.Policy) map[string]any {
	const maxFindings = 5
	findings := make([]string, 0, maxFindings)
	truncated := len(value.Review.Findings) > maxFindings
	for i, finding := range value.Review.Findings {
		if i == maxFindings {
			break
		}
		text := policy.String(finding)
		text = strings.Map(func(r rune) rune {
			if unicode.IsControl(r) || unicode.Is(unicode.Cf, r) {
				return ' '
			}
			return r
		}, text)
		// Redact before truncation so a cut credential cannot evade matching.
		runes := []rune(policy.String(text))
		if len(runes) > 512 {
			runes = runes[:512]
			truncated = true
		}
		findings = append(findings, string(runes))
	}
	return map[string]any{"available": true, "historical": true,
		"attempt": value.Attempt, "ticket_version": value.TicketVersion,
		"decision": value.Review.Decision, "reviewed_head": value.Review.ReviewedHead,
		"findings": findings, "truncated": truncated}
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
