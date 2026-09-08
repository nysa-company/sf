package store

import (
	"context"
	"database/sql"

	"github.com/nysa-company/sf/internal/domain"
)

// Only the completed first candidate of the accepted Building entry may cross
// the otherwise strict no-downstream boundary. Publication and later generations
// are not an amendment continuation. This reader grants no physical admission.
func (s *Store) postbuildAmendmentCandidateHandoffFrom(ctx context.Context, q *sql.Conn, ref domain.TicketRef, version uint64, fence domain.Fence, value PostbuildVerificationAmendmentContext) (*StoredCandidate, error) {
	entry, err := loadProviderPhaseEntryAt(ctx, q, ref, domain.PhaseBuild, version)
	if value.Decision != VerificationAmendmentAccepted {
		if err := assertNoVerificationAmendmentDownstream(ctx, q, ref); err != nil {
			return nil, err
		}
		return nil, nil
	}
	if err != nil || entry.Trigger != "amendment_accepted" || entry.Version <= value.Amendment.TransitionTicketVersion {
		return nil, ErrEvidenceConflict
	}
	return s.postbuildCandidateHandoffFrom(ctx, q, ref, version, fence, entry.Version, value.CurrentVerification)
}

// entryVersion and verification must come from the caller's authenticated
// current direct-repair or accepted-amendment entry, never inferred counters.
func (s *Store) postbuildCandidateHandoffFrom(ctx context.Context, q *sql.Conn, ref domain.TicketRef, version uint64, fence domain.Fence, entryVersion uint64, verification StoredVerification) (*StoredCandidate, error) {
	var count int
	for _, table := range []string{"publication_evidence", "ci_observations", "merge_intents", "manual_merge_observations"} {
		if q.QueryRowContext(ctx, `SELECT COUNT(*) FROM `+table+` WHERE channel=? AND project_id=? AND ticket_id=?`, ref.Channel, ref.Project, ref.Ticket).Scan(&count) != nil || count != 0 {
			return nil, ErrEvidenceConflict
		}
	}
	if q.QueryRowContext(ctx, `SELECT COUNT(*) FROM candidate_snapshots WHERE channel=? AND project_id=? AND ticket_id=?`, ref.Channel, ref.Project, ref.Ticket).Scan(&count) != nil {
		return nil, ErrEvidenceConflict
	}
	if count == 0 {
		return nil, nil
	}
	if count != 1 {
		return nil, ErrEvidenceConflict
	}
	candidate, err := s.latestCandidateFrom(ctx, q, ref, false)
	if err != nil || candidate.Snapshot.Generation != 1 || candidate.TicketVersion < entryVersion || candidate.TicketVersion > version || candidate.Commit.ParentOID != verification.Checkpoint.CommitOID || s.authenticateCandidateVerificationParentFrom(ctx, q, candidate, verification) != nil || s.reauthenticateStoredCandidateCheckpointFrom(ctx, q, ref, candidate) != nil {
		return nil, ErrEvidenceConflict
	}
	var source, base string
	if q.QueryRowContext(ctx, `SELECT t.source_digest,w.base_sha FROM tickets t JOIN worktrees w ON w.channel=t.channel AND w.project_id=t.project_id AND w.ticket_id=t.id WHERE t.channel=? AND t.project_id=? AND t.id=? AND w.state='registered'`, ref.Channel, ref.Project, ref.Ticket).Scan(&source, &base) != nil || candidate.Snapshot.SourceDigest != source || candidate.Snapshot.BaseSHA != base {
		return nil, ErrEvidenceConflict
	}
	builder, parsed, err := s.loadHistoricalProviderAttemptResult(ctx, q, candidate.BuilderResult)
	if err != nil || parsed.Builder == nil || parsed.Builder.AmendmentRequest != nil || builder.Claim.ExpectedVersion < entryVersion || assertNewestBoundResult(ctx, q, ref, domain.PhaseBuild, "builder", candidate.BuilderResult) != nil || providerResultReachesFence(ctx, q, candidate.BuilderResult, builder, version, fence) != nil {
		return nil, ErrEvidenceConflict
	}
	// The candidate binding is historical after a later restart. Authenticate
	// both bounded segments explicitly: the generic historical provider reader
	// deliberately rejects ledger rows beyond its target and therefore cannot
	// be used at this intermediate binding. The checkpoint reader above checks
	// all immutable material evidence, but grants no recovery authority itself.
	if postbuildRepairSignedSourcePrefix(ctx, q, ref, builder.Claim.ExpectedVersion, domain.Fence{LeaderEpoch: builder.Claim.LeaderEpoch, RunnerEpoch: builder.Claim.RunnerEpoch}, candidate.TicketVersion, candidate.Fence) != nil {
		return nil, ErrEvidenceConflict
	}
	if q.QueryRowContext(ctx, `SELECT COUNT(*) FROM provider_phase_attempt_entries WHERE channel=? AND project_id=? AND ticket_id=? AND phase='build' AND role='builder' AND provider_attempt_id=? AND attempt=? AND entry_ticket_version=?`, ref.Channel, ref.Project, ref.Ticket, candidate.BuilderResult.AttemptID, candidate.BuilderResult.Attempt, entryVersion).Scan(&count) != nil || count != 1 {
		return nil, ErrEvidenceConflict
	}
	if postbuildRepairSignedSourcePrefix(ctx, q, ref, candidate.TicketVersion, candidate.Fence, version, fence) != nil {
		return nil, ErrEvidenceConflict
	}
	return &candidate, nil
}
