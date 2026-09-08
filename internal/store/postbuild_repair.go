package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/nysa-company/sf/internal/domain"
)

type PostbuildRepairRequest struct {
	PostbuildFailureRequest
	RetainedWorktreeDigest string
}

// PostbuildRepair retains the consumed evidence unchanged. RetainedWorktreeDigest
// is a locator for a separately authenticated physical snapshot, not permission
// to admit arbitrary dirty files or to change protected verification.
type PostbuildRepair struct {
	PostbuildFailureRequest
	EntryVersion           uint64
	VerificationRevision   uint64
	OriginalCheckpointOID  string
	RetainedWorktreeDigest string
	FailedResultDigest     string
	BuilderTypedDigest     string
	BudgetRequestID        string
	BindingDigest          string
	CreatedAt              string
	EventID                int64
	Verification           StoredVerification
}

func postbuildRepairDigest(value PostbuildRepair) (string, error) {
	// Only persisted identity fields enter the canonical envelope. Derived
	// verification bytes and event id are authenticated independently below.
	value.BindingDigest, value.EventID, value.Verification = "", 0, StoredVerification{}
	raw, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	return repositoryResultDigest(raw), nil
}

func (s *Store) TransitionPostbuildRepair(ctx context.Context, request PostbuildRepairRequest) (TransitionResult, error) {
	if s == nil || request.Ref.Validate() != nil || request.ExpectedVersion == 0 || request.ExpectedVersion == ^uint64(0) || request.Fence.LeaderEpoch == 0 || request.Fence.RunnerEpoch == 0 || request.Fence.ClaimEpoch != 0 || !validClaimDigest(request.RetainedWorktreeDigest) {
		return TransitionResult{}, ErrEvidenceConflict
	}
	var replay TransitionResult
	err := s.write(ctx, func(conn *sql.Conn) error {
		var entry uint64
		err := conn.QueryRowContext(ctx, `SELECT entry_ticket_version FROM postbuild_repair_entries WHERE failed_command_semantic_key=? AND failed_command_claim_epoch=?`, request.CommandResult.SemanticKey, request.CommandResult.ClaimEpoch).Scan(&entry)
		if err == nil {
			value, err := loadPostbuildRepairEntry(ctx, conn, request.Ref, entry)
			if err != nil || value.PostbuildFailureRequest != request.PostbuildFailureRequest || value.RetainedWorktreeDigest != request.RetainedWorktreeDigest {
				return ErrEvidenceConflict
			}
			if err := s.assertTicketFence(ctx, conn, request.Ref, entry, request.Fence); err != nil {
				return err
			}
			var state domain.State
			if conn.QueryRowContext(ctx, `SELECT state FROM tickets WHERE channel=? AND project_id=? AND id=?`, request.Ref.Channel, request.Ref.Project, request.Ref.Ticket).Scan(&state) != nil || state != domain.StateBuilding {
				return ErrStaleFence
			}
			replay = TransitionResult{Version: entry, EventID: value.EventID}
			return nil
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		return s.assertTicketFence(ctx, conn, request.Ref, request.ExpectedVersion, request.Fence)
	})
	if err != nil || replay.Version != 0 {
		return replay, err
	}
	transition := Transition{Ref: request.Ref, ExpectedVersion: request.ExpectedVersion, Fence: request.Fence, From: domain.StateBuilding, To: domain.StateBuilding, Trigger: "postbuild_repair", EventPayload: "{}"}
	return s.transitionWithEvidence(ctx, transition, func(ctx context.Context, conn *sql.Conn, version, runner uint64) error {
		// transitionWithEvidence has drained/revoked mutation admission at the
		// consumed endpoint. Check its exact database fence, not that deliberately
		// revoked mutation token. Never un-revoke the predecessor.
		if err := s.assertCurrentTicketFence(ctx, conn, request.Ref, version, request.Fence); err != nil {
			return err
		}
		var controls int
		if conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM runtime_ticket_controls WHERE channel=? AND project_id=? AND ticket_id=? AND state IN ('sealed','armed')`, request.Ref.Channel, request.Ref.Project, request.Ref.Ticket).Scan(&controls) != nil || controls != 0 {
			return ErrStaleFence
		}
		failure, err := s.authenticatePostbuildFailureFrom(ctx, conn, request.PostbuildFailureRequest)
		if err != nil {
			return err
		}
		value := PostbuildRepair{PostbuildFailureRequest: request.PostbuildFailureRequest, EntryVersion: version + 1, VerificationRevision: failure.Verification.Revision.Revision, OriginalCheckpointOID: failure.Verification.Checkpoint.CommitOID, RetainedWorktreeDigest: request.RetainedWorktreeDigest, FailedResultDigest: failure.CommandResult.ResultDigest, BuilderTypedDigest: failure.BuilderTypedDigest, CreatedAt: now()}
		value.BudgetRequestID = fmt.Sprintf("postbuild-repair/%d/%s", request.BuilderResult.AttemptID, failure.CommandResult.ResultDigest)
		// Refuse unsupported historical source shapes before appending a row
		// which the nonrecursive historical reader would be unable to prove.
		if _, err := authenticatePostbuildRepairSource(ctx, conn, value); err != nil {
			return err
		}
		value.BindingDigest, err = postbuildRepairDigest(value)
		if err != nil {
			return err
		}
		if _, err := s.consumeBudgetDuringTransition(ctx, conn, BudgetUse{Ref: request.Ref, ExpectedVersion: version, Fence: request.Fence, Kind: "correction", RequestID: value.BudgetRequestID}); err != nil {
			return err
		}
		_, err = conn.ExecContext(ctx, `INSERT INTO postbuild_repair_entries(channel,project_id,ticket_id,entry_ticket_version,consumed_ticket_version,consumed_leader_epoch,consumed_runner_epoch,phase,builder_result_attempt_id,builder_result_attempt,builder_result_phase,builder_result_role,failed_command_semantic_key,failed_command_claim_epoch,verification_revision,original_checkpoint_oid,retained_worktree_digest,failed_result_digest,builder_typed_digest,correction_budget_kind,correction_budget_request_id,binding_digest,created_at) VALUES(?,?,?,?,?,?,?,'build',?,?,'build','builder',?,?,?,?,?,?,?,'correction',?,?,?)`, request.Ref.Channel, request.Ref.Project, request.Ref.Ticket, value.EntryVersion, version, request.Fence.LeaderEpoch, runner, request.BuilderResult.AttemptID, request.BuilderResult.Attempt, request.CommandResult.SemanticKey, request.CommandResult.ClaimEpoch, value.VerificationRevision, value.OriginalCheckpointOID, value.RetainedWorktreeDigest, value.FailedResultDigest, value.BuilderTypedDigest, value.BudgetRequestID, value.BindingDigest, value.CreatedAt)
		return err
	})
}

// loadPostbuildRepairEntry authenticates one exact historical entry. It never
// calls loadProviderPhaseEntryAt or the global/live recovery validator: those
// consumers may themselves use this proof as a semantic boundary.
func loadPostbuildRepairEntry(ctx context.Context, q candidateEvidenceQuerier, ref domain.TicketRef, entryVersion uint64) (PostbuildRepair, error) {
	var value PostbuildRepair
	value.Ref, value.EntryVersion = ref, entryVersion
	if ref.Validate() != nil || entryVersion < 2 {
		return value, ErrEvidenceConflict
	}
	err := q.QueryRowContext(ctx, `SELECT consumed_ticket_version,consumed_leader_epoch,consumed_runner_epoch,builder_result_attempt_id,builder_result_attempt,failed_command_semantic_key,failed_command_claim_epoch,verification_revision,original_checkpoint_oid,retained_worktree_digest,failed_result_digest,builder_typed_digest,correction_budget_request_id,binding_digest,created_at FROM postbuild_repair_entries WHERE channel=? AND project_id=? AND ticket_id=? AND entry_ticket_version=? AND phase='build' AND builder_result_phase='build' AND builder_result_role='builder' AND correction_budget_kind='correction'`, ref.Channel, ref.Project, ref.Ticket, entryVersion).Scan(&value.ExpectedVersion, &value.Fence.LeaderEpoch, &value.Fence.RunnerEpoch, &value.BuilderResult.AttemptID, &value.BuilderResult.Attempt, &value.CommandResult.SemanticKey, &value.CommandResult.ClaimEpoch, &value.VerificationRevision, &value.OriginalCheckpointOID, &value.RetainedWorktreeDigest, &value.FailedResultDigest, &value.BuilderTypedDigest, &value.BudgetRequestID, &value.BindingDigest, &value.CreatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return PostbuildRepair{}, ErrNotFound
	}
	value.BuilderResult.Ref, value.BuilderResult.Phase = ref, domain.PhaseBuild
	if err != nil || value.ExpectedVersion != entryVersion-1 || value.Fence.LeaderEpoch == 0 || value.Fence.RunnerEpoch == 0 || !validClaimDigest(value.RetainedWorktreeDigest) || !validClaimDigest(value.FailedResultDigest) || !validDigest(value.BuilderTypedDigest) || value.BudgetRequestID != fmt.Sprintf("postbuild-repair/%d/%s", value.BuilderResult.AttemptID, value.FailedResultDigest) {
		return PostbuildRepair{}, ErrEvidenceConflict
	}
	created, err := time.Parse(time.RFC3339Nano, value.CreatedAt)
	if err != nil || created.Location() != time.UTC || created.Format(time.RFC3339Nano) != value.CreatedAt {
		return PostbuildRepair{}, ErrEvidenceConflict
	}
	digest, err := postbuildRepairDigest(value)
	if err != nil || digest != value.BindingDigest {
		return PostbuildRepair{}, ErrEvidenceConflict
	}
	var count int
	if q.QueryRowContext(ctx, `SELECT COUNT(*) FROM ticket_budget_uses WHERE channel=? AND project_id=? AND ticket_id=? AND kind='correction' AND request_id=? AND ticket_version=? AND leader_epoch=? AND runner_epoch=?`, ref.Channel, ref.Project, ref.Ticket, value.BudgetRequestID, value.ExpectedVersion, value.Fence.LeaderEpoch, value.Fence.RunnerEpoch).Scan(&count) != nil || count != 1 {
		return PostbuildRepair{}, ErrEvidenceConflict
	}
	var entry providerPhaseEntry
	entry.Phase, entry.Version = domain.PhaseBuild, entryVersion
	if q.QueryRowContext(ctx, `SELECT entry_event_id,entry_event_created_at,entry_leader_epoch,entry_runner_epoch,entry_from_state,entry_state,entry_trigger,entry_digest FROM provider_phase_entries WHERE channel=? AND project_id=? AND ticket_id=? AND phase='build' AND entry_ticket_version=?`, ref.Channel, ref.Project, ref.Ticket, entryVersion).Scan(&entry.EventID, &entry.EventCreated, &entry.Leader, &entry.Runner, &entry.From, &entry.State, &entry.Trigger, &entry.Digest) != nil || entry.Leader != value.Fence.LeaderEpoch || entry.Runner != value.Fence.RunnerEpoch || entry.From != domain.StateBuilding || entry.State != domain.StateBuilding || entry.Trigger != "postbuild_repair" {
		return PostbuildRepair{}, ErrEvidenceConflict
	}
	if digest, err := providerPhaseEntryDigest(ref, entry); err != nil || digest != entry.Digest {
		return PostbuildRepair{}, ErrEvidenceConflict
	}
	if q.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version=? AND id=? AND created_at=? AND trigger='postbuild_repair' AND from_state='building' AND to_state='building' AND payload='{}'`, ref.Channel, ref.Project, ref.Ticket, entryVersion, entry.EventID, entry.EventCreated).Scan(&count) != nil || count != 1 {
		return PostbuildRepair{}, ErrEvidenceConflict
	}
	if q.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version=? AND (from_state<>to_state OR trigger='postbuild_repair')`, ref.Channel, ref.Project, ref.Ticket, entryVersion).Scan(&count) != nil || count != 1 {
		return PostbuildRepair{}, ErrEvidenceConflict
	}
	value.EventID = entry.EventID
	value.Verification, err = authenticatePostbuildRepairSource(ctx, q, value)
	if err != nil {
		return PostbuildRepair{}, err
	}
	return value, nil
}

func authenticatePostbuildRepairSource(ctx context.Context, q candidateEvidenceQuerier, value PostbuildRepair) (StoredVerification, error) {
	s := &Store{}
	builder, parsed, err := s.loadHistoricalProviderAttemptResult(ctx, q, value.BuilderResult)
	if err != nil || parsed.Builder == nil || parsed.Builder.AmendmentRequest != nil || builder.TypedSHA256 != value.BuilderTypedDigest || builder.Claim.Role != "builder" {
		return StoredVerification{}, ErrEvidenceConflict
	}
	if err := postbuildRepairSignedSourcePrefix(ctx, q, value.Ref, builder.Claim.ExpectedVersion, domain.Fence{LeaderEpoch: builder.Claim.LeaderEpoch, RunnerEpoch: builder.Claim.RunnerEpoch}, value.ExpectedVersion, value.Fence); err != nil {
		return StoredVerification{}, err
	}
	var newestID int64
	var newestAttempt int
	if q.QueryRowContext(ctx, `SELECT r.provider_attempt_id,a.attempt FROM provider_attempts a LEFT JOIN provider_attempt_results r ON r.provider_attempt_id=a.id WHERE a.channel=? AND a.project_id=? AND a.ticket_id=? AND a.phase='build' AND a.role='builder' AND a.expected_ticket_version<=? AND a.finished_at IS NOT NULL ORDER BY a.attempt DESC,a.id DESC LIMIT 1`, value.Ref.Channel, value.Ref.Project, value.Ref.Ticket, value.ExpectedVersion).Scan(&newestID, &newestAttempt) != nil || newestID != value.BuilderResult.AttemptID || newestAttempt != value.BuilderResult.Attempt {
		return StoredVerification{}, ErrEvidenceConflict
	}
	var intent, proof string
	if q.QueryRowContext(ctx, `SELECT intent_digest,proof_digest FROM verification_revisions WHERE channel=? AND project_id=? AND ticket_id=? AND revision=? AND checkpoint_id=?`, value.Ref.Channel, value.Ref.Project, value.Ref.Ticket, value.VerificationRevision, value.OriginalCheckpointOID).Scan(&intent, &proof) != nil {
		return StoredVerification{}, ErrEvidenceConflict
	}
	verification, err := s.verificationEvidenceForIdentityFrom(ctx, q, value.Ref, intent, proof, value.OriginalCheckpointOID)
	if err != nil || verification.Revision.Revision != value.VerificationRevision {
		return StoredVerification{}, ErrEvidenceConflict
	}
	// Freeze an actual pre-consumption binding instead of substituting the
	// identity reader's latest recovery projection. The provider key and Git
	// checkpoint must still agree with the independently hydrated revision.
	var providerID int64
	var attempt int
	var checkpoint CommitObservation
	if q.QueryRowContext(ctx, `SELECT binding_ticket_version,leader_epoch,runner_epoch,provider_attempt_id,provider_attempt,checkpoint_commit_oid,checkpoint_parent_oid,checkpoint_tree_oid FROM verification_result_bindings WHERE channel=? AND project_id=? AND ticket_id=? AND revision=? AND binding_ticket_version<=? ORDER BY binding_ticket_version,leader_epoch,runner_epoch LIMIT 1`, value.Ref.Channel, value.Ref.Project, value.Ref.Ticket, value.VerificationRevision, value.ExpectedVersion).Scan(&verification.TicketVersion, &verification.Fence.LeaderEpoch, &verification.Fence.RunnerEpoch, &providerID, &attempt, &checkpoint.CommitOID, &checkpoint.ParentOID, &checkpoint.TreeOID) != nil || providerID != verification.ProviderResult.AttemptID || attempt != verification.ProviderResult.Attempt || checkpoint != verification.Checkpoint {
		return StoredVerification{}, ErrEvidenceConflict
	}
	verifier, verifyParsed, err := s.loadHistoricalProviderAttemptResult(ctx, q, verification.ProviderResult)
	if err != nil || verifyParsed.Verify == nil || postbuildRepairSignedSourcePrefix(ctx, q, value.Ref, verifier.Claim.ExpectedVersion, domain.Fence{LeaderEpoch: verifier.Claim.LeaderEpoch, RunnerEpoch: verifier.Claim.RunnerEpoch}, verification.TicketVersion, verification.Fence) != nil {
		return StoredVerification{}, ErrEvidenceConflict
	}
	prebuild, found, err := loadRepositoryCommandResult(ctx, q, verification.CommandBinding.Key, true)
	if err != nil || !found || !matchingBinding(verification.CommandBinding, prebuild) || prebuild.Claim.TicketRef != value.Ref || prebuild.Claim.Worktree != verifier.Claim.Worktree || prebuild.Claim.WorktreeIdentity != verifier.Claim.WorktreeIdentity || prebuild.Claim.BaseSHA != verifier.Claim.BaseSHA || prebuild.Claim.Worktree != builder.Claim.Worktree || prebuild.Claim.WorktreeIdentity != builder.Claim.WorktreeIdentity || prebuild.Claim.BaseSHA != builder.Claim.BaseSHA || prebuild.Claim.TicketVersion > verification.TicketVersion || expectedVerificationExit(verifyParsed.Verify.PrebuildOutcome, prebuild.Result.ExitCode) != nil {
		return StoredVerification{}, ErrEvidenceConflict
	}
	if postbuildRepairSignedSourcePrefix(ctx, q, value.Ref, verifier.Claim.ExpectedVersion, domain.Fence{LeaderEpoch: verifier.Claim.LeaderEpoch, RunnerEpoch: verifier.Claim.RunnerEpoch}, prebuild.Claim.TicketVersion, domain.Fence{LeaderEpoch: prebuild.Claim.LeaderEpoch, RunnerEpoch: prebuild.Claim.RunnerEpoch}) != nil {
		return StoredVerification{}, ErrEvidenceConflict
	}
	command, found, err := loadRepositoryCommandResult(ctx, q, value.CommandResult, true)
	if err != nil || !found || command.ResultDigest != value.FailedResultDigest || command.Result.ExitCode < 1 || command.Result.ExitCode > 255 || command.Claim.TicketRef != value.Ref || command.Claim.TicketVersion != value.ExpectedVersion || command.Claim.LeaderEpoch != value.Fence.LeaderEpoch || command.Claim.RunnerEpoch != value.Fence.RunnerEpoch || command.Claim.Worktree != builder.Claim.Worktree || command.Claim.WorktreeIdentity != builder.Claim.WorktreeIdentity || command.Claim.BaseSHA != builder.Claim.BaseSHA {
		return StoredVerification{}, ErrEvidenceConflict
	}
	entryCreated, err := time.Parse(time.RFC3339Nano, value.CreatedAt)
	if err != nil || command.CreatedAt.After(entryCreated) {
		return StoredVerification{}, ErrEvidenceConflict
	}
	var snapshot []byte
	var configDigest, finished string
	if q.QueryRowContext(ctx, `SELECT t.config_snapshot_bytes,t.config_digest,r.created_at FROM tickets t JOIN provider_attempt_results r ON r.provider_attempt_id=? WHERE t.channel=? AND t.project_id=? AND t.id=?`, value.BuilderResult.AttemptID, value.Ref.Channel, value.Ref.Project, value.Ref.Ticket).Scan(&snapshot, &configDigest, &finished) != nil {
		return StoredVerification{}, ErrEvidenceConflict
	}
	argv, err := frozenVerifyArgv(snapshot, configDigest)
	if err != nil {
		return StoredVerification{}, err
	}
	digest, err := exactRepositoryCommandDigest(argv)
	completed, timeErr := time.Parse(time.RFC3339Nano, finished)
	if err != nil || timeErr != nil || command.Claim.CommandDigest != digest || prebuild.Claim.CommandDigest != digest || !equalStringSlices(argv, verifyParsed.Verify.Command) || command.Result.ObservedAt.Before(completed) || verification.CreatedAt.After(command.Result.ObservedAt) {
		return StoredVerification{}, ErrEvidenceConflict
	}
	if q.QueryRowContext(ctx, `SELECT created_at FROM provider_attempt_results WHERE provider_attempt_id=?`, verification.ProviderResult.AttemptID).Scan(&finished) != nil {
		return StoredVerification{}, ErrEvidenceConflict
	}
	completed, err = time.Parse(time.RFC3339Nano, finished)
	if err != nil || prebuild.Result.ObservedAt.Before(completed) {
		return StoredVerification{}, ErrEvidenceConflict
	}
	prebuildRequest := commandEvidenceRequest(RepositoryCommandPurposePrebuildVerification, value.Ref, prebuild.Claim.TicketVersion, domain.Fence{LeaderEpoch: prebuild.Claim.LeaderEpoch, RunnerEpoch: prebuild.Claim.RunnerEpoch}, verification.ProviderResult, intent, proof, "", digest, prebuild)
	if assertCommandEvidenceRequest(prebuildRequest, prebuild) != nil {
		return StoredVerification{}, ErrEvidenceConflict
	}
	request := commandEvidenceRequest(RepositoryCommandPurposePostbuildCandidate, value.Ref, value.ExpectedVersion, value.Fence, value.BuilderResult, intent, proof, value.OriginalCheckpointOID, digest, command)
	if assertCommandEvidenceRequest(request, command) != nil {
		return StoredVerification{}, ErrEvidenceConflict
	}
	return verification, nil
}

// Only exact endpoints and contiguous signed runner recovery are supported
// here. Controls, amendments and semantic corrections require their own bounded
// proof; never call the general ledger validator from this historical anchor.
func postbuildRepairSignedSourcePrefix(ctx context.Context, q candidateEvidenceQuerier, ref domain.TicketRef, source uint64, sourceFence domain.Fence, target uint64, targetFence domain.Fence) error {
	if source == 0 || sourceFence.LeaderEpoch == 0 || sourceFence.RunnerEpoch == 0 || target < source || target-source > 64 || targetFence.RunnerEpoch < sourceFence.RunnerEpoch || target-source != targetFence.RunnerEpoch-sourceFence.RunnerEpoch {
		return ErrEvidenceConflict
	}
	if validateRunnerRecoveryCardinality(ctx, q, ref) != nil {
		return ErrEvidenceConflict
	}
	for source < target {
		step, found, err := loadRunnerRecoveryAt(ctx, q, ref, source+1)
		if err != nil || !found || authenticateRunnerRecoveryStep(ctx, q, ref, source, sourceFence, step.TicketVersion, domain.Fence{LeaderEpoch: step.LeaderEpoch, RunnerEpoch: step.RunnerEpoch}) != nil {
			return ErrEvidenceConflict
		}
		var count int
		if q.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version=? AND (from_state<>to_state OR trigger='postbuild_repair')`, ref.Channel, ref.Project, ref.Ticket, source+1).Scan(&count) != nil || count != 0 {
			return ErrEvidenceConflict
		}
		source, sourceFence = step.TicketVersion, domain.Fence{LeaderEpoch: step.LeaderEpoch, RunnerEpoch: step.RunnerEpoch}
	}
	if sourceFence != targetFence {
		return ErrEvidenceConflict
	}
	return nil
}
