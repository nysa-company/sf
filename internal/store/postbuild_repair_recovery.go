package store

import (
	"context"
	"database/sql"
	"errors"

	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/phaseartifact"
)

type PostbuildRepairBuildContext struct {
	Repair          PostbuildRepair
	Worktree        StoredWorktree
	Plan            StoredPlan
	Verification    StoredVerification
	Builder         ProviderAttemptResult
	BuilderArtifact phaseartifact.Builder
	FailedCommand   RepositoryCommandResult
	Candidate       *StoredCandidate
}

// PostbuildRepairBuildContext is initial admission evidence only. Once a new
// Builder attempt exists, its normal result path owns subsequent completion.
// Physical retained edits must still be authenticated by the Git coordinator.
func (s *Store) PostbuildRepairBuildContext(ctx context.Context, ref domain.TicketRef, version uint64, fence domain.Fence) (PostbuildRepairBuildContext, error) {
	return s.readPostbuildRepairContext(ctx, ref, version, fence, "initial")
}

// PostbuildRepairContext authenticates immutable prompt inputs without granting
// physical edit admission. It remains available while the new Builder runs.
func (s *Store) PostbuildRepairContext(ctx context.Context, ref domain.TicketRef, version uint64, fence domain.Fence) (PostbuildRepairBuildContext, error) {
	return s.readPostbuildRepairContext(ctx, ref, version, fence, "logical")
}

func (s *Store) PostbuildRepairCompletedBuildContext(ctx context.Context, ref domain.TicketRef, version uint64, fence domain.Fence) (PostbuildRepairBuildContext, error) {
	return s.readPostbuildRepairContext(ctx, ref, version, fence, "completed")
}

// PostbuildRepairPendingAmendment recovers the typed decision already returned
// by the fresh Builder. It neither starts another provider nor adopts edits.
func (s *Store) PostbuildRepairPendingAmendment(ctx context.Context, ref domain.TicketRef, version uint64, fence domain.Fence) (ProviderAttemptResultKey, error) {
	value, err := s.readPostbuildRepairContext(ctx, ref, version, fence, "amendment")
	if err != nil {
		return ProviderAttemptResultKey{}, err
	}
	return ProviderAttemptResultKey{Ref: ref, Phase: domain.PhaseBuild, AttemptID: value.Builder.Claim.ID, Attempt: value.Builder.Claim.Attempt}, nil
}

func (s *Store) readPostbuildRepairContext(ctx context.Context, ref domain.TicketRef, version uint64, fence domain.Fence, mode string) (PostbuildRepairBuildContext, error) {
	var result PostbuildRepairBuildContext
	if s == nil || ref.Validate() != nil || version == 0 || fence.LeaderEpoch == 0 || fence.RunnerEpoch == 0 || fence.ClaimEpoch != 0 {
		return result, ErrEvidenceConflict
	}
	err := s.readProtectedBaseRefreshSnapshot(ctx, func(conn *sql.Conn) error {
		if superseded, err := postbuildRepairSupersededAt(ctx, conn, ref, version, fence, true); err != nil {
			return err
		} else if superseded {
			return ErrNotFound
		}
		repair, err := postbuildRepairAtFence(ctx, conn, ref, version, fence)
		if err != nil {
			return err
		}
		if err := s.assertTicketFence(ctx, conn, ref, version, fence); err != nil {
			return err
		}
		if validateRunnerRecoveryAuthority(ctx, conn, ref, version, fence) != nil {
			return ErrEvidenceConflict
		}
		var state domain.State
		if conn.QueryRowContext(ctx, `SELECT state FROM tickets WHERE channel=? AND project_id=? AND id=?`, ref.Channel, ref.Project, ref.Ticket).Scan(&state) != nil || state != domain.StateBuilding {
			return ErrEvidenceConflict
		}
		if mode == "initial" {
			var newer int
			if conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM provider_attempts WHERE channel=? AND project_id=? AND ticket_id=? AND phase='build' AND (attempt>? OR id>?)`, ref.Channel, ref.Project, ref.Ticket, repair.BuilderResult.Attempt, repair.BuilderResult.AttemptID).Scan(&newer) != nil || newer != 0 {
				return ErrEvidenceConflict
			}
		}
		builder, parsed, err := s.loadHistoricalProviderAttemptResult(ctx, conn, repair.BuilderResult)
		if err != nil || parsed.Builder == nil {
			return ErrEvidenceConflict
		}
		if mode == "completed" || mode == "amendment" {
			var fresh int
			if conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM provider_attempts WHERE channel=? AND project_id=? AND ticket_id=? AND phase='build' AND (attempt>? OR id>?)`, ref.Channel, ref.Project, ref.Ticket, repair.BuilderResult.Attempt, repair.BuilderResult.AttemptID).Scan(&fresh) != nil {
				return ErrEvidenceConflict
			}
			if fresh == 0 {
				return ErrNotFound
			}
			key := ProviderAttemptResultKey{Ref: ref, Phase: domain.PhaseBuild}
			if conn.QueryRowContext(ctx, `SELECT id,attempt FROM provider_attempts WHERE channel=? AND project_id=? AND ticket_id=? AND phase='build' AND role='builder' ORDER BY attempt DESC,id DESC LIMIT 1`, ref.Channel, ref.Project, ref.Ticket).Scan(&key.AttemptID, &key.Attempt) != nil || key.AttemptID <= repair.BuilderResult.AttemptID || key.Attempt <= repair.BuilderResult.Attempt {
				return ErrEvidenceConflict
			}
			builder, parsed, err = s.loadHistoricalProviderAttemptResult(ctx, conn, key)
			if err != nil || parsed.Builder == nil || (mode == "completed" && parsed.Builder.AmendmentRequest != nil) || providerResultReachesFence(ctx, conn, key, builder, version, fence) != nil {
				return ErrEvidenceConflict
			}
			var bindingCount int
			if conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM provider_phase_attempt_entries WHERE channel=? AND project_id=? AND ticket_id=? AND phase='build' AND role='builder' AND provider_attempt_id=? AND attempt=? AND entry_ticket_version=?`, ref.Channel, ref.Project, ref.Ticket, key.AttemptID, key.Attempt, repair.EntryVersion).Scan(&bindingCount) != nil || bindingCount != 1 {
				return ErrEvidenceConflict
			}
		}
		var activeEffects int
		if mode != "logical" {
			if repositoryHasProviderWriter(ctx, conn, builder.Claim.Repository) != nil || repositoryHasCommandWriter(ctx, conn, builder.Claim.Repository) != nil {
				return ErrControlNotDrained
			}
			var active int
			if conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM git_mutation_leases WHERE repository_path=?`, builder.Claim.Repository).Scan(&active) != nil || active != 0 {
				return ErrControlNotDrained
			}
			if conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM effects WHERE channel=? AND project_id=? AND ticket_id=? AND state IN ('planned','executing','uncertain')`, ref.Channel, ref.Project, ref.Ticket).Scan(&activeEffects) != nil {
				return ErrControlNotDrained
			}
		}
		var worktree StoredWorktree
		if conn.QueryRowContext(ctx, `SELECT path,branch_ref,state,identity_json,base_sha,head_sha,ticket_version,leader_epoch,runner_epoch FROM worktrees WHERE channel=? AND project_id=? AND ticket_id=?`, ref.Channel, ref.Project, ref.Ticket).Scan(&worktree.Path, &worktree.Branch, &worktree.State, &worktree.IdentityJSON, &worktree.BaseSHA, &worktree.HeadSHA, &worktree.TicketVersion, &worktree.Fence.LeaderEpoch, &worktree.Fence.RunnerEpoch) != nil || worktree.State != "registered" || worktree.Path != builder.Claim.Worktree || string(worktree.IdentityJSON) != builder.Claim.WorktreeIdentity || worktree.BaseSHA != builder.Claim.BaseSHA || !validOID(worktree.HeadSHA) {
			return ErrEvidenceConflict
		}
		verification, err := s.currentVerificationFrom(ctx, conn, ref)
		if err != nil || verification.ProviderResult != repair.Verification.ProviderResult || verification.Checkpoint != repair.Verification.Checkpoint {
			return ErrEvidenceConflict
		}
		plan, err := s.planFrom(ctx, conn, ref)
		if err != nil {
			return ErrEvidenceConflict
		}
		failed, found, err := loadRepositoryCommandResult(ctx, conn, repair.CommandResult, true)
		if err != nil || !found || failed.ResultDigest != repair.FailedResultDigest {
			return ErrEvidenceConflict
		}
		result = PostbuildRepairBuildContext{Repair: repair, Worktree: worktree, Plan: plan, Verification: verification, Builder: builder, BuilderArtifact: *parsed.Builder, FailedCommand: failed}
		candidate, err := s.postbuildCandidateHandoffFrom(ctx, conn, ref, version, fence, repair.EntryVersion, verification)
		if err != nil {
			return err
		}
		result.Candidate = candidate
		if activeEffects != 0 {
			if mode == "initial" || candidate != nil || parsed.Builder.AmendmentRequest != nil {
				return ErrControlNotDrained
			}
			key := ProviderAttemptResultKey{Ref: ref, Phase: domain.PhaseBuild, AttemptID: builder.Claim.ID, Attempt: builder.Claim.Attempt}
			if _, found, err := s.postbuildPreparedCandidateWitnessFrom(ctx, conn, ref, version, fence, key, postbuildCandidateSource{EntryVersion: repair.EntryVersion, Verification: verification, Worktree: worktree, Plan: plan}); err != nil || !found {
				return ErrControlNotDrained
			}
		}
		if mode == "amendment" && parsed.Builder.AmendmentRequest == nil {
			return ErrNotFound
		}
		return nil
	})
	return result, err
}

// A later semantic entry retires the diagnostic context only when its own
// authority is proven. A version gap or a newly spelled trigger is not enough.
func postbuildRepairSupersededAt(ctx context.Context, q candidateEvidenceQuerier, ref domain.TicketRef, version uint64, fence domain.Fence, live bool) (bool, error) {
	repair, err := latestPostbuildRepairAt(ctx, q, ref, version)
	if errors.Is(err, ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, ErrEvidenceConflict
	}
	entry, err := loadProviderPhaseEntryAt(ctx, q, ref, domain.PhaseBuild, version)
	if err != nil || entry.Version < repair.EntryVersion {
		return false, ErrEvidenceConflict
	}
	if superseded, err := postbuildRepairCandidateSupersededAt(ctx, q, ref, version, fence, repair, live); err != nil || superseded {
		return superseded, err
	}
	if entry.Version == repair.EntryVersion {
		return false, nil
	}
	if superseded, err := (&Store{}).postbuildAmendmentSupersededAt(ctx, q, ref, version, fence, live); err != nil || superseded {
		return superseded, err
	}
	switch entry.Trigger {
	case "amendment_accepted", "amendment_rejected":
		boundary, err := loadVerificationAmendmentBoundaryAt(ctx, q, ref, version, fence, live)
		if err != nil || boundary.DecisionVersion != entry.Version {
			return false, ErrEvidenceConflict
		}
	case "review_repair":
		boundary, err := reviewRepairBoundaryFrom(ctx, q, ref, domain.PhaseBuild, version, repair.EntryVersion)
		if err != nil || !boundary || postbuildRepairSignedSourcePrefix(ctx, q, ref, entry.Version, domain.Fence{LeaderEpoch: entry.Leader, RunnerEpoch: entry.Runner}, version, fence) != nil {
			return false, ErrEvidenceConflict
		}
	default:
		return false, ErrEvidenceConflict
	}
	return true, nil
}

// Missing evidence is distinguishable from corrupt/future evidence. This
// historical lookup never calls the live phase-entry or recovery validators.
func latestPostbuildRepairAt(ctx context.Context, q candidateEvidenceQuerier, ref domain.TicketRef, version uint64) (PostbuildRepair, error) {
	var entry uint64
	err := q.QueryRowContext(ctx, `SELECT entry_ticket_version FROM postbuild_repair_entries WHERE channel=? AND project_id=? AND ticket_id=? AND entry_ticket_version<=? ORDER BY entry_ticket_version DESC LIMIT 1`, ref.Channel, ref.Project, ref.Ticket, version).Scan(&entry)
	if errors.Is(err, sql.ErrNoRows) {
		var count int
		if q.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE channel=? AND project_id=? AND ticket_id=? AND trigger='postbuild_repair' AND ticket_version<=?`, ref.Channel, ref.Project, ref.Ticket, version).Scan(&count) != nil || count != 0 {
			return PostbuildRepair{}, ErrEvidenceConflict
		}
		return PostbuildRepair{}, ErrNotFound
	}
	if err != nil {
		return PostbuildRepair{}, ErrEvidenceConflict
	}
	return loadPostbuildRepairEntry(ctx, q, ref, entry)
}

func postbuildRepairAtFence(ctx context.Context, q candidateEvidenceQuerier, ref domain.TicketRef, version uint64, fence domain.Fence) (PostbuildRepair, error) {
	var future int
	if q.QueryRowContext(ctx, `SELECT (SELECT COUNT(*) FROM postbuild_repair_entries WHERE channel=? AND project_id=? AND ticket_id=? AND entry_ticket_version>?) + (SELECT COUNT(*) FROM runner_recovery_ledger WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version>?)`, ref.Channel, ref.Project, ref.Ticket, version, ref.Channel, ref.Project, ref.Ticket, version).Scan(&future) != nil || future != 0 {
		return PostbuildRepair{}, ErrEvidenceConflict
	}
	repair, err := latestPostbuildRepairAt(ctx, q, ref, version)
	if err != nil {
		return PostbuildRepair{}, err
	}
	if postbuildRepairSignedSourcePrefix(ctx, q, ref, repair.EntryVersion, repair.Fence, version, fence) != nil {
		return PostbuildRepair{}, ErrEvidenceConflict
	}
	return repair, nil
}

// validPostbuildRepairGap consumes every version in a bounded interval. Only
// exact signed recovery, canonical phase_pass, and authenticated repair edges
// are accepted. At least one repair must occur; this is no generic gap escape.
func validPostbuildRepairGap(ctx context.Context, q candidateEvidenceQuerier, ref domain.TicketRef, fromVersion, fromRunner, fromLeader, toVersion, toRunner, toLeader uint64) bool {
	if fromVersion == 0 || fromRunner == 0 || fromLeader == 0 || toVersion == ^uint64(0) || toVersion <= fromVersion || toVersion-fromVersion > 64 {
		return false
	}
	fence := domain.Fence{LeaderEpoch: fromLeader, RunnerEpoch: fromRunner}
	repaired := false
	var repairVersion uint64
	var priorState domain.State
	for version := fromVersion + 1; version <= toVersion; version++ {
		var count int
		if q.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version=? AND (from_state<>to_state OR trigger='postbuild_repair')`, ref.Channel, ref.Project, ref.Ticket, version).Scan(&count) != nil {
			return false
		}
		step, found, err := loadRunnerRecoveryAt(ctx, q, ref, version)
		if err != nil {
			return false
		}
		if found {
			next := domain.Fence{LeaderEpoch: step.LeaderEpoch, RunnerEpoch: step.RunnerEpoch}
			if count != 0 || authenticateRunnerRecoveryStep(ctx, q, ref, version-1, fence, version, next) != nil {
				return false
			}
			fence = next
			continue
		}
		if count != 1 {
			return false
		}
		var from, to domain.State
		if q.QueryRowContext(ctx, `SELECT from_state,to_state FROM events WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version=? AND (from_state<>to_state OR trigger='postbuild_repair')`, ref.Channel, ref.Project, ref.Ticket, version).Scan(&from, &to) != nil || (priorState != "" && from != priorState) {
			return false
		}
		priorState = to
		repair, err := loadPostbuildRepairEntry(ctx, q, ref, version)
		if err == nil {
			if repair.ExpectedVersion != version-1 || repair.Fence != fence {
				return false
			}
			repaired = true
			repairVersion = repair.EntryVersion
			continue
		}
		if !errors.Is(err, ErrNotFound) {
			return false
		}
		if repaired && from == domain.StateBuilding && to == domain.StateVerifying {
			amendment, err := (&Store{}).loadVerificationAmendment(ctx, q, ref, version, fence)
			if err != nil {
				return false
			}
			binding, _, err := loadPostbuildAmendmentBinding(ctx, q, amendment)
			if err != nil || binding.RepairEntryVersion != repairVersion || binding.ConsumedVersion != version-1 || binding.ConsumedFence != fence {
				return false
			}
			continue
		}
		if repaired && from == domain.StateVerifying && to == domain.StateBuilding {
			var trigger string
			if q.QueryRowContext(ctx, `SELECT trigger FROM events WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version=? AND from_state<>to_state`, ref.Channel, ref.Project, ref.Ticket, version).Scan(&trigger) != nil {
				return false
			}
			if trigger == "amendment_accepted" || trigger == "amendment_rejected" {
				boundary, err := loadVerificationAmendmentBoundaryAt(ctx, q, ref, version, fence, false)
				if err != nil || boundary.DecisionVersion != version {
					return false
				}
				binding, _, err := loadPostbuildAmendmentBinding(ctx, q, boundary.Amendment)
				if err != nil || binding.RepairEntryVersion != repairVersion {
					return false
				}
				continue
			}
		}
		if validateRunnerPhaseAdvance(ctx, q, ref, version-1, fence.RunnerEpoch, version, fence.RunnerEpoch) != nil {
			return false
		}
	}
	return repaired && fence == (domain.Fence{LeaderEpoch: toLeader, RunnerEpoch: toRunner})
}

func validInitialPostbuildRepairTarget(ctx context.Context, q candidateEvidenceQuerier, ref domain.TicketRef, version, runner, leader uint64) bool {
	var first uint64
	if q.QueryRowContext(ctx, `SELECT MIN(entry_ticket_version) FROM postbuild_repair_entries WHERE channel=? AND project_id=? AND ticket_id=? AND entry_ticket_version<=?`, ref.Channel, ref.Project, ref.Ticket, version).Scan(&first) != nil {
		return false
	}
	repair, err := loadPostbuildRepairEntry(ctx, q, ref, first)
	if err != nil || repair.Fence.RunnerEpoch != 1 || validateInitialLifecycleAdvance(ctx, q, ref, repair.ExpectedVersion) != nil {
		return false
	}
	return validPostbuildRepairGap(ctx, q, ref, repair.ExpectedVersion, repair.Fence.RunnerEpoch, repair.Fence.LeaderEpoch, version, runner, leader)
}

// Startup has already acquired the next leader. Authenticate the old endpoint
// from the immutable entry and signed rows, never from that new live leader.
func postbuildRepairRecoveryPredecessor(ctx context.Context, q candidateEvidenceQuerier, ref domain.TicketRef, state domain.State, version, runner, nextLeader uint64) (uint64, bool, error) {
	if state != domain.StateBuilding {
		return 0, false, nil
	}
	repair, err := latestPostbuildRepairAt(ctx, q, ref, version)
	if errors.Is(err, ErrNotFound) {
		return 0, false, nil
	}
	if err != nil {
		return 0, true, ErrPublicationEvidence
	}
	leader := repair.Fence.LeaderEpoch
	if version != repair.EntryVersion {
		step, found, err := loadRunnerRecoveryAt(ctx, q, ref, version)
		if err != nil {
			return 0, true, ErrPublicationEvidence
		}
		if found {
			leader = step.LeaderEpoch
		} else {
			entry, err := loadProviderPhaseEntryAt(ctx, q, ref, domain.PhaseBuild, version)
			if err != nil {
				return 0, true, ErrPublicationEvidence
			}
			leader = entry.Leader
		}
	}
	if superseded, err := postbuildRepairSupersededAt(ctx, q, ref, version, domain.Fence{LeaderEpoch: leader, RunnerEpoch: runner}, false); err != nil {
		return 0, true, ErrPublicationEvidence
	} else if superseded {
		return 0, false, nil
	}
	if leader == 0 || leader >= nextLeader || postbuildRepairSignedSourcePrefix(ctx, q, ref, repair.EntryVersion, repair.Fence, version, domain.Fence{LeaderEpoch: leader, RunnerEpoch: runner}) != nil {
		return 0, true, ErrPublicationEvidence
	}
	if authenticatePostbuildRecoveryPrefix(ctx, q, ref, version, runner, leader) != nil {
		return 0, true, ErrPublicationEvidence
	}
	return leader, true, nil
}

func authenticatePostbuildRecoveryPrefix(ctx context.Context, q candidateEvidenceQuerier, ref domain.TicketRef, version, runner, leader uint64) error {
	var firstVersion, firstRunner, firstLeader uint64
	err := q.QueryRowContext(ctx, `SELECT prior_ticket_version,prior_runner_epoch,prior_leader_epoch FROM runner_recovery_ledger WHERE channel=? AND project_id=? AND ticket_id=? ORDER BY ticket_version LIMIT 1`, ref.Channel, ref.Project, ref.Ticket).Scan(&firstVersion, &firstRunner, &firstLeader)
	if errors.Is(err, sql.ErrNoRows) {
		if !validInitialPostbuildRepairTarget(ctx, q, ref, version, runner, leader) {
			return ErrPublicationEvidence
		}
	} else if err != nil || firstRunner != 1 || (validateInitialLifecycleAdvance(ctx, q, ref, firstVersion) != nil && !validInitialPostbuildRepairTarget(ctx, q, ref, firstVersion, firstRunner, firstLeader)) || validateRunnerRecoveryLedgerPrefix(ctx, q, ref, firstVersion, firstRunner, firstLeader, version, runner, leader) != nil {
		return ErrPublicationEvidence
	}
	return nil
}
