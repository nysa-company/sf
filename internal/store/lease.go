package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/nysa-company/sf/internal/config"
	"github.com/nysa-company/sf/internal/domain"
)

var ErrLeaseCapacity = errors.New("lease capacity is exhausted")

type phaseRecoveryBaseline struct{ version, runner, leader, currentLeader uint64 }

// reviewRepairRecoveryPredecessor is the single startup bridge for the crash
// window after an atomic final-review repair transition and before the fresh
// target-phase claim exists. It is intentionally narrower than normal phase
// recovery: the marker must name this exact state/version/runner and bind the
// completed final reviewer plus the correction budget it consumed.
func reviewRepairRecoveryPredecessor(ctx context.Context, conn *sql.Conn, ref domain.TicketRef, state domain.State, version, runner, newLeader uint64) (uint64, bool, error) {
	if (state != domain.StateBuilding && state != domain.StateVerifying) || version == 0 || runner == 0 || newLeader == 0 {
		return 0, false, nil
	}
	var attemptID int64
	var attempt int
	var typedSHA, requestID string
	var consumedVersion, consumedLeader, consumedRunner uint64
	err := conn.QueryRowContext(ctx, `SELECT reviewer_attempt_id,reviewer_attempt,reviewer_typed_sha256,correction_budget_request_id,consumed_ticket_version,consumed_leader_epoch,consumed_runner_epoch
		FROM final_review_repair_boundaries WHERE channel=? AND project_id=? AND ticket_id=? AND target_state=? AND transition_ticket_version=?`, ref.Channel, ref.Project, ref.Ticket, state, version).Scan(&attemptID, &attempt, &typedSHA, &requestID, &consumedVersion, &consumedLeader, &consumedRunner)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	if attemptID <= 0 || attempt <= 0 || !validSHA256(typedSHA) || !boundedText(requestID, 300) || consumedVersion+1 != version || consumedLeader == 0 || consumedLeader >= newLeader || consumedRunner != runner {
		return 0, false, ErrPublicationEvidence
	}
	var storedAttempt int
	var storedTyped, phase, role, attemptState, outcome string
	var expected, claimLeader, claimRunner uint64
	if err := conn.QueryRowContext(ctx, `SELECT a.attempt,r.typed_sha256,a.phase,a.role,a.state,a.outcome,a.expected_ticket_version,a.leader_epoch,a.runner_epoch
		FROM provider_attempt_results r JOIN provider_attempts a ON a.id=r.provider_attempt_id WHERE r.provider_attempt_id=? AND r.channel=? AND r.project_id=? AND r.ticket_id=?`, attemptID, ref.Channel, ref.Project, ref.Ticket).Scan(&storedAttempt, &storedTyped, &phase, &role, &attemptState, &outcome, &expected, &claimLeader, &claimRunner); err != nil || storedAttempt != attempt || storedTyped != typedSHA || phase != string(domain.PhaseReview) || role != "reviewer" || attemptState != "completed" || outcome != "completed" || expected != consumedVersion || claimLeader != consumedLeader || claimRunner != consumedRunner {
		return 0, false, ErrPublicationEvidence
	}
	var uses, transitions int
	if err := conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM ticket_budget_uses WHERE channel=? AND project_id=? AND ticket_id=? AND kind='correction' AND request_id=? AND ticket_version=? AND leader_epoch=? AND runner_epoch=?`, ref.Channel, ref.Project, ref.Ticket, requestID, consumedVersion, consumedLeader, consumedRunner).Scan(&uses); err != nil || uses != 1 {
		return 0, false, ErrPublicationEvidence
	}
	if err := conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version=? AND trigger='review_repair' AND from_state='reviewing' AND to_state=?`, ref.Channel, ref.Project, ref.Ticket, version, state).Scan(&transitions); err != nil || transitions != 1 {
		return 0, false, ErrPublicationEvidence
	}
	return consumedLeader, true, nil
}

// durableCandidateRepairBaseline proves the narrow correction path where a
// Builder completed and atomically persisted a successor candidate. It is not
// a shape-only lifecycle bridge: the immutable repair binding, sealed recovery
// prefix, exact predecessor publication/revision, Builder result, candidate
// command, and completion must all authenticate before startup may use it.
func (s *Store) durableCandidateRepairBaseline(ctx context.Context, conn *sql.Conn, ref domain.TicketRef, baseline phaseRecoveryBaseline) (bool, error) {
	var bindings int
	var targetGeneration uint64
	if err := conn.QueryRowContext(ctx, `SELECT COUNT(*),COALESCE(MAX(target_generation),0) FROM candidate_repair_bindings WHERE channel=? AND project_id=? AND ticket_id=?`, ref.Channel, ref.Project, ref.Ticket).Scan(&bindings, &targetGeneration); err != nil {
		return false, normalizeBusy(ctx, err)
	}
	if bindings == 0 {
		return false, nil
	}
	if bindings != 1 {
		return true, ErrPublicationEvidence
	}
	candidate, err := s.latestCandidateFrom(ctx, conn, ref, false)
	if err != nil {
		return true, ErrPublicationEvidence
	}
	// A retained repair binding must not shadow a later ordinary candidate, and
	// a repair that has not produced its successor candidate is not yet a
	// completed repair baseline. Once the target generation is current, every
	// remaining mismatch is corruption rather than a reason to fall back to a
	// weaker generic phase proof.
	if candidate.Snapshot.Generation != targetGeneration {
		return false, nil
	}
	entry, err := loadProviderPhaseEntryAt(ctx, conn, ref, domain.PhaseBuild, baseline.version)
	if err != nil {
		return true, ErrPublicationEvidence
	}
	if entry.From != domain.StateWaitingCI || entry.State != domain.StateBuilding || entry.Trigger != "checks_red" {
		return false, nil
	}
	result, _, err := s.loadHistoricalProviderAttemptResult(ctx, conn, candidate.BuilderResult)
	if err != nil {
		return true, ErrPublicationEvidence
	}
	if result.Claim.ExpectedVersion != baseline.version || result.Claim.LeaderEpoch != baseline.leader || result.Claim.RunnerEpoch != baseline.runner {
		return true, ErrPublicationEvidence
	}
	if _, err := completedCandidateRepairContextAt(ctx, conn, candidate, result); err != nil {
		return true, ErrPublicationEvidence
	}
	if err := s.reauthenticateStoredCandidateCommandHistoricalFrom(ctx, conn, ref, candidate); err != nil {
		return true, ErrPublicationEvidence
	}
	return true, nil
}

// candidateRepairRecoveryPredecessor authenticates the crash window after the
// Store atomically consumes red CI into the one allowed checks_red Build entry
// and before that Builder has produced a successor candidate. The immutable
// repair binding and phase entry are the authority; an older completed Builder
// result is only predecessor context and must never be reused as this entry's
// startup baseline.
//
// A retained binding is deliberately non-applicable once a later Build entry
// exists (provider retry, final-review repair, or another ordinary transition).
// Those entries must recover through their own dedicated authority.
func (s *Store) candidateRepairRecoveryPredecessor(ctx context.Context, conn *sql.Conn, ref domain.TicketRef, state domain.State, version, runner, newLeader uint64, latest RunnerRecoveryLedger, latestFound bool) (uint64, bool, error) {
	if state != domain.StateBuilding || version == 0 || runner == 0 || newLeader == 0 {
		return 0, false, nil
	}
	authority, found, err := candidateRepairRecoveryAnchor(ctx, conn, ref)
	if err != nil || !found {
		return 0, found, err
	}
	entry, err := loadProviderPhaseEntryAt(ctx, conn, ref, domain.PhaseBuild, version)
	if err != nil {
		return 0, true, ErrPublicationEvidence
	}
	if entry.Version != authority.context.EntryTicketVersion {
		return 0, false, nil
	}

	priorLeader := authority.context.EntryFence.LeaderEpoch
	if latestFound && latest.TicketVersion == version && latest.RunnerEpoch == runner {
		priorLeader = latest.LeaderEpoch
	} else if controlLeader, controlFound, controlErr := loadRuntimeControlEndpointLeader(ctx, conn, ref, version, runner); controlErr != nil {
		return 0, true, controlErr
	} else if controlFound {
		priorLeader = controlLeader
	}
	if priorLeader == 0 || priorLeader >= newLeader {
		return 0, true, ErrPublicationEvidence
	}
	if repairFound, repairErr := validateCandidateRepairRecoveryTarget(ctx, conn, ref, version, runner, priorLeader); repairErr != nil || !repairFound {
		return 0, true, ErrPublicationEvidence
	}
	// This additionally proves that either no target candidate/completion exists,
	// or the complete immutable candidate/result/command/completion tuple exists.
	// A partial Builder result is never treated as completed repair evidence.
	if err := s.authenticateCandidateRepairBuildContextAt(ctx, conn, ref, version, domain.Fence{LeaderEpoch: priorLeader, RunnerEpoch: runner}); err != nil {
		return 0, true, ErrPublicationEvidence
	}
	return priorLeader, true, nil
}

func recoveryProviderPhase(state domain.State) (domain.Phase, string, bool) {
	switch state {
	case domain.StatePlanning:
		return domain.PhasePlanning, "planner", true
	case domain.StateVerifying:
		return domain.PhaseVerification, "reviewer", true
	case domain.StateBuilding:
		return domain.PhaseBuild, "builder", true
	case domain.StateReviewing:
		// A completed final Reviewer result is a durable recovery predecessor
		// just like Planner/Verifier/Builder. Its source endpoint is the
		// authenticated checks_green transition, not a publication shortcut.
		return domain.PhaseReview, "reviewer", true
	default:
		return "", "", false
	}
}

// loadActiveProviderEndpoint authenticates the exact provider claim that was
// still running when the daemon died. It is a narrow recovery predecessor for
// the no-result crash window; counters, events, and older worktree rows cannot
// substitute for this immutable claim/binding.
func loadActiveProviderEndpoint(ctx context.Context, q rowQueryer, ref domain.TicketRef, phase domain.Phase, role string, version, runner, newLeader uint64) (uint64, bool, error) {
	if ref.Validate() != nil || phase == "" || role == "" || version == 0 || runner == 0 || newLeader == 0 {
		return 0, false, ErrPublicationEvidence
	}
	rows, err := q.QueryContext(ctx, `SELECT id FROM provider_attempts WHERE channel=? AND project_id=? AND ticket_id=? AND phase=? AND role=? AND expected_ticket_version=? AND runner_epoch=? AND leader_epoch>0 AND leader_epoch<? AND state IN ('active','quarantined') ORDER BY id`, ref.Channel, ref.Project, ref.Ticket, phase, role, version, runner, newLeader)
	if err != nil {
		return 0, false, err
	}
	defer rows.Close()
	var id int64
	count := 0
	for rows.Next() {
		if err := rows.Scan(&id); err != nil {
			return 0, false, err
		}
		count++
		if count > 1 {
			return 0, false, ErrPublicationEvidence
		}
	}
	if err := rows.Err(); err != nil {
		return 0, false, err
	}
	if count == 0 {
		return 0, false, nil
	}
	claim, err := loadAuthenticatedProviderAttemptClaim(ctx, q, id)
	if err != nil || claim.Ref != ref || claim.Phase != phase || claim.Role != role || claim.ExpectedVersion != version || claim.RunnerEpoch != runner || claim.LeaderEpoch == 0 || claim.LeaderEpoch >= newLeader {
		return 0, false, ErrPublicationEvidence
	}
	return claim.LeaderEpoch, true, nil
}

// loadPhaseRecoveryBaseline intentionally reads the newest completed source
// claim.  A later malformed completion must not be skipped by adopting an
// older result.
func (s *Store) loadPhaseRecoveryBaseline(ctx context.Context, conn *sql.Conn, ref domain.TicketRef, phase domain.Phase, role string) (phaseRecoveryBaseline, bool, error) {
	var b phaseRecoveryBaseline
	var attempt int
	var resultID sql.NullInt64
	err := conn.QueryRowContext(ctx, `SELECT r.provider_attempt_id,a.attempt
		FROM provider_attempts a LEFT JOIN provider_attempt_results r ON r.provider_attempt_id=a.id
		WHERE a.channel=? AND a.project_id=? AND a.ticket_id=? AND a.phase=? AND a.role=?
		AND a.state='completed' AND a.outcome='completed' AND a.finished_at IS NOT NULL AND a.finished_at <> ''
		ORDER BY a.attempt DESC,a.id DESC LIMIT 1`, ref.Channel, ref.Project, ref.Ticket, phase, role).Scan(&resultID, &attempt)
	if errors.Is(err, sql.ErrNoRows) {
		return phaseRecoveryBaseline{}, false, nil
	}
	if err != nil {
		return phaseRecoveryBaseline{}, false, err
	}
	// The newest terminal attempt is authoritative even when its immutable
	// result row is missing. Never hide a resultless/tampered completion by
	// falling back to an older successful attempt.
	if !resultID.Valid {
		return phaseRecoveryBaseline{}, false, ErrPublicationEvidence
	}
	resultKey := ProviderAttemptResultKey{AttemptID: resultID.Int64, Ref: ref, Phase: phase, Attempt: attempt}
	result, _, err := s.loadHistoricalProviderAttemptResult(ctx, conn, resultKey)
	if err != nil || result.Claim.Role != role || result.Claim.Ref != ref || result.Claim.Phase != phase {
		return phaseRecoveryBaseline{}, false, ErrPublicationEvidence
	}
	b.version, b.runner, b.leader, b.currentLeader = result.Claim.ExpectedVersion, result.Claim.RunnerEpoch, result.Claim.LeaderEpoch, result.Claim.LeaderEpoch
	return b, true, nil
}

// ErrLeaseAdoption keeps capacity held when the ownership of an invalidated
// lease cannot be proven safe to transfer to a replacement runner.
var ErrLeaseAdoption = errors.New("invalidated leases cannot be adopted")

// ErrStartState identifies a queued ticket that cannot be admitted to a
// workflow without an operator-visible state decision.
var ErrStartState = errors.New("ticket cannot be started in its current state")

// FenceRecoveredRunners advances every actively owned ticket runner under the
// new durable leader. It never releases leases: only a later supervisor proof
// may free capacity that could still belong to a live old process.
func (s *Store) FenceRecoveredRunners(ctx context.Context, channel domain.Channel, leaderEpoch uint64) (int64, error) {
	if !channel.Valid() || leaderEpoch == 0 {
		return 0, errors.New("valid channel and leader epoch are required")
	}
	var changed int64
	err := s.write(ctx, func(conn *sql.Conn) error {
		var current uint64
		if err := conn.QueryRowContext(ctx, `SELECT leader_epoch FROM daemon_instances WHERE channel=?`, channel).Scan(&current); err != nil {
			return err
		}
		if current != leaderEpoch {
			return ErrStaleFence
		}
		rows, err := conn.QueryContext(ctx, `SELECT project_id,id,state,version,runner_epoch,blocked_code FROM tickets WHERE channel=? AND (state IN ('planning','verifying','building','publishing','waiting_ci','reviewing','waiting_approval','waiting_manual_merge','merging','reconciling','stopping','cancelling') OR (state='blocked' AND blocked_code='legacy_provider_phase_entry_unverifiable' AND EXISTS(SELECT 1 FROM provider_attempts a WHERE a.channel=tickets.channel AND a.project_id=tickets.project_id AND a.ticket_id=tickets.id AND a.runner_epoch=tickets.runner_epoch AND a.state IN ('active','quarantined')) AND EXISTS(SELECT 1 FROM events e WHERE e.channel=tickets.channel AND e.project_id=tickets.project_id AND e.ticket_id=tickets.id AND e.ticket_version=tickets.version AND e.trigger='typed_blocker' AND e.to_state='blocked'))) ORDER BY project_id,id`, channel)
		if err != nil {
			return err
		}
		type activeTicket struct {
			project domain.ProjectID
			id      domain.TicketID
			state   domain.State
			version uint64
			runner  uint64
			code    string
		}
		var active []activeTicket
		for rows.Next() {
			var ticket activeTicket
			if err := rows.Scan(&ticket.project, &ticket.id, &ticket.state, &ticket.version, &ticket.runner, &ticket.code); err != nil {
				rows.Close()
				return err
			}
			active = append(active, ticket)
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return err
		}
		rows.Close()
		for _, ticket := range active {
			ref := domain.TicketRef{Channel: channel, Project: ticket.project, Ticket: ticket.id}
			latest, found, err := loadLatestRunnerRecovery(ctx, conn, ref)
			if err != nil {
				return err
			}
			if found {
				if !validRunnerRecovery(latest) {
					return ErrPublicationEvidence
				}
				if latest.TicketVersion == ticket.version && latest.RunnerEpoch == ticket.runner && latest.LeaderEpoch == leaderEpoch {
					continue // lost-response replay under the same leader
				}
				// A direct control invalidation may advance the ticket without writing
				// a recovery row. It is not an authenticated predecessor for startup
				// fencing; do not mint a zero-prior or bridged recovery row from it.
				if latest.TicketVersion > ticket.version || latest.RunnerEpoch > ticket.runner || leaderEpoch <= latest.LeaderEpoch {
					return ErrStaleFence
				}
			}
			latestFound := found
			var waitingPublication PublishedCandidateEvidence
			// Runner recovery is also an append-only, bounded authority. Its cap
			// is lifetime-wide for the ticket: publication's waiting-ci semantic
			// chain starts at the transition, but must not reset the finite
			// recovery resource consumed before that transition. A same-leader
			// lost-response returned above is the only replay allowed at the cap.
			if ticket.state == domain.StateWaitingCI {
				publication, publicationFound, err := loadPublicationEvidenceRow(ctx, conn, ref)
				if err != nil || !publicationFound {
					return ErrPublicationEvidence
				}
				if err := loadLatestPublicationRebind(ctx, conn, &publication); err != nil {
					return ErrPublicationEvidence
				}
				waitingPublication = publication
			}
			// AcquireLeader advances the daemon epoch before this startup fence can
			// append a ticket-local recovery row. A waiting_ci ticket with pending
			// evidence is therefore authenticated at its prior signed leader, not
			// against the new daemon epoch. The complete CI chain (including any
			// earlier runner recovery) is the only admissible predecessor.
			ciWaitingPriorLeader := uint64(0)
			if ticket.state == domain.StateWaitingCI {
				var pendingEvidence int
				if err := conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM ci_transition_evidence WHERE channel=? AND project_id=? AND ticket_id=? AND observation_classification='pending' AND resulting_state='waiting_ci'`, ref.Channel, ref.Project, ref.Ticket).Scan(&pendingEvidence); err != nil {
					return err
				}
				waitingVersion := waitingPublication.CurrentTicketVersion + 1
				pollPair, pollPairFound, pollPairErr := findCIPollResumePair(ctx, conn, ref, waitingVersion, ticket.version)
				if pollPairErr != nil {
					return pollPairErr
				}
				pollRetryResume := pollPairFound && pollPair.resumeVersion <= ticket.version && ticket.runner == waitingPublication.CurrentFence.RunnerEpoch
				if pendingEvidence > 0 || pollRetryResume {
					ciWaitingPriorLeader = waitingPublication.CurrentFence.LeaderEpoch
					if latestFound {
						ciWaitingPriorLeader = latest.LeaderEpoch
					}
					if ciWaitingPriorLeader == 0 || ciWaitingPriorLeader >= leaderEpoch {
						return ErrPublicationEvidence
					}
					if _, err := loadCICurrentPublicationAt(ctx, conn, ref, ciWaitingPriorLeader); err != nil {
						return ErrPublicationEvidence
					}
				}
			}
			var recoveryCount int
			if err := conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM runner_recovery_ledger WHERE channel=? AND project_id=? AND ticket_id=?`, channel, ticket.project, ticket.id).Scan(&recoveryCount); err != nil {
				return err
			}
			if recoveryCount >= 64 {
				return ErrPublicationEvidence
			}
			// Do not advance a publishing ticket into a recovery state that can
			// never be rebound. The 64-row rebind cap is a terminal recovery
			// limit, so check it before changing either ticket counters or the
			// runner ledger. The surrounding Store write rolls back every ticket
			// in this startup pass if one is capped.
			priorLeader := uint64(0)
			if ticket.state == domain.StateCancelling {
				if found && latest.TicketVersion == ticket.version && latest.RunnerEpoch == ticket.runner {
					priorLeader = latest.LeaderEpoch
				} else {
					control, controlFound, controlErr := exactCancellationControlPredecessor(ctx, conn, ref, ticket.version, ticket.runner)
					if controlErr != nil || !controlFound {
						return ErrPublicationEvidence
					}
					if control.leader >= leaderEpoch {
						return ErrPublicationEvidence
					}
					priorLeader = control.leader
				}
			}
			if ticket.state == domain.StateStopping {
				if found && latest.TicketVersion == ticket.version && latest.RunnerEpoch == ticket.runner {
					priorLeader = latest.LeaderEpoch
				} else {
					control, controlFound, controlErr := exactStoppingControlPredecessor(ctx, conn, ref, ticket.version, ticket.runner)
					if controlErr != nil || !controlFound {
						return ErrPublicationEvidence
					}
					if control.leader >= leaderEpoch {
						return ErrPublicationEvidence
					}
					priorLeader = control.leader
				}
			}
			if ticket.state == domain.StatePublishing {
				publication, candidate, publicationFound, err := s.publicationForLatestCandidateFrom(ctx, conn, ref)
				if err != nil {
					return ErrPublicationEvidence
				}
				if publicationFound {
					if err := loadLatestPublicationRebind(ctx, conn, &publication); err != nil || publication.CurrentFence.RunnerEpoch != ticket.runner {
						return ErrPublicationEvidence
					}
					if publication.CurrentTicketVersion != ticket.version {
						pair := ticket.version == publication.CurrentTicketVersion+2 &&
							ticket.runner == publication.CurrentFence.RunnerEpoch &&
							(authenticateBlockedPublicationResume(ctx, conn, ref, publication.CurrentTicketVersion+1, ticket.version, domain.StatePublishing, domain.StatePublishing) == nil ||
								authenticateSemanticPublicationResume(ctx, conn, ref, publication.CurrentTicketVersion+1, ticket.version, domain.StatePublishing) == nil)
						if !pair || publication.CurrentFence.LeaderEpoch == 0 || publication.CurrentFence.LeaderEpoch >= leaderEpoch {
							return ErrPublicationEvidence
						}
						priorLeader = publication.CurrentFence.LeaderEpoch
					} else {
						priorLeader = publication.CurrentFence.LeaderEpoch
					}
					var rebinds int
					if err := conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM publication_evidence_rebinds WHERE channel=? AND project_id=? AND ticket_id=? AND candidate_generation=? AND candidate_head_sha=?`, channel, ticket.project, ticket.id, publication.Candidate.Snapshot.Generation, publication.Candidate.Snapshot.HeadSHA).Scan(&rebinds); err != nil {
						return err
					}
					if rebinds >= 64 {
						return ErrPublicationEvidence
					}
				} else if found && latest.TicketVersion == ticket.version && latest.RunnerEpoch == ticket.runner {
					priorLeader = latest.LeaderEpoch
				} else {
					// A candidate-only publishing ticket can also be resumed through
					// the sealed post-publication control boundary before its external
					// publication witness exists. Authenticate that exact stop/resume
					// chain and pre-stop candidate here; otherwise the ordinary
					// candidate-only path below would reject the resumed counter gap.
					// A source-only takeover begins in Building and resumes into
					// Verifying. It can later cross fresh verification/build and become
					// candidate-only Publishing before an external witness exists. That
					// is not a post-publication control triplet: authenticate its exact
					// immutable source lineage, then let the candidate-only fallback
					// below prove the candidate and build->publishing transition. Any
					// unrelated pre-publication control remains subject to the normal
					// post-publication baseline and fails closed.
					_, sourceFound, sourceErr := s.operatorSourceResumeEndpointFrom(ctx, conn, ref)
					if sourceErr != nil {
						return ErrPublicationEvidence
					}
					if !sourceFound {
						if postLeader, postFound, postErr := s.postPublicationRecoveryBaseline(ctx, conn, ref, ticket.state, ticket.version, ticket.runner, leaderEpoch); postErr != nil {
							return postErr
						} else if postFound {
							priorLeader = postLeader
						}
					}
					if priorLeader == 0 {
						// A candidate can be durable before the build->publishing
						// signal, while publication evidence is intentionally absent
						// until the external boundary runs. Authenticate that exact
						// candidate and transition as the first recovery predecessor.
						if candidate.TicketVersion == ^uint64(0) || candidate.TicketVersion > ticket.version {
							return ErrPublicationEvidence
						}
						checkpointVersion, checkpointFence, checkpointErr := candidateSnapshotEndpointAt(ctx, conn, ref, candidate)
						if checkpointErr != nil {
							return ErrPublicationEvidence
						}
						buildVersion, buildErr := candidatePublishingTransitionVersionAt(ctx, conn, ref, checkpointVersion, ticket.version)
						if buildErr != nil {
							return ErrPublicationEvidence
						}
						buildFence := checkpointFence
						if buildVersion != checkpointVersion+1 {
							buildFence, buildErr = candidatePublishingTransitionFenceAt(ctx, conn, ref, candidate, buildVersion, ticket.version, domain.Fence{LeaderEpoch: candidate.Fence.LeaderEpoch, RunnerEpoch: ticket.runner})
							if buildErr != nil {
								return ErrPublicationEvidence
							}
						}
						priorLeader = candidate.Fence.LeaderEpoch
						exactLatest := found && latest.TicketVersion == ticket.version && latest.RunnerEpoch == ticket.runner
						authenticatedEndpoint := exactLatest
						if exactLatest {
							priorLeader = latest.LeaderEpoch
						}
						// Before the first publication witness, one or more exact
						// publication blocker/recover pairs may follow either the
						// latest signed recovery endpoint or the direct
						// building->publishing endpoint. Authenticate every pair;
						// never infer this predecessor from counter distance alone.
						if !authenticatedEndpoint && found && latest.TicketVersion >= buildVersion && validPublishingResumeGap(ctx, conn, ref, latest.TicketVersion, latest.RunnerEpoch, latest.LeaderEpoch, ticket.version, ticket.runner, latest.LeaderEpoch) {
							priorLeader = latest.LeaderEpoch
							authenticatedEndpoint = true
						}
						if !authenticatedEndpoint && (!found || latest.TicketVersion < buildVersion) {
							publishingVersion := buildVersion
							if (ticket.version == publishingVersion && ticket.runner == buildFence.RunnerEpoch) || validPublishingResumeGap(ctx, conn, ref, publishingVersion, buildFence.RunnerEpoch, buildFence.LeaderEpoch, ticket.version, ticket.runner, buildFence.LeaderEpoch) {
								priorLeader = buildFence.LeaderEpoch
								authenticatedEndpoint = true
							}
						}
						if controlLeader, controlFound, controlErr := loadRuntimeControlEndpointLeader(ctx, conn, ref, ticket.version, ticket.runner); controlErr != nil {
							return controlErr
						} else if controlFound {
							priorLeader = controlLeader
							authenticatedEndpoint = true
						} else if !authenticatedEndpoint {
							return ErrPublicationEvidence
						}
						if priorLeader == 0 || priorLeader >= leaderEpoch {
							return ErrPublicationEvidence
						}
						predecessorFence := domain.Fence{LeaderEpoch: priorLeader, RunnerEpoch: ticket.runner}
						if s.authenticateCandidateOnlyPublishingRecoveryAt(ctx, conn, ref, candidate, ticket.version, predecessorFence, false) != nil {
							return ErrPublicationEvidence
						}
					}
				}
			}
			if repairLeader, repairFound, repairErr := reviewRepairRecoveryPredecessor(ctx, conn, ref, ticket.state, ticket.version, ticket.runner, leaderEpoch); repairErr != nil {
				return repairErr
			} else if repairFound {
				priorLeader = repairLeader
			}
			// A source-only takeover deliberately enters Verifying before the fresh
			// Reviewer exists.  Its prior reviewer is immutable historical input,
			// not a recovery predecessor: authenticate the sealed source-resume
			// endpoint first so generic phase/worktree fallbacks cannot bypass the
			// required fresh verification after a daemon restart.
			if priorLeader == 0 {
				if sourceLeader, sourceFound, sourceErr := s.operatorSourceResumeRecoveryPredecessor(ctx, conn, ref, ticket.state, ticket.version, ticket.runner); sourceErr != nil {
					return ErrPublicationEvidence
				} else if sourceFound {
					priorLeader = sourceLeader
				}
			}
			if priorLeader == 0 {
				if amendmentLeader, amendmentFound, amendmentErr := verificationAmendmentRecoveryPredecessor(ctx, conn, ref, ticket.state, ticket.version, ticket.runner, leaderEpoch); amendmentErr != nil {
					return amendmentErr
				} else if amendmentFound {
					priorLeader = amendmentLeader
				}
			}
			if priorLeader == 0 {
				if retryLeader, retryFound, retryErr := providerRetryRecoveryPredecessor(ctx, conn, ref, ticket.state, ticket.version, ticket.runner, leaderEpoch); retryErr != nil {
					return retryErr
				} else if retryFound {
					priorLeader = retryLeader
				}
			}
			if priorLeader == 0 {
				if blockerLeader, blockerFound, blockerErr := providerBlockedRecoveryPredecessor(ctx, conn, ref, ticket.state, ticket.version, ticket.runner, leaderEpoch); blockerErr != nil {
					return blockerErr
				} else if blockerFound {
					priorLeader = blockerLeader
				}
			}
			if priorLeader == 0 {
				// A crash after the ordinary provider pause/resume transition but
				// before Controller.Rearm must retain the sealed control endpoint.
				// This runs before phase-baseline fallback so an old claim or
				// worktree cannot silently bless a malformed resumed control row.
				if pausedLeader, pausedFound, pausedErr := providerPausedRecoveryPredecessor(ctx, conn, ref, ticket.state, ticket.version, ticket.runner, leaderEpoch); pausedErr != nil {
					return pausedErr
				} else if pausedFound {
					priorLeader = pausedLeader
				}
			}
			if priorLeader == 0 && ticket.state == domain.StateBuilding {
				// ConsumeCIObservation creates the repair Build entry and binding in
				// one transaction, before any fresh Builder claim or successor
				// candidate is required to exist. Authenticate that Store-owned entry
				// before the generic phase baseline sees the predecessor generation's
				// completed Builder. The generic load below still runs as an integrity
				// check so a newest completed-success attempt with a missing or
				// malformed result remains fatal.
				if candidateLeader, candidateFound, candidateErr := s.candidateRepairRecoveryPredecessor(ctx, conn, ref, ticket.state, ticket.version, ticket.runner, leaderEpoch, latest, found); candidateErr != nil {
					return candidateErr
				} else if candidateFound {
					priorLeader = candidateLeader
				}
			}
			if phase, role, ok := recoveryProviderPhase(ticket.state); ok {
				baseline, baselineFound, baselineErr := s.loadPhaseRecoveryBaseline(ctx, conn, ref, phase, role)
				if baselineErr != nil {
					return ErrPublicationEvidence
				}
				repairBaseline := false
				if baselineFound && phase == domain.PhaseBuild && role == "builder" {
					var repairErr error
					repairBaseline, repairErr = s.durableCandidateRepairBaseline(ctx, conn, ref, baseline)
					if repairErr != nil {
						return ErrPublicationEvidence
					}
				}
				if baselineFound && found && latest.TicketVersion == ticket.version && latest.RunnerEpoch == ticket.runner {
					baseline.currentLeader = latest.LeaderEpoch
				}
				if baselineFound && priorLeader == 0 {
					// The recovery row being appended starts from the authenticated
					// pre-fence endpoint, not the provider claim's original leader.
					// Prefer an exact prior ledger row, then the durable control
					// authority created by pause/take + resume; only the first recovery
					// falls back to the provider claim's source leader.
					exactLatest := found && latest.TicketVersion == ticket.version && latest.RunnerEpoch == ticket.runner
					priorLeader = baseline.currentLeader
					if exactLatest {
						priorLeader = latest.LeaderEpoch
					}
					if controlLeader, controlFound, controlErr := loadRuntimeControlEndpointLeader(ctx, conn, ref, ticket.version, ticket.runner); controlErr != nil {
						return controlErr
					} else if controlFound {
						priorLeader = controlLeader
					} else if !exactLatest && (ticket.version != baseline.version || ticket.runner != baseline.runner) && !(ticket.version == baseline.version+1 && ticket.runner == baseline.runner) {
						// More than the single authenticated phase bridge means a
						// pause/take or other control advance occurred. Without its
						// durable endpoint, the source leader is not an authority for
						// this live fence.
						return ErrPublicationEvidence
					}
					if priorLeader == 0 || priorLeader >= leaderEpoch {
						return ErrPublicationEvidence
					}
					if !found && baseline.runner == 1 {
						// Bootstrap only through the immutable provider claim's
						// own endpoint. Any later pause/resume or phase bridge is
						// authenticated separately by the source-to-ticket ledger
						// check below; folding it into the initial lifecycle would
						// reject valid post-result operator control.
						if err := validateInitialLifecycleAdvance(ctx, conn, ref, baseline.version); err != nil && !repairBaseline {
							return ErrPublicationEvidence
						}
					}
					// Authenticate the source claim to the exact pre-fence endpoint.
					// Passing leaderEpoch here would let a generic provider reader
					// bless an unrecorded leader-only takeover.
					if err := validateRunnerRecoveryLedger(ctx, conn, ref, baseline.version, baseline.runner, baseline.leader, ticket.version, ticket.runner, priorLeader); err != nil {
						return ErrPublicationEvidence
					}
				} else if priorLeader == 0 && !baselineFound && ticket.state == domain.StateVerifying {
					// Planning may have been consumed by the exact planning->verifying
					// transition before the first reviewer claim was issued. That is a
					// narrow predecessor: the completed Planner source, one canonical
					// phase_pass endpoint, and any prior signed recovery rows must all
					// reach this pre-fence ticket identity.
					planning, planningFound, planningErr := s.loadPhaseRecoveryBaseline(ctx, conn, ref, domain.PhasePlanning, "planner")
					if planningErr != nil {
						return ErrPublicationEvidence
					}
					if planningFound && !(found && latest.TicketVersion == ticket.version && latest.RunnerEpoch == ticket.runner) {
						var transitions int
						if err := conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version=? AND trigger='phase_pass' AND from_state='planning' AND to_state='verifying'`, channel, ticket.project, ticket.id, ticket.version).Scan(&transitions); err != nil || transitions != 1 {
							return ErrPublicationEvidence
						}
						exactSource := ticket.version == planning.version+1 && ticket.runner == planning.runner
						exactLatest := found && latest.TicketVersion+1 == ticket.version && latest.RunnerEpoch == ticket.runner
						if !exactSource && !exactLatest {
							return ErrPublicationEvidence
						}
						priorLeader = planning.currentLeader
						if exactLatest {
							priorLeader = latest.LeaderEpoch
						}
						if priorLeader == 0 || priorLeader >= leaderEpoch {
							return ErrPublicationEvidence
						}
						if !found && planning.runner == 1 && validateInitialLifecycleAdvance(ctx, conn, ref, ticket.version) != nil {
							return ErrPublicationEvidence
						}
						if err := validateRunnerRecoveryLedger(ctx, conn, ref, planning.version, planning.runner, planning.leader, ticket.version, ticket.runner, priorLeader); err != nil {
							return ErrPublicationEvidence
						}
					}
				}
			}
			if priorLeader == 0 && ciWaitingPriorLeader != 0 {
				priorLeader = ciWaitingPriorLeader
			} else if priorLeader == 0 && found && latest.TicketVersion == ticket.version && latest.RunnerEpoch == ticket.runner {
				priorLeader = latest.LeaderEpoch
			} else if priorLeader == 0 && ticket.state == domain.StateBuilding {
				// Verification may have committed and transitioned the ticket to
				// Building before Builder started. Preserve that reviewer leader as
				// the first recovery predecessor only when the one intervening
				// verification->building event is exact; this is the durable bridge
				// consumed by the candidate-less building recovery path.
				verification, verificationFound, verificationErr := s.loadPhaseRecoveryBaseline(ctx, conn, ref, domain.PhaseVerification, "reviewer")
				if verificationErr != nil {
					return ErrPublicationEvidence
				}
				if verificationFound {
					var transitions int
					if err := conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version=? AND trigger='phase_pass' AND from_state='verifying' AND to_state='building'`, channel, ticket.project, ticket.id, ticket.version).Scan(&transitions); err != nil || transitions != 1 || verification.version+1 != ticket.version || verification.runner != ticket.runner {
						return ErrPublicationEvidence
					}
					exactLatest := found && latest.TicketVersion == ticket.version && latest.RunnerEpoch == ticket.runner
					priorLeader = verification.currentLeader
					if exactLatest {
						priorLeader = latest.LeaderEpoch
					}
					if controlLeader, controlFound, controlErr := loadRuntimeControlEndpointLeader(ctx, conn, ref, ticket.version, ticket.runner); controlErr != nil {
						return controlErr
					} else if controlFound {
						priorLeader = controlLeader
					} else if !exactLatest && (ticket.version != verification.version+1 || ticket.runner != verification.runner) {
						return ErrPublicationEvidence
					}
					if priorLeader == 0 || priorLeader >= leaderEpoch {
						return ErrPublicationEvidence
					}
					if err := validateRunnerRecoveryLedger(ctx, conn, ref, verification.version, verification.runner, verification.leader, ticket.version, ticket.runner, priorLeader); err != nil {
						return ErrPublicationEvidence
					}
				}
			} else if priorLeader == 0 && found {
				// Non-phase states (including final review) have no dedicated
				// baseline, but may still cross a complete pause/take handoff.
				if controlLeader, controlFound, controlErr := loadRuntimeControlEndpointLeader(ctx, conn, ref, ticket.version, ticket.runner); controlErr != nil {
					return controlErr
				} else if controlFound && validateRunnerControlAdvance(ctx, conn, ref, latest.TicketVersion, latest.RunnerEpoch, latest.LeaderEpoch, ticket.version, ticket.runner, controlLeader) == nil {
					priorLeader = controlLeader
				} else if postLeader, postFound, postErr := s.postPublicationRecoveryBaseline(ctx, conn, ref, ticket.state, ticket.version, ticket.runner, leaderEpoch); postErr != nil {
					return postErr
				} else if postFound {
					priorLeader = postLeader
				} else if mergeLeader, mergeFound, mergeErr := s.normalPostPublicationRecoveryPredecessor(ctx, conn, ref, ticket.state, ticket.version, ticket.runner, leaderEpoch); mergeErr != nil {
					return mergeErr
				} else if mergeFound {
					priorLeader = mergeLeader
				} else if publication, ok, publicationErr := s.publicationRecoveryBaseline(ctx, conn, ref, ticket.version, ticket.runner); publicationErr != nil {
					return publicationErr
				} else if ok {
					priorLeader = publication
				}
			} else if priorLeader == 0 {
				if postLeader, postFound, postErr := s.postPublicationRecoveryBaseline(ctx, conn, ref, ticket.state, ticket.version, ticket.runner, leaderEpoch); postErr != nil {
					return postErr
				} else if postFound {
					priorLeader = postLeader
				}
				if priorLeader == 0 {
					mergeLeader, mergeFound, mergeErr := s.normalPostPublicationRecoveryPredecessor(ctx, conn, ref, ticket.state, ticket.version, ticket.runner, leaderEpoch)
					if mergeErr != nil {
						return mergeErr
					}
					if mergeFound {
						priorLeader = mergeLeader
					}
				}
				if priorLeader == 0 {
					publication, ok, err := s.publicationRecoveryBaseline(ctx, conn, ref, ticket.version, ticket.runner)
					if err != nil {
						return err
					} else if ok {
						priorLeader = publication
					}
				}
			}
			if priorLeader == 0 {
				if ticket.state == domain.StateBlocked && ticket.code == "legacy_provider_phase_entry_unverifiable" {
					legacyLeader, legacyErr := legacyProviderMigrationRecoveryPredecessor(ctx, conn, ref, ticket.version, ticket.runner, leaderEpoch)
					if legacyErr != nil {
						return legacyErr
					}
					priorLeader = legacyLeader
				}
			}
			if priorLeader == 0 {
				if phase, role, ok := recoveryProviderPhase(ticket.state); ok {
					activeLeader, activeFound, activeErr := loadActiveProviderEndpoint(ctx, conn, ref, phase, role, ticket.version, ticket.runner, leaderEpoch)
					if activeErr != nil {
						return activeErr
					}
					if activeFound {
						priorLeader = activeLeader
					}
				}
			}
			if priorLeader == 0 {
				// A daemon can die after the atomic queued->planning start and
				// before asynchronous worktree registration. The dedicated start
				// authority is the only predecessor allowed in that crash window.
				if !found && ticket.state == domain.StatePlanning && ticket.version == 2 && ticket.runner == 1 {
					startAuthority, startAuthorityFound, startAuthorityErr := loadRunnerStartAuthority(ctx, conn, ref, ticket.version, ticket.runner)
					if startAuthorityErr != nil {
						return startAuthorityErr
					} else if startAuthorityFound {
						if startAuthority.LeaderEpoch >= leaderEpoch || validateInitialLifecycleAdvance(ctx, conn, ref, ticket.version) != nil {
							return ErrPublicationEvidence
						}
						priorLeader = startAuthority.LeaderEpoch
					}
				}
			}
			if priorLeader == 0 {
				if worktreeLeader, worktreeFound, worktreeErr := loadRegisteredWorktreeEndpoint(ctx, conn, ref, ticket.version, ticket.runner); worktreeErr != nil {
					return worktreeErr
				} else if worktreeFound && worktreeLeader < leaderEpoch {
					priorLeader = worktreeLeader
				}
			}
			if priorLeader == 0 {
				return ErrPublicationEvidence
			}
			result, err := conn.ExecContext(ctx, `UPDATE tickets SET runner_epoch=runner_epoch+1, version=version+1 WHERE channel=? AND project_id=? AND id=? AND version=? AND runner_epoch=? AND (state IN ('planning','verifying','building','publishing','waiting_ci','reviewing','waiting_approval','waiting_manual_merge','merging','reconciling','stopping','cancelling') OR (state='blocked' AND blocked_code='legacy_provider_phase_entry_unverifiable'))`, channel, ticket.project, ticket.id, ticket.version, ticket.runner)
			if err != nil {
				return err
			}
			updated, _ := result.RowsAffected()
			if updated != 1 {
				return ErrStaleFence
			}
			step := RunnerRecoveryLedger{Ref: ref, PriorTicketVersion: ticket.version, PriorRunnerEpoch: ticket.runner, PriorLeaderEpoch: priorLeader, TicketVersion: ticket.version + 1, RunnerEpoch: ticket.runner + 1, LeaderEpoch: leaderEpoch, CreatedAt: time.Now().UTC()}
			step.RecoveryDigest = runnerRecoveryDigest(step)
			if err := recordRunnerRecovery(ctx, conn, step); err != nil {
				return err
			}
			changed++
		}
		return nil
	})
	return changed, err
}

// LeaseRequest describes one capacity dimension. Resource names are durable
// semantic identities such as "machine", a canonical project ID, or a
// qualified provider/version. Generic callers provide already-resolved
// capacities; production queued-ticket admission resolves its immutable
// project generation inside StartWithProjectOwnership's transaction.
type LeaseRequest struct {
	Scope    string
	Resource string
	Capacity int
}

type Lease struct {
	Ref         domain.TicketRef
	Scope       string
	ScopeKey    string
	RunnerEpoch uint64
	AcquiredAt  time.Time
}

type startAdmission struct {
	requests []LeaseRequest
	project  *Project
}

// SetProjectStartOwnershipHookForTest installs a deterministic test hook. It
// must not be used by production callers.
func (s *Store) SetProjectStartOwnershipHookForTest(hook func()) {
	s.faultMu.Lock()
	s.startProjectOwnershipHook = hook
	s.faultMu.Unlock()
}

// StartWithOwnership admits a queued ticket, reserves every capacity
// dimension, and establishes workflow ownership in one SQLite transaction.
// A replay of the same start observes the existing planning owner and leases
// without emitting another transition event.
func (s *Store) StartWithOwnership(ctx context.Context, ref domain.TicketRef, expectedVersion uint64, fence domain.Fence, workflowID string, requests []LeaseRequest, at time.Time) (Ticket, bool, error) {
	if err := ref.Validate(); err != nil {
		return Ticket{}, false, err
	}
	if workflowID == "" || fence.LeaderEpoch == 0 || fence.RunnerEpoch == 0 {
		return Ticket{}, false, errors.New("workflow identity and fence are required")
	}
	requests, err := validateLeaseRequests(requests)
	if err != nil {
		return Ticket{}, false, err
	}
	if at.IsZero() {
		return Ticket{}, false, errors.New("start time is required")
	}
	return s.startWithOwnership(ctx, ref, expectedVersion, fence, workflowID, at, func(context.Context, *sql.Conn) (startAdmission, error) {
		return startAdmission{requests: requests}, nil
	})
}

// StartWithProjectOwnership is the production queued-ticket admission
// boundary. It resolves the current immutable project generation and derives
// its global/project lease capacities inside the same IMMEDIATE transaction
// that reserves those slots and freezes the snapshot onto the ticket. A
// concurrent config apply therefore either happens first (and its tighter
// capacity is used) or happens after this start (and the ticket retains the
// earlier generation); it can never mix generation N+1 with generation N's
// capacity.
func (s *Store) StartWithProjectOwnership(ctx context.Context, ref domain.TicketRef, expectedVersion uint64, fence domain.Fence, workflowID string, at time.Time) (Ticket, bool, error) {
	if err := ref.Validate(); err != nil {
		return Ticket{}, false, err
	}
	if workflowID == "" || fence.LeaderEpoch == 0 || fence.RunnerEpoch == 0 {
		return Ticket{}, false, errors.New("workflow identity and fence are required")
	}
	if at.IsZero() {
		return Ticket{}, false, errors.New("start time is required")
	}
	s.faultMu.RLock()
	hook := s.startProjectOwnershipHook
	s.faultMu.RUnlock()
	if hook != nil {
		hook()
	}
	return s.startWithOwnership(ctx, ref, expectedVersion, fence, workflowID, at, func(ctx context.Context, conn *sql.Conn) (startAdmission, error) {
		project, err := loadCurrentProjectConfiguration(ctx, conn, ref.Channel, ref.Project)
		if err != nil {
			return startAdmission{}, err
		}
		requests, err := projectStartLeaseRequests(ctx, conn, project, ref)
		if err != nil {
			return startAdmission{}, err
		}
		if project.ConfigGeneration == 0 {
			// There is intentionally no configuration row to CAS for the exact
			// legacy tuple. Preserve its historical empty ticket snapshot while
			// using the default capacities derived above. Once config apply
			// bootstraps generation one, every future admission takes the fully
			// bound configured branch below.
			return startAdmission{requests: requests}, nil
		}
		return startAdmission{requests: requests, project: &project}, nil
	})
}

func (s *Store) startWithOwnership(ctx context.Context, ref domain.TicketRef, expectedVersion uint64, fence domain.Fence, workflowID string, at time.Time, resolve func(context.Context, *sql.Conn) (startAdmission, error)) (Ticket, bool, error) {
	observed := false
	err := s.write(ctx, func(conn *sql.Conn) error {
		var state domain.State
		var version, runner uint64
		var persistedWorkflow string
		if err := conn.QueryRowContext(ctx, `SELECT state, version, runner_epoch, workflow_id FROM tickets WHERE channel=? AND project_id=? AND id=?`, ref.Channel, ref.Project, ref.Ticket).Scan(&state, &version, &runner, &persistedWorkflow); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return ErrNotFound
			}
			return err
		}
		if err := s.assertTicketFence(ctx, conn, ref, version, fence); err != nil {
			return err
		}
		if state == domain.StatePlanning {
			if version != expectedVersion || persistedWorkflow != workflowID {
				return ErrStaleFence
			}
			observed = true
		} else {
			if state != domain.StateQueued || version != expectedVersion {
				return fmt.Errorf("%w: state=%s", ErrStartState, state)
			}
			admission, err := resolve(ctx, conn)
			if err != nil {
				return err
			}
			for _, request := range admission.requests {
				lease, ok, err := acquireLease(ctx, conn, ref, runner, request, at.UTC())
				if err != nil {
					return err
				}
				if !ok {
					return fmt.Errorf("%w: scope=%s resource=%s capacity=%d", ErrLeaseCapacity, request.Scope, request.Resource, request.Capacity)
				}
				_ = lease
			}
			var updated sql.Result
			if admission.project != nil {
				updated, err = conn.ExecContext(ctx, `UPDATE tickets SET state='planning', version=version+1, workflow_id=?,
					config_generation=?, config_digest=?, config_snapshot_bytes=?
					WHERE channel=? AND project_id=? AND id=? AND state='queued' AND version=? AND runner_epoch=?
					AND EXISTS (SELECT 1 FROM projects p JOIN project_configurations c
						ON c.channel=p.channel AND c.project_id=p.id AND c.generation=p.current_config_generation
						WHERE p.channel=? AND p.id=? AND p.current_config_generation=? AND c.digest=? AND c.snapshot_bytes=?)`, workflowID,
					admission.project.ConfigGeneration, admission.project.ConfigDigest, admission.project.ConfigSnapshot,
					ref.Channel, ref.Project, ref.Ticket, expectedVersion, runner,
					ref.Channel, ref.Project, admission.project.ConfigGeneration, admission.project.ConfigDigest, admission.project.ConfigSnapshot)
			} else {
				updated, err = conn.ExecContext(ctx, `UPDATE tickets SET state='planning', version=version+1, workflow_id=?,
					config_generation=(SELECT current_config_generation FROM projects WHERE channel=? AND id=?),
					config_digest=COALESCE((SELECT c.digest FROM projects p JOIN project_configurations c ON c.channel=p.channel AND c.project_id=p.id AND c.generation=p.current_config_generation WHERE p.channel=? AND p.id=?), ''),
					config_snapshot_bytes=COALESCE((SELECT c.snapshot_bytes FROM projects p JOIN project_configurations c ON c.channel=p.channel AND c.project_id=p.id AND c.generation=p.current_config_generation WHERE p.channel=? AND p.id=?), X'')
					WHERE channel=? AND project_id=? AND id=? AND state='queued' AND version=? AND runner_epoch=?`, workflowID,
					ref.Channel, ref.Project, ref.Channel, ref.Project, ref.Channel, ref.Project,
					ref.Channel, ref.Project, ref.Ticket, expectedVersion, runner)
			}
			if err != nil {
				return err
			}
			if changed, _ := updated.RowsAffected(); changed != 1 {
				return ErrStaleFence
			}
			version++
			if _, err := conn.ExecContext(ctx, `INSERT INTO workflow_owners(channel, project_id, ticket_id, workflow_id, state, created_at) VALUES (?, ?, ?, ?, 'owned', ?)`, ref.Channel, ref.Project, ref.Ticket, workflowID, at.UTC().Format(time.RFC3339Nano)); err != nil {
				return err
			}
			createdAt := at.UTC().Format(time.RFC3339Nano)
			created, err := conn.ExecContext(ctx, `INSERT INTO events(channel, project_id, ticket_id, ticket_version, trigger, from_state, to_state, payload, created_at) VALUES (?, ?, ?, ?, 'operator_start', 'queued', 'planning', '{}', ?)`, ref.Channel, ref.Project, ref.Ticket, version, createdAt)
			if err != nil {
				return err
			}
			eventID, err := created.LastInsertId()
			if err != nil {
				return err
			}
			if err := recordProviderPhaseEntry(ctx, conn, ref, domain.PhasePlanning, version, fence.LeaderEpoch, runner, eventID, createdAt, domain.StateQueued, domain.StatePlanning, "operator_start"); err != nil {
				return err
			}
			if err := recordRunnerStartAuthority(ctx, conn, ref, version, fence, workflowID, createdAt); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return Ticket{}, false, err
	}
	result, err := s.Ticket(ctx, ref)
	return result, observed, err
}

func loadCurrentProjectConfiguration(ctx context.Context, conn *sql.Conn, channel domain.Channel, id domain.ProjectID) (Project, error) {
	project := Project{Channel: channel, ID: id}
	err := conn.QueryRowContext(ctx, `SELECT p.canonical_path,p.base_ref,p.current_config_generation,
		COALESCE(c.digest,''),COALESCE(c.snapshot_bytes,X'')
		FROM projects p LEFT JOIN project_configurations c
		ON c.channel=p.channel AND c.project_id=p.id AND c.generation=p.current_config_generation
		WHERE p.channel=? AND p.id=?`, channel, id).Scan(&project.Path, &project.BaseRef, &project.ConfigGeneration, &project.ConfigDigest, &project.ConfigSnapshot)
	if errors.Is(err, sql.ErrNoRows) {
		return Project{}, ErrNotFound
	}
	if err != nil {
		return Project{}, err
	}
	return project, nil
}

func projectStartLeaseRequests(ctx context.Context, conn *sql.Conn, project Project, ref domain.TicketRef) ([]LeaseRequest, error) {
	if project.ConfigGeneration == 0 && project.ConfigDigest == "" && len(project.ConfigSnapshot) == 0 {
		// A legacy pointer can mean either a deliberately unconfigured project
		// or a malformed/dangling generation history. Only the former keeps its
		// historic default admission behavior; a row at any generation makes the
		// empty current pointer contradictory and fail-closed.
		var generations int
		if err := conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM project_configurations WHERE channel=? AND project_id=?`, project.Channel, project.ID).Scan(&generations); err != nil {
			return nil, err
		}
		if generations != 0 {
			return nil, ErrProjectConflict
		}
		// Pre-generation registrations remain readable during a rolling v1
		// upgrade. Their historical start contract used the conservative local
		// defaults; config apply can bootstrap generation one before any later
		// ticket needs a reviewed immutable snapshot.
		defaults := config.DefaultMachineLimits()
		return validateLeaseRequests([]LeaseRequest{
			{Scope: "global", Resource: "machine", Capacity: defaults.MaxConcurrentTickets},
			{Scope: "project", Resource: string(ref.Project), Capacity: defaults.MaxConcurrentTickets},
		})
	}
	if project.ConfigGeneration == 0 || project.ConfigDigest == "" || len(project.ConfigSnapshot) == 0 {
		return nil, ErrProjectConflict
	}
	effective, err := config.DecodeSnapshot(project.ConfigSnapshot, project.ConfigDigest)
	if err != nil {
		return nil, err
	}
	if effective.Name != string(project.ID) || effective.Repository != project.Path || effective.BaseBranch != project.BaseRef {
		return nil, ErrProjectConflict
	}
	return validateLeaseRequests([]LeaseRequest{
		{Scope: "global", Resource: "machine", Capacity: effective.Machine.MaxConcurrentTickets},
		{Scope: "project", Resource: string(ref.Project), Capacity: effective.MaxConcurrentTickets},
	})
}

// StartPreflight authenticates the current project configuration and reports
// whether a queued ticket could presently obtain every required local slot.
// It is an advisory read: StartWithProjectOwnership re-derives the same
// generation and capacities inside its IMMEDIATE transaction before changing
// the ticket. Keeping validation here makes malformed legacy generation-zero
// history fail before daemon state-machine admission as well.
func (s *Store) StartPreflight(ctx context.Context, ref domain.TicketRef) (Project, bool, error) {
	if err := ref.Validate(); err != nil {
		return Project{}, false, err
	}
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return Project{}, false, normalizeBusy(ctx, err)
	}
	defer conn.Close()
	project, err := loadCurrentProjectConfiguration(ctx, conn, ref.Channel, ref.Project)
	if err != nil {
		return Project{}, false, normalizeBusy(ctx, err)
	}
	requests, err := projectStartLeaseRequests(ctx, conn, project, ref)
	if err != nil {
		return Project{}, false, normalizeBusy(ctx, err)
	}
	for _, request := range requests {
		available := false
		for slot := 0; slot < request.Capacity; slot++ {
			var occupied int
			err := conn.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM leases WHERE channel=? AND scope=? AND scope_key=?)`, ref.Channel, request.Scope, leaseKey(request.Resource, slot)).Scan(&occupied)
			if err != nil {
				return Project{}, false, normalizeBusy(ctx, err)
			}
			if occupied == 0 {
				available = true
				break
			}
		}
		if !available {
			return project, false, nil
		}
	}
	return project, true, nil
}

// AcquireLeases admits a ticket to every requested capacity dimension in one
// transaction. If any dimension is full, none of the new leases are retained.
// Replaying the same fenced request returns the ticket's existing slots.
func (s *Store) AcquireLeases(ctx context.Context, ref domain.TicketRef, expectedVersion uint64, fence domain.Fence, requests []LeaseRequest, at time.Time) ([]Lease, error) {
	if err := ref.Validate(); err != nil {
		return nil, err
	}
	requests, err := validateLeaseRequests(requests)
	if err != nil {
		return nil, err
	}
	if at.IsZero() {
		return nil, errors.New("lease acquisition time is required")
	}
	var acquired []Lease
	err = s.write(ctx, func(conn *sql.Conn) error {
		if err := s.assertTicketFence(ctx, conn, ref, expectedVersion, fence); err != nil {
			return err
		}
		var version, runner uint64
		var state domain.State
		if err := conn.QueryRowContext(ctx, `SELECT state, version, runner_epoch FROM tickets WHERE channel=? AND project_id=? AND id=?`, ref.Channel, ref.Project, ref.Ticket).Scan(&state, &version, &runner); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return ErrNotFound
			}
			return err
		}
		if state.Terminal() || version != expectedVersion {
			return ErrStaleFence
		}
		acquired = make([]Lease, 0, len(requests))
		for _, request := range requests {
			lease, ok, err := acquireLease(ctx, conn, ref, runner, request, at.UTC())
			if err != nil {
				return err
			}
			if !ok {
				return fmt.Errorf("%w: scope=%s resource=%s capacity=%d", ErrLeaseCapacity, request.Scope, request.Resource, request.Capacity)
			}
			acquired = append(acquired, lease)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return acquired, nil
}

func acquireLease(ctx context.Context, conn *sql.Conn, ref domain.TicketRef, runner uint64, request LeaseRequest, at time.Time) (Lease, bool, error) {
	for slot := 0; slot < request.Capacity; slot++ {
		key := leaseKey(request.Resource, slot)
		var project domain.ProjectID
		var ticket domain.TicketID
		var persistedRunner uint64
		var acquiredAt string
		err := conn.QueryRowContext(ctx, `SELECT project_id, ticket_id, runner_epoch, acquired_at FROM leases WHERE channel=? AND scope=? AND scope_key=?`, ref.Channel, request.Scope, key).Scan(&project, &ticket, &persistedRunner, &acquiredAt)
		if err == nil {
			if project == ref.Project && ticket == ref.Ticket && persistedRunner == runner {
				parsed, parseErr := time.Parse(time.RFC3339Nano, acquiredAt)
				if parseErr != nil {
					return Lease{}, false, fmt.Errorf("decode lease time: %w", parseErr)
				}
				return Lease{Ref: ref, Scope: request.Scope, ScopeKey: key, RunnerEpoch: runner, AcquiredAt: parsed}, true, nil
			}
			continue
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return Lease{}, false, err
		}
		result, err := conn.ExecContext(ctx, `INSERT INTO leases(channel, project_id, scope, scope_key, ticket_id, runner_epoch, acquired_at)
			VALUES (?, ?, ?, ?, ?, ?, ?) ON CONFLICT(channel, scope, scope_key) DO NOTHING`, ref.Channel, ref.Project, request.Scope, key, ref.Ticket, runner, at.Format(time.RFC3339Nano))
		if err != nil {
			return Lease{}, false, err
		}
		if changed, _ := result.RowsAffected(); changed == 1 {
			return Lease{Ref: ref, Scope: request.Scope, ScopeKey: key, RunnerEpoch: runner, AcquiredAt: at}, true, nil
		}
	}
	return Lease{}, false, nil
}

// ReleaseLeases releases only the current runner's capacity. Callers must
// first prove that processes and uncertain effects are drained.
func (s *Store) ReleaseLeases(ctx context.Context, ref domain.TicketRef, expectedVersion uint64, fence domain.Fence) (int64, error) {
	if err := ref.Validate(); err != nil {
		return 0, err
	}
	var released int64
	err := s.write(ctx, func(conn *sql.Conn) error {
		var version, runner uint64
		if err := conn.QueryRowContext(ctx, `SELECT version, runner_epoch FROM tickets WHERE channel=? AND project_id=? AND id=?`, ref.Channel, ref.Project, ref.Ticket).Scan(&version, &runner); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return ErrNotFound
			}
			return err
		}
		if version != expectedVersion {
			return ErrStaleFence
		}
		if err := s.currentFence(ctx, conn, ref.Channel, version, runner, fence); err != nil {
			return err
		}
		result, err := conn.ExecContext(ctx, `DELETE FROM leases WHERE channel=? AND project_id=? AND ticket_id=? AND runner_epoch=?`, ref.Channel, ref.Project, ref.Ticket, runner)
		if err != nil {
			return err
		}
		released, _ = result.RowsAffected()
		return nil
	})
	return released, err
}

// releaseTerminalCapacity is the single transaction-scoped authority for
// retiring a terminal ticket's admission leases. A lifecycle transition may
// call it only from the same write that commits a terminal state. The durable
// writer rows are rechecked here so a caller cannot infer process drain from a
// terminal target or from an already-settled effect alone.
func releaseTerminalCapacity(ctx context.Context, conn *sql.Conn, ref domain.TicketRef) error {
	counts := []struct {
		query string
		value int
	}{
		{query: `SELECT COUNT(*) FROM provider_attempts WHERE channel=? AND project_id=? AND ticket_id=? AND state IN ('active','quarantined')`},
		{query: `SELECT COUNT(*) FROM repository_command_leases WHERE channel=? AND project_id=? AND ticket_id=? AND state IN ('active','quarantined')`},
		{query: `SELECT COUNT(*) FROM git_mutation_leases WHERE channel=? AND project_id=? AND ticket_id=? AND state IN ('active','quarantined')`},
		{query: `SELECT COUNT(*) FROM effects WHERE channel=? AND project_id=? AND ticket_id=? AND state IN ('executing','uncertain')`},
	}
	for index := range counts {
		if err := conn.QueryRowContext(ctx, counts[index].query, ref.Channel, ref.Project, ref.Ticket).Scan(&counts[index].value); err != nil {
			return err
		}
		if counts[index].value != 0 {
			return ErrControlNotDrained
		}
	}
	_, err := conn.ExecContext(ctx, `DELETE FROM leases WHERE channel=? AND project_id=? AND ticket_id=?`, ref.Channel, ref.Project, ref.Ticket)
	return err
}

// StaleLeases is a leader-fenced startup observation. Runner invalidation alone
// never frees capacity: the supervisor must first prove the old runner has no
// live writer, then call ReleaseInvalidatedLeases for that exact epoch.
func (s *Store) StaleLeases(ctx context.Context, channel domain.Channel, leaderEpoch uint64) ([]Lease, error) {
	if !channel.Valid() || leaderEpoch == 0 {
		return nil, errors.New("valid channel and leader epoch are required")
	}
	var current uint64
	if err := s.db.QueryRowContext(ctx, `SELECT leader_epoch FROM daemon_instances WHERE channel=?`, channel).Scan(&current); err != nil {
		return nil, normalizeBusy(ctx, err)
	}
	if current != leaderEpoch {
		return nil, ErrStaleFence
	}
	rows, err := s.db.QueryContext(ctx, `SELECT l.project_id, l.ticket_id, l.scope, l.scope_key, l.runner_epoch, l.acquired_at
		FROM leases AS l JOIN tickets AS t ON t.channel=l.channel AND t.project_id=l.project_id AND t.id=l.ticket_id
		WHERE l.channel=? AND l.runner_epoch<>t.runner_epoch ORDER BY l.project_id, l.ticket_id, l.runner_epoch, l.scope, l.scope_key`, channel)
	if err != nil {
		return nil, normalizeBusy(ctx, err)
	}
	defer rows.Close()
	return scanLeases(rows, channel)
}

// ReleaseInvalidatedLeases is intentionally separate from observation. Its
// caller asserts that the exact old runner has passed process/effect drain.
func (s *Store) ReleaseInvalidatedLeases(ctx context.Context, ref domain.TicketRef, staleRunner, leaderEpoch uint64) (int64, error) {
	if err := ref.Validate(); err != nil {
		return 0, err
	}
	if staleRunner == 0 || leaderEpoch == 0 {
		return 0, errors.New("stale runner and leader epochs are required")
	}
	var released int64
	err := s.write(ctx, func(conn *sql.Conn) error {
		var currentLeader, currentRunner uint64
		if err := conn.QueryRowContext(ctx, `SELECT leader_epoch FROM daemon_instances WHERE channel=?`, ref.Channel).Scan(&currentLeader); err != nil {
			return err
		}
		if err := conn.QueryRowContext(ctx, `SELECT runner_epoch FROM tickets WHERE channel=? AND project_id=? AND id=?`, ref.Channel, ref.Project, ref.Ticket).Scan(&currentRunner); err != nil {
			return err
		}
		if currentLeader != leaderEpoch || currentRunner <= staleRunner {
			return ErrStaleFence
		}
		result, err := conn.ExecContext(ctx, `DELETE FROM leases WHERE channel=? AND project_id=? AND ticket_id=? AND runner_epoch=?`, ref.Channel, ref.Project, ref.Ticket, staleRunner)
		if err != nil {
			return err
		}
		released, _ = result.RowsAffected()
		return nil
	})
	return released, err
}

// AdoptInvalidatedLeases transfers a recovered ticket's durable global and
// project capacity from one invalidated runner to its current runner. It is a
// startup-only repair: it neither frees a slot nor changes its scope identity
// or acquisition time. Provider capacity is deliberately never transferable.
//
// The transfer is fail-closed. In particular, an active or quarantined
// provider attempt or Git mutation lease for the ticket means an old process
// could still be writing, so no capacity ownership is changed.
func (s *Store) AdoptInvalidatedLeases(ctx context.Context, ref domain.TicketRef, staleRunner, leaderEpoch uint64) (int64, error) {
	if err := ref.Validate(); err != nil {
		return 0, err
	}
	if staleRunner == 0 || leaderEpoch == 0 {
		return 0, errors.New("stale runner and leader epochs are required")
	}
	var adopted int64
	err := s.write(ctx, func(conn *sql.Conn) error {
		var currentLeader, currentRunner uint64
		if err := conn.QueryRowContext(ctx, `SELECT leader_epoch FROM daemon_instances WHERE channel=?`, ref.Channel).Scan(&currentLeader); err != nil {
			return err
		}
		if err := conn.QueryRowContext(ctx, `SELECT runner_epoch FROM tickets WHERE channel=? AND project_id=? AND id=?`, ref.Channel, ref.Project, ref.Ticket).Scan(&currentRunner); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return ErrNotFound
			}
			return err
		}
		if currentLeader != leaderEpoch || currentRunner <= staleRunner {
			return ErrStaleFence
		}

		// Repository-command rows are an independent writer/process witness.
		// Check them before the no-capacity replay branch: a ticket may have no
		// transferable capacity rows yet still be blocked by a quarantined or
		// live guarded command.
		var repositoryWriters int
		if err := conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM repository_command_leases
			WHERE channel=? AND project_id=? AND ticket_id=? AND state IN ('active','quarantined')`, ref.Channel, ref.Project, ref.Ticket).Scan(&repositoryWriters); err != nil {
			return err
		}
		if repositoryWriters != 0 {
			return ErrRepositoryCommandLease
		}

		var found, unsupported int
		if err := conn.QueryRowContext(ctx, `SELECT COUNT(*), COALESCE(SUM(CASE WHEN scope NOT IN ('global','project') THEN 1 ELSE 0 END), 0)
			FROM leases WHERE channel=? AND project_id=? AND ticket_id=? AND runner_epoch=?`, ref.Channel, ref.Project, ref.Ticket, staleRunner).Scan(&found, &unsupported); err != nil {
			return err
		}
		if unsupported != 0 {
			return ErrLeaseAdoption
		}
		// A prior successful transfer leaves no rows for this stale epoch. This
		// is the intentional replay result for a crash after commit.
		if found == 0 {
			return nil
		}

		var providerWriters, gitWriters int
		if err := conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM provider_attempts
			WHERE channel=? AND project_id=? AND ticket_id=? AND state IN ('active','quarantined')`, ref.Channel, ref.Project, ref.Ticket).Scan(&providerWriters); err != nil {
			return err
		}
		if providerWriters != 0 {
			return ErrLeaseAdoption
		}
		if err := conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM git_mutation_leases
			WHERE channel=? AND project_id=? AND ticket_id=? AND state IN ('active','quarantined')`, ref.Channel, ref.Project, ref.Ticket).Scan(&gitWriters); err != nil {
			return err
		}
		if gitWriters != 0 {
			return ErrLeaseAdoption
		}

		result, err := conn.ExecContext(ctx, `UPDATE leases SET runner_epoch=?
			WHERE channel=? AND project_id=? AND ticket_id=? AND runner_epoch=? AND scope IN ('global','project')`,
			currentRunner, ref.Channel, ref.Project, ref.Ticket, staleRunner)
		if err != nil {
			return err
		}
		adopted, _ = result.RowsAffected()
		return nil
	})
	return adopted, err
}

func (s *Store) Leases(ctx context.Context, channel domain.Channel) ([]Lease, error) {
	if !channel.Valid() {
		return nil, fmt.Errorf("invalid channel %q", channel)
	}
	rows, err := s.db.QueryContext(ctx, `SELECT project_id, ticket_id, scope, scope_key, runner_epoch, acquired_at FROM leases WHERE channel=? ORDER BY scope, scope_key`, channel)
	if err != nil {
		return nil, normalizeBusy(ctx, err)
	}
	defer rows.Close()
	return scanLeases(rows, channel)
}

func scanLeases(rows *sql.Rows, channel domain.Channel) ([]Lease, error) {
	var leases []Lease
	for rows.Next() {
		var lease Lease
		lease.Ref.Channel = channel
		var acquiredAt string
		if err := rows.Scan(&lease.Ref.Project, &lease.Ref.Ticket, &lease.Scope, &lease.ScopeKey, &lease.RunnerEpoch, &acquiredAt); err != nil {
			return nil, err
		}
		parsedAt, err := time.Parse(time.RFC3339Nano, acquiredAt)
		if err != nil {
			return nil, fmt.Errorf("decode lease time: %w", err)
		}
		lease.AcquiredAt = parsedAt
		leases = append(leases, lease)
	}
	return leases, rows.Err()
}

func validateLeaseRequests(requests []LeaseRequest) ([]LeaseRequest, error) {
	if len(requests) == 0 || len(requests) > 16 {
		return nil, errors.New("between one and sixteen lease requests are required")
	}
	copy := append([]LeaseRequest(nil), requests...)
	sort.Slice(copy, func(i, j int) bool {
		if copy[i].Scope != copy[j].Scope {
			return copy[i].Scope < copy[j].Scope
		}
		return copy[i].Resource < copy[j].Resource
	})
	for index, request := range copy {
		if request.Scope != "global" && request.Scope != "project" && request.Scope != "provider" {
			return nil, fmt.Errorf("invalid lease scope %q", request.Scope)
		}
		if strings.TrimSpace(request.Resource) != request.Resource || request.Resource == "" || len(request.Resource) > 200 || strings.ContainsRune(request.Resource, '\x00') {
			return nil, errors.New("lease resource must be a bounded nonempty identity")
		}
		if request.Capacity < 1 || request.Capacity > 64 {
			return nil, errors.New("lease capacity must be between one and sixty-four")
		}
		if index > 0 && request.Scope == copy[index-1].Scope && request.Resource == copy[index-1].Resource {
			return nil, errors.New("duplicate lease request")
		}
	}
	return copy, nil
}

func leaseKey(resource string, slot int) string {
	return strconv.Itoa(len(resource)) + ":" + resource + ":" + strconv.Itoa(slot)
}
