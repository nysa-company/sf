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

	"github.com/nysa-company/sf/internal/domain"
)

var ErrLeaseCapacity = errors.New("lease capacity is exhausted")

type phaseRecoveryBaseline struct{ version, runner, leader, currentLeader uint64 }

// hasDurableCandidateRepairBaseline proves the narrow correction path where a
// Builder completed and persisted a successor candidate before the daemon
// could record its repair completion.  It is not a generic lifecycle bridge:
// the immutable repair binding, successor candidate, and Builder result
// binding must all agree on the exact old Builder fence.
func hasDurableCandidateRepairBaseline(ctx context.Context, conn *sql.Conn, ref domain.TicketRef, baseline phaseRecoveryBaseline) bool {
	var count int
	err := conn.QueryRowContext(ctx, `SELECT COUNT(*)
		FROM candidate_repair_bindings r
		JOIN candidate_snapshots c ON c.channel=r.channel AND c.project_id=r.project_id AND c.ticket_id=r.ticket_id AND c.generation=r.target_generation
		JOIN candidate_result_bindings b ON b.channel=r.channel AND b.project_id=r.project_id AND b.ticket_id=r.ticket_id AND b.generation=r.target_generation
		JOIN provider_attempts a ON a.id=b.provider_attempt_id AND a.attempt=b.provider_attempt AND a.channel=b.channel AND a.project_id=b.project_id AND a.ticket_id=b.ticket_id AND a.phase='build' AND a.role='builder' AND a.state='completed' AND a.outcome='completed'
		WHERE r.channel=? AND r.project_id=? AND r.ticket_id=?
		  AND c.ticket_version=? AND c.leader_epoch=? AND c.runner_epoch=?
		  AND b.binding_ticket_version=? AND b.leader_epoch=? AND b.runner_epoch=?
		  AND a.expected_ticket_version=? AND a.leader_epoch=? AND a.runner_epoch=?`,
		ref.Channel, ref.Project, ref.Ticket,
		baseline.version, baseline.leader, baseline.runner,
		baseline.version, baseline.leader, baseline.runner,
		baseline.version, baseline.leader, baseline.runner).Scan(&count)
	return err == nil && count == 1
}

func recoveryProviderPhase(state domain.State) (domain.Phase, string, bool) {
	switch state {
	case domain.StatePlanning:
		return domain.PhasePlanning, "planner", true
	case domain.StateVerifying:
		return domain.PhaseVerification, "reviewer", true
	case domain.StateBuilding:
		return domain.PhaseBuild, "builder", true
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
		rows, err := conn.QueryContext(ctx, `SELECT project_id,id,state,version,runner_epoch FROM tickets WHERE channel=? AND state IN ('planning','verifying','building','publishing','waiting_ci','reviewing','waiting_approval','waiting_manual_merge','merging','reconciling','stopping','cancelling') ORDER BY project_id,id`, channel)
		if err != nil {
			return err
		}
		type activeTicket struct {
			project domain.ProjectID
			id      domain.TicketID
			state   domain.State
			version uint64
			runner  uint64
		}
		var active []activeTicket
		for rows.Next() {
			var ticket activeTicket
			if err := rows.Scan(&ticket.project, &ticket.id, &ticket.state, &ticket.version, &ticket.runner); err != nil {
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
				pollRetryResume := ticket.version >= waitingVersion+2 && ticket.runner == waitingPublication.CurrentFence.RunnerEpoch && authenticateCIPollResume(ctx, conn, ref, waitingVersion+1, waitingVersion+2)
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
			if ticket.state == domain.StatePublishing {
				publication, publicationFound, err := loadPublicationEvidenceRow(ctx, conn, ref)
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
					// A candidate can be durable before the build->publishing
					// signal, while publication evidence is intentionally absent
					// until the external boundary runs. Authenticate that exact
					// candidate and transition as the first recovery predecessor.
					candidate, candidateErr := s.latestCandidateFrom(ctx, conn, ref, false)
					if candidateErr != nil || candidate.TicketVersion == ^uint64(0) || candidate.TicketVersion >= ticket.version {
						return ErrPublicationEvidence
					}
					var transitions int
					if err := conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version=? AND trigger='phase_pass' AND from_state='building' AND to_state='publishing'`, channel, ticket.project, ticket.id, candidate.TicketVersion+1).Scan(&transitions); err != nil || transitions != 1 {
						return ErrPublicationEvidence
					}
					priorLeader = candidate.Fence.LeaderEpoch
					exactLatest := found && latest.TicketVersion == ticket.version && latest.RunnerEpoch == ticket.runner
					if exactLatest {
						priorLeader = latest.LeaderEpoch
					}
					if controlLeader, controlFound, controlErr := loadRuntimeControlEndpointLeader(ctx, conn, ref, ticket.version, ticket.runner); controlErr != nil {
						return controlErr
					} else if controlFound {
						priorLeader = controlLeader
					} else if !exactLatest && (ticket.version != candidate.TicketVersion+1 || ticket.runner != candidate.Fence.RunnerEpoch) {
						return ErrPublicationEvidence
					}
					if priorLeader == 0 || priorLeader >= leaderEpoch {
						return ErrPublicationEvidence
					}
					if !found && candidate.Fence.RunnerEpoch == 1 {
						if err := validateInitialLifecycleAdvance(ctx, conn, ref, ticket.version); err != nil {
							return ErrPublicationEvidence
						}
					}
					// This is a startup-only pre-fence proof.  The builder result
					// must reach the exact old endpoint, never the new daemon leader:
					// the signed recovery row below is what transfers authority.
					provider, _, providerErr := s.loadHistoricalProviderAttemptResult(ctx, conn, candidate.BuilderResult)
					if providerErr != nil || provider.Claim.ExpectedVersion != candidate.TicketVersion || provider.Claim.RunnerEpoch != candidate.Fence.RunnerEpoch || provider.Claim.LeaderEpoch != candidate.Fence.LeaderEpoch || provider.Claim.Ref != ref || provider.Claim.Phase != domain.PhaseBuild || provider.Claim.Role != "builder" || providerResultReachesHistoricalFence(ctx, conn, candidate.BuilderResult, provider, ticket.version, domain.Fence{LeaderEpoch: priorLeader, RunnerEpoch: ticket.runner}) != nil {
						return ErrPublicationEvidence
					}
				}
			}
			if phase, role, ok := recoveryProviderPhase(ticket.state); ok {
				baseline, baselineFound, baselineErr := s.loadPhaseRecoveryBaseline(ctx, conn, ref, phase, role)
				if baselineErr != nil {
					return ErrPublicationEvidence
				}
				if baselineFound && found && latest.TicketVersion == ticket.version && latest.RunnerEpoch == ticket.runner {
					baseline.currentLeader = latest.LeaderEpoch
				}
				if baselineFound {
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
						if err := validateInitialLifecycleAdvance(ctx, conn, ref, ticket.version); err != nil && !hasDurableCandidateRepairBaseline(ctx, conn, ref, baseline) {
							return ErrPublicationEvidence
						}
					}
					// Authenticate the source claim to the exact pre-fence endpoint.
					// Passing leaderEpoch here would let a generic provider reader
					// bless an unrecorded leader-only takeover.
					if err := validateRunnerRecoveryLedger(ctx, conn, ref, baseline.version, baseline.runner, baseline.leader, ticket.version, ticket.runner, priorLeader); err != nil {
						return ErrPublicationEvidence
					}
				} else if ticket.state == domain.StateVerifying {
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
				} else if publication, ok, publicationErr := s.publicationRecoveryBaseline(ctx, conn, ref, ticket.version, ticket.runner); publicationErr != nil {
					return publicationErr
				} else if ok {
					priorLeader = publication
				}
			} else if priorLeader == 0 {
				publication, ok, err := s.publicationRecoveryBaseline(ctx, conn, ref, ticket.version, ticket.runner)
				if err != nil {
					return err
				} else if ok {
					priorLeader = publication
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
			result, err := conn.ExecContext(ctx, `UPDATE tickets SET runner_epoch=runner_epoch+1, version=version+1 WHERE channel=? AND project_id=? AND id=? AND version=? AND runner_epoch=? AND state IN ('planning','verifying','building','publishing','waiting_ci','reviewing','waiting_approval','waiting_manual_merge','merging','reconciling','stopping','cancelling')`, channel, ticket.project, ticket.id, ticket.version, ticket.runner)
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
// qualified provider/version. Capacity is resolved from the frozen ticket
// configuration before this boundary is called.
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
	observed := false
	err = s.write(ctx, func(conn *sql.Conn) error {
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
			for _, request := range requests {
				lease, ok, err := acquireLease(ctx, conn, ref, runner, request, at.UTC())
				if err != nil {
					return err
				}
				if !ok {
					return fmt.Errorf("%w: scope=%s resource=%s capacity=%d", ErrLeaseCapacity, request.Scope, request.Resource, request.Capacity)
				}
				_ = lease
			}
			updated, err := conn.ExecContext(ctx, `UPDATE tickets SET state='planning', version=version+1, workflow_id=?,
				config_generation=(SELECT current_config_generation FROM projects WHERE channel=? AND id=?),
				config_digest=COALESCE((SELECT c.digest FROM projects p JOIN project_configurations c ON c.channel=p.channel AND c.project_id=p.id AND c.generation=p.current_config_generation WHERE p.channel=? AND p.id=?), ''),
				config_snapshot_bytes=COALESCE((SELECT c.snapshot_bytes FROM projects p JOIN project_configurations c ON c.channel=p.channel AND c.project_id=p.id AND c.generation=p.current_config_generation WHERE p.channel=? AND p.id=?), X'')
				WHERE channel=? AND project_id=? AND id=? AND state='queued' AND version=? AND runner_epoch=?`, workflowID,
				ref.Channel, ref.Project, ref.Channel, ref.Project, ref.Channel, ref.Project,
				ref.Channel, ref.Project, ref.Ticket, expectedVersion, runner)
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
			if _, err := conn.ExecContext(ctx, `INSERT INTO events(channel, project_id, ticket_id, ticket_version, trigger, from_state, to_state, payload, created_at) VALUES (?, ?, ?, ?, 'operator_start', 'queued', 'planning', '{}', ?)`, ref.Channel, ref.Project, ref.Ticket, version, createdAt); err != nil {
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
