package store

import (
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/domain"
)

func requireCandidateRepairCurrentReaders(t *testing.T, db *Store, key ProviderAttemptResultKey, claim ProviderAttemptClaim, expected uint64, fence domain.Fence) {
	t.Helper()
	if _, err := db.CandidateRepairBuildContext(t.Context(), key.Ref, expected, fence); err != nil {
		t.Fatalf("candidate repair context: %v", err)
	}
	if _, _, err := db.LoadCurrentProviderAttemptResult(t.Context(), key, expected, fence); err != nil {
		t.Fatalf("current repair result: %v", err)
	}
	if _, _, err := db.LoadProviderAttemptResult(t.Context(), claim, expected, fence); err != nil {
		t.Fatalf("claimed repair result: %v", err)
	}
	reusable, err := db.LatestReusableProviderAttempt(t.Context(), LatestReusableProviderAttemptRequest{
		Ref: key.Ref, Phase: domain.PhaseBuild, Role: "builder", ExpectedVersion: expected, Fence: fence,
	})
	if err != nil || reusable.Key != key {
		t.Fatalf("reusable repair result=%+v err=%v", reusable, err)
	}
	if err := db.ProviderResultReachesFence(t.Context(), key, expected, fence); err != nil {
		t.Fatalf("repair result fence: %v", err)
	}
}

func requireCandidateRepairCurrentReadersReject(t *testing.T, db *Store, key ProviderAttemptResultKey, claim ProviderAttemptClaim, expected uint64, fence domain.Fence) {
	t.Helper()
	if _, err := db.CandidateRepairBuildContext(t.Context(), key.Ref, expected, fence); err == nil {
		t.Fatal("candidate repair context accepted malformed recovery ledger")
	}
	if _, _, err := db.LoadCurrentProviderAttemptResult(t.Context(), key, expected, fence); err == nil {
		t.Fatal("current repair result accepted malformed recovery ledger")
	}
	if _, _, err := db.LoadProviderAttemptResult(t.Context(), claim, expected, fence); err == nil {
		t.Fatal("claimed repair result accepted malformed recovery ledger")
	}
	if _, err := db.LatestReusableProviderAttempt(t.Context(), LatestReusableProviderAttemptRequest{
		Ref: key.Ref, Phase: domain.PhaseBuild, Role: "builder", ExpectedVersion: expected, Fence: fence,
	}); err == nil {
		t.Fatal("reusable repair result accepted malformed recovery ledger")
	}
	if err := db.ProviderResultReachesFence(t.Context(), key, expected, fence); err == nil {
		t.Fatal("repair result fence accepted malformed recovery ledger")
	}
}

type recoveredCandidateRepairTestFixture struct {
	completed            completedCandidateRepairTestFixture
	db                   *Store
	live                 Ticket
	liveFence            domain.Fence
	claim                ProviderAttemptClaim
	firstRecoveryVersion uint64
}

func newRecoveredCandidateRepairTestFixture(t *testing.T, name string) *recoveredCandidateRepairTestFixture {
	t.Helper()
	result := &recoveredCandidateRepairTestFixture{completed: newCompletedCandidateRepairTestFixture(t)}
	result.db = result.completed.db
	t.Cleanup(func() { _ = result.db.Close() })
	ctx := t.Context()

	prior := result.completed.building
	priorLeader := result.completed.buildFence.LeaderEpoch
	for index, identity := range []string{name + "-first", name + "-second"} {
		leader, err := result.db.AcquireLeader(ctx, prior.Ref.Channel, identity)
		if err != nil {
			t.Fatal(err)
		}
		if changed, err := result.db.FenceRecoveredRunners(ctx, prior.Ref.Channel, leader); err != nil || changed != 1 {
			t.Fatalf("candidate-repair fence %d changed=%d err=%v", index+1, changed, err)
		}
		current, err := result.db.Ticket(ctx, prior.Ref)
		if err != nil || current.State != domain.StateBuilding || current.Version != prior.Version+1 || current.RunnerEpoch != prior.RunnerEpoch+1 {
			t.Fatalf("candidate-repair recovery %d ticket=%+v prior=%+v err=%v", index+1, current, prior, err)
		}
		step, found, err := loadRunnerRecoveryAt(ctx, result.db.db, current.Ref, current.Version)
		if err != nil || !found || !validRunnerRecovery(step) ||
			step.PriorTicketVersion != prior.Version || step.PriorRunnerEpoch != prior.RunnerEpoch || step.PriorLeaderEpoch != priorLeader ||
			step.TicketVersion != current.Version || step.RunnerEpoch != current.RunnerEpoch || step.LeaderEpoch != leader {
			t.Fatalf("candidate-repair recovery %d ledger=%+v found=%v err=%v", index+1, step, found, err)
		}
		if index == 0 {
			result.firstRecoveryVersion = current.Version
		}
		prior = current
		priorLeader = leader
		result.live, result.liveFence = current, domain.Fence{LeaderEpoch: leader, RunnerEpoch: current.RunnerEpoch}
	}
	historical, _, err := result.db.LoadHistoricalProviderAttemptResult(ctx, result.completed.candidate.BuilderResult)
	if err != nil {
		t.Fatal(err)
	}
	result.claim = historical.Claim
	return result
}

func TestCandidateRepairCurrentReadersRejectFutureRecoveryRow(t *testing.T) {
	fixture := newCompletedCandidateRepairTestFixture(t)
	defer fixture.db.Close()
	ctx := t.Context()
	key := fixture.candidate.BuilderResult
	historical, _, err := fixture.db.LoadHistoricalProviderAttemptResult(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	requireCandidateRepairCurrentReaders(t, fixture.db, key, historical.Claim, fixture.building.Version, fixture.buildFence)

	forged := RunnerRecoveryLedger{
		Ref:                fixture.building.Ref,
		PriorTicketVersion: fixture.building.Version,
		PriorRunnerEpoch:   fixture.building.RunnerEpoch,
		PriorLeaderEpoch:   fixture.buildFence.LeaderEpoch,
		TicketVersion:      fixture.building.Version + 1,
		RunnerEpoch:        fixture.building.RunnerEpoch + 1,
		LeaderEpoch:        fixture.buildFence.LeaderEpoch + 1,
		CreatedAt:          time.Now().UTC(),
	}
	forged.RecoveryDigest = runnerRecoveryDigest(forged)
	if !validRunnerRecovery(forged) {
		t.Fatalf("forged future row is not structurally valid: %+v", forged)
	}
	if _, err := fixture.db.db.ExecContext(ctx, `INSERT INTO runner_recovery_ledger(
		channel,project_id,ticket_id,prior_ticket_version,prior_runner_epoch,prior_leader_epoch,
		ticket_version,runner_epoch,leader_epoch,recovery_digest,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?)`,
		forged.Ref.Channel, forged.Ref.Project, forged.Ref.Ticket,
		forged.PriorTicketVersion, forged.PriorRunnerEpoch, forged.PriorLeaderEpoch,
		forged.TicketVersion, forged.RunnerEpoch, forged.LeaderEpoch,
		forged.RecoveryDigest, forged.CreatedAt.Format(time.RFC3339Nano)); err != nil {
		t.Fatal(err)
	}
	requireCandidateRepairCurrentReadersReject(t, fixture.db, key, historical.Claim, fixture.building.Version, fixture.buildFence)
}

func TestCandidateRepairStartupRejectsTamperedConsumedRecoveryPrefix(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*testing.T, *completedCandidateRepairTestFixture)
	}{
		{
			name: "deleted sealed row",
			mutate: func(t *testing.T, fixture *completedCandidateRepairTestFixture) {
				if _, err := fixture.db.db.ExecContext(t.Context(), `DROP TRIGGER runner_recovery_ledger_immutable_delete`); err != nil {
					t.Fatal(err)
				}
				result, err := fixture.db.db.ExecContext(t.Context(), `DELETE FROM runner_recovery_ledger WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version=?`, fixture.building.Ref.Channel, fixture.building.Ref.Project, fixture.building.Ref.Ticket, fixture.preRedRecoveryVersion)
				if err != nil {
					t.Fatal(err)
				}
				if rows, err := result.RowsAffected(); err != nil || rows != 1 {
					t.Fatalf("deleted sealed recovery rows=%d err=%v", rows, err)
				}
			},
		},
		{
			name: "structurally valid row mutation",
			mutate: func(t *testing.T, fixture *completedCandidateRepairTestFixture) {
				row, found, err := loadRunnerRecoveryAt(t.Context(), fixture.db.db, fixture.building.Ref, fixture.preRedRecoveryVersion)
				if err != nil || !found {
					t.Fatalf("load sealed recovery row=%+v found=%v err=%v", row, found, err)
				}
				row.CreatedAt = row.CreatedAt.Add(time.Millisecond)
				row.RecoveryDigest = runnerRecoveryDigest(row)
				if !validRunnerRecovery(row) {
					t.Fatalf("mutated recovery row is not structurally valid: %+v", row)
				}
				if _, err := fixture.db.db.ExecContext(t.Context(), `DROP TRIGGER runner_recovery_ledger_immutable_update`); err != nil {
					t.Fatal(err)
				}
				result, err := fixture.db.db.ExecContext(t.Context(), `UPDATE runner_recovery_ledger SET recovery_digest=?,created_at=? WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version=?`, row.RecoveryDigest, row.CreatedAt.Format(time.RFC3339Nano), row.Ref.Channel, row.Ref.Project, row.Ref.Ticket, row.TicketVersion)
				if err != nil {
					t.Fatal(err)
				}
				if rows, err := result.RowsAffected(); err != nil || rows != 1 {
					t.Fatalf("mutated sealed recovery rows=%d err=%v", rows, err)
				}
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newCompletedCandidateRepairTestFixtureWithPreRedRecovery(t, true)
			defer fixture.db.Close()
			if fixture.preRedRecoveryVersion == 0 {
				t.Fatal("fixture did not create a recovery row before red consumption")
			}
			key := fixture.candidate.BuilderResult
			historical, _, err := fixture.db.LoadHistoricalProviderAttemptResult(t.Context(), key)
			if err != nil {
				t.Fatal(err)
			}
			requireCandidateRepairCurrentReaders(t, fixture.db, key, historical.Claim, fixture.building.Version, fixture.buildFence)
			before, err := fixture.db.Ticket(t.Context(), fixture.building.Ref)
			if err != nil {
				t.Fatal(err)
			}

			test.mutate(t, &fixture)
			requireCandidateRepairCurrentReadersReject(t, fixture.db, key, historical.Claim, fixture.building.Version, fixture.buildFence)
			if _, err := completedCandidateRepairContextAt(t.Context(), fixture.db.db, fixture.candidate, historical); err == nil {
				t.Fatal("historical repair context accepted a changed consumed recovery prefix")
			}

			leader, err := fixture.db.AcquireLeader(t.Context(), fixture.building.Ref.Channel, "candidate-repair-prefix-tamper")
			if err != nil {
				t.Fatal(err)
			}
			changed, err := fixture.db.FenceRecoveredRunners(t.Context(), fixture.building.Ref.Channel, leader)
			if !errors.Is(err, ErrPublicationEvidence) || changed != 0 {
				t.Fatalf("startup accepted changed consumed prefix changed=%d err=%v", changed, err)
			}
			after, ticketErr := fixture.db.Ticket(t.Context(), fixture.building.Ref)
			if ticketErr != nil || !reflect.DeepEqual(after, before) {
				t.Fatalf("failed startup mutated ticket before=%+v after=%+v err=%v", before, after, ticketErr)
			}
			var appended int
			if err := fixture.db.db.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM runner_recovery_ledger WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version=?`, before.Ref.Channel, before.Ref.Project, before.Ref.Ticket, before.Version+1).Scan(&appended); err != nil || appended != 0 {
				t.Fatalf("failed startup appended recovery rows=%d err=%v", appended, err)
			}
		})
	}
}

func TestCandidateRepairCurrentReadersAndRearmRejectBrokenRecoveryPrefix(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*testing.T, *recoveredCandidateRepairTestFixture)
	}{
		{
			name: "deleted first recovery",
			mutate: func(t *testing.T, fixture *recoveredCandidateRepairTestFixture) {
				if _, err := fixture.db.db.ExecContext(t.Context(), `DROP TRIGGER runner_recovery_ledger_immutable_delete`); err != nil {
					t.Fatal(err)
				}
				result, err := fixture.db.db.ExecContext(t.Context(), `DELETE FROM runner_recovery_ledger WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version=?`, fixture.live.Ref.Channel, fixture.live.Ref.Project, fixture.live.Ref.Ticket, fixture.firstRecoveryVersion)
				if err != nil {
					t.Fatal(err)
				}
				if rows, err := result.RowsAffected(); err != nil || rows != 1 {
					t.Fatalf("deleted first recovery rows=%d err=%v", rows, err)
				}
			},
		},
		{
			name: "tampered first recovery digest",
			mutate: func(t *testing.T, fixture *recoveredCandidateRepairTestFixture) {
				if _, err := fixture.db.db.ExecContext(t.Context(), `DROP TRIGGER runner_recovery_ledger_immutable_update`); err != nil {
					t.Fatal(err)
				}
				result, err := fixture.db.db.ExecContext(t.Context(), `UPDATE runner_recovery_ledger SET recovery_digest=? WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version=?`, "sha256:"+strings.Repeat("0", 64), fixture.live.Ref.Channel, fixture.live.Ref.Project, fixture.live.Ref.Ticket, fixture.firstRecoveryVersion)
				if err != nil {
					t.Fatal(err)
				}
				if rows, err := result.RowsAffected(); err != nil || rows != 1 {
					t.Fatalf("tampered first recovery rows=%d err=%v", rows, err)
				}
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			name := "candidate-repair-ledger-" + strings.ReplaceAll(test.name, " ", "-")
			t.Run("current readers", func(t *testing.T) {
				fixture := newRecoveredCandidateRepairTestFixture(t, name+"-readers")
				key := fixture.completed.candidate.BuilderResult
				if _, err := fixture.db.CandidateRepairBuildContext(t.Context(), fixture.live.Ref, fixture.live.Version, fixture.liveFence); err != nil {
					t.Fatalf("healthy recovered repair context: %v", err)
				}
				reusable, err := fixture.db.LatestReusableProviderAttempt(t.Context(), LatestReusableProviderAttemptRequest{
					Ref: fixture.live.Ref, Phase: domain.PhaseBuild, Role: "builder", ExpectedVersion: fixture.live.Version, Fence: fixture.liveFence,
				})
				if err != nil || reusable.Key != key || !reusable.Recovered {
					t.Fatalf("healthy recovered repair result=%+v err=%v", reusable, err)
				}
				if err := fixture.db.ProviderResultReachesFence(t.Context(), key, fixture.live.Version, fixture.liveFence); err != nil {
					t.Fatalf("healthy recovered repair result fence: %v", err)
				}

				test.mutate(t, fixture)
				// The two exact-claim loaders already reject a recovered immutable
				// claim at a later fence. The other three calls prove the now-current
				// recovery path itself also fails closed when its prefix is broken.
				requireCandidateRepairCurrentReadersReject(t, fixture.db, key, fixture.claim, fixture.live.Version, fixture.liveFence)
			})
			t.Run("rearm proof", func(t *testing.T) {
				fixture := newRecoveredCandidateRepairTestFixture(t, name+"-rearm")
				stopped, resumed := postPublicationPauseResumeAt(t, fixture.db, fixture.live, fixture.liveFence, domain.StateBuilding)
				if resumed.Version != fixture.live.Version+3 || resumed.RunnerEpoch != fixture.live.RunnerEpoch+1 {
					t.Fatalf("candidate-repair resumed=%+v recovery endpoint=%+v", resumed, fixture.live)
				}
				test.mutate(t, fixture)
				if capability, err := fixture.db.CandidateRepairRearmProof(t.Context(), resumed.Ref, stopped); err == nil || capability != nil {
					t.Fatalf("candidate repair rearm accepted broken recovery prefix capability=%v err=%v", capability, err)
				}
			})
		})
	}
}

func candidateRepairResumedBuildingFixture(t *testing.T) (*Store, Ticket, Ticket) {
	t.Helper()
	db, waiting, _, observation := redCIConsumptionFixture(t)
	authority := redCICorrectionAuthority(t, waiting, observation)
	if _, err := db.ConsumeCIObservation(t.Context(), CIObservationTransition{
		Ref: waiting.Ref, ObservationDigest: observation.ObservationDigest,
		ExpectedVersion: waiting.Version, Fence: observation.ObservedFence,
		CorrectionBudget: &authority,
	}); err != nil {
		db.Close()
		t.Fatalf("enter candidate repair Building: %v", err)
	}
	building, err := db.Ticket(t.Context(), waiting.Ref)
	if err != nil || building.State != domain.StateBuilding {
		db.Close()
		t.Fatalf("candidate repair ticket=%+v err=%v", building, err)
	}
	preStop, resumed := postPublicationPauseResumeAt(t, db, building, domain.Fence{
		LeaderEpoch: observation.ObservedFence.LeaderEpoch,
		RunnerEpoch: building.RunnerEpoch,
	}, domain.StateBuilding)
	// Controller.Drain runs after the runner-invalidating transition and keeps
	// this exact durable stopping tuple. The proof also accepts the pre-stop
	// tuple used by lower-level Store callers, but production dispatch must be
	// covered with the controller's shape.
	stopped := Ticket{Ref: preStop.Ref, State: domain.StateStopping, Version: preStop.Version + 1, RunnerEpoch: preStop.RunnerEpoch + 1}
	return db, stopped, resumed
}

func TestCandidateRepairRearmProofAuthenticatesResumedBuilding(t *testing.T) {
	db, stopped, current := candidateRepairResumedBuildingFixture(t)
	defer db.Close()

	if capability, err := db.RearmProof(t.Context(), current.Ref, stopped); err == nil || capability != nil {
		t.Fatalf("ordinary pre-publication proof admitted retained publication state: %v", err)
	}
	capability, err := db.CandidateRepairRearmProof(t.Context(), current.Ref, stopped)
	if err != nil || capability == nil {
		t.Fatalf("candidate repair rearm capability=%v err=%v", capability, err)
	}
	if needed, err := db.RuntimeRearmNeeded(t.Context(), current.Ref); err != nil || !needed {
		t.Fatalf("candidate repair rearm needed=%v err=%v", needed, err)
	}
	var consumed bool
	if err := db.ActivateRearm(t.Context(), capability, func(admission *RuntimeAdmissionCapability) error {
		_, version, fence, ok := admission.ConsumeRuntimeAdmission()
		consumed = ok && version == current.Version && fence.LeaderEpoch != 0 && fence.RunnerEpoch == current.RunnerEpoch
		return nil
	}); err != nil || !consumed {
		t.Fatalf("candidate repair activation consumed=%v err=%v", consumed, err)
	}
	if err := db.ActivateRearm(t.Context(), capability, func(*RuntimeAdmissionCapability) error { return nil }); !errors.Is(err, ErrStaleFence) {
		t.Fatalf("candidate repair capability replay=%v", err)
	}
}

func TestCandidateRepairRearmProofAuthenticatesCompletedRepairAcrossRecoveryControlRecovery(t *testing.T) {
	fixture := newCompletedCandidateRepairTestFixture(t)
	db := fixture.db
	defer func() { _ = db.Close() }()
	ctx := t.Context()

	// The completed repair is still bound to the Builder endpoint at which its
	// successor candidate was recorded. Recover that endpoint once before the
	// operator stop so the later rearm cannot pass through an exact-current
	// candidate shortcut.
	firstLeader, err := db.AcquireLeader(ctx, fixture.building.Ref.Channel, "candidate-repair-rearm-before-stop")
	if err != nil {
		t.Fatal(err)
	}
	if changed, err := db.FenceRecoveredRunners(ctx, fixture.building.Ref.Channel, firstLeader); err != nil || changed != 1 {
		t.Fatalf("first candidate-repair fence changed=%d err=%v", changed, err)
	}
	firstRecovered, err := db.Ticket(ctx, fixture.building.Ref)
	if err != nil || firstRecovered.State != domain.StateBuilding || firstRecovered.Version != fixture.building.Version+1 || firstRecovered.RunnerEpoch != fixture.building.RunnerEpoch+1 {
		t.Fatalf("first recovered repair ticket=%+v err=%v", firstRecovered, err)
	}
	firstStep, found, err := loadRunnerRecoveryAt(ctx, db.db, fixture.building.Ref, firstRecovered.Version)
	if err != nil || !found || !validRunnerRecovery(firstStep) ||
		firstStep.PriorTicketVersion != fixture.building.Version || firstStep.PriorRunnerEpoch != fixture.building.RunnerEpoch || firstStep.PriorLeaderEpoch != fixture.buildFence.LeaderEpoch ||
		firstStep.TicketVersion != firstRecovered.Version || firstStep.RunnerEpoch != firstRecovered.RunnerEpoch || firstStep.LeaderEpoch != firstLeader {
		t.Fatalf("first candidate-repair recovery=%+v found=%v err=%v", firstStep, found, err)
	}

	controlBaseline, resumed := postPublicationPauseResumeAt(t, db, firstRecovered, domain.Fence{
		LeaderEpoch: firstLeader,
		RunnerEpoch: firstRecovered.RunnerEpoch,
	}, domain.StateBuilding)
	if controlBaseline.State != domain.StateBuilding || controlBaseline.Version != firstRecovered.Version || controlBaseline.RunnerEpoch != firstRecovered.RunnerEpoch ||
		resumed.State != domain.StateBuilding || resumed.Version != firstRecovered.Version+3 || resumed.RunnerEpoch != firstRecovered.RunnerEpoch+1 {
		t.Fatalf("candidate-repair control baseline=%+v resumed=%+v", controlBaseline, resumed)
	}

	// Reopen the Store, advance the daemon leader, and append a second signed
	// recovery after the resume. CandidateRepairRearmProof must authenticate
	// both the historical completion -> stop baseline edge and this resumed ->
	// live suffix while the durable runtime control remains sealed.
	reopened, stopped, live, liveFence := reopenAndFencePostPublication(t, db, resumed.Ref, "candidate-repair-rearm-after-resume")
	db = reopened
	if stopped.Version != firstRecovered.Version+1 || stopped.RunnerEpoch != firstRecovered.RunnerEpoch+1 ||
		live.State != domain.StateBuilding || live.Version != resumed.Version+1 || live.RunnerEpoch != resumed.RunnerEpoch+1 {
		t.Fatalf("candidate-repair stopped=%+v live=%+v", stopped, live)
	}
	secondStep, found, err := loadRunnerRecoveryAt(ctx, db.db, live.Ref, live.Version)
	if err != nil || !found || !validRunnerRecovery(secondStep) ||
		secondStep.PriorTicketVersion != resumed.Version || secondStep.PriorRunnerEpoch != resumed.RunnerEpoch || secondStep.PriorLeaderEpoch != firstLeader ||
		secondStep.TicketVersion != live.Version || secondStep.RunnerEpoch != live.RunnerEpoch || secondStep.LeaderEpoch != liveFence.LeaderEpoch {
		t.Fatalf("second candidate-repair recovery=%+v found=%v err=%v", secondStep, found, err)
	}

	historical, err := db.RecoverableCandidate(ctx, live.Ref)
	if err != nil || historical.TicketVersion != fixture.building.Version || historical.Fence != fixture.buildFence ||
		historical.Snapshot.Generation != fixture.candidate.Snapshot.Generation || historical.Snapshot.HeadSHA != fixture.candidate.Snapshot.HeadSHA || historical.Snapshot.TreeSHA != fixture.candidate.Snapshot.TreeSHA {
		t.Fatalf("historical completed repair candidate=%+v err=%v", historical, err)
	}
	var exactCompletionRows int
	if err := db.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM candidate_repair_completions
		WHERE channel=? AND project_id=? AND ticket_id=? AND target_generation=?
		AND builder_result_attempt_id=? AND builder_result_attempt=?
		AND builder_binding_ticket_version=? AND builder_binding_leader_epoch=? AND builder_binding_runner_epoch=?
		AND final_candidate_head_sha=? AND final_candidate_tree_sha=?`,
		live.Ref.Channel, live.Ref.Project, live.Ref.Ticket, historical.Snapshot.Generation,
		historical.BuilderResult.AttemptID, historical.BuilderResult.Attempt,
		fixture.building.Version, fixture.buildFence.LeaderEpoch, fixture.buildFence.RunnerEpoch,
		historical.Snapshot.HeadSHA, historical.Snapshot.TreeSHA).Scan(&exactCompletionRows); err != nil {
		t.Fatal(err)
	}
	if exactCompletionRows != 1 || fixture.building.Version+1 != stopped.Version-1 {
		t.Fatalf("historical completion rows=%d building=%d stop-baseline=%d", exactCompletionRows, fixture.building.Version, stopped.Version-1)
	}

	if fallback, err := db.RearmProof(ctx, live.Ref, stopped); err == nil || fallback != nil {
		t.Fatalf("ordinary pre-publication proof admitted completed repair capability=%v err=%v", fallback, err)
	}
	capability, err := db.CandidateRepairRearmProof(ctx, live.Ref, stopped)
	if err != nil || capability == nil {
		t.Fatalf("completed candidate-repair rearm capability=%v err=%v", capability, err)
	}
	var consumed bool
	if err := db.ActivateRearm(ctx, capability, func(admission *RuntimeAdmissionCapability) error {
		_, version, fence, ok := admission.ConsumeRuntimeAdmission()
		consumed = ok && version == live.Version && fence == liveFence
		return nil
	}); err != nil || !consumed {
		t.Fatalf("completed candidate-repair activation consumed=%v err=%v", consumed, err)
	}
}

func TestCandidateRepairRearmProofRejectsMissingOrMalformedBinding(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*testing.T, *Store, Ticket)
	}{
		{
			name: "missing",
			mutate: func(t *testing.T, db *Store, ticket Ticket) {
				if _, err := db.db.ExecContext(t.Context(), `DROP TRIGGER candidate_repair_bindings_immutable_delete`); err != nil {
					t.Fatal(err)
				}
				if _, err := db.db.ExecContext(t.Context(), `DELETE FROM candidate_repair_bindings WHERE channel=? AND project_id=? AND ticket_id=?`, ticket.Ref.Channel, ticket.Ref.Project, ticket.Ref.Ticket); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "malformed",
			mutate: func(t *testing.T, db *Store, ticket Ticket) {
				if _, err := db.db.ExecContext(t.Context(), `DROP TRIGGER candidate_repair_bindings_immutable_update`); err != nil {
					t.Fatal(err)
				}
				if _, err := db.db.ExecContext(t.Context(), `UPDATE candidate_repair_bindings SET repair_context_digest=? WHERE channel=? AND project_id=? AND ticket_id=?`, "sha256:"+strings.Repeat("0", 64), ticket.Ref.Channel, ticket.Ref.Project, ticket.Ref.Ticket); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "malformed runtime authority",
			mutate: func(t *testing.T, db *Store, ticket Ticket) {
				if _, err := db.db.ExecContext(t.Context(), `UPDATE runtime_ticket_controls SET authority_version=authority_version+17 WHERE channel=? AND project_id=? AND ticket_id=?`, ticket.Ref.Channel, ticket.Ref.Project, ticket.Ref.Ticket); err != nil {
					t.Fatal(err)
				}
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			db, stopped, current := candidateRepairResumedBuildingFixture(t)
			defer db.Close()
			test.mutate(t, db, current)
			capability, err := db.CandidateRepairRearmProof(t.Context(), current.Ref, stopped)
			if capability != nil || err == nil {
				t.Fatalf("candidate repair capability=%v err=%v", capability, err)
			}
			if fallback, err := db.RearmProof(t.Context(), current.Ref, stopped); fallback != nil || err == nil {
				t.Fatalf("malformed repair fell through ordinary proof capability=%v err=%v", fallback, err)
			}
			if needed, err := db.RuntimeRearmNeeded(t.Context(), current.Ref); err != nil || !needed {
				t.Fatalf("failed proof opened runtime needed=%v err=%v", needed, err)
			}
		})
	}
}

func TestCandidateRepairRearmProofLeavesOrdinaryBuildingToPrePublicationProof(t *testing.T) {
	db, ctx := openTestStore(t)
	defer db.Close()
	setupProviderProject(t, db, ctx)
	leader, err := db.AcquireLeader(ctx, domain.ChannelDev, "ordinary-building-rearm")
	if err != nil {
		t.Fatal(err)
	}
	building := providerState(t, db, ctx, setupProviderTicket(t, db, ctx, "SF-ordinary-building-rearm", leader), leader, domain.StateBuilding)
	preStop, current := postPublicationPauseResumeAt(t, db, building, domain.Fence{LeaderEpoch: leader, RunnerEpoch: building.RunnerEpoch}, domain.StateBuilding)
	stopped := Ticket{Ref: preStop.Ref, State: domain.StateStopping, Version: preStop.Version + 1, RunnerEpoch: preStop.RunnerEpoch + 1}

	if capability, err := db.CandidateRepairRearmProof(ctx, current.Ref, stopped); capability != nil || !errors.Is(err, ErrNotFound) {
		t.Fatalf("ordinary Building candidate-repair classification capability=%v err=%v", capability, err)
	}
	capability, err := db.RearmProof(ctx, current.Ref, stopped)
	if err != nil || capability == nil {
		t.Fatalf("ordinary Building rearm capability=%v err=%v", capability, err)
	}
}

func TestCandidateRepairRearmProofLeavesSourceResumeBuildingToPrePublicationProof(t *testing.T) {
	db, ctx, firstLeader, resumed, source := operatorSourceResumeResumedFixture(t)
	defer db.Close()
	openExactRuntimeAdmission(t, db, resumed.Ref)
	artifact := recordOperatorSourceFreshVerificationAtEndpoint(t, db, ctx, firstLeader, resumed, source)
	if _, err := db.TransitionVerification(ctx, Transition{
		Ref:             resumed.Ref,
		ExpectedVersion: resumed.Version,
		From:            domain.StateVerifying,
		To:              domain.StateBuilding,
		Trigger:         "phase_pass",
		Fence:           artifact.Fence,
		EventPayload:    `{}`,
	}); err != nil {
		t.Fatalf("advance source resume to Building: %v", err)
	}
	building, err := db.Ticket(ctx, resumed.Ref)
	if err != nil || building.State != domain.StateBuilding {
		t.Fatalf("source-resume Building ticket=%+v err=%v", building, err)
	}

	// Model process death after the fresh Reviewer advanced the source-only
	// workflow to Building. Startup seals the retained source-resume control,
	// then fences the live ticket to the new daemon. Candidate-repair admission
	// must classify the clean absence of a checks_red binding before applying its
	// repair-specific direct pause-to-Building control shape.
	if err := db.restoreRuntimeControls(ctx); err != nil {
		t.Fatal(err)
	}
	secondLeader, err := db.AcquireLeader(ctx, domain.ChannelDev, "source-resume-building-rearm")
	if err != nil || secondLeader <= firstLeader {
		t.Fatalf("source-resume recovery leader=%d prior=%d err=%v", secondLeader, firstLeader, err)
	}
	if changed, err := db.FenceRecoveredRunners(ctx, domain.ChannelDev, secondLeader); err != nil || changed != 1 {
		t.Fatalf("source-resume Building fence changed=%d err=%v", changed, err)
	}
	current, err := db.Ticket(ctx, resumed.Ref)
	if err != nil || current.State != domain.StateBuilding || current.Version != building.Version+1 || current.RunnerEpoch != building.RunnerEpoch+1 {
		t.Fatalf("recovered source-resume Building ticket=%+v prior=%+v err=%v", current, building, err)
	}
	stopped, err := db.StoppedRuntimeTicket(ctx, current.Ref)
	if err != nil {
		t.Fatal(err)
	}
	if capability, err := db.CandidateRepairRearmProof(ctx, current.Ref, stopped); capability != nil || !errors.Is(err, ErrNotFound) {
		t.Fatalf("source-resume Building candidate-repair classification capability=%v err=%v", capability, err)
	}
	capability, err := db.RearmProof(ctx, current.Ref, stopped)
	if err != nil || capability == nil {
		t.Fatalf("source-resume Building ordinary rearm capability=%v err=%v", capability, err)
	}
}
