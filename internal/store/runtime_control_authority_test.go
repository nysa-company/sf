package store

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/domain"
)

// raceControlProofAdmission holds a proof immediately after it has counted
// every current authority but before its IMMEDIATE transaction commits. The
// contender therefore cannot commit in the old identity after a drained
// certificate: it either committed before the proof (and is counted) or sees
// the in-transaction revocation after the proof commits.
func raceControlProofAdmission(t *testing.T, db *Store, ref domain.TicketRef, contender func() error) TicketControlProof {
	t.Helper()
	entered := make(chan struct{})
	release := make(chan struct{})
	db.controlProofHook = func() {
		close(entered)
		<-release
	}
	t.Cleanup(func() { db.controlProofHook = nil })

	proofs := make(chan TicketControlProof, 1)
	proofErrs := make(chan error, 1)
	go func() {
		proof, err := db.ControlProof(context.Background(), ref)
		proofs <- proof
		proofErrs <- err
	}()
	<-entered
	contended := make(chan error, 1)
	go func() { contended <- contender() }()
	select {
	case err := <-contended:
		t.Fatalf("authority escaped proof writer before linearization: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	proof := <-proofs
	if err := <-proofErrs; err != nil {
		t.Fatalf("control proof=%+v err=%v", proof, err)
	}
	if err := <-contended; !errors.Is(err, ErrStaleFence) {
		t.Fatalf("old identity admitted after proof: %v", err)
	}
	return proof
}

func TestControlProofFencesEveryStoreAdmissionAtLinearization(t *testing.T) {
	t.Run("workflow start or adopt", func(t *testing.T) {
		db, ref, leader, ticket := runtimeControlTicket(t)
		raceControlProofAdmission(t, db, ref, func() error {
			_, err := db.StartOrAdopt(context.Background(), ref, ticket.Version-1, "dev/nysa/runtime-control", domain.Fence{LeaderEpoch: leader, RunnerEpoch: ticket.RunnerEpoch})
			return err
		})
	})
	t.Run("workflow start with ownership", func(t *testing.T) {
		db, ref, leader, ticket := runtimeControlTicket(t)
		raceControlProofAdmission(t, db, ref, func() error {
			_, _, err := db.StartWithOwnership(context.Background(), ref, ticket.Version, domain.Fence{LeaderEpoch: leader, RunnerEpoch: ticket.RunnerEpoch}, "dev/nysa/runtime-control", []LeaseRequest{{Scope: "global", Resource: "ownership", Capacity: 1}}, time.Now().UTC())
			return err
		})
	})
	t.Run("plan effect", func(t *testing.T) {
		db, ref, leader, ticket := runtimeControlTicket(t)
		proof := raceControlProofAdmission(t, db, ref, func() error {
			_, err := db.PlanEffect(context.Background(), EffectPlan{SemanticKey: "proof/plan", Ref: ref, Kind: "repository_command", TicketVersion: ticket.Version, Fence: domain.Fence{LeaderEpoch: leader, RunnerEpoch: ticket.RunnerEpoch}, RequestDigest: "proof-plan"})
			return err
		})
		if !proof.Drained() {
			t.Fatalf("new plan was neither rejected nor excluded: %+v", proof)
		}
	})
	t.Run("claim effect", func(t *testing.T) {
		db, ref, leader, ticket := runtimeControlTicket(t)
		if _, err := db.PlanEffect(t.Context(), EffectPlan{SemanticKey: "proof/claim", Ref: ref, Kind: "repository_command", TicketVersion: ticket.Version, Fence: domain.Fence{LeaderEpoch: leader, RunnerEpoch: ticket.RunnerEpoch}, RequestDigest: "proof-claim"}); err != nil {
			t.Fatal(err)
		}
		proof := raceControlProofAdmission(t, db, ref, func() error {
			_, err := db.ClaimEffect(context.Background(), EffectFence{SemanticKey: "proof/claim", Ref: ref, TicketVersion: ticket.Version, Fence: domain.Fence{LeaderEpoch: leader, RunnerEpoch: ticket.RunnerEpoch}})
			return err
		})
		if !proof.Drained() {
			t.Fatalf("new claim was neither rejected nor excluded: %+v", proof)
		}
	})
	t.Run("generic lease", func(t *testing.T) {
		db, ref, leader, ticket := runtimeControlTicket(t)
		raceControlProofAdmission(t, db, ref, func() error {
			_, err := db.AcquireLeases(context.Background(), ref, ticket.Version, domain.Fence{LeaderEpoch: leader, RunnerEpoch: ticket.RunnerEpoch}, []LeaseRequest{{Scope: "global", Resource: "proof-machine", Capacity: 1}}, time.Now().UTC())
			return err
		})
	})
	t.Run("provider begin", func(t *testing.T) {
		db, ctx := openTestStore(t)
		digest := setupProviderProject(t, db, ctx)
		leader, err := db.AcquireLeader(ctx, domain.ChannelDev, "proof-provider")
		if err != nil {
			t.Fatal(err)
		}
		ticket := setupProviderTicket(t, db, ctx, "SF-proof-provider", leader)
		ticket = providerState(t, db, ctx, ticket, leader, domain.StateBuilding)
		builder, _ := setupProviderPair(t, db, ctx)
		raceControlProofAdmission(t, db, ticket.Ref, func() error {
			_, err := db.BeginProviderAttempt(context.Background(), supervised(t, ProviderAttemptRequest{Ref: ticket.Ref, ExpectedVersion: ticket.Version, Fence: domain.Fence{LeaderEpoch: leader, RunnerEpoch: ticket.RunnerEpoch}, Phase: domain.PhaseBuild, Role: "builder", Binding: runtime(builder), ConfigDigest: digest, Capacity: 1, At: time.Now().UTC()}))
			return err
		})
	})
	t.Run("git issue and acquire", func(t *testing.T) {
		db, ctx := openTestStore(t)
		intent := gitIntentFixture(t, db, ctx, "SF-proof-git-issue")
		raceControlProofAdmission(t, db, intent.Ref, func() error {
			_, err := db.IssueGitMutationClaim(context.Background(), intent)
			return err
		})

		db, ctx = openTestStore(t)
		intent = gitIntentFixture(t, db, ctx, "SF-proof-git-acquire")
		claim, err := db.IssueGitMutationClaim(ctx, intent)
		if err != nil {
			t.Fatal(err)
		}
		proof := raceControlProofAdmission(t, db, intent.Ref, func() error {
			_, err := db.AcquireGitMutation(context.Background(), claim)
			return err
		})
		if proof.UnreconciledEffects == 0 {
			t.Fatalf("pre-existing git claim was missing from proof: %+v", proof)
		}
	})
	t.Run("repository command issue and acquire", func(t *testing.T) {
		db, ctx := openTestStore(t)
		intent := repositoryCommandIntentFixture(t, db, ctx, "proof-issue")
		raceControlProofAdmission(t, db, intent.Ref, func() error {
			_, err := db.IssueRepositoryCommandClaim(context.Background(), intent)
			return err
		})

		db, ctx = openTestStore(t)
		intent = repositoryCommandIntentFixture(t, db, ctx, "proof-acquire")
		claim, err := db.IssueRepositoryCommandClaim(ctx, intent)
		if err != nil {
			t.Fatal(err)
		}
		proof := raceControlProofAdmission(t, db, intent.Ref, func() error {
			_, err := db.AcquireRepositoryCommand(context.Background(), claim)
			return err
		})
		if proof.UnreconciledEffects == 0 {
			t.Fatalf("pre-existing command claim was missing from proof: %+v", proof)
		}
	})
	t.Run("external mutation guard", func(t *testing.T) {
		db, ref, leader, ticket := runtimeControlTicket(t)
		if _, err := db.PlanEffect(t.Context(), EffectPlan{SemanticKey: "proof/external", Ref: ref, Kind: "repository_command", TicketVersion: ticket.Version, Fence: domain.Fence{LeaderEpoch: leader, RunnerEpoch: ticket.RunnerEpoch}, RequestDigest: "proof-external"}); err != nil {
			t.Fatal(err)
		}
		claim, err := db.ClaimEffect(t.Context(), EffectFence{SemanticKey: "proof/external", Ref: ref, TicketVersion: ticket.Version, Fence: domain.Fence{LeaderEpoch: leader, RunnerEpoch: ticket.RunnerEpoch}})
		if err != nil || !claim.Claimed {
			t.Fatalf("claim=%+v err=%v", claim, err)
		}
		proof := raceControlProofAdmission(t, db, ref, func() error {
			_, err := db.ExternalMutationGuard().RunExternalMutation(context.Background(), claim.ExternalClaim(), func(context.Context) ([]byte, error) { return nil, nil })
			return err
		})
		if proof.UnreconciledEffects == 0 {
			t.Fatalf("pre-existing external claim was missing from proof: %+v", proof)
		}
	})
}

func TestRearmProofTemporarilyFencesNewIdentityUntilAtomicActivation(t *testing.T) {
	db, ref, leader, stopped := runtimeControlTicket(t)
	oldProof, err := db.ControlProof(t.Context(), ref)
	if err != nil || !oldProof.Drained() {
		t.Fatalf("old proof=%+v err=%v", oldProof, err)
	}
	if _, err := db.Transition(t.Context(), Transition{Ref: ref, ExpectedVersion: stopped.Version, From: domain.StatePlanning, To: domain.StateVerifying, Trigger: "test_resume", Fence: domain.Fence{LeaderEpoch: leader, RunnerEpoch: stopped.RunnerEpoch}, EventPayload: "{}"}); err != nil {
		t.Fatal(err)
	}
	newer, err := db.Ticket(t.Context(), ref)
	if err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{})
	release := make(chan struct{})
	db.controlProofHook = func() {
		close(entered)
		<-release
	}
	t.Cleanup(func() { db.controlProofHook = nil })
	caps := make(chan *RuntimeRearmCapability, 1)
	errs := make(chan error, 1)
	go func() {
		capability, err := db.RearmProof(context.Background(), ref, oldProof.Ticket)
		caps <- capability
		errs <- err
	}()
	<-entered
	contended := make(chan error, 1)
	go func() {
		_, err := db.PlanEffect(context.Background(), EffectPlan{SemanticKey: "rearm/new-identity", Ref: ref, Kind: "repository_command", TicketVersion: newer.Version, Fence: domain.Fence{LeaderEpoch: leader, RunnerEpoch: newer.RunnerEpoch}, RequestDigest: "rearm-new-identity"})
		contended <- err
	}()
	select {
	case err := <-contended:
		t.Fatalf("new identity escaped rearm proof before temporary fence: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	capability := <-caps
	if err := <-errs; err != nil || capability == nil {
		t.Fatalf("rearm proof capability=%v err=%v", capability, err)
	}
	if err := <-contended; !errors.Is(err, ErrStaleFence) {
		t.Fatalf("new identity was admitted before local token installation: %v", err)
	}
	installed := false
	var pending *RuntimeAdmissionCapability
	if err := db.ActivateRearm(t.Context(), capability, func(capability *RuntimeAdmissionCapability) error {
		_, _, _, _ = capability.ConsumeRuntimeAdmission()
		pending = capability
		installed = true
		return nil
	}); err != nil || !installed {
		t.Fatalf("activate rearm installed=%v err=%v", installed, err)
	}
	if _, err := db.PlanEffect(t.Context(), EffectPlan{SemanticKey: "rearm/before-begin", Ref: ref, Kind: "repository_command", TicketVersion: newer.Version, Fence: domain.Fence{LeaderEpoch: leader, RunnerEpoch: newer.RunnerEpoch}, RequestDigest: "rearm-before-begin"}); !errors.Is(err, ErrStaleFence) {
		t.Fatalf("activation opened Store before exact Begin: %v", err)
	}
	if err := pending.OpenStoreAdmission(t.Context()); err != nil {
		t.Fatalf("exact Begin handoff did not open Store: %v", err)
	}
	if _, err := db.PlanEffect(t.Context(), EffectPlan{SemanticKey: "rearm/after-begin", Ref: ref, Kind: "repository_command", TicketVersion: newer.Version, Fence: domain.Fence{LeaderEpoch: leader, RunnerEpoch: newer.RunnerEpoch}, RequestDigest: "rearm-after-begin"}); err != nil {
		t.Fatalf("new identity remained fenced after exact Begin handoff: %v", err)
	}
	if err := db.ActivateRearm(t.Context(), capability, func(*RuntimeAdmissionCapability) error { return nil }); !errors.Is(err, ErrStaleFence) {
		t.Fatalf("rearm capability replay=%v", err)
	}
}

func TestRearmProofKeepsPriorLatchUntilExactBegin(t *testing.T) {
	db, ref, leader, stopped := runtimeControlTicket(t)
	oldProof, err := db.ControlProof(t.Context(), ref)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Transition(t.Context(), Transition{Ref: ref, ExpectedVersion: stopped.Version, From: domain.StatePlanning, To: domain.StateVerifying, Trigger: "test_resume", Fence: domain.Fence{LeaderEpoch: leader, RunnerEpoch: stopped.RunnerEpoch}, EventPayload: "{}"}); err != nil {
		t.Fatal(err)
	}
	newer, err := db.Ticket(t.Context(), ref)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.PlanEffect(t.Context(), EffectPlan{SemanticKey: "rearm/still-latched", Ref: ref, Kind: "repository_command", TicketVersion: newer.Version, Fence: domain.Fence{LeaderEpoch: leader, RunnerEpoch: newer.RunnerEpoch}, RequestDigest: "rearm-still-latched"}); !errors.Is(err, ErrStaleFence) {
		t.Fatalf("control latch admitted a newer tuple: %v", err)
	}
	capability, err := db.RearmProof(t.Context(), ref, oldProof.Ticket)
	if err != nil {
		t.Fatalf("rearm did not recover after writer drained: %v", err)
	}
	var pending *RuntimeAdmissionCapability
	if err := db.ActivateRearm(t.Context(), capability, func(token *RuntimeAdmissionCapability) error {
		_, _, _, _ = token.ConsumeRuntimeAdmission()
		pending = token
		return nil
	}); err != nil {
		t.Fatalf("retry activation=%v", err)
	}
	if err := pending.OpenStoreAdmission(t.Context()); err != nil {
		t.Fatalf("exact Begin release=%v", err)
	}
}

func TestActivateRearmRejectsUnconsumedOrReplayedAdmissionCapability(t *testing.T) {
	db, ref, leader, stopped := runtimeControlTicket(t)
	oldProof, err := db.ControlProof(t.Context(), ref)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Transition(t.Context(), Transition{Ref: ref, ExpectedVersion: stopped.Version, From: domain.StatePlanning, To: domain.StateVerifying, Trigger: "test_resume", Fence: domain.Fence{LeaderEpoch: leader, RunnerEpoch: stopped.RunnerEpoch}, EventPayload: "{}"}); err != nil {
		t.Fatal(err)
	}
	newer, err := db.Ticket(t.Context(), ref)
	if err != nil {
		t.Fatal(err)
	}
	capability, err := db.RearmProof(t.Context(), ref, oldProof.Ticket)
	if err != nil {
		t.Fatal(err)
	}
	installErr := errors.New("local admission install failed")
	if err := db.ActivateRearm(t.Context(), capability, func(*RuntimeAdmissionCapability) error { return installErr }); !errors.Is(err, installErr) {
		t.Fatalf("install failure=%v", err)
	}
	if _, err := db.PlanEffect(t.Context(), EffectPlan{SemanticKey: "rearm/install-failed-still-fenced", Ref: ref, Kind: "repository_command", TicketVersion: newer.Version, Fence: domain.Fence{LeaderEpoch: leader, RunnerEpoch: newer.RunnerEpoch}, RequestDigest: "rearm-install-failed-still-fenced"}); !errors.Is(err, ErrStaleFence) {
		t.Fatalf("failed install opened Store admission: %v", err)
	}
	var captured *RuntimeAdmissionCapability
	if err := db.ActivateRearm(t.Context(), capability, func(token *RuntimeAdmissionCapability) error {
		captured = token
		return nil // A callback that did not install the local token is invalid.
	}); !errors.Is(err, ErrStaleFence) {
		t.Fatalf("unconsumed token activation=%v", err)
	}
	if _, _, _, ok := captured.ConsumeRuntimeAdmission(); ok {
		t.Fatal("callback retained a replayable Store admission token")
	}
	if _, err := db.PlanEffect(t.Context(), EffectPlan{SemanticKey: "rearm/still-fenced", Ref: ref, Kind: "repository_command", TicketVersion: newer.Version, Fence: domain.Fence{LeaderEpoch: leader, RunnerEpoch: newer.RunnerEpoch}, RequestDigest: "rearm-still-fenced"}); !errors.Is(err, ErrStaleFence) {
		t.Fatalf("unconsumed callback opened Store admission: %v", err)
	}
	if err := db.ActivateRearm(t.Context(), capability, func(token *RuntimeAdmissionCapability) error {
		_, _, _, _ = token.ConsumeRuntimeAdmission()
		return nil
	}); err != nil {
		t.Fatalf("safe retry after unconsumed callback=%v", err)
	}
	if err := db.ActivateRearm(t.Context(), capability, func(*RuntimeAdmissionCapability) error { return nil }); !errors.Is(err, ErrStaleFence) {
		t.Fatalf("replayed rearm proof=%v", err)
	}
}
