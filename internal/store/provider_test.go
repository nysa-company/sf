package store

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
)

var providerTestSigner, _ = contracts.NewDrainSigner()

func supervised(t *testing.T, request ProviderAttemptRequest) ProviderAttemptRequest {
	t.Helper()
	request.Repository = "/tmp/provider"
	request.Worktree = "/tmp/provider/" + string(request.Ref.Ticket)
	request.WorktreeIdentity = `{"repository":"/tmp/provider"}`
	request.BaseSHA = strings.Repeat("a", 40)
	request.SupervisorKey = providerTestSigner.PublicKey()
	return request
}
func proof(t *testing.T, claim ProviderAttemptClaim) contracts.DrainProof {
	t.Helper()
	value, err := providerTestSigner.ProveDrained(drainRequestForClaim(claim))
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func TestProviderAdmissionUsesPairCapacityAndFreshFences(t *testing.T) {
	db, ctx := openTestStore(t)
	digest := setupProviderProject(t, db, ctx)
	leader, _ := db.AcquireLeader(ctx, domain.ChannelDev, "provider-test")
	first := setupProviderTicket(t, db, ctx, "SF-provider-a", leader)
	second := setupProviderTicket(t, db, ctx, "SF-provider-b", leader)
	builder, reviewer := setupProviderPair(t, db, ctx)
	binding := runtime(builder)
	first = providerState(t, db, ctx, first, leader, domain.StateBuilding)
	second = providerState(t, db, ctx, second, leader, domain.StateBuilding)
	claim, err := db.BeginProviderAttempt(ctx, supervised(t, ProviderAttemptRequest{Ref: first.Ref, ExpectedVersion: first.Version, Fence: domain.Fence{LeaderEpoch: leader, RunnerEpoch: first.RunnerEpoch}, Phase: domain.PhaseBuild, Role: "builder", Binding: binding, ConfigDigest: digest, Capacity: 1, At: time.Now().UTC()}))
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.BeginProviderAttempt(ctx, supervised(t, ProviderAttemptRequest{Ref: second.Ref, ExpectedVersion: second.Version, Fence: domain.Fence{LeaderEpoch: leader, RunnerEpoch: second.RunnerEpoch}, Phase: domain.PhaseBuild, Role: "builder", Binding: binding, ConfigDigest: digest, Capacity: 1, At: time.Now().UTC()}))
	if !errors.Is(err, ErrProviderCapacity) {
		t.Fatalf("second admission=%v", err)
	}
	if err := db.FinishProviderAttempt(ctx, claim, proof(t, claim), first.Version, domain.Fence{LeaderEpoch: leader, RunnerEpoch: first.RunnerEpoch}, "completed", "completed", 1, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	var provider, model, family, version, outcome string
	if err := db.db.QueryRowContext(ctx, `SELECT provider,model,family,provider_version,outcome FROM phase_runs WHERE channel=? AND project_id=? AND ticket_id=? AND phase=? AND attempt=?`, first.Ref.Channel, first.Ref.Project, first.Ref.Ticket, domain.PhaseBuild, claim.Attempt).Scan(&provider, &model, &family, &version, &outcome); err != nil {
		t.Fatal(err)
	}
	if provider != binding.Identity.Provider || model != binding.Identity.Model || family != binding.Identity.Family || version != binding.Identity.Version || outcome != "completed" {
		t.Fatalf("phase row lost provider binding: %s/%s/%s/%s outcome=%s", provider, model, family, version, outcome)
	}
	if _, err := db.BeginProviderAttempt(ctx, supervised(t, ProviderAttemptRequest{Ref: second.Ref, ExpectedVersion: second.Version, Fence: domain.Fence{LeaderEpoch: leader, RunnerEpoch: second.RunnerEpoch}, Phase: domain.PhaseBuild, Role: "builder", Binding: binding, ConfigDigest: digest, Capacity: 1, At: time.Now().UTC()})); err != nil {
		t.Fatal(err)
	}
	_ = reviewer
}

func TestReviewerMayBeFreshTwiceButNeverShareBuilderFamily(t *testing.T) {
	db, ctx := openTestStore(t)
	digest := setupProviderProject(t, db, ctx)
	leader, _ := db.AcquireLeader(ctx, domain.ChannelDev, "reviewer-test")
	ticket := setupProviderTicket(t, db, ctx, "SF-review", leader)
	builder, reviewer := setupProviderPair(t, db, ctx)
	ticket = providerState(t, db, ctx, ticket, leader, domain.StateVerifying)
	for _, phase := range []domain.Phase{domain.PhaseVerification, domain.PhaseReview} {
		request := supervised(t, ProviderAttemptRequest{Ref: ticket.Ref, ExpectedVersion: ticket.Version, Fence: domain.Fence{LeaderEpoch: leader, RunnerEpoch: ticket.RunnerEpoch}, Phase: phase, Role: "reviewer", Binding: runtime(reviewer), ConfigDigest: digest, Capacity: 1, At: time.Now().UTC()})
		if phase == domain.PhaseReview {
			if _, err := db.db.ExecContext(ctx, `UPDATE tickets SET source_digest=? WHERE channel=? AND project_id=? AND id=?`, sha256Digest([]byte("review-source")), ticket.Ref.Channel, ticket.Ref.Project, ticket.Ref.Ticket); err != nil {
				t.Fatal(err)
			}
			intent, proof := []byte("review intent"), []byte("review proof")
			revision, err := db.RecordVerification(ctx, VerificationArtifact{Ref: ticket.Ref, ExpectedVersion: ticket.Version, Fence: request.Fence, Intent: intent, Proof: proof, OwnedFiles: []string{"verify.txt"}, CheckpointID: "review-checkpoint"})
			if err != nil {
				t.Fatal(err)
			}
			snapshot := domain.CandidateSnapshot{BaseSHA: strings.Repeat("a", 40), HeadSHA: strings.Repeat("b", 40), TreeSHA: strings.Repeat("c", 40), SourceDigest: sha256Digest([]byte("review-source")), VerificationIntentDigest: revision.IntentDigest, ProofDigest: revision.ProofDigest, CommandPolicyDigest: sha256Digest([]byte("policy"))}
			if _, err := db.RecordCandidate(ctx, CandidateEvidence{Ref: ticket.Ref, ExpectedVersion: ticket.Version, Fence: request.Fence, Snapshot: snapshot, Reason: "review candidate"}); err != nil {
				t.Fatal(err)
			}
			request.ExpectedHead, request.ExpectedProof = snapshot.HeadSHA, snapshot.ProofDigest
		}
		claim, err := db.BeginProviderAttempt(ctx, request)
		if err != nil {
			t.Fatalf("%s: %v", phase, err)
		}
		if err := db.FinishProviderAttempt(ctx, claim, proof(t, claim), ticket.Version, domain.Fence{LeaderEpoch: leader, RunnerEpoch: ticket.RunnerEpoch}, "completed", "completed", 1, time.Now().UTC()); err != nil {
			t.Fatal(err)
		}
		if phase == domain.PhaseVerification {
			ticket = providerState(t, db, ctx, ticket, leader, domain.StateReviewing)
		}
	}
	// A builder family equal to any reviewer outcome remains refused.
	same := builder
	same.Provider.Family = reviewer.Provider.Family
	ticket = providerState(t, db, ctx, ticket, leader, domain.StateBuilding)
	if _, err := db.BeginProviderAttempt(ctx, supervised(t, ProviderAttemptRequest{Ref: ticket.Ref, ExpectedVersion: ticket.Version, Fence: domain.Fence{LeaderEpoch: leader, RunnerEpoch: ticket.RunnerEpoch}, Phase: domain.PhaseBuild, Role: "builder", Binding: runtime(same), ConfigDigest: digest, Capacity: 1, At: time.Now().UTC()})); !errors.Is(err, ErrProviderPairRefused) {
		t.Fatalf("same family builder=%v", err)
	}
}

func TestProviderRecoveryRequiresDrainAndReleasesOnlyOldClaim(t *testing.T) {
	db, ctx := openTestStore(t)
	digest := setupProviderProject(t, db, ctx)
	leader, _ := db.AcquireLeader(ctx, domain.ChannelDev, "recovery-test")
	ticket := setupProviderTicket(t, db, ctx, "SF-recover", leader)
	builder, _ := setupProviderPair(t, db, ctx)
	ticket = providerState(t, db, ctx, ticket, leader, domain.StateBuilding)
	_, err := db.BeginProviderAttempt(ctx, supervised(t, ProviderAttemptRequest{Ref: ticket.Ref, ExpectedVersion: ticket.Version, Fence: domain.Fence{LeaderEpoch: leader, RunnerEpoch: ticket.RunnerEpoch}, Phase: domain.PhaseBuild, Role: "builder", Binding: runtime(builder), ConfigDigest: digest, Capacity: 1, At: time.Now().UTC()}))
	if err != nil {
		t.Fatal(err)
	}
	advanced, err := db.InvalidateRunner(ctx, ticket.Ref, ticket.Version, domain.Fence{LeaderEpoch: leader, RunnerEpoch: ticket.RunnerEpoch})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.recoverProviderAttempts(ctx, ticket.Ref, ticket.RunnerEpoch, leader, time.Now().UTC()); err != nil {
		t.Fatalf("undrained=%v", err)
	}
	claims, err := db.ProviderAttempts(ctx, ticket.Ref)
	if err != nil || len(claims) != 1 || claims[0].State != "quarantined" {
		t.Fatalf("undrained claim=%+v err=%v", claims, err)
	}
	leases, err := db.Leases(ctx, domain.ChannelDev)
	if err != nil || len(leases) != 1 {
		t.Fatalf("undrained leases=%+v err=%v", leases, err)
	}
	claims, _ = db.ActiveProviderAttempts(ctx, domain.ChannelDev)
	if err := db.RecoverProviderAttemptClaimWithProof(ctx, claims[0], leader, proof(t, claims[0].ProviderAttemptClaim), time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	attempts, err := db.ProviderAttempts(ctx, ticket.Ref)
	if err != nil || len(attempts) != 1 || attempts[0].State != "cancelled" {
		t.Fatalf("attempts=%+v err=%v", attempts, err)
	}
	if advanced.RunnerEpoch == ticket.RunnerEpoch {
		t.Fatal("runner was not fenced")
	}
}

func TestProviderRecoveryAcrossLeaderRestartUsesOriginalClaimEpoch(t *testing.T) {
	db, ctx := openTestStore(t)
	digest := setupProviderProject(t, db, ctx)
	oldLeader, _ := db.AcquireLeader(ctx, domain.ChannelDev, "old-leader")
	ticket := setupProviderTicket(t, db, ctx, "SF-restart-recover", oldLeader)
	builder, _ := setupProviderPair(t, db, ctx)
	ticket = providerState(t, db, ctx, ticket, oldLeader, domain.StateBuilding)
	if _, err := db.BeginProviderAttempt(ctx, supervised(t, ProviderAttemptRequest{Ref: ticket.Ref, ExpectedVersion: ticket.Version, Fence: domain.Fence{LeaderEpoch: oldLeader, RunnerEpoch: ticket.RunnerEpoch}, Phase: domain.PhaseBuild, Role: "builder", Binding: runtime(builder), ConfigDigest: digest, Capacity: 1, At: time.Now().UTC()})); err != nil {
		t.Fatal(err)
	}
	newLeader, _ := db.AcquireLeader(ctx, domain.ChannelDev, "new-leader")
	if _, err := db.FenceRecoveredRunners(ctx, domain.ChannelDev, newLeader); err != nil {
		t.Fatal(err)
	}
	claims, err := db.ActiveProviderAttempts(ctx, domain.ChannelDev)
	if err != nil || len(claims) != 1 {
		t.Fatalf("restart claims=%+v err=%v", claims, err)
	}
	if claims[0].LeaderEpoch != oldLeader {
		t.Fatalf("claim leader epoch changed before recovery: %+v", claims[0])
	}
	if err := db.RecoverProviderAttemptClaimWithProof(ctx, claims[0], newLeader, proof(t, claims[0].ProviderAttemptClaim), time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	attempts, err := db.ProviderAttempts(ctx, ticket.Ref)
	if err != nil || len(attempts) != 1 || attempts[0].State != "cancelled" {
		t.Fatalf("recovered attempts=%+v err=%v", attempts, err)
	}
	leases, err := db.Leases(ctx, domain.ChannelDev)
	if err != nil || len(leases) != 0 {
		t.Fatalf("recovered leases=%+v err=%v", leases, err)
	}
}

func TestProviderFinishRejectsStaleClaimAndPreservesLease(t *testing.T) {
	db, ctx := openTestStore(t)
	digest := setupProviderProject(t, db, ctx)
	leader, _ := db.AcquireLeader(ctx, domain.ChannelDev, "stale-finish")
	ticket := setupProviderTicket(t, db, ctx, "SF-stale-finish", leader)
	ticket = providerState(t, db, ctx, ticket, leader, domain.StateBuilding)
	builder, _ := setupProviderPair(t, db, ctx)
	fence := domain.Fence{LeaderEpoch: leader, RunnerEpoch: ticket.RunnerEpoch}
	claim, err := db.BeginProviderAttempt(ctx, supervised(t, ProviderAttemptRequest{Ref: ticket.Ref, ExpectedVersion: ticket.Version, Fence: fence, Phase: domain.PhaseBuild, Role: "builder", Binding: runtime(builder), ConfigDigest: digest, Capacity: 1, At: time.Now().UTC()}))
	if err != nil {
		t.Fatal(err)
	}
	advanced, err := db.InvalidateRunner(ctx, ticket.Ref, ticket.Version, fence)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.FinishProviderAttempt(ctx, claim, proof(t, claim), advanced.Version, domain.Fence{LeaderEpoch: leader, RunnerEpoch: advanced.RunnerEpoch}, "completed", "completed", 1, time.Now().UTC()); !errors.Is(err, ErrStaleFence) {
		t.Fatalf("stale finish=%v", err)
	}
	claims, err := db.ProviderAttempts(ctx, ticket.Ref)
	if err != nil || len(claims) != 1 || claims[0].State != "active" {
		t.Fatalf("stale claim mutated=%+v err=%v", claims, err)
	}
	leases, err := db.Leases(ctx, domain.ChannelDev)
	if err != nil || len(leases) != 1 {
		t.Fatalf("stale finish leases=%+v err=%v", leases, err)
	}
}

func TestProviderAdmissionRequiresMatchingTicketStateAndPhase(t *testing.T) {
	db, ctx := openTestStore(t)
	digest := setupProviderProject(t, db, ctx)
	leader, _ := db.AcquireLeader(ctx, domain.ChannelDev, "state-admission")
	ticket := setupProviderTicket(t, db, ctx, "SF-state-admission", leader)
	builder, _ := setupProviderPair(t, db, ctx)
	_, err := db.BeginProviderAttempt(ctx, supervised(t, ProviderAttemptRequest{Ref: ticket.Ref, ExpectedVersion: ticket.Version, Fence: domain.Fence{LeaderEpoch: leader, RunnerEpoch: ticket.RunnerEpoch}, Phase: domain.PhaseBuild, Role: "builder", Binding: runtime(builder), ConfigDigest: digest, Capacity: 1, At: time.Now().UTC()}))
	if !errors.Is(err, ErrStaleFence) {
		t.Fatalf("queued builder admission=%v", err)
	}
}

func TestProviderLegacyAttemptRowsRemainReadable(t *testing.T) {
	db, ctx := openTestStore(t)
	setupProviderProject(t, db, ctx)
	leader, _ := db.AcquireLeader(ctx, domain.ChannelDev, "legacy-attempt")
	ticket := setupProviderTicket(t, db, ctx, "SF-legacy-attempt", leader)
	_, err := db.db.ExecContext(ctx, `INSERT INTO provider_attempts(channel,project_id,ticket_id,phase,attempt,provider,model,family,version,outcome) VALUES(?,?,?,?,?,?,?,?,?,?)`, ticket.Ref.Channel, ticket.Ref.Project, ticket.Ref.Ticket, domain.PhasePlanning, 1, "cursor", "cursor-model", "cursor-family", "1", "completed")
	if err != nil {
		t.Fatal(err)
	}
	attempts, err := db.ProviderAttempts(ctx, ticket.Ref)
	if err != nil || len(attempts) != 1 || !attempts[0].StartedAt.IsZero() || attempts[0].QualificationID != 0 {
		t.Fatalf("legacy attempts=%+v err=%v", attempts, err)
	}
}

func TestProviderFinishRejectsUsageBeyondTicketCeiling(t *testing.T) {
	db, ctx := openTestStore(t)
	digest := setupProviderProject(t, db, ctx)
	leader, _ := db.AcquireLeader(ctx, domain.ChannelDev, "cost-ceiling")
	ticket := setupProviderTicket(t, db, ctx, "SF-cost-ceiling", leader)
	ticket = providerState(t, db, ctx, ticket, leader, domain.StateBuilding)
	builder, _ := setupProviderPair(t, db, ctx)
	fence := domain.Fence{LeaderEpoch: leader, RunnerEpoch: ticket.RunnerEpoch}
	claim, err := db.BeginProviderAttempt(ctx, supervised(t, ProviderAttemptRequest{Ref: ticket.Ref, ExpectedVersion: ticket.Version, Fence: fence, Phase: domain.PhaseBuild, Role: "builder", Binding: runtime(builder), ConfigDigest: digest, Capacity: 1, At: time.Now().UTC()}))
	if err != nil {
		t.Fatal(err)
	}
	if err := db.FinishProviderAttempt(ctx, claim, proof(t, claim), ticket.Version, fence, "completed", "completed", ticket.MaxCostMicroUSD+1, time.Now().UTC()); !errors.Is(err, ErrBudgetExhausted) {
		t.Fatalf("overspend finish=%v", err)
	}
	if err := db.FinishProviderAttempt(ctx, claim, proof(t, claim), ticket.Version, fence, "completed", "completed", 1, ticket.CreatedAt.Add(ticket.MaxDuration).Add(time.Nanosecond)); !errors.Is(err, ErrBudgetExhausted) {
		t.Fatalf("late finish=%v", err)
	}
	attempts, err := db.ProviderAttempts(ctx, ticket.Ref)
	if err != nil || len(attempts) != 1 || attempts[0].State != "active" {
		t.Fatalf("overspend claim=%+v err=%v", attempts, err)
	}
}

func TestFinalReviewValidationUsesDurableCandidateAndVerification(t *testing.T) {
	db, ctx := openTestStore(t)
	setupProviderProject(t, db, ctx)
	leader, _ := db.AcquireLeader(ctx, domain.ChannelDev, "review-evidence")
	ticket := setupProviderTicket(t, db, ctx, "SF-review-evidence", leader)
	if _, err := db.db.ExecContext(ctx, `UPDATE tickets SET source_digest=? WHERE channel=? AND project_id=? AND id=?`, sha256Digest([]byte("source")), ticket.Ref.Channel, ticket.Ref.Project, ticket.Ref.Ticket); err != nil {
		t.Fatal(err)
	}
	ticket, err := db.Ticket(ctx, ticket.Ref)
	if err != nil {
		t.Fatal(err)
	}
	fence := domain.Fence{LeaderEpoch: leader, RunnerEpoch: ticket.RunnerEpoch}
	intent, proof := []byte("durable intent"), []byte("durable proof")
	revision, err := db.RecordVerification(ctx, VerificationArtifact{Ref: ticket.Ref, ExpectedVersion: ticket.Version, Fence: fence, Intent: intent, Proof: proof, OwnedFiles: []string{"verify.txt"}, CheckpointID: "checkpoint"})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := domain.CandidateSnapshot{BaseSHA: strings.Repeat("a", 40), HeadSHA: strings.Repeat("b", 40), TreeSHA: strings.Repeat("c", 40), SourceDigest: sha256Digest([]byte("source")), VerificationIntentDigest: revision.IntentDigest, ProofDigest: revision.ProofDigest, CommandPolicyDigest: sha256Digest([]byte("policy"))}
	if _, err := db.RecordCandidate(ctx, CandidateEvidence{Ref: ticket.Ref, ExpectedVersion: ticket.Version, Fence: fence, Snapshot: snapshot, Reason: "fresh candidate"}); err != nil {
		t.Fatal(err)
	}
	if err := db.ValidateFinalReviewEvidence(ctx, ticket.Ref, ticket.Version, fence, snapshot.HeadSHA, snapshot.ProofDigest); err != nil {
		t.Fatalf("durable review evidence rejected: %v", err)
	}
	if err := db.ValidateFinalReviewEvidence(ctx, ticket.Ref, ticket.Version, fence, snapshot.HeadSHA, sha256Digest([]byte("wrong"))); !errors.Is(err, ErrEvidenceConflict) {
		t.Fatalf("caller proof claim accepted: %v", err)
	}
	advanced, err := db.InvalidateRunner(ctx, ticket.Ref, ticket.Version, fence)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.ValidateFinalReviewEvidence(ctx, ticket.Ref, advanced.Version, domain.Fence{LeaderEpoch: leader, RunnerEpoch: advanced.RunnerEpoch}, snapshot.HeadSHA, snapshot.ProofDigest); !errors.Is(err, ErrEvidenceConflict) {
		t.Fatalf("stale candidate accepted after runner fence: %v", err)
	}
}

func setupProviderProject(t *testing.T, db *Store, ctx context.Context) string {
	t.Helper()
	raw := []byte(`{"providers":"frozen"}`)
	sum := sha256.Sum256(raw)
	digest := fmt.Sprintf("%x", sum)
	if err := db.CreateProject(ctx, Project{Channel: domain.ChannelDev, ID: "provider", Path: "/tmp/provider", BaseRef: "main", ConfigGeneration: 1, ConfigDigest: digest, ConfigSnapshot: raw}); err != nil {
		t.Fatal(err)
	}
	return digest
}
func setupProviderTicket(t *testing.T, db *Store, ctx context.Context, id string, leader uint64) Ticket {
	t.Helper()
	ref := domain.TicketRef{Channel: domain.ChannelDev, Project: "provider", Ticket: domain.TicketID(id)}
	if err := db.CreateTicket(ctx, Ticket{Ref: ref, SourceDigest: "digest-" + id, Type: domain.TicketFeature, MergeMode: domain.MergeGuarded, CreatedAt: time.Now().UTC(), MaxDuration: time.Hour, MaxCostMicroUSD: 100}); err != nil {
		t.Fatal(err)
	}
	started, err := db.StartOrAdopt(ctx, ref, 1, "dev/provider/"+id, domain.Fence{LeaderEpoch: leader, RunnerEpoch: 1})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.RegisterWorktree(ctx, WorktreeRegistration{Ref: ref, ExpectedVersion: started.Version, Fence: domain.Fence{LeaderEpoch: leader, RunnerEpoch: started.RunnerEpoch}, Path: "/tmp/provider/" + id, Branch: "dev/provider/" + id, IdentityJSON: []byte(`{"repository":"/tmp/provider"}`), BaseSHA: strings.Repeat("a", 40), HeadSHA: strings.Repeat("b", 40)}); err != nil {
		t.Fatal(err)
	}
	return started
}
func setupProviderPair(t *testing.T, db *Store, ctx context.Context) (ProviderQualification, ProviderQualification) {
	t.Helper()
	b, _, err := db.RecordProviderQualification(ctx, qualificationValue("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "cursor", "cursor-family", QualificationGuarded))
	if err != nil {
		t.Fatal(err)
	}
	r, _, err := db.RecordProviderQualification(ctx, qualificationValue("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", "claude", "claude-family", QualificationGuarded))
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := db.SelectProviderPair(ctx, domain.ChannelDev, b.ID, r.ID, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	return b, r
}
func runtime(q ProviderQualification) contracts.RuntimeBinding {
	auth := sha256.Sum256([]byte("test-auth:" + q.Provider.Provider))
	return contracts.RuntimeBinding{Identity: q.Provider, BinaryDigest: q.BinaryDigest, PolicyDigest: q.PolicyDigest, FixtureDigest: q.FixtureDigest, AuthDigest: fmt.Sprintf("%x", auth)}
}

func providerState(t *testing.T, db *Store, ctx context.Context, ticket Ticket, leader uint64, state domain.State) Ticket {
	t.Helper()
	from := ticket.State
	result, err := db.Transition(ctx, Transition{Ref: ticket.Ref, ExpectedVersion: ticket.Version, From: from, To: state, Trigger: "test_phase", Fence: domain.Fence{LeaderEpoch: leader, RunnerEpoch: ticket.RunnerEpoch}, EventPayload: "{}"})
	if err != nil {
		t.Fatal(err)
	}
	ticket.State, ticket.Version = state, result.Version
	return ticket
}
