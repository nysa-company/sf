package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/phaseartifact"
)

var providerTestSigner, _ = contracts.NewDrainSigner()

func supervised(t *testing.T, request ProviderAttemptRequest) ProviderAttemptRequest {
	t.Helper()
	request.Repository = "/tmp/provider"
	request.Worktree = "/tmp/provider/" + string(request.Ref.Ticket)
	request.WorktreeIdentity = `{"repository":"/tmp/provider"}`
	request.BaseSHA = strings.Repeat("a", 40)
	request.SupervisorKey = providerTestSigner.PublicKey()
	if request.Input.Ticket == (domain.TicketRef{}) {
		request.Input = contracts.PhaseInput{Ticket: request.Ref, Phase: request.Phase, LeaderEpoch: request.Fence.LeaderEpoch, RunnerEpoch: request.Fence.RunnerEpoch, ExpectedVersion: request.ExpectedVersion, Prompt: "provider test", Repository: request.Repository, Worktree: request.Worktree, WorktreeIdentity: request.WorktreeIdentity, BaseSHA: request.BaseSHA, AllowedPaths: []string{"."}, Provider: request.Binding.Identity, AuthMode: request.Binding.AuthMode, Timeout: time.Minute, Profile: contracts.ProfileGuarded, Schema: []byte(`{"type":"object"}`)}
	}
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
	if err := db.FinishProviderAttempt(ctx, claim, proof(t, claim), first.Version, domain.Fence{LeaderEpoch: leader, RunnerEpoch: first.RunnerEpoch}, "failed", "failed", 1, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	var provider, model, family, version, outcome string
	if err := db.db.QueryRowContext(ctx, `SELECT provider,model,family,provider_version,outcome FROM phase_runs WHERE channel=? AND project_id=? AND ticket_id=? AND phase=? AND attempt=?`, first.Ref.Channel, first.Ref.Project, first.Ref.Ticket, domain.PhaseBuild, claim.Attempt).Scan(&provider, &model, &family, &version, &outcome); err != nil {
		t.Fatal(err)
	}
	if provider != binding.Identity.Provider || model != binding.Identity.Model || family != binding.Identity.Family || version != binding.Identity.Version || outcome != "failed" {
		t.Fatalf("phase row lost provider binding: %s/%s/%s/%s outcome=%s", provider, model, family, version, outcome)
	}
	if _, err := db.BeginProviderAttempt(ctx, supervised(t, ProviderAttemptRequest{Ref: second.Ref, ExpectedVersion: second.Version, Fence: domain.Fence{LeaderEpoch: leader, RunnerEpoch: second.RunnerEpoch}, Phase: domain.PhaseBuild, Role: "builder", Binding: binding, ConfigDigest: digest, Capacity: 1, At: time.Now().UTC()})); err != nil {
		t.Fatal(err)
	}
	_ = reviewer
}

func TestRecordProviderLaunchBindsTheEntireClaimAndStartIdentity(t *testing.T) {
	db, ctx := openTestStore(t)
	digest := setupProviderProject(t, db, ctx)
	leader, _ := db.AcquireLeader(ctx, domain.ChannelDev, "launch-record")
	ticket := setupProviderTicket(t, db, ctx, "SF-launch", leader)
	builder, _ := setupProviderPair(t, db, ctx)
	ticket = providerState(t, db, ctx, ticket, leader, domain.StateBuilding)
	claim, err := db.BeginProviderAttempt(ctx, supervised(t, ProviderAttemptRequest{Ref: ticket.Ref, ExpectedVersion: ticket.Version, Fence: domain.Fence{LeaderEpoch: leader, RunnerEpoch: ticket.RunnerEpoch}, Phase: domain.PhaseBuild, Role: "builder", Binding: runtime(builder), ConfigDigest: digest, Capacity: 1, At: time.Now().UTC()}))
	if err != nil {
		t.Fatal(err)
	}
	launch := contracts.ProviderLaunch{PID: 123, PGID: 123, BootIdentity: "test-boot", ProcessStartIdentity: "Thu Aug 29 12:34:56 2026", Worktree: claim.Worktree}
	if err := db.RecordProviderLaunch(ctx, claim, launch); err != nil {
		t.Fatal(err)
	}
	got, err := db.ProviderLaunchIdentity(ctx, claim)
	if err != nil || got != launch {
		t.Fatalf("launch=%+v err=%v want=%+v", got, err, launch)
	}
	wrong := claim
	wrong.Attempt++
	if err := db.RecordProviderLaunch(ctx, wrong, launch); !errors.Is(err, ErrStaleFence) && !errors.Is(err, ErrProviderAttempt) {
		t.Fatalf("wrong attempt recorded: %v", err)
	}
}

func TestProviderAttemptLaunchInputIsAppendOnly(t *testing.T) {
	db, ctx := openTestStore(t)
	digest := setupProviderProject(t, db, ctx)
	leader, _ := db.AcquireLeader(ctx, domain.ChannelDev, "launch-input-immutable")
	ticket := setupProviderTicket(t, db, ctx, "SF-launch-input", leader)
	builder, _ := setupProviderPair(t, db, ctx)
	ticket = providerState(t, db, ctx, ticket, leader, domain.StateBuilding)
	claim, err := db.BeginProviderAttempt(ctx, supervised(t, ProviderAttemptRequest{Ref: ticket.Ref, ExpectedVersion: ticket.Version, Fence: domain.Fence{LeaderEpoch: leader, RunnerEpoch: ticket.RunnerEpoch}, Phase: domain.PhaseBuild, Role: "builder", Binding: runtime(builder), ConfigDigest: digest, Capacity: 1, At: time.Now().UTC()}))
	if err != nil {
		t.Fatal(err)
	}
	if len(claim.RequestDigest) != 64 || !contracts.PhaseInputDigestMatches(claim.Input, claim.RequestDigest) || string(claim.RequestPayload) == "" {
		t.Fatalf("store did not issue complete launch input claim: %+v", claim)
	}
	if _, err := db.db.ExecContext(ctx, `UPDATE provider_attempt_inputs SET request_digest=? WHERE provider_attempt_id=?`, strings.Repeat("0", 64), claim.ID); err == nil {
		t.Fatal("launch input record was mutable")
	}
	if _, err := db.db.ExecContext(ctx, `DELETE FROM provider_attempt_inputs WHERE provider_attempt_id=?`, claim.ID); err == nil {
		t.Fatal("launch input record was deletable")
	}
}

func TestBeginProviderAttemptRejectsInvalidDirectLaunchInput(t *testing.T) {
	for name, mutate := range map[string]func(*ProviderAttemptRequest){
		"prompt":   func(r *ProviderAttemptRequest) { r.Input.Prompt = " " },
		"schema":   func(r *ProviderAttemptRequest) { r.Input.Schema = []byte("not-json") },
		"paths":    func(r *ProviderAttemptRequest) { r.Input.AllowedPaths = []string{"../escape"} },
		"profile":  func(r *ProviderAttemptRequest) { r.Input.Profile = contracts.ProfileAutonomous },
		"timeout":  func(r *ProviderAttemptRequest) { r.Input.Timeout = 46 * time.Minute },
		"ticket":   func(r *ProviderAttemptRequest) { r.Input.Ticket.Ticket = "SF-other" },
		"phase":    func(r *ProviderAttemptRequest) { r.Input.Phase = domain.PhasePlanning },
		"fence":    func(r *ProviderAttemptRequest) { r.Input.RunnerEpoch++ },
		"worktree": func(r *ProviderAttemptRequest) { r.Input.Worktree = "/tmp/other" },
		"provider": func(r *ProviderAttemptRequest) { r.Input.Provider.Model = "other" },
		"auth":     func(r *ProviderAttemptRequest) { r.Input.AuthMode = "other" },
	} {
		t.Run(name, func(t *testing.T) {
			db, ctx := openTestStore(t)
			digest := setupProviderProject(t, db, ctx)
			leader, _ := db.AcquireLeader(ctx, domain.ChannelDev, "invalid-input")
			ticket := providerState(t, db, ctx, setupProviderTicket(t, db, ctx, "SF-direct-input", leader), leader, domain.StateBuilding)
			builder, _ := setupProviderPair(t, db, ctx)
			request := supervised(t, ProviderAttemptRequest{Ref: ticket.Ref, ExpectedVersion: ticket.Version, Fence: domain.Fence{LeaderEpoch: leader, RunnerEpoch: ticket.RunnerEpoch}, Phase: domain.PhaseBuild, Role: "builder", Binding: runtime(builder), ConfigDigest: digest, Capacity: 1, At: time.Now().UTC()})
			mutate(&request)
			if _, err := db.BeginProviderAttempt(ctx, request); !errors.Is(err, ErrProviderAttempt) {
				t.Fatalf("invalid %s direct input admitted: %v", name, err)
			}
		})
	}
}

func TestFailProviderAttemptBeforeLaunchClosesExactClaimAndReleasesLease(t *testing.T) {
	db, ctx := openTestStore(t)
	digest := setupProviderProject(t, db, ctx)
	leader, _ := db.AcquireLeader(ctx, domain.ChannelDev, "invocation-failure")
	ticket := providerState(t, db, ctx, setupProviderTicket(t, db, ctx, "SF-invocation-failure", leader), leader, domain.StateBuilding)
	builder, _ := setupProviderPair(t, db, ctx)
	claim, err := db.BeginProviderAttempt(ctx, supervised(t, ProviderAttemptRequest{Ref: ticket.Ref, ExpectedVersion: ticket.Version, Fence: domain.Fence{LeaderEpoch: leader, RunnerEpoch: ticket.RunnerEpoch}, Phase: domain.PhaseBuild, Role: "builder", Binding: runtime(builder), ConfigDigest: digest, Capacity: 1, At: time.Now().UTC()}))
	if err != nil {
		t.Fatal(err)
	}
	if err := db.FailProviderAttemptBeforeLaunch(ctx, claim, ticket.Version, domain.Fence{LeaderEpoch: leader, RunnerEpoch: ticket.RunnerEpoch}, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	attempts, err := db.ProviderAttempts(ctx, ticket.Ref)
	if err != nil || len(attempts) != 1 || attempts[0].State != "failed" || attempts[0].Outcome != "invocation_failed" {
		t.Fatalf("attempt=%+v err=%v", attempts, err)
	}
	var phaseState, phaseOutcome string
	if err := db.db.QueryRowContext(ctx, `SELECT state,outcome FROM phase_runs WHERE channel=? AND project_id=? AND ticket_id=?`, ticket.Ref.Channel, ticket.Ref.Project, ticket.Ref.Ticket).Scan(&phaseState, &phaseOutcome); err != nil || phaseState != "failed" || phaseOutcome != "invocation_failed" {
		t.Fatalf("phase=%s/%s err=%v", phaseState, phaseOutcome, err)
	}
	leases, err := db.Leases(ctx, domain.ChannelDev)
	if err != nil || len(leases) != 0 {
		t.Fatalf("pre-launch provider lease was retained: %+v err=%v", leases, err)
	}
}

func TestReviewerMayBeFreshTwiceButNeverShareBuilderFamily(t *testing.T) {
	db, ctx := openTestStore(t)
	digest := setupProviderProject(t, db, ctx)
	leader, _ := db.AcquireLeader(ctx, domain.ChannelDev, "reviewer-test")
	ticket := setupProviderTicket(t, db, ctx, "SF-review", leader)
	builder, reviewer := setupProviderPair(t, db, ctx)
	ticket = providerState(t, db, ctx, ticket, leader, domain.StateVerifying)
	for _, phase := range []domain.Phase{domain.PhaseVerification} {
		request := supervised(t, ProviderAttemptRequest{Ref: ticket.Ref, ExpectedVersion: ticket.Version, Fence: domain.Fence{LeaderEpoch: leader, RunnerEpoch: ticket.RunnerEpoch}, Phase: phase, Role: "reviewer", Binding: runtime(reviewer), ConfigDigest: digest, Capacity: 1, At: time.Now().UTC()})
		claim, err := db.BeginProviderAttempt(ctx, request)
		if err != nil {
			t.Fatalf("%s: %v", phase, err)
		}
		if err := db.FinishProviderAttempt(ctx, claim, proof(t, claim), ticket.Version, domain.Fence{LeaderEpoch: leader, RunnerEpoch: ticket.RunnerEpoch}, "failed", "failed", 1, time.Now().UTC()); err != nil {
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
	if err := db.SetRecoveryAuthority(ctx, domain.ChannelDev, leader, providerTestSigner.PublicKey()); err != nil {
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
	if err := db.SetRecoveryAuthority(ctx, domain.ChannelDev, newLeader, providerTestSigner.PublicKey()); err != nil {
		t.Fatal(err)
	}
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
	if err := db.FinishProviderAttempt(ctx, claim, proof(t, claim), advanced.Version, domain.Fence{LeaderEpoch: leader, RunnerEpoch: advanced.RunnerEpoch}, "failed", "failed", 1, time.Now().UTC()); !errors.Is(err, ErrStaleFence) {
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

func TestCompleteProviderAttemptSuccessPersistsAndReparses(t *testing.T) {
	db, ctx := openTestStore(t)
	digest := setupProviderProject(t, db, ctx)
	leader, _ := db.AcquireLeader(ctx, domain.ChannelDev, "result-persist")
	ticket := providerState(t, db, ctx, setupProviderTicket(t, db, ctx, "SF-result-persist", leader), leader, domain.StateBuilding)
	builder, _ := setupProviderPair(t, db, ctx)
	fence := domain.Fence{LeaderEpoch: leader, RunnerEpoch: ticket.RunnerEpoch}
	claim, err := db.BeginProviderAttempt(ctx, supervised(t, ProviderAttemptRequest{Ref: ticket.Ref, ExpectedVersion: ticket.Version, Fence: fence, Phase: domain.PhaseBuild, Role: "builder", Binding: runtime(builder), ConfigDigest: digest, Capacity: 1, At: time.Now().UTC()}))
	if err != nil {
		t.Fatal(err)
	}
	// Codex supplies no ChangedFiles inventory; typed artifact paths remain the authority.
	raw := contracts.PhaseResult{Provider: claim.Binding.Identity, Artifact: []byte(`{"schema":"sf.builder/v1","summary":"done","changed_files":["main.go"],"commands":[["go","test","./..."]]}`), UsageTrusted: true, UsageUnits: 1, Transcript: "never stored"}
	mutatedClaim := claim
	mutatedClaim.Input.AllowedPaths = []string{"other"}
	if _, err := db.CompleteProviderAttemptSuccess(ctx, mutatedClaim, proof(t, claim), ticket.Version, fence, raw, phaseartifact.Validation{TicketType: domain.TicketFeature}, time.Now().UTC()); !errors.Is(err, ErrProviderAttempt) {
		t.Fatalf("mutated allowed paths=%v", err)
	}
	mutatedClaim = claim
	mutatedClaim.Input.RequestDigest = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	if _, err := db.CompleteProviderAttemptSuccess(ctx, mutatedClaim, proof(t, claim), ticket.Version, fence, raw, phaseartifact.Validation{TicketType: domain.TicketFeature}, time.Now().UTC()); !errors.Is(err, ErrProviderAttempt) {
		t.Fatalf("mutated input request digest=%v", err)
	}
	if err := db.FinishProviderAttempt(ctx, claim, proof(t, claim), ticket.Version, fence, "completed", "completed", 1, time.Now().UTC()); !errors.Is(err, ErrProviderAttempt) {
		t.Fatalf("legacy success finish=%v", err)
	}
	badUsage := raw
	badUsage.UsageTrusted = false
	if _, err := db.CompleteProviderAttemptSuccess(ctx, claim, proof(t, claim), ticket.Version, fence, badUsage, phaseartifact.Validation{TicketType: domain.TicketFeature}, time.Now().UTC()); !errors.Is(err, ErrProviderAttempt) {
		t.Fatalf("untrusted usage=%v", err)
	}
	var count int
	if err := db.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM provider_attempt_results WHERE provider_attempt_id=?`, claim.ID).Scan(&count); err != nil || count != 0 {
		t.Fatalf("premature result count=%d err=%v", count, err)
	}
	stored, err := db.CompleteProviderAttemptSuccess(ctx, claim, proof(t, claim), ticket.Version, fence, raw, phaseartifact.Validation{TicketType: domain.TicketFeature}, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	loaded, parsed, err := db.LoadProviderAttemptResult(ctx, claim, ticket.Version, fence)
	if err != nil || parsed.Builder == nil || loaded.RawSHA256 != stored.RawSHA256 || loaded.Claim.BindingDigest != claim.BindingDigest || loaded.Claim.LeaseKey != claim.LeaseKey || !bytes.Equal(loaded.Claim.SupervisorKey, claim.SupervisorKey) || loaded.Claim.Input.RequestDigest != claim.Input.RequestDigest {
		t.Fatalf("load=%+v parsed=%+v err=%v", loaded, parsed, err)
	}
	// Adoption binds the exact immutable Builder result, current source and
	// verification, registered worktree/base, and a Store-neutral commit
	// observation. An exact generation replay creates no new receipts.
	source := sha256Digest([]byte("candidate source"))
	if _, err := db.db.ExecContext(ctx, `UPDATE tickets SET source_digest=? WHERE channel=? AND project_id=? AND id=?`, source, ticket.Ref.Channel, ticket.Ref.Project, ticket.Ref.Ticket); err != nil {
		t.Fatal(err)
	}
	revision, err := db.RecordVerification(ctx, VerificationArtifact{Ref: ticket.Ref, ExpectedVersion: ticket.Version, Fence: fence, Intent: []byte("candidate intent"), Proof: []byte("candidate proof"), OwnedFiles: []string{"verify.txt"}, CheckpointID: strings.Repeat("d", 40)})
	if err != nil {
		t.Fatal(err)
	}
	builderDigest, err := phaseartifact.BuilderEvidenceDigest(*parsed.Builder)
	if err != nil {
		t.Fatal(err)
	}
	snapshot := domain.CandidateSnapshot{Generation: 1, BaseSHA: strings.Repeat("a", 40), HeadSHA: strings.Repeat("b", 40), TreeSHA: strings.Repeat("c", 40), SourceDigest: source, VerificationIntentDigest: revision.IntentDigest, ProofDigest: revision.ProofDigest, CommandPolicyDigest: sha256Digest([]byte("policy")), BuilderEvidenceDigest: builderDigest}
	evidence := CandidateEvidence{Ref: ticket.Ref, ExpectedVersion: ticket.Version, Fence: fence, Snapshot: snapshot, BuilderResult: ProviderAttemptResultKey{AttemptID: claim.ID, Ref: ticket.Ref, Phase: domain.PhaseBuild, Attempt: claim.Attempt}, Commit: CommitObservation{CommitOID: snapshot.HeadSHA, ParentOID: snapshot.BaseSHA, TreeOID: snapshot.TreeSHA}, Reason: "candidate created"}
	if receipts, err := db.RecordCandidate(ctx, evidence); err != nil || len(receipts) != 4 {
		t.Fatalf("candidate receipts=%+v err=%v", receipts, err)
	}
	if receipts, err := db.RecordCandidate(ctx, evidence); err != nil || len(receipts) != 0 {
		t.Fatalf("candidate replay receipts=%+v err=%v", receipts, err)
	}
	if _, err := db.db.ExecContext(ctx, `INSERT INTO candidate_snapshots(channel,project_id,ticket_id,generation,ticket_version,leader_epoch,runner_epoch,base_sha,head_sha,tree_sha,source_digest,verification_intent_digest,proof_digest,command_policy_digest,builder_evidence_digest,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, ticket.Ref.Channel, ticket.Ref.Project, ticket.Ref.Ticket, 2, ticket.Version, fence.LeaderEpoch, fence.RunnerEpoch, snapshot.BaseSHA, strings.Repeat("e", 40), strings.Repeat("f", 40), snapshot.SourceDigest, snapshot.VerificationIntentDigest, snapshot.ProofDigest, snapshot.CommandPolicyDigest, snapshot.BuilderEvidenceDigest, now()); err != nil {
		t.Fatal(err)
	}
	if _, err := db.RecordCandidate(ctx, evidence); !errors.Is(err, ErrEvidenceConflict) {
		t.Fatalf("older candidate replay=%v", err)
	}
	if _, err := db.db.ExecContext(ctx, `UPDATE provider_attempt_results SET raw_sha256='aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa' WHERE provider_attempt_id=?`, claim.ID); err == nil {
		t.Fatal("immutable result updated")
	}
	if _, err := db.CompleteProviderAttemptSuccess(ctx, claim, proof(t, claim), ticket.Version, fence, raw, phaseartifact.Validation{TicketType: domain.TicketFeature}, time.Now().UTC()); err != nil {
		t.Fatalf("exact replay=%v", err)
	}
	conflict := raw
	conflict.Artifact = []byte(`{"schema":"sf.builder/v1","summary":"different","changed_files":["main.go"],"commands":[["go","test","./..."]]}`)
	if _, err := db.CompleteProviderAttemptSuccess(ctx, claim, proof(t, claim), ticket.Version, fence, conflict, phaseartifact.Validation{TicketType: domain.TicketFeature}, time.Now().UTC()); !errors.Is(err, ErrProviderAttempt) {
		t.Fatalf("conflicting replay=%v", err)
	}
	if _, err := db.db.ExecContext(ctx, `UPDATE daemon_instances SET leader_epoch=leader_epoch+1 WHERE channel=?`, ticket.Ref.Channel); err != nil {
		t.Fatal(err)
	}
	if _, _, err := db.LoadProviderAttemptResult(ctx, claim, ticket.Version, fence); !errors.Is(err, ErrProviderAttempt) {
		t.Fatalf("stale current fence load=%v", err)
	}
	if _, err := db.db.ExecContext(ctx, `UPDATE daemon_instances SET leader_epoch=? WHERE channel=?`, leader, ticket.Ref.Channel); err != nil {
		t.Fatal(err)
	}
	if _, err := db.db.ExecContext(ctx, `UPDATE provider_qualifications SET binary_digest='dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd' WHERE id=?`, claim.QualificationID); err != nil {
		t.Fatal(err)
	}
	if _, _, err := db.LoadHistoricalProviderAttemptResult(ctx, ProviderAttemptResultKey{AttemptID: claim.ID, Ref: claim.Ref, Phase: claim.Phase, Attempt: claim.Attempt}); !errors.Is(err, ErrProviderAttempt) {
		t.Fatalf("tampered qualification load=%v", err)
	}
	if _, err := db.db.ExecContext(ctx, `UPDATE provider_attempts SET worktree_path='tampered' WHERE id=?`, claim.ID); err != nil {
		t.Fatal(err)
	}
	if _, _, err := db.LoadProviderAttemptResult(ctx, claim, ticket.Version, fence); err == nil {
		t.Fatalf("tampered source load=%v", err)
	}
}

func TestProviderResultInsertFailureRollsBackTerminalCompletion(t *testing.T) {
	db, ctx := openTestStore(t)
	digest := setupProviderProject(t, db, ctx)
	leader, _ := db.AcquireLeader(ctx, domain.ChannelDev, "result-rollback")
	ticket := providerState(t, db, ctx, setupProviderTicket(t, db, ctx, "SF-result-rollback", leader), leader, domain.StateBuilding)
	builder, _ := setupProviderPair(t, db, ctx)
	fence := domain.Fence{LeaderEpoch: leader, RunnerEpoch: ticket.RunnerEpoch}
	claim, err := db.BeginProviderAttempt(ctx, supervised(t, ProviderAttemptRequest{Ref: ticket.Ref, ExpectedVersion: ticket.Version, Fence: fence, Phase: domain.PhaseBuild, Role: "builder", Binding: runtime(builder), ConfigDigest: digest, Capacity: 1, At: time.Now().UTC()}))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.db.ExecContext(ctx, `CREATE TRIGGER provider_result_fault AFTER INSERT ON provider_attempt_results BEGIN SELECT RAISE(ABORT,'injected result insert failure'); END`); err != nil {
		t.Fatal(err)
	}
	defer db.db.ExecContext(ctx, `DROP TRIGGER provider_result_fault`)
	raw := contracts.PhaseResult{Provider: claim.Binding.Identity, Artifact: []byte(`{"schema":"sf.builder/v1","summary":"done","changed_files":["main.go"],"commands":[["go","test","./..."]]}`), UsageTrusted: true, UsageUnits: 1}
	if _, err := db.CompleteProviderAttemptSuccess(ctx, claim, proof(t, claim), ticket.Version, fence, raw, phaseartifact.Validation{TicketType: domain.TicketFeature}, time.Now().UTC()); err == nil {
		t.Fatal("injected success completed")
	}
	var state string
	if err := db.db.QueryRowContext(ctx, `SELECT state FROM provider_attempts WHERE id=?`, claim.ID).Scan(&state); err != nil || state != "active" {
		t.Fatalf("attempt state=%q err=%v", state, err)
	}
	var results, leases int
	if err := db.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM provider_attempt_results WHERE provider_attempt_id=?`, claim.ID).Scan(&results); err != nil || results != 0 {
		t.Fatalf("results=%d err=%v", results, err)
	}
	if err := db.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM leases WHERE channel=? AND scope='provider'`, ticket.Ref.Channel).Scan(&leases); err != nil || leases != 1 {
		t.Fatalf("leases=%d err=%v", leases, err)
	}
}

func TestProviderCompletionMissingLeaseRollsBack(t *testing.T) {
	db, ctx := openTestStore(t)
	digest := setupProviderProject(t, db, ctx)
	leader, _ := db.AcquireLeader(ctx, domain.ChannelDev, "result-missing-lease")
	ticket := providerState(t, db, ctx, setupProviderTicket(t, db, ctx, "SF-result-missing-lease", leader), leader, domain.StateBuilding)
	builder, _ := setupProviderPair(t, db, ctx)
	fence := domain.Fence{LeaderEpoch: leader, RunnerEpoch: ticket.RunnerEpoch}
	claim, err := db.BeginProviderAttempt(ctx, supervised(t, ProviderAttemptRequest{Ref: ticket.Ref, ExpectedVersion: ticket.Version, Fence: fence, Phase: domain.PhaseBuild, Role: "builder", Binding: runtime(builder), ConfigDigest: digest, Capacity: 1, At: time.Now().UTC()}))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.db.ExecContext(ctx, `DELETE FROM leases WHERE channel=? AND scope='provider' AND scope_key=?`, ticket.Ref.Channel, claim.LeaseKey); err != nil {
		t.Fatal(err)
	}
	raw := contracts.PhaseResult{Provider: claim.Binding.Identity, Artifact: []byte(`{"schema":"sf.builder/v1","summary":"done","changed_files":["main.go"],"commands":[["go","test","./..."]]}`), UsageTrusted: true, UsageUnits: 1}
	if _, err := db.CompleteProviderAttemptSuccess(ctx, claim, proof(t, claim), ticket.Version, fence, raw, phaseartifact.Validation{TicketType: domain.TicketFeature}, time.Now().UTC()); !errors.Is(err, ErrStaleFence) {
		t.Fatalf("missing lease completion=%v", err)
	}
	var state, phaseState string
	var results int
	if err := db.db.QueryRowContext(ctx, `SELECT state FROM provider_attempts WHERE id=?`, claim.ID).Scan(&state); err != nil || state != "active" {
		t.Fatalf("attempt=%s err=%v", state, err)
	}
	if err := db.db.QueryRowContext(ctx, `SELECT state FROM phase_runs WHERE channel=? AND project_id=? AND ticket_id=? AND phase=? AND attempt=?`, ticket.Ref.Channel, ticket.Ref.Project, ticket.Ref.Ticket, claim.Phase, claim.Attempt).Scan(&phaseState); err != nil || phaseState != "active" {
		t.Fatalf("phase=%s err=%v", phaseState, err)
	}
	if err := db.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM provider_attempt_results WHERE provider_attempt_id=?`, claim.ID).Scan(&results); err != nil || results != 0 {
		t.Fatalf("results=%d err=%v", results, err)
	}
}

func TestHistoricalProviderResultSurvivesTransitionAndLeaderRestart(t *testing.T) {
	db, ctx := openTestStore(t)
	digest := setupProviderProject(t, db, ctx)
	leader, _ := db.AcquireLeader(ctx, domain.ChannelDev, "historical-result")
	ticket := providerState(t, db, ctx, setupProviderTicket(t, db, ctx, "SF-historical-result", leader), leader, domain.StatePlanning)
	planner, _ := setupProviderPair(t, db, ctx)
	fence := domain.Fence{LeaderEpoch: leader, RunnerEpoch: ticket.RunnerEpoch}
	claim, err := db.BeginProviderAttempt(ctx, supervised(t, ProviderAttemptRequest{Ref: ticket.Ref, ExpectedVersion: ticket.Version, Fence: fence, Phase: domain.PhasePlanning, Role: "planner", Binding: runtime(planner), ConfigDigest: digest, Capacity: 1, At: time.Now().UTC()}))
	if err != nil {
		t.Fatal(err)
	}
	raw := contracts.PhaseResult{Provider: claim.Binding.Identity, Artifact: []byte(`{"schema":"sf.planner/v1","acceptance":["works"],"proof":{"kind":"acceptance","command":["go","test"],"details":"proof"},"paths":["main.go"],"commands":[["go","test"]],"risks":["risk"]}`), UsageTrusted: true, UsageUnits: 1}
	if _, err := db.CompleteProviderAttemptSuccess(ctx, claim, proof(t, claim), ticket.Version, fence, raw, phaseartifact.Validation{TicketType: domain.TicketFeature}, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	key := ProviderAttemptResultKey{AttemptID: claim.ID, Ref: ticket.Ref, Phase: domain.PhasePlanning, Attempt: claim.Attempt}
	if _, parsed, err := db.LoadHistoricalProviderAttemptResult(ctx, key); err != nil || parsed.Planner == nil {
		t.Fatalf("initial historical=%+v err=%v", parsed, err)
	}
	if reused, err := db.LatestReusableProviderAttempt(ctx, LatestReusableProviderAttemptRequest{Ref: ticket.Ref, Phase: domain.PhasePlanning, Role: "planner", ExpectedVersion: ticket.Version, Fence: fence}); err != nil || reused.Recovered || reused.Parsed.Planner == nil {
		t.Fatalf("exact reusable=%+v err=%v", reused, err)
	}
	advanced, err := db.InvalidateRunner(ctx, ticket.Ref, ticket.Version, fence)
	if err != nil {
		t.Fatal(err)
	}
	newLeader, err := db.AcquireLeader(ctx, domain.ChannelDev, "historical-restart")
	if err != nil {
		t.Fatal(err)
	}
	currentFence := domain.Fence{LeaderEpoch: newLeader, RunnerEpoch: advanced.RunnerEpoch}
	if reused, err := db.LatestReusableProviderAttempt(ctx, LatestReusableProviderAttemptRequest{Ref: ticket.Ref, Phase: domain.PhasePlanning, Role: "planner", ExpectedVersion: advanced.Version, Fence: currentFence}); err != nil || !reused.Recovered || reused.Parsed.Planner == nil {
		t.Fatalf("recovered reusable=%+v err=%v", reused, err)
	}
	if _, err := db.LatestReusableProviderAttempt(ctx, LatestReusableProviderAttemptRequest{Ref: ticket.Ref, Phase: domain.PhasePlanning, Role: "planner", ExpectedVersion: ticket.Version, Fence: fence}); !errors.Is(err, ErrStaleFence) {
		t.Fatalf("stale reuse request=%v", err)
	}
	ticket = Ticket{Ref: ticket.Ref, State: domain.StatePlanning, Version: advanced.Version, RunnerEpoch: advanced.RunnerEpoch}
	ticket = providerState(t, db, ctx, ticket, newLeader, domain.StateVerifying)
	if _, err := db.LatestReusableProviderAttempt(ctx, LatestReusableProviderAttemptRequest{Ref: ticket.Ref, Phase: domain.PhasePlanning, Role: "planner", ExpectedVersion: ticket.Version, Fence: domain.Fence{LeaderEpoch: newLeader, RunnerEpoch: ticket.RunnerEpoch}}); !errors.Is(err, ErrStaleFence) {
		t.Fatalf("ordinary transition reuse=%v", err)
	}
	if _, parsed, err := db.LoadHistoricalProviderAttemptResult(ctx, key); err != nil || parsed.Planner == nil {
		t.Fatalf("post restart historical=%+v err=%v", parsed, err)
	}
	wrong := key
	wrong.Attempt++
	if _, _, err := db.LoadHistoricalProviderAttemptResult(ctx, wrong); !errors.Is(err, ErrNotFound) {
		t.Fatalf("wrong historical key=%v", err)
	}
	wrong = key
	wrong.Ref.Ticket = "SF-other"
	if _, _, err := db.LoadHistoricalProviderAttemptResult(ctx, wrong); !errors.Is(err, ErrNotFound) {
		t.Fatalf("wrong ref=%v", err)
	}
	if _, _, err := db.LoadHistoricalProviderAttemptResult(ctx, ProviderAttemptResultKey{AttemptID: claim.ID, Ref: ticket.Ref, Phase: domain.PhaseBuild, Attempt: claim.Attempt}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("wrong phase=%v", err)
	}
	if newLeader == leader {
		t.Fatal("leader did not advance")
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
	if err := db.FinishProviderAttempt(ctx, claim, proof(t, claim), ticket.Version, fence, "failed", "failed", ticket.MaxCostMicroUSD+1, time.Now().UTC()); !errors.Is(err, ErrBudgetExhausted) {
		t.Fatalf("overspend finish=%v", err)
	}
	if err := db.FinishProviderAttempt(ctx, claim, proof(t, claim), ticket.Version, fence, "failed", "failed", 1, ticket.CreatedAt.Add(ticket.MaxDuration).Add(time.Nanosecond)); !errors.Is(err, ErrBudgetExhausted) {
		t.Fatalf("late finish=%v", err)
	}
	attempts, err := db.ProviderAttempts(ctx, ticket.Ref)
	if err != nil || len(attempts) != 1 || attempts[0].State != "active" {
		t.Fatalf("overspend claim=%+v err=%v", attempts, err)
	}
}

func TestFailProviderAttemptBudgetMarksLaunchDrained(t *testing.T) {
	db, ctx := openTestStore(t)
	digest := setupProviderProject(t, db, ctx)
	leader, _ := db.AcquireLeader(ctx, domain.ChannelDev, "budget-launch-state")
	ticket := setupProviderTicket(t, db, ctx, "SF-budget-launch-state", leader)
	ticket = providerState(t, db, ctx, ticket, leader, domain.StateBuilding)
	builder, _ := setupProviderPair(t, db, ctx)
	fence := domain.Fence{LeaderEpoch: leader, RunnerEpoch: ticket.RunnerEpoch}
	claim, err := db.BeginProviderAttempt(ctx, supervised(t, ProviderAttemptRequest{
		Ref: ticket.Ref, ExpectedVersion: ticket.Version, Fence: fence, Phase: domain.PhaseBuild,
		Role: "builder", Binding: runtime(builder), ConfigDigest: digest, Capacity: 1, At: time.Now().UTC(),
	}))
	if err != nil {
		t.Fatal(err)
	}
	if err := db.FailProviderAttemptBudget(ctx, claim, proof(t, claim), ticket.Version, fence, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	attempts, err := db.ProviderAttempts(ctx, ticket.Ref)
	if err != nil || len(attempts) != 1 || attempts[0].State != "failed" || attempts[0].Outcome != "budget_exhausted" {
		t.Fatalf("budget attempt=%+v err=%v", attempts, err)
	}
	var launchState string
	if err := db.db.QueryRowContext(ctx, `SELECT launch_state FROM provider_attempts WHERE id=?`, claim.ID).Scan(&launchState); err != nil || launchState != "drained" {
		t.Fatalf("budget launch state=%q err=%v", launchState, err)
	}
	leases, err := db.Leases(ctx, domain.ChannelDev)
	if err != nil || len(leases) != 0 {
		t.Fatalf("budget provider lease=%+v err=%v", leases, err)
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
	revision, err := db.RecordVerification(ctx, VerificationArtifact{Ref: ticket.Ref, ExpectedVersion: ticket.Version, Fence: fence, Intent: intent, Proof: proof, OwnedFiles: []string{"verify.txt"}, CheckpointID: strings.Repeat("d", 40)})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := domain.CandidateSnapshot{Generation: 1, BaseSHA: strings.Repeat("a", 40), HeadSHA: strings.Repeat("b", 40), TreeSHA: strings.Repeat("c", 40), SourceDigest: sha256Digest([]byte("source")), VerificationIntentDigest: revision.IntentDigest, ProofDigest: revision.ProofDigest, CommandPolicyDigest: sha256Digest([]byte("policy")), BuilderEvidenceDigest: sha256Digest([]byte("builder"))}
	if _, err := db.db.ExecContext(ctx, `INSERT INTO candidate_snapshots(channel,project_id,ticket_id,generation,ticket_version,leader_epoch,runner_epoch,base_sha,head_sha,tree_sha,source_digest,verification_intent_digest,proof_digest,command_policy_digest,builder_evidence_digest,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, ticket.Ref.Channel, ticket.Ref.Project, ticket.Ref.Ticket, snapshot.Generation, ticket.Version, fence.LeaderEpoch, fence.RunnerEpoch, snapshot.BaseSHA, snapshot.HeadSHA, snapshot.TreeSHA, snapshot.SourceDigest, snapshot.VerificationIntentDigest, snapshot.ProofDigest, snapshot.CommandPolicyDigest, snapshot.BuilderEvidenceDigest, now()); err != nil {
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
