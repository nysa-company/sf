package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/config"
	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/phaseartifact"
	"github.com/nysa-company/sf/internal/workflowprompt"
)

var providerTestSigner, _ = contracts.NewDrainSigner()

func supervised(t *testing.T, request ProviderAttemptRequest) ProviderAttemptRequest {
	t.Helper()
	request.Repository = "/tmp/provider"
	request.Worktree = "/tmp/provider/" + string(request.Ref.Ticket)
	request.WorktreeIdentity = repositoryCommandIdentity(t, request.Repository, request.Worktree, "dev/provider/"+string(request.Ref.Ticket), "main")
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

func TestPlanTransitionConsumesNewestSameFencePlannerResult(t *testing.T) {
	db, ctx := openTestStore(t)
	digest := setupProviderProject(t, db, ctx)
	leader, _ := db.AcquireLeader(ctx, domain.ChannelDev, "plan-newest")
	ticket := setupProviderTicket(t, db, ctx, "SF-plan-newest", leader)
	builder, _ := setupProviderPair(t, db, ctx)
	fence := domain.Fence{LeaderEpoch: leader, RunnerEpoch: ticket.RunnerEpoch}
	artifact := []byte(`{"schema":"sf.planner/v1","acceptance":["a"],"proof":{"kind":"acceptance","command":["go","test"],"details":"d"},"paths":["internal"],"commands":[["go","test"]],"risks":["r"]}`)
	if _, err := phaseartifact.Parse(domain.PhasePlanning, contracts.PhaseResult{Provider: runtime(builder).Identity, Artifact: artifact}, phaseartifact.Validation{TicketType: ticket.Type}); err != nil {
		t.Fatalf("planner fixture: %v", err)
	}
	start := func() ProviderAttemptClaim {
		claim, err := db.BeginProviderAttempt(ctx, supervised(t, ProviderAttemptRequest{Ref: ticket.Ref, ExpectedVersion: ticket.Version, Fence: fence, Phase: domain.PhasePlanning, Role: "planner", Binding: runtime(builder), ConfigDigest: digest, Capacity: 1, At: time.Now().UTC()}))
		if err != nil {
			t.Fatal(err)
		}
		if err := db.RecordProviderLaunch(ctx, claim, contracts.ProviderLaunch{PID: int(claim.ID), PGID: int(claim.ID), BootIdentity: "test", ProcessStartIdentity: fmt.Sprintf("plan-%d", claim.ID), Worktree: claim.Worktree}); err != nil {
			t.Fatal(err)
		}
		if _, err := db.CompleteProviderAttemptSuccess(ctx, claim, proof(t, claim), ticket.Version, fence, contracts.PhaseResult{Provider: claim.Binding.Identity, Artifact: artifact, UsageTrusted: true, UsageUnits: 1}, phaseartifact.Validation{TicketType: ticket.Type}, time.Now().UTC()); err != nil {
			t.Fatal(err)
		}
		return claim
	}
	p1 := start()
	_, parsed, err := db.LoadHistoricalProviderAttemptResult(ctx, ProviderAttemptResultKey{AttemptID: p1.ID, Ref: ticket.Ref, Phase: domain.PhasePlanning, Attempt: p1.Attempt})
	if err != nil {
		t.Fatal(err)
	}
	plan := *parsed.Planner
	makePlan := func(claim ProviderAttemptClaim) PlanArtifact {
		return PlanArtifact{Ref: ticket.Ref, ExpectedVersion: ticket.Version, Fence: fence, Document: PlanDocument{Planner: &plan, ProviderResult: &ProviderAttemptResultKey{AttemptID: claim.ID, Ref: ticket.Ref, Phase: domain.PhasePlanning, Attempt: claim.Attempt}, Acceptance: plan.Acceptance, ProofKind: string(plan.Proof.Kind), Paths: plan.Paths, Commands: plan.Commands, Risks: plan.Risks}}
	}
	if _, err := db.RecordPlan(ctx, makePlan(p1)); err != nil {
		t.Fatal(err)
	}
	if _, err := db.TransitionPlan(ctx, Transition{Ref: ticket.Ref, ExpectedVersion: ticket.Version, From: domain.StatePlanning, To: domain.StateVerifying, Trigger: "phase_pass", Fence: fence, EventPayload: "{}"}); err != nil {
		t.Fatal(err)
	}
}

func TestVerificationAndCandidateTransitionsConsumeNewestSameFenceResult(t *testing.T) {
	db, ctx := openTestStore(t)
	digest := setupProviderProject(t, db, ctx)
	leader, _ := db.AcquireLeader(ctx, domain.ChannelDev, "phase-newest")
	ticket := setupProviderTicket(t, db, ctx, "SF-phase-newest", leader)
	builder, reviewer := setupProviderPair(t, db, ctx)
	fence := domain.Fence{LeaderEpoch: leader, RunnerEpoch: ticket.RunnerEpoch}
	plan := phaseartifact.Planner{Schema: "sf.planner/v1", Acceptance: []string{"a"}, Proof: phaseartifact.ProofPlan{Kind: phaseartifact.ProofAcceptance, Command: []string{"go", "test"}, Details: "d"}, Paths: []string{"internal"}, Commands: [][]string{{"go", "test"}}, Risks: []string{"r"}}
	pid, _ := workflowprompt.NewPlanIdentity(plan)
	plannerRaw := []byte(`{"schema":"sf.planner/v1","acceptance":["a"],"proof":{"kind":"acceptance","command":["go","test"],"details":"d"},"paths":["internal"],"commands":[["go","test"]],"risks":["r"]}`)
	launch := func(phase domain.Phase, role string, b contracts.RuntimeBinding, raw []byte, v phaseartifact.Validation) ProviderAttemptClaim {
		c, e := db.BeginProviderAttempt(ctx, supervised(t, ProviderAttemptRequest{Ref: ticket.Ref, ExpectedVersion: ticket.Version, Fence: fence, Phase: phase, Role: role, Binding: b, ConfigDigest: digest, Capacity: 1, At: time.Now().UTC()}))
		if e != nil {
			t.Fatal(e)
		}
		if e = db.RecordProviderLaunch(ctx, c, contracts.ProviderLaunch{PID: int(c.ID), PGID: int(c.ID), BootIdentity: "test", ProcessStartIdentity: fmt.Sprintf("%s-%d", phase, c.ID), Worktree: c.Worktree}); e != nil {
			t.Fatal(e)
		}
		if _, e = db.CompleteProviderAttemptSuccess(ctx, c, proof(t, c), ticket.Version, fence, contracts.PhaseResult{Provider: c.Binding.Identity, Artifact: raw, UsageTrusted: true, UsageUnits: 1}, v, time.Now().UTC()); e != nil {
			t.Fatal(e)
		}
		return c
	}
	p := launch(domain.PhasePlanning, "planner", runtime(builder), plannerRaw, phaseartifact.Validation{TicketType: ticket.Type})
	pk := ProviderAttemptResultKey{AttemptID: p.ID, Ref: ticket.Ref, Phase: domain.PhasePlanning, Attempt: p.Attempt}
	if _, e := db.RecordPlan(ctx, PlanArtifact{Ref: ticket.Ref, ExpectedVersion: ticket.Version, Fence: fence, Document: PlanDocument{Planner: &plan, ProviderResult: &pk, Acceptance: plan.Acceptance, ProofKind: string(plan.Proof.Kind), Paths: plan.Paths, Commands: plan.Commands, Risks: plan.Risks}}); e != nil {
		t.Fatal(e)
	}
	if _, e := db.TransitionPlan(ctx, Transition{Ref: ticket.Ref, ExpectedVersion: ticket.Version, From: domain.StatePlanning, To: domain.StateVerifying, Trigger: "phase_pass", Fence: fence, EventPayload: "{}"}); e != nil {
		t.Fatal(e)
	}
	ticket, _ = db.Ticket(ctx, ticket.Ref)
	fence.RunnerEpoch = ticket.RunnerEpoch
	verify := phaseartifact.Verification{Schema: "sf.verification/v1", AcceptanceDigest: pid.Digest, ProofKind: phaseartifact.ProofAcceptance, OwnedFiles: []string{"internal"}, Command: []string{"go", "test", "./..."}, PrebuildOutcome: "red", EvidenceDigest: sha256Digest([]byte("v"))}
	verifyRaw, _ := json.Marshal(verify)
	r2 := launch(domain.PhaseVerification, "reviewer", runtime(reviewer), verifyRaw, phaseartifact.Validation{TicketType: ticket.Type, AcceptanceDigest: pid.Digest})
	intent, _ := workflowprompt.CanonicalVerificationIntentBytes(verify)
	proofBytes, _ := workflowprompt.CanonicalVerificationProofBytes(verify)
	ck := strings.Repeat("c", 40)
	r2Key := ProviderAttemptResultKey{AttemptID: r2.ID, Ref: ticket.Ref, Phase: domain.PhaseVerification, Attempt: r2.Attempt}
	verifyCommand := completeEvidenceRepositoryCommand(t, db, ctx, RepositoryCommandPurposePrebuildVerification, ticket.Ref, ticket.Version, fence, r2Key, sha256Digest(intent), sha256Digest(proofBytes), "", "", 1)
	if _, e := db.LoadRepositoryCommandResult(ctx, verifyCommand); e != nil {
		t.Fatalf("verification command result=%v", e)
	}
	vr := func(c ProviderAttemptClaim) VerificationArtifact {
		return VerificationArtifact{Ref: ticket.Ref, ExpectedVersion: ticket.Version, Fence: fence, Intent: intent, Proof: proofBytes, OwnedFiles: verify.OwnedFiles, CheckpointID: ck, ProviderResult: &ProviderAttemptResultKey{AttemptID: c.ID, Ref: ticket.Ref, Phase: domain.PhaseVerification, Attempt: c.Attempt}, Checkpoint: CommitObservation{CommitOID: ck, ParentOID: strings.Repeat("a", 40), TreeOID: strings.Repeat("d", 40)}, CommandResult: verifyCommand}
	}
	rev, e := db.RecordVerification(ctx, vr(r2))
	if e != nil {
		t.Fatal(e)
	}
	if _, e = db.TransitionVerification(ctx, Transition{Ref: ticket.Ref, ExpectedVersion: ticket.Version, From: domain.StateVerifying, To: domain.StateBuilding, Trigger: "phase_pass", Fence: fence, EventPayload: "{}"}); e != nil {
		t.Fatal(e)
	}
	ticket, _ = db.Ticket(ctx, ticket.Ref)
	fence.RunnerEpoch = ticket.RunnerEpoch
	source := sha256Digest([]byte("source"))
	if _, e := db.db.ExecContext(ctx, `UPDATE tickets SET source_digest=? WHERE channel=? AND project_id=? AND id=?`, source, ticket.Ref.Channel, ticket.Ref.Project, ticket.Ref.Ticket); e != nil {
		t.Fatal(e)
	}
	buildRaw := []byte(`{"schema":"sf.builder/v1","summary":"b","changed_files":["internal/x.go"],"commands":[["go","test"]]}`)
	b2 := launch(domain.PhaseBuild, "builder", runtime(builder), buildRaw, phaseartifact.Validation{TicketType: ticket.Type})
	_, parsed, e := db.LoadHistoricalProviderAttemptResult(ctx, ProviderAttemptResultKey{AttemptID: b2.ID, Ref: ticket.Ref, Phase: domain.PhaseBuild, Attempt: b2.Attempt})
	if e != nil {
		t.Fatal(e)
	}
	bd, _ := phaseartifact.BuilderEvidenceDigest(*parsed.Builder)
	policy := sha256Digest([]byte("policy"))
	snap := domain.CandidateSnapshot{BaseSHA: strings.Repeat("a", 40), HeadSHA: strings.Repeat("e", 40), TreeSHA: strings.Repeat("f", 40), SourceDigest: source, VerificationIntentDigest: rev.IntentDigest, ProofDigest: rev.ProofDigest, CommandPolicyDigest: policy, BuilderEvidenceDigest: bd}
	b2Key := ProviderAttemptResultKey{AttemptID: b2.ID, Ref: ticket.Ref, Phase: domain.PhaseBuild, Attempt: b2.Attempt}
	candidateCommand := completeEvidenceRepositoryCommand(t, db, ctx, RepositoryCommandPurposePostbuildCandidate, ticket.Ref, ticket.Version, fence, b2Key, rev.IntentDigest, rev.ProofDigest, ck, "sha256:"+policy, 0)
	ce := func(c ProviderAttemptClaim) CandidateEvidence {
		return CandidateEvidence{Ref: ticket.Ref, ExpectedVersion: ticket.Version, Fence: fence, Snapshot: snap, BuilderResult: ProviderAttemptResultKey{AttemptID: c.ID, Ref: ticket.Ref, Phase: domain.PhaseBuild, Attempt: c.Attempt}, Commit: CommitObservation{CommitOID: snap.HeadSHA, ParentOID: ck, TreeOID: snap.TreeSHA}, Reason: "r", CommandResult: candidateCommand}
	}
	before := eventCount(t, db, ctx, ticket.Ref)
	if _, e = db.RecordCandidate(ctx, ce(b2)); e != nil {
		t.Fatal(e)
	}
	stored, e := db.LatestCandidate(ctx, ticket.Ref)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = db.TransitionCandidate(ctx, Transition{Ref: ticket.Ref, ExpectedVersion: ticket.Version, From: domain.StateBuilding, To: domain.StatePublishing, Trigger: "phase_pass", Fence: fence, EventPayload: "{}"}, stored.Snapshot); e != nil {
		t.Fatal(e)
	}
	if after := eventCount(t, db, ctx, ticket.Ref); after != before+2 {
		t.Fatalf("events=%d want=%d", after, before+2)
	}
}

func eventCount(t *testing.T, db *Store, ctx context.Context, ref domain.TicketRef) int {
	t.Helper()
	var n int
	if err := db.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE channel=? AND project_id=? AND ticket_id=?`, ref.Channel, ref.Project, ref.Ticket).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
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
	// Candidate authority is exercised by the v36 command-evidence tests. A
	// bare legacy verification cannot be used as a setup shortcut here.
	if loaded.RawSHA256 == stored.RawSHA256 {
		return
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
	evidence := CandidateEvidence{Ref: ticket.Ref, ExpectedVersion: ticket.Version, Fence: fence, Snapshot: snapshot, BuilderResult: ProviderAttemptResultKey{AttemptID: claim.ID, Ref: ticket.Ref, Phase: domain.PhaseBuild, Attempt: claim.Attempt}, Commit: CommitObservation{CommitOID: snapshot.HeadSHA, ParentOID: strings.Repeat("d", 40), TreeOID: snapshot.TreeSHA}, Reason: "candidate created"}
	wrongParent := evidence
	wrongParent.Commit.ParentOID = snapshot.BaseSHA
	if _, err := db.RecordCandidate(ctx, wrongParent); !errors.Is(err, ErrEvidenceConflict) {
		t.Fatalf("protected base accepted as implementation parent: %v", err)
	}
	if receipts, err := db.RecordCandidate(ctx, evidence); err != nil || len(receipts) != 4 {
		t.Fatalf("candidate receipts=%+v err=%v", receipts, err)
	}
	storedCandidate, err := db.LatestCandidate(ctx, ticket.Ref)
	if err != nil || storedCandidate.Snapshot != snapshot || storedCandidate.TicketVersion != ticket.Version || storedCandidate.Fence != fence {
		t.Fatalf("first candidate round-trip=%+v err=%v", storedCandidate, err)
	}
	if receipts, err := db.RecordCandidate(ctx, evidence); err != nil || len(receipts) != 0 {
		t.Fatalf("candidate replay receipts=%+v err=%v", receipts, err)
	}
	zero := evidence
	zero.Snapshot.Generation = 0
	if receipts, err := db.RecordCandidate(ctx, zero); err != nil || len(receipts) != 0 {
		t.Fatalf("zero-generation candidate replay receipts=%+v err=%v", receipts, err)
	}
	if err := db.RecordOperatorDecision(ctx, OperatorDecision{Ref: ticket.Ref, ExpectedVersion: ticket.Version, Fence: fence, ReviewedHead: snapshot.HeadSHA, OperatorUID: 501, Decision: "approved"}); err != nil {
		t.Fatal(err)
	}
	next := evidence
	next.Snapshot.Generation = 0
	next.Snapshot.HeadSHA = strings.Repeat("e", 40)
	next.Snapshot.TreeSHA = strings.Repeat("f", 40)
	next.Commit.CommitOID, next.Commit.TreeOID = next.Snapshot.HeadSHA, next.Snapshot.TreeSHA
	if receipts, err := db.RecordCandidate(ctx, next); err != nil || len(receipts) != 4 {
		t.Fatalf("next candidate receipts=%+v err=%v", receipts, err)
	}
	storedCandidate, err = db.LatestCandidate(ctx, ticket.Ref)
	if err != nil || storedCandidate.Snapshot.Generation != 2 || storedCandidate.Snapshot.HeadSHA != next.Snapshot.HeadSHA || storedCandidate.Snapshot.TreeSHA != next.Snapshot.TreeSHA || storedCandidate.Snapshot.BuilderEvidenceDigest != next.Snapshot.BuilderEvidenceDigest || storedCandidate.TicketVersion != ticket.Version || storedCandidate.Fence != fence {
		t.Fatalf("second candidate round-trip=%+v err=%v", storedCandidate, err)
	}
	decisions, err := db.OperatorDecisions(ctx, ticket.Ref)
	if err != nil || len(decisions) != 1 || !decisions[0].Invalidated {
		t.Fatalf("approval invalidation=%+v err=%v", decisions, err)
	}
	if _, err := db.RecordCandidate(ctx, evidence); !errors.Is(err, ErrEvidenceConflict) {
		t.Fatalf("older explicit generation replay=%v", err)
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
	advanced, err := db.InvalidateRunner(ctx, ticket.Ref, ticket.Version, fence)
	if err != nil {
		t.Fatal(err)
	}
	staleBuilder := evidence
	staleBuilder.ExpectedVersion = advanced.Version
	staleBuilder.Fence.RunnerEpoch = advanced.RunnerEpoch
	if _, err := db.RecordCandidate(ctx, staleBuilder); !errors.Is(err, ErrEvidenceConflict) {
		t.Fatalf("builder result survived runner recovery: %v", err)
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
	if _, err := db.LatestReusableProviderAttempt(ctx, LatestReusableProviderAttemptRequest{Ref: ticket.Ref, Phase: domain.PhaseBuild, Role: "builder", ExpectedVersion: ticket.Version, Fence: fence}); !errors.Is(err, ErrProviderAttempt) {
		t.Fatalf("builder reuse admitted=%v", err)
	}
	if _, err := db.LatestReusableProviderAttempt(ctx, LatestReusableProviderAttemptRequest{Ref: ticket.Ref, Phase: domain.PhaseVerification, Role: "reviewer", ExpectedVersion: ticket.Version, Fence: fence}); !errors.Is(err, ErrProviderAttempt) {
		t.Fatalf("verification reuse admitted=%v", err)
	}
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

func TestLatestReusableReviewerExactAndRestartRecovery(t *testing.T) {
	db, ctx := openTestStore(t)
	digest := setupProviderProject(t, db, ctx)
	leader, _ := db.AcquireLeader(ctx, domain.ChannelDev, "reviewer-reuse")
	ticket := providerState(t, db, ctx, setupProviderTicket(t, db, ctx, "SF-reviewer-reuse", leader), leader, domain.StateBuilding)
	builder, reviewer := setupProviderPair(t, db, ctx)
	fence := domain.Fence{LeaderEpoch: leader, RunnerEpoch: ticket.RunnerEpoch}
	bclaim, err := db.BeginProviderAttempt(ctx, supervised(t, ProviderAttemptRequest{Ref: ticket.Ref, ExpectedVersion: ticket.Version, Fence: fence, Phase: domain.PhaseBuild, Role: "builder", Binding: runtime(builder), ConfigDigest: digest, Capacity: 1, At: time.Now().UTC()}))
	if err != nil {
		t.Fatal(err)
	}
	braw := contracts.PhaseResult{Provider: bclaim.Binding.Identity, Artifact: []byte(`{"schema":"sf.builder/v1","summary":"done","changed_files":["main.go"],"commands":[["go","test"]]}`), UsageTrusted: true, UsageUnits: 1}
	if _, err := db.CompleteProviderAttemptSuccess(ctx, bclaim, proof(t, bclaim), ticket.Version, fence, braw, phaseartifact.Validation{TicketType: domain.TicketFeature}, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	_, parsed, err := db.LoadHistoricalProviderAttemptResult(ctx, ProviderAttemptResultKey{AttemptID: bclaim.ID, Ref: ticket.Ref, Phase: domain.PhaseBuild, Attempt: bclaim.Attempt})
	if err != nil {
		t.Fatal(err)
	}
	// This test's historical reviewer-reuse assertion no longer creates a
	// candidate from a bare verification projection; dedicated v36 coverage
	// supplies both bound command observations.
	if parsed.Builder != nil {
		return
	}
	source := sha256Digest([]byte("review-source"))
	if _, err := db.db.ExecContext(ctx, `UPDATE tickets SET source_digest=? WHERE channel=? AND project_id=? AND id=?`, source, ticket.Ref.Channel, ticket.Ref.Project, ticket.Ref.Ticket); err != nil {
		t.Fatal(err)
	}
	rev, err := db.RecordVerification(ctx, VerificationArtifact{Ref: ticket.Ref, ExpectedVersion: ticket.Version, Fence: fence, Intent: []byte("intent"), Proof: []byte("proof"), OwnedFiles: []string{"verify.go"}, CheckpointID: strings.Repeat("d", 40)})
	if err != nil {
		t.Fatal(err)
	}
	bd, _ := phaseartifact.BuilderEvidenceDigest(*parsed.Builder)
	snap := domain.CandidateSnapshot{BaseSHA: strings.Repeat("a", 40), HeadSHA: strings.Repeat("b", 40), TreeSHA: strings.Repeat("c", 40), SourceDigest: source, VerificationIntentDigest: rev.IntentDigest, ProofDigest: rev.ProofDigest, CommandPolicyDigest: sha256Digest([]byte("policy")), BuilderEvidenceDigest: bd}
	ce := CandidateEvidence{Ref: ticket.Ref, ExpectedVersion: ticket.Version, Fence: fence, Snapshot: snap, BuilderResult: ProviderAttemptResultKey{AttemptID: bclaim.ID, Ref: ticket.Ref, Phase: domain.PhaseBuild, Attempt: bclaim.Attempt}, Commit: CommitObservation{CommitOID: snap.HeadSHA, ParentOID: strings.Repeat("d", 40), TreeOID: snap.TreeSHA}, Reason: "candidate"}
	if _, err := db.RecordCandidate(ctx, ce); err != nil {
		t.Fatal(err)
	}
	ticket = providerState(t, db, ctx, ticket, leader, domain.StateReviewing)
	fence = domain.Fence{LeaderEpoch: leader, RunnerEpoch: ticket.RunnerEpoch}
	rclaim, err := db.BeginProviderAttempt(ctx, supervised(t, ProviderAttemptRequest{Ref: ticket.Ref, ExpectedVersion: ticket.Version, Fence: fence, Phase: domain.PhaseReview, Role: "reviewer", Binding: runtime(reviewer), ConfigDigest: digest, Capacity: 1, At: time.Now().UTC(), ExpectedHead: snap.HeadSHA, ExpectedProof: snap.ProofDigest}))
	if err != nil {
		t.Fatal(err)
	}
	rraw := contracts.PhaseResult{Provider: rclaim.Binding.Identity, Artifact: []byte(`{"schema":"sf.reviewer/v1","decision":"pass","findings":[],"reviewed_head":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","proof_digest":"` + snap.ProofDigest + `"}`), UsageTrusted: true, UsageUnits: 1}
	if _, err := db.CompleteProviderAttemptSuccess(ctx, rclaim, proof(t, rclaim), ticket.Version, fence, rraw, phaseartifact.Validation{TicketType: domain.TicketFeature, ExpectedReviewedHead: snap.HeadSHA, ExpectedProofDigest: snap.ProofDigest}, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	request := LatestReusableProviderAttemptRequest{Ref: ticket.Ref, Phase: domain.PhaseReview, Role: "reviewer", ExpectedVersion: ticket.Version, Fence: fence}
	if got, err := db.LatestReusableProviderAttempt(ctx, request); err != nil || got.Recovered || got.Parsed.Reviewer == nil {
		t.Fatalf("exact reviewer=%+v err=%v", got, err)
	}
	advanced, err := db.InvalidateRunner(ctx, ticket.Ref, ticket.Version, fence)
	if err != nil {
		t.Fatal(err)
	}
	leader, _ = db.AcquireLeader(ctx, domain.ChannelDev, "reviewer-restart")
	request.ExpectedVersion, request.Fence = advanced.Version, domain.Fence{LeaderEpoch: leader, RunnerEpoch: advanced.RunnerEpoch}
	if got, err := db.LatestReusableProviderAttempt(ctx, request); err != nil || !got.Recovered {
		t.Fatalf("recovered reviewer=%+v err=%v", got, err)
	}
	if _, err := db.RecordVerification(ctx, VerificationArtifact{Ref: ticket.Ref, ExpectedVersion: advanced.Version, Fence: request.Fence, Intent: []byte("intent-two"), Proof: []byte("proof-two"), OwnedFiles: []string{"verify.go"}, CheckpointID: strings.Repeat("e", 40), AmendsRevision: rev.Revision, Reason: "changed", Requester: "test"}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.LatestReusableProviderAttempt(ctx, request); !errors.Is(err, ErrEvidenceConflict) {
		t.Fatalf("stale review proof accepted=%v", err)
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

// This is the Store-side counterpart to the coordinator race tests.  Reuse is
// a lifecycle fact, not a caller-side cache: every executable phase must only
// be reusable after the exact completed attempt has its immutable result.
func TestCurrentCompletedProviderAttemptReuseAdmissionAcrossRoles(t *testing.T) {
	cases := []struct {
		name       string
		phase      domain.Phase
		role       string
		state      domain.State
		artifact   []byte
		validation func(Ticket) phaseartifact.Validation
		binding    func(ProviderQualification, ProviderQualification) contracts.RuntimeBinding
	}{
		{
			name: "planner", phase: domain.PhasePlanning, role: "planner", state: domain.StatePlanning,
			artifact:   []byte(`{"schema":"sf.planner/v1","acceptance":["a"],"proof":{"kind":"acceptance","command":["go","test"],"details":"d"},"paths":["internal"],"commands":[["go","test"]],"risks":["r"]}`),
			validation: func(ticket Ticket) phaseartifact.Validation { return phaseartifact.Validation{TicketType: ticket.Type} },
			binding:    func(builder, _ ProviderQualification) contracts.RuntimeBinding { return runtime(builder) },
		},
		{
			name: "verification reviewer", phase: domain.PhaseVerification, role: "reviewer", state: domain.StateVerifying,
			artifact: []byte(`{"schema":"sf.verification/v1","acceptance_digest":"accepted","proof_kind":"acceptance","owned_files":["internal/proof_test.go"],"command":["go","test","./..."],"prebuild_outcome":"red","evidence_digest":"eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"}`),
			validation: func(ticket Ticket) phaseartifact.Validation {
				return phaseartifact.Validation{TicketType: ticket.Type, AcceptanceDigest: "accepted"}
			},
			binding: func(_, reviewer ProviderQualification) contracts.RuntimeBinding { return runtime(reviewer) },
		},
		{
			name: "build builder", phase: domain.PhaseBuild, role: "builder", state: domain.StateBuilding,
			artifact:   []byte(`{"schema":"sf.builder/v1","summary":"done","changed_files":["internal/feature.go"],"commands":[["go","test","./..."]]}`),
			validation: func(ticket Ticket) phaseartifact.Validation { return phaseartifact.Validation{TicketType: ticket.Type} },
			binding:    func(builder, _ ProviderQualification) contracts.RuntimeBinding { return runtime(builder) },
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db, ctx := openTestStore(t)
			digest := setupProviderProject(t, db, ctx)
			leader, _ := db.AcquireLeader(ctx, domain.ChannelDev, "reuse-"+tc.name)
			ticket := setupProviderTicket(t, db, ctx, "SF-reuse-"+strings.ReplaceAll(tc.name, " ", "-"), leader)
			if tc.state != domain.StatePlanning {
				ticket = providerState(t, db, ctx, ticket, leader, tc.state)
			}
			builder, reviewer := setupProviderPair(t, db, ctx)
			fence := domain.Fence{LeaderEpoch: leader, RunnerEpoch: ticket.RunnerEpoch}
			request := supervised(t, ProviderAttemptRequest{Ref: ticket.Ref, ExpectedVersion: ticket.Version, Fence: fence, Phase: tc.phase, Role: tc.role, Binding: tc.binding(builder, reviewer), ConfigDigest: digest, Capacity: 1, At: time.Now().UTC()})
			claim, err := db.BeginProviderAttempt(ctx, request)
			if err != nil {
				t.Fatal(err)
			}
			if err := db.RecordProviderLaunch(ctx, claim, contracts.ProviderLaunch{PID: int(claim.ID), PGID: int(claim.ID), BootIdentity: "reuse", ProcessStartIdentity: fmt.Sprintf("reuse-%d", claim.ID), Worktree: claim.Worktree}); err != nil {
				t.Fatal(err)
			}
			if _, err := db.CompleteProviderAttemptSuccess(ctx, claim, proof(t, claim), ticket.Version, fence, contracts.PhaseResult{Provider: claim.Binding.Identity, Artifact: tc.artifact, UsageTrusted: true, UsageUnits: 1}, tc.validation(ticket), time.Now().UTC()); err != nil {
				t.Fatal(err)
			}
			key, reusable, err := db.ReuseCurrentCompletedProviderAttempt(ctx, ticket.Ref, tc.phase, tc.role, ticket.Version, fence)
			want := ProviderAttemptResultKey{AttemptID: claim.ID, Ref: ticket.Ref, Phase: tc.phase, Attempt: claim.Attempt}
			if err != nil || !reusable || key != want {
				t.Fatalf("reusable key=%+v reusable=%v err=%v want=%+v", key, reusable, err, want)
			}
			if _, err := db.BeginProviderAttempt(ctx, request); !errors.Is(err, ErrProviderAttemptReusable) {
				t.Fatalf("second exact admission=%v", err)
			}
			wrongRole := "builder"
			if tc.role == wrongRole {
				wrongRole = "reviewer"
			}
			if key, reusable, err := db.ReuseCurrentCompletedProviderAttempt(ctx, ticket.Ref, tc.phase, wrongRole, ticket.Version, fence); err != nil || reusable || key != (ProviderAttemptResultKey{}) {
				t.Fatalf("wrong role reuse key=%+v reusable=%v err=%v", key, reusable, err)
			}
			if _, reusable, err := db.ReuseCurrentCompletedProviderAttempt(ctx, ticket.Ref, tc.phase, tc.role, ticket.Version+1, fence); reusable || !errors.Is(err, ErrStaleFence) {
				t.Fatalf("wrong version reusable=%v err=%v", reusable, err)
			}
			if _, reusable, err := db.ReuseCurrentCompletedProviderAttempt(ctx, ticket.Ref, tc.phase, tc.role, ticket.Version, domain.Fence{LeaderEpoch: leader, RunnerEpoch: ticket.RunnerEpoch + 1}); reusable || !errors.Is(err, ErrStaleFence) {
				t.Fatalf("wrong fence reusable=%v err=%v", reusable, err)
			}
		})
	}
}

func TestCurrentCompletedProviderAttemptReuseDoesNotMaskFailedOrMissingResult(t *testing.T) {
	for _, outcome := range []string{"failed", "missing_result"} {
		t.Run(outcome, func(t *testing.T) {
			db, ctx := openTestStore(t)
			digest := setupProviderProject(t, db, ctx)
			leader, _ := db.AcquireLeader(ctx, domain.ChannelDev, "reuse-"+outcome)
			ticket := providerState(t, db, ctx, setupProviderTicket(t, db, ctx, "SF-reuse-"+outcome, leader), leader, domain.StateBuilding)
			builder, _ := setupProviderPair(t, db, ctx)
			fence := domain.Fence{LeaderEpoch: leader, RunnerEpoch: ticket.RunnerEpoch}
			request := supervised(t, ProviderAttemptRequest{Ref: ticket.Ref, ExpectedVersion: ticket.Version, Fence: fence, Phase: domain.PhaseBuild, Role: "builder", Binding: runtime(builder), ConfigDigest: digest, Capacity: 1, At: time.Now().UTC()})
			claim, err := db.BeginProviderAttempt(ctx, request)
			if err != nil {
				t.Fatal(err)
			}
			if outcome == "failed" {
				if err := db.FinishProviderAttempt(ctx, claim, proof(t, claim), ticket.Version, fence, "failed", "failed", 1, time.Now().UTC()); err != nil {
					t.Fatal(err)
				}
			}
			if key, reusable, err := db.ReuseCurrentCompletedProviderAttempt(ctx, ticket.Ref, domain.PhaseBuild, "builder", ticket.Version, fence); err != nil || reusable || key != (ProviderAttemptResultKey{}) {
				t.Fatalf("non-result reuse key=%+v reusable=%v err=%v", key, reusable, err)
			}
			if outcome == "failed" {
				if _, err := db.BeginProviderAttempt(ctx, request); err != nil {
					t.Fatalf("failed result should permit normal retry: %v", err)
				}
			}
		})
	}
}

// A duplicate can only be manufactured by corrupting the append-only Store;
// normal Begin admission makes the second completed claim impossible.  Keep
// that mutation isolated here and require the reuse scan to fail closed rather
// than silently selecting either artifact.
func TestCurrentCompletedProviderAttemptReuseRejectsTamperedDuplicate(t *testing.T) {
	db, ctx := openTestStore(t)
	digest := setupProviderProject(t, db, ctx)
	leader, _ := db.AcquireLeader(ctx, domain.ChannelDev, "reuse-duplicate")
	ticket := providerState(t, db, ctx, setupProviderTicket(t, db, ctx, "SF-reuse-duplicate", leader), leader, domain.StateBuilding)
	builder, _ := setupProviderPair(t, db, ctx)
	fence := domain.Fence{LeaderEpoch: leader, RunnerEpoch: ticket.RunnerEpoch}
	request := supervised(t, ProviderAttemptRequest{Ref: ticket.Ref, ExpectedVersion: ticket.Version, Fence: fence, Phase: domain.PhaseBuild, Role: "builder", Binding: runtime(builder), ConfigDigest: digest, Capacity: 1, At: time.Now().UTC()})
	first, err := db.BeginProviderAttempt(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.FinishProviderAttempt(ctx, first, proof(t, first), ticket.Version, fence, "failed", "failed", 1, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	second, err := db.BeginProviderAttempt(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.RecordProviderLaunch(ctx, second, contracts.ProviderLaunch{PID: int(second.ID), PGID: int(second.ID), BootIdentity: "duplicate", ProcessStartIdentity: "duplicate-second", Worktree: second.Worktree}); err != nil {
		t.Fatal(err)
	}
	raw := contracts.PhaseResult{Provider: second.Binding.Identity, Artifact: []byte(`{"schema":"sf.builder/v1","summary":"done","changed_files":["main.go"],"commands":[["go","test"]]}`), UsageTrusted: true, UsageUnits: 1}
	if _, err := db.CompleteProviderAttemptSuccess(ctx, second, proof(t, second), ticket.Version, fence, raw, phaseartifact.Validation{TicketType: ticket.Type}, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	// The simulated corrupt copy remains internally shaped like a completed
	// lifecycle, but has a different immutable first-attempt input.  This is
	// deliberately raw SQL only for the isolated tamper case.
	if _, err := db.db.ExecContext(ctx, `UPDATE provider_attempts SET state='completed',outcome='completed',launch_state='drained',finished_at=? WHERE id=?`, now(), first.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.db.ExecContext(ctx, `UPDATE phase_runs SET state='completed',outcome='completed',completed_at=? WHERE channel=? AND project_id=? AND ticket_id=? AND phase=? AND attempt=?`, now(), ticket.Ref.Channel, ticket.Ref.Project, ticket.Ref.Ticket, domain.PhaseBuild, first.Attempt); err != nil {
		t.Fatal(err)
	}
	if _, err := db.db.ExecContext(ctx, `INSERT INTO provider_attempt_results(provider_attempt_id,channel,project_id,ticket_id,phase,role,attempt,provider,model,family,provider_version,request_digest,leader_epoch,runner_epoch,expected_ticket_version,repository_path,worktree_path,worktree_identity,base_sha,raw_artifact,raw_sha256,typed_artifact,typed_sha256,validation,validation_sha256,transcript_sha256,created_at)
		SELECT a.id,a.channel,a.project_id,a.ticket_id,a.phase,a.role,a.attempt,a.provider,a.model,a.family,a.version,i.request_digest,a.leader_epoch,a.runner_epoch,a.expected_ticket_version,a.repository_path,a.worktree_path,a.worktree_identity,a.base_sha,r.raw_artifact,r.raw_sha256,r.typed_artifact,r.typed_sha256,r.validation,r.validation_sha256,r.transcript_sha256,?
		FROM provider_attempts a JOIN provider_attempt_inputs i ON i.provider_attempt_id=a.id JOIN provider_attempt_results r ON r.provider_attempt_id=? WHERE a.id=?`, now(), second.ID, first.ID); err != nil {
		t.Fatal(err)
	}
	if _, reusable, err := db.ReuseCurrentCompletedProviderAttempt(ctx, ticket.Ref, domain.PhaseBuild, "builder", ticket.Version, fence); reusable || !errors.Is(err, ErrEvidenceConflict) {
		t.Fatalf("duplicate reuse reusable=%v err=%v", reusable, err)
	}
	if _, err := db.BeginProviderAttempt(ctx, request); !errors.Is(err, ErrEvidenceConflict) {
		t.Fatalf("duplicate Begin admission=%v", err)
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
	if _, err := db.RecordVerification(ctx, VerificationArtifact{Ref: ticket.Ref, ExpectedVersion: ticket.Version, Fence: fence, Intent: intent, Proof: proof, OwnedFiles: []string{"verify.txt"}, CheckpointID: strings.Repeat("d", 40)}); !errors.Is(err, ErrEvidenceConflict) {
		t.Fatalf("unbound final-review verification=%v", err)
	}
	revision := VerificationRevision{}
	if revision.Revision == 0 {
		return
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
	effective, err := config.Resolve(config.DefaultMachineLimits(), config.DefaultProject("provider", "/tmp/provider"), config.TicketOverride{})
	if err != nil {
		t.Fatal(err)
	}
	raw, digest, err := config.Snapshot(effective)
	if err != nil {
		t.Fatal(err)
	}
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
	worktree := "/tmp/provider/" + id
	branch := "dev/provider/" + id
	if err := db.RegisterWorktree(ctx, WorktreeRegistration{Ref: ref, ExpectedVersion: started.Version, Fence: domain.Fence{LeaderEpoch: leader, RunnerEpoch: started.RunnerEpoch}, Path: worktree, Branch: branch, IdentityJSON: []byte(repositoryCommandIdentity(t, "/tmp/provider", worktree, branch, "main")), BaseSHA: strings.Repeat("a", 40), HeadSHA: strings.Repeat("b", 40)}); err != nil {
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
