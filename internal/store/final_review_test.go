package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/phaseartifact"
	"github.com/nysa-company/sf/internal/workflowprompt"
)

type finalReviewFixture struct {
	db        *Store
	ctx       context.Context
	ticket    Ticket
	fence     domain.Fence
	candidate StoredCandidate
}

func TestStoreRejectsSpikeAutonomousTicket(t *testing.T) {
	db, ctx := openTestStore(t)
	setupProviderProject(t, db, ctx)
	err := db.CreateTicket(ctx, Ticket{Ref: domain.TicketRef{Channel: domain.ChannelDev, Project: "provider", Ticket: "SF-spike-autonomous"}, SourceDigest: sha256Digest([]byte("spike autonomous")), Type: domain.TicketSpike, MergeMode: domain.MergeAutonomous})
	if err == nil {
		t.Fatal("spike autonomous ticket was accepted")
	}
}

// finalReviewLifecycleFixture starts from the production Store-backed
// planner/verification/builder/candidate/publication sequence, then appends a
// green CI observation and its immutable reviewing transition. CI polling is
// not composed yet; the fixture deliberately writes the immutable boundary
// that the future poller is required to produce.
func finalReviewLifecycleFixture(t *testing.T) finalReviewFixture {
	return finalReviewLifecycleFixtureFor(t, domain.TicketFeature, domain.MergeGuarded)
}

func finalReviewLifecycleFixtureFor(t *testing.T, ticketType domain.TicketType, mergeMode domain.MergeMode) finalReviewFixture {
	return finalReviewLifecycleFixtureForPending(t, ticketType, mergeMode, 0)
}

func finalReviewLifecycleFixtureWithPending(t *testing.T, pending int) finalReviewFixture {
	return finalReviewLifecycleFixtureForPending(t, domain.TicketFeature, domain.MergeGuarded, pending)
}

func finalReviewLifecycleFixtureForPending(t *testing.T, ticketType domain.TicketType, mergeMode domain.MergeMode, pending int) finalReviewFixture {
	t.Helper()
	if pending < 0 || pending > 4 {
		t.Fatalf("invalid pending CI fixture count=%d", pending)
	}
	db, ctx, publishing, fence := publicationLifecycleFixtureFor(t, ticketType, mergeMode)
	recordFixturePublication(t, db, ctx, publishing, fence)
	if _, err := db.TransitionPublishedCandidate(ctx, Transition{Ref: publishing.Ref, ExpectedVersion: publishing.Version, From: domain.StatePublishing, To: domain.StateWaitingCI, Trigger: "effects_confirmed", Fence: fence, EventPayload: `{}`}); err != nil {
		t.Fatal(err)
	}
	waiting, err := db.Ticket(ctx, publishing.Ref)
	if err != nil {
		t.Fatal(err)
	}
	candidate, err := db.RecoverableCandidate(ctx, waiting.Ref)
	if err != nil {
		t.Fatal(err)
	}
	publication, err := db.LoadPublishedCandidate(ctx, waiting.Ref)
	if err != nil {
		t.Fatal(err)
	}
	policy := CIRequiredCheckPolicy{
		Ref: waiting.Ref, CandidateGeneration: candidate.Snapshot.Generation,
		CandidateHeadSHA: candidate.Snapshot.HeadSHA, CandidateTreeSHA: candidate.Snapshot.TreeSHA,
		PublicationWitnessDigest: publication.WitnessDigest, ProtectedBranchRef: publication.PullRequest.BaseRef,
		ProtectedBranchOID: publication.PullRequest.BaseOID, PolicySourceDigest: strings.Repeat("a", 64),
		AuthenticatedPrincipal: "fixture", RequiredChecks: []CIObservationCheck{{CanonicalName: "unit", ExternalID: "run-1"}},
		authenticated: true,
	}
	canonicalPolicy, err := canonicalCIPolicy(policy)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.RecordCIRequiredCheckPolicy(ctx, policy); err != nil {
		t.Fatal(err)
	}
	record := func(classification, state string) {
		observation := CIObservation{
			Ref: waiting.Ref, CandidateGeneration: candidate.Snapshot.Generation,
			CandidateHeadSHA: candidate.Snapshot.HeadSHA, CandidateTreeSHA: candidate.Snapshot.TreeSHA,
			PublicationWitnessDigest: publication.WitnessDigest, PolicyWitnessDigest: canonicalPolicy.PolicyWitnessDigest,
			PullRequest: publication.PullRequest, ObservedTicketVersion: waiting.Version,
			ObservedFence: fence, ObservedAt: time.Now().UTC(),
			RequiredChecks: []CIObservationCheck{{CanonicalName: "unit", ExternalID: "run-1", NormalizedState: state}},
			Classification: classification,
		}
		canonicalObservation, err := canonicalCIObservation(observation)
		if err != nil {
			t.Fatal(err)
		}
		if err := db.RecordCIObservation(ctx, observation); err != nil {
			t.Fatal(err)
		}
		if _, err := db.ConsumeCIObservation(ctx, CIObservationTransition{Ref: waiting.Ref, ObservationDigest: canonicalObservation.ObservationDigest, ExpectedVersion: waiting.Version, Fence: fence}); err != nil {
			t.Fatal(err)
		}
		waiting, err = db.Ticket(ctx, waiting.Ref)
		if err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < pending; i++ {
		record("pending", "pending")
	}
	record("green", "success")
	ticket := waiting
	if ticket.State != domain.StateReviewing {
		t.Fatalf("reviewing ticket=%+v", ticket)
	}
	if _, _, _, err := finalReviewCIAuthorityFrom(ctx, db.db, waiting.Ref, candidate); err != nil {
		t.Fatalf("fixture CI chain: %v", err)
	}
	return finalReviewFixture{db: db, ctx: ctx, ticket: ticket, fence: domain.Fence{LeaderEpoch: fence.LeaderEpoch, RunnerEpoch: ticket.RunnerEpoch}, candidate: candidate}
}

// spikeReviewLifecycleFixture intentionally does not reuse the publication
// fixture. It drives a report-ready spike through the public Store and Engine
// authorities only: no publication witness, CI observation, PR, or merge
// effect is created on this path.
func spikeReviewLifecycleFixture(t *testing.T) finalReviewFixture {
	t.Helper()
	db, ctx := openTestStore(t)
	configDigest := setupProviderProject(t, db, ctx)
	leader, err := db.AcquireLeader(ctx, domain.ChannelDev, "spike-review-lifecycle")
	if err != nil {
		t.Fatal(err)
	}
	ref := domain.TicketRef{Channel: domain.ChannelDev, Project: "provider", Ticket: "SF-spike-review-lifecycle"}
	source := sha256Digest([]byte("spike-review-source"))
	if err := db.CreateTicket(ctx, Ticket{Ref: ref, SourceDigest: source, Type: domain.TicketSpike, MergeMode: domain.MergeManual, CreatedAt: time.Now().UTC(), MaxDuration: time.Hour, MaxCostMicroUSD: 100}); err != nil {
		t.Fatal(err)
	}
	ticket, err := db.StartOrAdopt(ctx, ref, 1, "dev/provider/SF-spike-review-lifecycle", domain.Fence{LeaderEpoch: leader, RunnerEpoch: 1})
	if err != nil {
		t.Fatal(err)
	}
	branch := testAllocatedBranch(ref, strings.Repeat("ab", 16))
	base := strings.Repeat("a", 40)
	identity := []byte(strings.ReplaceAll(strings.ReplaceAll(repositoryCommandIdentity(t, "/tmp/provider", "/tmp/provider/SF-spike-review-lifecycle", branch, "main"), "git@example.test:nysa.git", "https://github.com/acme/app.git"), "/tmp/nysa-origin", "git@github.com:acme/app.git"))
	fence := domain.Fence{LeaderEpoch: leader, RunnerEpoch: ticket.RunnerEpoch}
	branchKey := string(ref.Channel) + "\x00" + string(ref.Project) + "\x00" + string(ref.Ticket)
	if _, err := db.LoadOrStoreBranchUnderFence(ctx, branchKey, branch, ticket.Version, fence); err != nil {
		t.Fatal(err)
	}
	if err := db.RegisterWorktree(ctx, WorktreeRegistration{Ref: ref, ExpectedVersion: ticket.Version, Fence: fence, Path: "/tmp/provider/SF-spike-review-lifecycle", Branch: branch, IdentityJSON: identity, BaseSHA: base, HeadSHA: base}); err != nil {
		t.Fatal(err)
	}
	builder, reviewer := setupProviderPair(t, db, ctx)
	launch := func(phase domain.Phase, role string, binding contracts.RuntimeBinding, raw []byte, validation phaseartifact.Validation) ProviderAttemptClaim {
		request := supervised(t, ProviderAttemptRequest{Ref: ref, ExpectedVersion: ticket.Version, Fence: fence, Phase: phase, Role: role, Binding: binding, ConfigDigest: configDigest, Capacity: 1, At: time.Now().UTC()})
		request.WorktreeIdentity, request.Input.WorktreeIdentity = string(identity), string(identity)
		claim, err := db.BeginProviderAttempt(ctx, request)
		if err != nil {
			t.Fatal(err)
		}
		if err := db.RecordProviderLaunch(ctx, claim, contracts.ProviderLaunch{PID: int(claim.ID), PGID: int(claim.ID), BootIdentity: "spike-review", ProcessStartIdentity: fmt.Sprintf("spike-review-%d", claim.ID), Worktree: claim.Worktree}); err != nil {
			t.Fatal(err)
		}
		if _, err := db.CompleteProviderAttemptSuccess(ctx, claim, proof(t, claim), ticket.Version, fence, contracts.PhaseResult{Provider: claim.Binding.Identity, Artifact: raw, UsageTrusted: true, UsageUnits: 1}, validation, time.Now().UTC()); err != nil {
			t.Fatal(err)
		}
		return claim
	}
	plan := phaseartifact.Planner{Schema: "sf.planner/v1", Acceptance: []string{"report delivered"}, Proof: phaseartifact.ProofPlan{Kind: phaseartifact.ProofReport, Command: []string{"go", "test"}, Details: "report"}, Paths: []string{"docs"}, Commands: [][]string{{"go", "test"}}, Risks: []string{"report only"}}
	planRaw, _ := json.Marshal(plan)
	planner := launch(domain.PhasePlanning, "planner", runtime(builder), planRaw, phaseartifact.Validation{TicketType: domain.TicketSpike})
	planKey := ProviderAttemptResultKey{AttemptID: planner.ID, Ref: ref, Phase: domain.PhasePlanning, Attempt: planner.Attempt}
	if _, err := db.RecordPlan(ctx, PlanArtifact{Ref: ref, ExpectedVersion: ticket.Version, Fence: fence, Document: PlanDocument{Planner: &plan, ProviderResult: &planKey, Acceptance: plan.Acceptance, ProofKind: string(plan.Proof.Kind), Paths: plan.Paths, Commands: plan.Commands, Risks: plan.Risks}}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.TransitionPlan(ctx, Transition{Ref: ref, ExpectedVersion: ticket.Version, From: domain.StatePlanning, To: domain.StateVerifying, Trigger: "phase_pass", Fence: fence, EventPayload: "{}"}); err != nil {
		t.Fatal(err)
	}
	ticket, _ = db.Ticket(ctx, ref)
	fence.RunnerEpoch = ticket.RunnerEpoch
	planIdentity, _ := workflowprompt.NewPlanIdentity(plan)
	verification := phaseartifact.Verification{Schema: "sf.verification/v1", AcceptanceDigest: planIdentity.Digest, ProofKind: phaseartifact.ProofReport, OwnedFiles: []string{"docs"}, Command: []string{"go", "test", "./..."}, PrebuildOutcome: "report_ready", EvidenceDigest: sha256Digest([]byte("spike-review-verification"))}
	verificationRaw, _ := json.Marshal(verification)
	verified := launch(domain.PhaseVerification, "reviewer", runtime(reviewer), verificationRaw, phaseartifact.Validation{TicketType: domain.TicketSpike, AcceptanceDigest: planIdentity.Digest})
	intent, _ := workflowprompt.CanonicalVerificationIntentBytes(verification)
	proofBytes, _ := workflowprompt.CanonicalVerificationProofBytes(verification)
	checkpoint := strings.Repeat("c", 40)
	verificationKey := ProviderAttemptResultKey{AttemptID: verified.ID, Ref: ref, Phase: domain.PhaseVerification, Attempt: verified.Attempt}
	command := completeEvidenceRepositoryCommand(t, db, ctx, RepositoryCommandPurposePrebuildVerification, ref, ticket.Version, fence, verificationKey, sha256Digest(intent), sha256Digest(proofBytes), "", "", 0)
	if _, err := db.RecordVerification(ctx, VerificationArtifact{Ref: ref, ExpectedVersion: ticket.Version, Fence: fence, Intent: intent, Proof: proofBytes, OwnedFiles: verification.OwnedFiles, CheckpointID: checkpoint, ProviderResult: &verificationKey, Checkpoint: CommitObservation{CommitOID: checkpoint, ParentOID: base, TreeOID: strings.Repeat("d", 40)}, CommandResult: command}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.TransitionVerification(ctx, Transition{Ref: ref, ExpectedVersion: ticket.Version, From: domain.StateVerifying, To: domain.StateBuilding, Trigger: "phase_pass", Fence: fence, EventPayload: "{}"}); err != nil {
		t.Fatal(err)
	}
	ticket, _ = db.Ticket(ctx, ref)
	fence.RunnerEpoch = ticket.RunnerEpoch
	builderRaw := []byte(`{"schema":"sf.builder/v1","summary":"spike report","changed_files":["docs/report.md"],"commands":[["go","test"]]}`)
	built := launch(domain.PhaseBuild, "builder", runtime(builder), builderRaw, phaseartifact.Validation{TicketType: domain.TicketSpike})
	_, parsed, err := db.LoadHistoricalProviderAttemptResult(ctx, ProviderAttemptResultKey{AttemptID: built.ID, Ref: ref, Phase: domain.PhaseBuild, Attempt: built.Attempt})
	if err != nil || parsed.Builder == nil {
		t.Fatalf("spike builder=%+v err=%v", parsed, err)
	}
	builderDigest, _ := phaseartifact.BuilderEvidenceDigest(*parsed.Builder)
	policy := sha256Digest([]byte("spike-review-policy"))
	snapshot := domain.CandidateSnapshot{Generation: 1, BaseSHA: base, HeadSHA: strings.Repeat("e", 40), TreeSHA: strings.Repeat("f", 40), SourceDigest: source, VerificationIntentDigest: sha256Digest(intent), ProofDigest: sha256Digest(proofBytes), CommandPolicyDigest: policy, BuilderEvidenceDigest: builderDigest}
	builtKey := ProviderAttemptResultKey{AttemptID: built.ID, Ref: ref, Phase: domain.PhaseBuild, Attempt: built.Attempt}
	candidateCommand := completeEvidenceRepositoryCommand(t, db, ctx, RepositoryCommandPurposePostbuildCandidate, ref, ticket.Version, fence, builtKey, sha256Digest(intent), sha256Digest(proofBytes), checkpoint, "sha256:"+policy, 0)
	if _, err := db.RecordCandidate(ctx, CandidateEvidence{Ref: ref, ExpectedVersion: ticket.Version, Fence: fence, Snapshot: snapshot, BuilderResult: builtKey, Commit: CommitObservation{CommitOID: snapshot.HeadSHA, ParentOID: checkpoint, TreeOID: snapshot.TreeSHA}, Reason: "spike report", CommandResult: candidateCommand}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.TransitionCandidate(ctx, Transition{Ref: ref, ExpectedVersion: ticket.Version, From: domain.StateBuilding, To: domain.StateReviewing, Trigger: "phase_pass", Fence: fence, EventPayload: "{}"}, snapshot); err != nil {
		t.Fatal(err)
	}
	ticket, err = db.Ticket(ctx, ref)
	if err != nil || ticket.State != domain.StateReviewing {
		t.Fatalf("spike reviewing ticket=%+v err=%v", ticket, err)
	}
	stored, err := db.RecoverableCandidate(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	return finalReviewFixture{db: db, ctx: ctx, ticket: ticket, fence: domain.Fence{LeaderEpoch: leader, RunnerEpoch: ticket.RunnerEpoch}, candidate: stored}
}

func completeFinalReview(t *testing.T, fixture finalReviewFixture) ProviderAttemptClaim {
	return completeFinalReviewWith(t, fixture, phaseartifact.ReviewPass, "")
}

func completeFinalReviewWith(t *testing.T, fixture finalReviewFixture, decision phaseartifact.ReviewDecision, owner string) ProviderAttemptClaim {
	t.Helper()
	_, reviewer := setupProviderPair(t, fixture.db, fixture.ctx)
	worktree, err := fixture.db.Worktree(fixture.ctx, fixture.ticket.Ref)
	if err != nil {
		t.Fatal(err)
	}
	request := ProviderAttemptRequest{Ref: fixture.ticket.Ref, ExpectedVersion: fixture.ticket.Version, Fence: fixture.fence, Phase: domain.PhaseReview, Role: "reviewer", Binding: runtime(reviewer), ConfigDigest: fixture.ticket.ConfigDigest, Capacity: 1, At: time.Now().UTC(), ExpectedHead: fixture.candidate.Snapshot.HeadSHA, ExpectedProof: fixture.candidate.Snapshot.ProofDigest, Repository: "/tmp/provider", Worktree: worktree.Path, WorktreeIdentity: string(worktree.IdentityJSON), BaseSHA: worktree.BaseSHA, SupervisorKey: providerTestSigner.PublicKey()}
	request.Input = contracts.PhaseInput{Ticket: request.Ref, Phase: request.Phase, LeaderEpoch: request.Fence.LeaderEpoch, RunnerEpoch: request.Fence.RunnerEpoch, ExpectedVersion: request.ExpectedVersion, Prompt: "final review", Repository: request.Repository, Worktree: request.Worktree, WorktreeIdentity: request.WorktreeIdentity, BaseSHA: request.BaseSHA, AllowedPaths: []string{"."}, Provider: request.Binding.Identity, AuthMode: request.Binding.AuthMode, Timeout: time.Minute, Profile: contracts.ProfileGuarded, Schema: []byte(`{"type":"object"}`)}
	claim, err := fixture.db.BeginProviderAttempt(fixture.ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if err := fixture.db.RecordProviderLaunch(fixture.ctx, claim, contracts.ProviderLaunch{PID: int(claim.ID), PGID: int(claim.ID), BootIdentity: "final-review", ProcessStartIdentity: fmt.Sprintf("final-review-%d", claim.ID), Worktree: claim.Worktree}); err != nil {
		t.Fatal(err)
	}
	findings := `[]`
	if decision != phaseartifact.ReviewPass {
		findings = `["fix exact finding"]`
	}
	raw := contracts.PhaseResult{Provider: claim.Binding.Identity, Artifact: []byte(fmt.Sprintf(`{"schema":"sf.reviewer/v1","decision":"%s","repair_owner":"%s","findings":%s,"reviewed_head":"%s","proof_digest":"%s"}`, decision, owner, findings, fixture.candidate.Snapshot.HeadSHA, fixture.candidate.Snapshot.ProofDigest)), UsageTrusted: true, UsageUnits: 1}
	if _, err := fixture.db.CompleteProviderAttemptSuccess(fixture.ctx, claim, proof(t, claim), fixture.ticket.Version, fixture.fence, raw, phaseartifact.Validation{TicketType: fixture.ticket.Type, ExpectedReviewedHead: fixture.candidate.Snapshot.HeadSHA, ExpectedProofDigest: fixture.candidate.Snapshot.ProofDigest}, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	return claim
}

func TestReviewRepairAndOperatorEscalationConsumeExactStoredReviewerResult(t *testing.T) {
	for _, tc := range []struct {
		name     string
		decision phaseartifact.ReviewDecision
		owner    string
		to       domain.State
		trigger  string
		payload  string
	}{
		{name: "builder repair", decision: phaseartifact.ReviewRepair, owner: "builder", to: domain.StateBuilding, trigger: "review_repair", payload: `{}`},
		{name: "verification repair", decision: phaseartifact.ReviewRepair, owner: "reviewer", to: domain.StateVerifying, trigger: "review_repair", payload: `{}`},
		{name: "operator escalation", decision: phaseartifact.ReviewNeedsOperator, owner: "operator", to: domain.StateBlocked, trigger: "typed_blocker", payload: `{"code":"review_needs_operator"}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fixture := finalReviewLifecycleFixture(t)
			claim := completeFinalReviewWith(t, fixture, tc.decision, tc.owner)
			reused, err := fixture.db.LatestReusableProviderAttempt(fixture.ctx, LatestReusableProviderAttemptRequest{Ref: fixture.ticket.Ref, Phase: domain.PhaseReview, Role: "reviewer", ExpectedVersion: fixture.ticket.Version, Fence: fixture.fence})
			if err != nil || reused.Key.AttemptID != claim.ID || reused.Parsed.Reviewer == nil || reused.Parsed.Reviewer.Decision != tc.decision || reused.Parsed.Reviewer.RepairOwner != tc.owner {
				t.Fatalf("replayed reviewer=%+v err=%v", reused, err)
			}
			var result TransitionResult
			if tc.trigger == "review_repair" {
				result, err = fixture.db.TransitionReviewRepair(fixture.ctx, Transition{Ref: fixture.ticket.Ref, ExpectedVersion: fixture.ticket.Version, From: domain.StateReviewing, To: tc.to, Trigger: tc.trigger, Fence: fixture.fence, EventPayload: tc.payload})
			} else {
				result, err = fixture.db.TransitionReviewNeedsOperator(fixture.ctx, Transition{Ref: fixture.ticket.Ref, ExpectedVersion: fixture.ticket.Version, From: domain.StateReviewing, To: tc.to, ResumeState: domain.StateReviewing, Trigger: tc.trigger, Fence: fixture.fence, EventPayload: tc.payload})
			}
			if err != nil || result.Version != fixture.ticket.Version+1 {
				t.Fatalf("transition=%+v err=%v", result, err)
			}
			current, err := fixture.db.Ticket(fixture.ctx, fixture.ticket.Ref)
			if err != nil || current.State != tc.to || (tc.to == domain.StateBlocked && current.BlockedCode != "review_needs_operator") {
				t.Fatalf("ticket=%+v err=%v", current, err)
			}
			var attempts, budget int
			if err := fixture.db.db.QueryRowContext(fixture.ctx, `SELECT COUNT(*) FROM provider_attempts WHERE id=?`, claim.ID).Scan(&attempts); err != nil || attempts != 1 {
				t.Fatalf("attempt replay=%d err=%v", attempts, err)
			}
			if tc.decision == phaseartifact.ReviewRepair {
				if err := fixture.db.db.QueryRowContext(fixture.ctx, `SELECT used FROM ticket_counters WHERE channel=? AND project_id=? AND ticket_id=? AND kind='correction'`, fixture.ticket.Ref.Channel, fixture.ticket.Ref.Project, fixture.ticket.Ref.Ticket).Scan(&budget); err != nil || budget != 1 {
					t.Fatalf("repair budget=%d err=%v", budget, err)
				}
				phase, role := domain.PhaseBuild, "builder"
				if tc.to == domain.StateVerifying {
					phase, role = domain.PhaseVerification, "reviewer"
				}
				// The old target-phase result is durable provenance but cannot be
				// selected as replay authority for this next repair cycle. A Worker
				// receiving ErrNotFound proceeds to launch a fresh provider attempt.
				if _, err := fixture.db.LatestReusableProviderAttempt(fixture.ctx, LatestReusableProviderAttemptRequest{Ref: fixture.ticket.Ref, Phase: phase, Role: role, ExpectedVersion: current.Version, Fence: domain.Fence{LeaderEpoch: fixture.fence.LeaderEpoch, RunnerEpoch: current.RunnerEpoch}}); !errors.Is(err, ErrNotFound) {
					t.Fatalf("repair boundary reused stale %s result: %v", phase, err)
				}
				var boundaries int
				if err := fixture.db.db.QueryRowContext(fixture.ctx, `SELECT COUNT(*) FROM final_review_repair_boundaries WHERE channel=? AND project_id=? AND ticket_id=? AND target_state=? AND transition_ticket_version=?`, fixture.ticket.Ref.Channel, fixture.ticket.Ref.Project, fixture.ticket.Ref.Ticket, tc.to, current.Version).Scan(&boundaries); err != nil || boundaries != 1 {
					t.Fatalf("repair boundary count=%d err=%v", boundaries, err)
				}
			}
			// A response-loss retry cannot consume the completed reviewer result a
			// second time. The old reviewing fence is intentionally stale after
			// the first atomic transition, so neither a second correction charge
			// nor a second blocked event is possible.
			if tc.trigger == "review_repair" {
				_, err = fixture.db.TransitionReviewRepair(fixture.ctx, Transition{Ref: fixture.ticket.Ref, ExpectedVersion: fixture.ticket.Version, From: domain.StateReviewing, To: tc.to, Trigger: tc.trigger, Fence: fixture.fence, EventPayload: tc.payload})
			} else {
				_, err = fixture.db.TransitionReviewNeedsOperator(fixture.ctx, Transition{Ref: fixture.ticket.Ref, ExpectedVersion: fixture.ticket.Version, From: domain.StateReviewing, To: tc.to, ResumeState: domain.StateReviewing, Trigger: tc.trigger, Fence: fixture.fence, EventPayload: tc.payload})
			}
			if !errors.Is(err, ErrStaleFence) {
				t.Fatalf("replayed final-review result err=%v, want stale fence", err)
			}
			if tc.decision == phaseartifact.ReviewRepair {
				if err := fixture.db.db.QueryRowContext(fixture.ctx, `SELECT used FROM ticket_counters WHERE channel=? AND project_id=? AND ticket_id=? AND kind='correction'`, fixture.ticket.Ref.Channel, fixture.ticket.Ref.Project, fixture.ticket.Ref.Ticket).Scan(&budget); err != nil || budget != 1 {
					t.Fatalf("replayed repair budget=%d err=%v", budget, err)
				}
			}
		})
	}
}

func TestFinalReviewStoreReplayUsesCompletedAttemptWithoutSecondProviderMutation(t *testing.T) {
	fixture := finalReviewLifecycleFixture(t)
	claim := completeFinalReview(t, fixture)
	reused, err := fixture.db.LatestReusableProviderAttempt(fixture.ctx, LatestReusableProviderAttemptRequest{Ref: fixture.ticket.Ref, Phase: domain.PhaseReview, Role: "reviewer", ExpectedVersion: fixture.ticket.Version, Fence: fixture.fence})
	if err != nil || reused.Key.AttemptID != claim.ID || reused.Recovered || reused.Parsed.Reviewer == nil || reused.Parsed.Reviewer.Decision != phaseartifact.ReviewPass {
		t.Fatalf("reused final review=%+v err=%v", reused, err)
	}
	if _, err := fixture.db.TransitionFinalReview(fixture.ctx, Transition{Ref: fixture.ticket.Ref, ExpectedVersion: fixture.ticket.Version, From: domain.StateReviewing, To: domain.StateWaitingApproval, Trigger: "review_pass", Fence: fixture.fence, EventPayload: `{}`}); err != nil {
		t.Fatal(err)
	}
	var attempts, transitions int
	if err := fixture.db.db.QueryRowContext(fixture.ctx, `SELECT COUNT(*) FROM provider_attempts WHERE channel=? AND project_id=? AND ticket_id=? AND phase='review'`, fixture.ticket.Ref.Channel, fixture.ticket.Ref.Project, fixture.ticket.Ref.Ticket).Scan(&attempts); err != nil || attempts != 1 {
		t.Fatalf("review attempt count=%d err=%v", attempts, err)
	}
	if err := fixture.db.db.QueryRowContext(fixture.ctx, `SELECT COUNT(*) FROM events WHERE channel=? AND project_id=? AND ticket_id=? AND trigger='review_pass'`, fixture.ticket.Ref.Channel, fixture.ticket.Ref.Project, fixture.ticket.Ref.Ticket).Scan(&transitions); err != nil || transitions != 1 {
		t.Fatalf("review transition count=%d err=%v", transitions, err)
	}
}

func TestOperatorDecisionAtomicallyBindsCurrentPublishedHead(t *testing.T) {
	fixture := finalReviewLifecycleFixture(t)
	completeFinalReview(t, fixture)
	if _, err := fixture.db.TransitionFinalReview(fixture.ctx, Transition{Ref: fixture.ticket.Ref, ExpectedVersion: fixture.ticket.Version, From: domain.StateReviewing, To: domain.StateWaitingApproval, Trigger: "review_pass", Fence: fixture.fence, EventPayload: `{}`}); err != nil {
		t.Fatal(err)
	}
	waiting, err := fixture.db.Ticket(fixture.ctx, fixture.ticket.Ref)
	if err != nil {
		t.Fatal(err)
	}
	result, err := fixture.db.ApplyOperatorDecision(fixture.ctx, OperatorDecisionRequest{OperatorDecision: OperatorDecision{Ref: waiting.Ref, ExpectedVersion: waiting.Version, Fence: domain.Fence{LeaderEpoch: fixture.fence.LeaderEpoch, RunnerEpoch: waiting.RunnerEpoch}, ReviewedHead: fixture.candidate.Snapshot.HeadSHA, OperatorUID: 501, Decision: "approved"}})
	if err != nil || result.Version != waiting.Version+1 {
		t.Fatalf("approval result=%+v err=%v", result, err)
	}
	merged, err := fixture.db.Ticket(fixture.ctx, waiting.Ref)
	if err != nil || merged.State != domain.StateMerging {
		t.Fatalf("merged ticket=%+v err=%v", merged, err)
	}
	decisions, err := fixture.db.OperatorDecisions(fixture.ctx, waiting.Ref)
	if err != nil || len(decisions) != 1 || decisions[0].ReviewedHead != fixture.candidate.Snapshot.HeadSHA || decisions[0].Decision != "approved" {
		t.Fatalf("decisions=%+v err=%v", decisions, err)
	}
	// Simulate a lost daemon response: the same authenticated request is a
	// read of its durable outcome, not a second approval or merge authority.
	replayed, err := fixture.db.ApplyOperatorDecision(fixture.ctx, OperatorDecisionRequest{OperatorDecision: OperatorDecision{Ref: waiting.Ref, ExpectedVersion: merged.Version, Fence: domain.Fence{LeaderEpoch: fixture.fence.LeaderEpoch, RunnerEpoch: merged.RunnerEpoch}, ReviewedHead: fixture.candidate.Snapshot.HeadSHA, OperatorUID: 501, Decision: "approved"}})
	if err != nil || replayed.Version != merged.Version {
		t.Fatalf("replayed approval=%+v err=%v", replayed, err)
	}
	decisions, err = fixture.db.OperatorDecisions(fixture.ctx, waiting.Ref)
	if err != nil || len(decisions) != 1 {
		t.Fatalf("replayed decisions=%+v err=%v", decisions, err)
	}
}

func TestOperatorDecisionRejectsHeadDriftWithoutMutation(t *testing.T) {
	fixture := finalReviewLifecycleFixture(t)
	completeFinalReview(t, fixture)
	if _, err := fixture.db.TransitionFinalReview(fixture.ctx, Transition{Ref: fixture.ticket.Ref, ExpectedVersion: fixture.ticket.Version, From: domain.StateReviewing, To: domain.StateWaitingApproval, Trigger: "review_pass", Fence: fixture.fence, EventPayload: `{}`}); err != nil {
		t.Fatal(err)
	}
	waiting, err := fixture.db.Ticket(fixture.ctx, fixture.ticket.Ref)
	if err != nil {
		t.Fatal(err)
	}
	_, err = fixture.db.ApplyOperatorDecision(fixture.ctx, OperatorDecisionRequest{OperatorDecision: OperatorDecision{Ref: waiting.Ref, ExpectedVersion: waiting.Version, Fence: domain.Fence{LeaderEpoch: fixture.fence.LeaderEpoch, RunnerEpoch: waiting.RunnerEpoch}, ReviewedHead: strings.Repeat("d", 40), OperatorUID: 501, Decision: "approved"}})
	if !errors.Is(err, ErrStaleFence) {
		t.Fatalf("approval after PR head drift err=%v", err)
	}
	current, err := fixture.db.Ticket(fixture.ctx, waiting.Ref)
	if err != nil || current.State != domain.StateWaitingApproval || current.Version != waiting.Version {
		t.Fatalf("ticket after refusal=%+v err=%v", current, err)
	}
}

func TestFinalReviewStoreRecoveryRebindsExactResultOnce(t *testing.T) {
	fixture := finalReviewLifecycleFixture(t)
	claim := completeFinalReview(t, fixture)
	leader, err := fixture.db.AcquireLeader(fixture.ctx, domain.ChannelDev, "final-review-recovery")
	if err != nil {
		t.Fatal(err)
	}
	if changed, err := fixture.db.FenceRecoveredRunners(fixture.ctx, domain.ChannelDev, leader); err != nil || changed != 1 {
		t.Fatalf("fence recovery changed=%d err=%v", changed, err)
	}
	live, err := fixture.db.Ticket(fixture.ctx, fixture.ticket.Ref)
	if err != nil {
		t.Fatal(err)
	}
	liveFence := domain.Fence{LeaderEpoch: leader, RunnerEpoch: live.RunnerEpoch}
	reused, err := fixture.db.LatestReusableProviderAttempt(fixture.ctx, LatestReusableProviderAttemptRequest{Ref: live.Ref, Phase: domain.PhaseReview, Role: "reviewer", ExpectedVersion: live.Version, Fence: liveFence})
	if err != nil || reused.Key.AttemptID != claim.ID || !reused.Recovered {
		t.Fatalf("recovered review=%+v err=%v", reused, err)
	}
	if _, err := fixture.db.TransitionFinalReview(fixture.ctx, Transition{Ref: live.Ref, ExpectedVersion: live.Version, From: domain.StateReviewing, To: domain.StateWaitingApproval, Trigger: "review_pass", Fence: liveFence, EventPayload: `{}`}); err != nil {
		t.Fatal(err)
	}
	var attempts, transitions int
	if err := fixture.db.db.QueryRowContext(fixture.ctx, `SELECT COUNT(*) FROM provider_attempts WHERE channel=? AND project_id=? AND ticket_id=? AND phase='review'`, live.Ref.Channel, live.Ref.Project, live.Ref.Ticket).Scan(&attempts); err != nil || attempts != 1 {
		t.Fatalf("review attempts=%d err=%v", attempts, err)
	}
	if err := fixture.db.db.QueryRowContext(fixture.ctx, `SELECT COUNT(*) FROM events WHERE channel=? AND project_id=? AND ticket_id=? AND trigger='review_pass'`, live.Ref.Channel, live.Ref.Project, live.Ref.Ticket).Scan(&transitions); err != nil || transitions != 1 {
		t.Fatalf("review transitions=%d err=%v", transitions, err)
	}
}

func TestReviewRepairBoundaryBridgesStartupBeforeFreshTargetClaim(t *testing.T) {
	for _, tc := range []struct {
		name  string
		owner string
		phase domain.Phase
		role  string
	}{
		{name: "builder", owner: "builder", phase: domain.PhaseBuild, role: "builder"},
		{name: "verification", owner: "reviewer", phase: domain.PhaseVerification, role: "reviewer"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fixture := finalReviewLifecycleFixture(t)
			completeFinalReviewWith(t, fixture, phaseartifact.ReviewRepair, tc.owner)
			to := domain.StateBuilding
			if tc.owner == "reviewer" {
				to = domain.StateVerifying
			}
			if _, err := fixture.db.TransitionReviewRepair(fixture.ctx, Transition{Ref: fixture.ticket.Ref, ExpectedVersion: fixture.ticket.Version, From: domain.StateReviewing, To: to, Trigger: "review_repair", Fence: fixture.fence, EventPayload: `{}`}); err != nil {
				t.Fatal(err)
			}
			// Simulate process death in the exact gap after the repair commit and
			// before a new Builder/Verifier claim is admitted.
			leader, err := fixture.db.AcquireLeader(fixture.ctx, domain.ChannelDev, "review-repair-restart-"+tc.owner)
			if err != nil {
				t.Fatal(err)
			}
			if changed, err := fixture.db.FenceRecoveredRunners(fixture.ctx, domain.ChannelDev, leader); err != nil || changed != 1 {
				t.Fatalf("repair startup bridge changed=%d err=%v", changed, err)
			}
			live, err := fixture.db.Ticket(fixture.ctx, fixture.ticket.Ref)
			if err != nil || live.State != to {
				t.Fatalf("recovered repair ticket=%+v err=%v", live, err)
			}
			if _, err := fixture.db.LatestReusableProviderAttempt(fixture.ctx, LatestReusableProviderAttemptRequest{Ref: live.Ref, Phase: tc.phase, Role: tc.role, ExpectedVersion: live.Version, Fence: domain.Fence{LeaderEpoch: leader, RunnerEpoch: live.RunnerEpoch}}); !errors.Is(err, ErrNotFound) {
				t.Fatalf("recovered repair reused old %s result: %v", tc.phase, err)
			}
		})
	}
}

func TestFinalReviewTransitionsDeriveManualGuardedSpikeAndRejectAutonomous(t *testing.T) {
	for _, tc := range []struct {
		name       string
		ticketType domain.TicketType
		mergeMode  domain.MergeMode
		to         domain.State
		wantErr    bool
	}{
		{name: "guarded", ticketType: domain.TicketFeature, mergeMode: domain.MergeGuarded, to: domain.StateWaitingApproval},
		{name: "manual", ticketType: domain.TicketFeature, mergeMode: domain.MergeManual, to: domain.StateWaitingManualMerge},
		{name: "spike report only", ticketType: domain.TicketSpike, mergeMode: domain.MergeManual, to: domain.StateDone},
		{name: "autonomous unavailable", ticketType: domain.TicketFeature, mergeMode: domain.MergeAutonomous, to: domain.StateWaitingApproval, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var fixture finalReviewFixture
			if tc.ticketType == domain.TicketSpike {
				fixture = spikeReviewLifecycleFixture(t)
			} else {
				fixture = finalReviewLifecycleFixtureFor(t, tc.ticketType, tc.mergeMode)
			}
			completeFinalReview(t, fixture)
			_, err := fixture.db.TransitionFinalReview(fixture.ctx, Transition{Ref: fixture.ticket.Ref, ExpectedVersion: fixture.ticket.Version, From: domain.StateReviewing, To: tc.to, Trigger: "review_pass", Fence: fixture.fence, EventPayload: `{}`})
			if tc.wantErr {
				if !errors.Is(err, ErrEvidenceConflict) {
					t.Fatalf("autonomous transition err=%v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			current, err := fixture.db.Ticket(fixture.ctx, fixture.ticket.Ref)
			if err != nil || current.State != tc.to {
				t.Fatalf("ticket=%+v err=%v", current, err)
			}
			if tc.ticketType == domain.TicketSpike {
				var publication, mergeEffects, mergeIntents int
				if err := fixture.db.db.QueryRowContext(fixture.ctx, `SELECT COUNT(*) FROM publication_evidence WHERE channel=? AND project_id=? AND ticket_id=?`, current.Ref.Channel, current.Ref.Project, current.Ref.Ticket).Scan(&publication); err != nil || publication != 0 {
					t.Fatalf("spike publication rows=%d err=%v", publication, err)
				}
				if err := fixture.db.db.QueryRowContext(fixture.ctx, `SELECT COUNT(*) FROM effects WHERE channel=? AND project_id=? AND ticket_id=? AND effect_kind='merge'`, current.Ref.Channel, current.Ref.Project, current.Ref.Ticket).Scan(&mergeEffects); err != nil || mergeEffects != 0 {
					t.Fatalf("spike merge effects=%d err=%v", mergeEffects, err)
				}
				if err := fixture.db.db.QueryRowContext(fixture.ctx, `SELECT COUNT(*) FROM merge_intents WHERE channel=? AND project_id=? AND ticket_id=?`, current.Ref.Channel, current.Ref.Project, current.Ref.Ticket).Scan(&mergeIntents); err != nil || mergeIntents != 0 {
					t.Fatalf("spike merge intents=%d err=%v", mergeIntents, err)
				}
			}
		})
	}
}

func TestFinalReviewAuthorityRejectsMissingAndForgedCILineage(t *testing.T) {
	// A candidate and publication witness alone are insufficient: a reviewing
	// state with no green CI observation/transition is fail-closed.
	missingDB, missingCtx, publishing, missingFence := publicationLifecycleFixture(t)
	recordFixturePublication(t, missingDB, missingCtx, publishing, missingFence)
	if _, err := missingDB.TransitionPublishedCandidate(missingCtx, Transition{Ref: publishing.Ref, ExpectedVersion: publishing.Version, From: domain.StatePublishing, To: domain.StateWaitingCI, Trigger: "effects_confirmed", Fence: missingFence, EventPayload: `{}`}); err != nil {
		t.Fatal(err)
	}
	missing, err := missingDB.Ticket(missingCtx, publishing.Ref)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := missingDB.db.ExecContext(missingCtx, `UPDATE tickets SET state='reviewing',version=version+1 WHERE channel=? AND project_id=? AND id=?`, missing.Ref.Channel, missing.Ref.Project, missing.Ref.Ticket); err != nil {
		t.Fatal(err)
	}
	if _, err := missingDB.FinalReviewAuthority(missingCtx, missing.Ref, missing.Version+1, domain.Fence{LeaderEpoch: missingFence.LeaderEpoch, RunnerEpoch: missing.RunnerEpoch}); !errors.Is(err, ErrEvidenceConflict) {
		t.Fatalf("missing CI lineage accepted: %v", err)
	}

	fixture := finalReviewLifecycleFixture(t)
	// The immutable observation/check rows reject physical tampering. The
	// authority below also recomputes the canonical required-set digest, which
	// is what protects it if a database was restored from inconsistent bytes.
	if _, err := fixture.db.db.ExecContext(fixture.ctx, `UPDATE ci_observation_checks SET normalized_state='failure'`); err == nil {
		t.Fatal("immutable check mutation unexpectedly succeeded")
	}
	if _, err := fixture.db.db.ExecContext(fixture.ctx, `UPDATE ci_transition_evidence SET transition_digest=?`, publicationIdentityDigest([]byte("tampered"))); err == nil {
		t.Fatal("immutable CI transition mutation unexpectedly succeeded")
	}
	if _, err := fixture.db.FinalReviewAuthority(fixture.ctx, fixture.ticket.Ref, fixture.ticket.Version, fixture.fence); err != nil {
		t.Fatalf("authentic CI lineage rejected: %v", err)
	}
	if _, err := fixture.db.db.ExecContext(fixture.ctx, `INSERT INTO runner_recovery_ledger(channel,project_id,ticket_id,prior_ticket_version,prior_runner_epoch,prior_leader_epoch,ticket_version,runner_epoch,leader_epoch,recovery_digest,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?)`, fixture.ticket.Ref.Channel, fixture.ticket.Ref.Project, fixture.ticket.Ref.Ticket, fixture.ticket.Version, fixture.ticket.RunnerEpoch, fixture.fence.LeaderEpoch, fixture.ticket.Version+1, fixture.ticket.RunnerEpoch+1, fixture.fence.LeaderEpoch+1, publicationIdentityDigest([]byte("forged")), time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.db.FinalReviewAuthority(fixture.ctx, fixture.ticket.Ref, fixture.ticket.Version, fixture.fence); !errors.Is(err, ErrEvidenceConflict) && !errors.Is(err, ErrStaleFence) {
		t.Fatalf("forged recovery lineage accepted: %v", err)
	}
}

func TestFinalReviewAuthorityRejectsLegacyAndTamperedV43CILineage(t *testing.T) {
	t.Run("legacy blank policy witness", func(t *testing.T) {
		fixture := finalReviewLifecycleFixture(t)
		if _, err := fixture.db.db.ExecContext(fixture.ctx, `DROP TRIGGER ci_observations_immutable_update`); err != nil {
			t.Fatal(err)
		}
		if _, err := fixture.db.db.ExecContext(fixture.ctx, `UPDATE ci_observations SET policy_witness_digest='' WHERE channel=? AND project_id=? AND ticket_id=?`, fixture.ticket.Ref.Channel, fixture.ticket.Ref.Project, fixture.ticket.Ref.Ticket); err != nil {
			t.Fatal(err)
		}
		if _, err := fixture.db.FinalReviewAuthority(fixture.ctx, fixture.ticket.Ref, fixture.ticket.Version, fixture.fence); !errors.Is(err, ErrEvidenceConflict) {
			t.Fatalf("legacy blank policy witness accepted: %v", err)
		}
	})
	t.Run("observation check tamper", func(t *testing.T) {
		fixture := finalReviewLifecycleFixture(t)
		if _, err := fixture.db.db.ExecContext(fixture.ctx, `DROP TRIGGER ci_observation_checks_immutable_update`); err != nil {
			t.Fatal(err)
		}
		if _, err := fixture.db.db.ExecContext(fixture.ctx, `UPDATE ci_observation_checks SET normalized_state='failure'`); err != nil {
			t.Fatal(err)
		}
		if _, err := fixture.db.FinalReviewAuthority(fixture.ctx, fixture.ticket.Ref, fixture.ticket.Version, fixture.fence); !errors.Is(err, ErrEvidenceConflict) {
			t.Fatalf("tampered CI observation accepted: %v", err)
		}
	})
	t.Run("green transition event tamper", func(t *testing.T) {
		fixture := finalReviewLifecycleFixture(t)
		if _, err := fixture.db.db.ExecContext(fixture.ctx, `UPDATE events SET payload='{}' WHERE channel=? AND project_id=? AND ticket_id=? AND trigger='checks_green'`, fixture.ticket.Ref.Channel, fixture.ticket.Ref.Project, fixture.ticket.Ref.Ticket); err != nil {
			t.Fatal(err)
		}
		if _, err := fixture.db.FinalReviewAuthority(fixture.ctx, fixture.ticket.Ref, fixture.ticket.Version, fixture.fence); !errors.Is(err, ErrEvidenceConflict) {
			t.Fatalf("tampered CI transition event accepted: %v", err)
		}
	})
}

func TestFinalReviewAuthorityAuthenticatesCompletePendingToGreenChain(t *testing.T) {
	fixture := finalReviewLifecycleFixtureWithPending(t, 2)
	authority, err := fixture.db.FinalReviewAuthority(fixture.ctx, fixture.ticket.Ref, fixture.ticket.Version, fixture.fence)
	if err != nil || authority.Candidate.Snapshot != fixture.candidate.Snapshot || len(authority.Checks.Required) != 1 || authority.Checks.Required[0].Status != "success" {
		t.Fatalf("pending-to-green authority=%+v err=%v", authority, err)
	}
}

func TestFinalReviewAuthorityRejectsBrokenPendingToGreenChain(t *testing.T) {
	t.Run("missing pending transition", func(t *testing.T) {
		fixture := finalReviewLifecycleFixtureWithPending(t, 2)
		if _, err := fixture.db.db.ExecContext(fixture.ctx, `DROP TRIGGER ci_transition_evidence_immutable_delete`); err != nil {
			t.Fatal(err)
		}
		if _, err := fixture.db.db.ExecContext(fixture.ctx, `DELETE FROM ci_transition_evidence WHERE channel=? AND project_id=? AND ticket_id=? AND observation_classification='pending'`, fixture.ticket.Ref.Channel, fixture.ticket.Ref.Project, fixture.ticket.Ref.Ticket); err != nil {
			t.Fatal(err)
		}
		if _, err := fixture.db.FinalReviewAuthority(fixture.ctx, fixture.ticket.Ref, fixture.ticket.Version, fixture.fence); !errors.Is(err, ErrEvidenceConflict) {
			t.Fatalf("missing pending chain accepted: %v", err)
		}
	})
	t.Run("out of order pending transition is physically rejected", func(t *testing.T) {
		fixture := finalReviewLifecycleFixtureWithPending(t, 2)
		if _, err := fixture.db.db.ExecContext(fixture.ctx, `DROP TRIGGER ci_transition_evidence_immutable_update`); err != nil {
			t.Fatal(err)
		}
		if _, err := fixture.db.db.ExecContext(fixture.ctx, `UPDATE ci_transition_evidence SET ticket_version=ticket_version+10 WHERE channel=? AND project_id=? AND ticket_id=? AND observation_classification='pending'`, fixture.ticket.Ref.Channel, fixture.ticket.Ref.Project, fixture.ticket.Ref.Ticket); err == nil {
			t.Fatal("out-of-order pending transition accepted")
		}
		if _, err := fixture.db.FinalReviewAuthority(fixture.ctx, fixture.ticket.Ref, fixture.ticket.Version, fixture.fence); err != nil {
			t.Fatalf("rejected out-of-order write corrupted CI authority: %v", err)
		}
	})
	t.Run("tampered pending event", func(t *testing.T) {
		fixture := finalReviewLifecycleFixtureWithPending(t, 1)
		if _, err := fixture.db.db.ExecContext(fixture.ctx, `UPDATE events SET payload='{}' WHERE channel=? AND project_id=? AND ticket_id=? AND trigger='checks_pending'`, fixture.ticket.Ref.Channel, fixture.ticket.Ref.Project, fixture.ticket.Ref.Ticket); err != nil {
			t.Fatal(err)
		}
		if _, err := fixture.db.FinalReviewAuthority(fixture.ctx, fixture.ticket.Ref, fixture.ticket.Version, fixture.fence); !errors.Is(err, ErrEvidenceConflict) {
			t.Fatalf("tampered pending chain accepted: %v", err)
		}
	})
	t.Run("duplicate transition is physically rejected", func(t *testing.T) {
		fixture := finalReviewLifecycleFixtureWithPending(t, 1)
		_, err := fixture.db.db.ExecContext(fixture.ctx, `INSERT INTO ci_transition_evidence(channel,project_id,ticket_id,candidate_generation,candidate_head_sha,candidate_tree_sha,ticket_version,event_id,event_created_at,observation_classification,observation_digest,observation_ticket_version,observation_leader_epoch,observation_runner_epoch,prior_publication_witness_digest,prior_state,resulting_state,resulting_trigger,transition_digest,created_at) SELECT channel,project_id,ticket_id,candidate_generation,candidate_head_sha,candidate_tree_sha,ticket_version,event_id,event_created_at,observation_classification,observation_digest,observation_ticket_version,observation_leader_epoch,observation_runner_epoch,prior_publication_witness_digest,prior_state,resulting_state,resulting_trigger,transition_digest,created_at FROM ci_transition_evidence WHERE channel=? AND project_id=? AND ticket_id=? AND observation_classification='pending'`, fixture.ticket.Ref.Channel, fixture.ticket.Ref.Project, fixture.ticket.Ref.Ticket)
		if err == nil {
			t.Fatal("duplicate pending transition accepted")
		}
	})
}

func TestFinalReviewTransitionAuthenticatesCILineageOnWriteConnection(t *testing.T) {
	fixture := finalReviewLifecycleFixture(t)
	completeFinalReview(t, fixture)
	fixture.db.db.SetMaxOpenConns(1)
	ctx, cancel := context.WithTimeout(fixture.ctx, time.Second)
	defer cancel()
	if _, err := fixture.db.TransitionFinalReview(ctx, Transition{Ref: fixture.ticket.Ref, ExpectedVersion: fixture.ticket.Version, From: domain.StateReviewing, To: domain.StateWaitingApproval, Trigger: "review_pass", Fence: fixture.fence, EventPayload: `{}`}); err != nil {
		t.Fatalf("single-connection final review transition: %v", err)
	}
}
