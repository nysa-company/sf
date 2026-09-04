package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
	gitboundary "github.com/nysa-company/sf/internal/git"
	"github.com/nysa-company/sf/internal/phaseartifact"
	"github.com/nysa-company/sf/internal/workflowprompt"
)

type providerRetryWorktreeFixture struct {
	db                *Store
	ctx               context.Context
	ticket            Ticket
	fence             domain.Fence
	configDigest      string
	builder, reviewer ProviderQualification
	worktree          StoredWorktree
	base              string
	registeredHead    string
	verificationHead  string
	candidateHead     string
	verificationTree  string
	candidateTree     string
	sourceDigest      string
	plan              phaseartifact.Planner
	verification      phaseartifact.Verification
}

func newProviderRetryWorktreeFixture(t *testing.T, id string, oidWidth int) *providerRetryWorktreeFixture {
	t.Helper()
	db, ctx := openTestStore(t)
	configDigest := setupProviderProject(t, db, ctx)
	leader, err := db.AcquireLeader(ctx, domain.ChannelDev, "provider-retry-worktree-"+id)
	if err != nil {
		t.Fatal(err)
	}
	ref := domain.TicketRef{Channel: domain.ChannelDev, Project: "provider", Ticket: domain.TicketID(id)}
	sourceDigest := sha256Digest([]byte("provider-retry-source-" + id))
	if err := db.CreateTicket(ctx, Ticket{Ref: ref, SourceDigest: sourceDigest, Type: domain.TicketSpike, MergeMode: domain.MergeGuarded, CreatedAt: time.Now().UTC(), MaxDuration: time.Hour, MaxCostMicroUSD: 100}); err != nil {
		t.Fatal(err)
	}
	ticket, err := db.StartOrAdopt(ctx, ref, 1, "dev/provider/"+id, domain.Fence{LeaderEpoch: leader, RunnerEpoch: 1})
	if err != nil {
		t.Fatal(err)
	}
	fence := domain.Fence{LeaderEpoch: leader, RunnerEpoch: ticket.RunnerEpoch}
	base := strings.Repeat("a", oidWidth)
	registeredHead := base
	branch := "sf/dev/" + branchDigestPart(string(ref.Project)) + "/" + branchDigestPart(string(ref.Ticket)) + "-" + strings.Repeat("b", 32)
	branchKey := string(ref.Channel) + "\x00" + string(ref.Project) + "\x00" + string(ref.Ticket)
	if allocated, err := db.LoadOrStoreBranchUnderFence(ctx, branchKey, branch, ticket.Version, fence); err != nil || allocated != branch {
		t.Fatalf("allocate retry fixture branch=%q err=%v", allocated, err)
	}
	worktreePath, err := db.TicketWorktreePath(ref)
	if err != nil {
		t.Fatal(err)
	}
	var identity gitboundary.Identity
	if err := json.Unmarshal([]byte(repositoryCommandIdentity(t, "/tmp/provider", worktreePath, branch, "main")), &identity); err != nil {
		t.Fatal(err)
	}
	identity.BaseHead = base
	identityJSON, err := json.Marshal(identity)
	if err != nil {
		t.Fatal(err)
	}
	creation := GitMutationIntent{EffectFence: EffectFence{Ref: ref, TicketVersion: ticket.Version, Fence: fence}, RequestDigest: "sha256:" + strings.Repeat("0", 64), Repository: "/tmp/provider", Worktree: worktreePath, Branch: branch, Operation: "create-worktree", BaseRef: "main", ExpectedBaseOID: base, ExpectedHeadOID: registeredHead}
	creation.SemanticKey = CanonicalGitMutationSemanticKey(creation)
	if _, err := db.PlanEffect(ctx, EffectPlan{SemanticKey: creation.SemanticKey, Ref: ref, Kind: "git/create-worktree", TicketVersion: ticket.Version, Fence: fence, RequestDigest: creation.RequestDigest}); err != nil {
		t.Fatal(err)
	}
	claim, err := db.IssueGitMutationClaim(ctx, creation)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ConfirmEffect(ctx, EffectFence{SemanticKey: claim.SemanticKey, Ref: claim.TicketRef, TicketVersion: claim.TicketVersion, Fence: domain.Fence{LeaderEpoch: claim.LeaderEpoch, RunnerEpoch: claim.RunnerEpoch, ClaimEpoch: claim.ClaimEpoch}}, string(identityJSON)); err != nil {
		t.Fatal(err)
	}
	if err := db.RegisterWorktree(ctx, WorktreeRegistration{Ref: ref, ExpectedVersion: ticket.Version, Fence: fence, Path: worktreePath, Branch: branch, IdentityJSON: identityJSON, BaseSHA: base, HeadSHA: registeredHead}); err != nil {
		t.Fatal(err)
	}
	worktree, err := db.Worktree(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	builder, reviewer := setupProviderPair(t, db, ctx)
	return &providerRetryWorktreeFixture{
		db: db, ctx: ctx, ticket: ticket, fence: fence, configDigest: configDigest,
		builder: builder, reviewer: reviewer, worktree: worktree,
		base: base, registeredHead: registeredHead,
		verificationHead: strings.Repeat("c", oidWidth), candidateHead: strings.Repeat("e", oidWidth),
		verificationTree: strings.Repeat("d", oidWidth), candidateTree: strings.Repeat("f", oidWidth),
		sourceDigest: sourceDigest,
	}
}

func (f *providerRetryWorktreeFixture) request(t *testing.T, phase domain.Phase, role string, binding contracts.RuntimeBinding) ProviderAttemptRequest {
	t.Helper()
	request := supervised(t, ProviderAttemptRequest{Ref: f.ticket.Ref, ExpectedVersion: f.ticket.Version, Fence: f.fence, Phase: phase, Role: role, Binding: binding, ConfigDigest: f.configDigest, Capacity: 1, At: time.Now().UTC()})
	request.Repository = "/tmp/provider"
	request.Worktree = f.worktree.Path
	request.WorktreeIdentity = string(f.worktree.IdentityJSON)
	request.BaseSHA = f.base
	request.Input.Repository = request.Repository
	request.Input.Worktree = request.Worktree
	request.Input.WorktreeIdentity = request.WorktreeIdentity
	request.Input.BaseSHA = request.BaseSHA
	if phase == domain.PhaseReview {
		proofBytes, err := workflowprompt.CanonicalVerificationProofBytes(f.verification)
		if err != nil {
			t.Fatal(err)
		}
		request.ExpectedHead = f.candidateHead
		request.ExpectedProof = sha256Digest(proofBytes)
	}
	return request
}

func (f *providerRetryWorktreeFixture) complete(t *testing.T, phase domain.Phase, role string, binding contracts.RuntimeBinding, raw []byte, validation phaseartifact.Validation) ProviderAttemptClaim {
	t.Helper()
	request := f.request(t, phase, role, binding)
	claim, err := f.db.BeginProviderAttempt(f.ctx, request)
	if err != nil {
		t.Fatalf("begin provider phase=%s role=%s valid_input=%v state=%s version=%d fence=%+v: %v", phase, role, validProviderAttemptInput(request), f.ticket.State, f.ticket.Version, f.fence, err)
	}
	if err := f.db.RecordProviderLaunch(f.ctx, claim, contracts.ProviderLaunch{PID: int(claim.ID), PGID: int(claim.ID), BootIdentity: "provider-retry-worktree", ProcessStartIdentity: fmt.Sprintf("provider-retry-worktree-%d", claim.ID), Worktree: claim.Worktree}); err != nil {
		t.Fatalf("record provider launch phase=%s role=%s: %v", phase, role, err)
	}
	if _, err := f.db.CompleteProviderAttemptSuccess(f.ctx, claim, proof(t, claim), f.ticket.Version, f.fence, contracts.PhaseResult{Provider: claim.Binding.Identity, Artifact: raw, UsageTrusted: true, UsageUnits: 1}, validation, time.Now().UTC()); err != nil {
		t.Fatalf("complete provider phase=%s role=%s: %v", phase, role, err)
	}
	return claim
}

func (f *providerRetryWorktreeFixture) recordConfirmedCommit(t *testing.T, parent, child, tree, digestChar string) {
	t.Helper()
	intent := GitMutationIntent{EffectFence: EffectFence{Ref: f.ticket.Ref, TicketVersion: f.ticket.Version, Fence: f.fence}, RequestDigest: "sha256:" + strings.Repeat(digestChar, 64), Repository: "/tmp/provider", Worktree: f.worktree.Path, Branch: f.worktree.Branch, Operation: "commit", BaseRef: "main", ExpectedBaseOID: f.base, ExpectedHeadOID: parent}
	intent.SemanticKey = CanonicalGitMutationSemanticKey(intent)
	if _, err := f.db.PlanEffect(f.ctx, EffectPlan{SemanticKey: intent.SemanticKey, Ref: intent.Ref, Kind: "git/commit", TicketVersion: intent.TicketVersion, Fence: intent.Fence, RequestDigest: intent.RequestDigest}); err != nil {
		t.Fatal(err)
	}
	claim, err := f.db.IssueGitMutationClaim(f.ctx, intent)
	if err != nil {
		t.Fatalf("issue confirmed commit mutation: %v", err)
	}
	lease, err := f.db.AcquireGitMutation(f.ctx, claim)
	if err != nil {
		t.Fatalf("acquire confirmed commit mutation: %v", err)
	}
	if err := lease.(contracts.GitMutationRecoveryFactsLease).RecordPreparedCommit(f.ctx, child, tree); err != nil {
		t.Fatalf("record prepared commit: %v", err)
	}
	if err := lease.Release(); err != nil {
		t.Fatalf("release confirmed commit mutation: %v", err)
	}
	if _, err := f.db.ConfirmPreparedCommit(f.ctx, claim, contracts.PreparedCommitObservation{CommitOID: child, ParentOID: parent, TreeOID: tree}); err != nil {
		t.Fatalf("confirm prepared commit: %v", err)
	}
}

func (f *providerRetryWorktreeFixture) completePlan(t *testing.T) {
	t.Helper()
	f.plan = phaseartifact.Planner{Schema: "sf.planner/v1", Acceptance: []string{"retry remains exact"}, Proof: phaseartifact.ProofPlan{Kind: phaseartifact.ProofReport, Command: []string{"go", "test"}, Details: "report"}, Paths: []string{"internal"}, Commands: [][]string{{"go", "test"}}, Risks: []string{"none"}}
	raw, err := json.Marshal(f.plan)
	if err != nil {
		t.Fatal(err)
	}
	claim := f.complete(t, domain.PhasePlanning, "planner", runtime(f.builder), raw, phaseartifact.Validation{TicketType: domain.TicketSpike})
	key := ProviderAttemptResultKey{AttemptID: claim.ID, Ref: f.ticket.Ref, Phase: domain.PhasePlanning, Attempt: claim.Attempt}
	if _, err := f.db.RecordPlan(f.ctx, PlanArtifact{Ref: f.ticket.Ref, ExpectedVersion: f.ticket.Version, Fence: f.fence, Document: PlanDocument{Planner: &f.plan, ProviderResult: &key, Acceptance: f.plan.Acceptance, ProofKind: string(f.plan.Proof.Kind), Paths: f.plan.Paths, Commands: f.plan.Commands, Risks: f.plan.Risks}}); err != nil {
		t.Fatal(err)
	}
}

func (f *providerRetryWorktreeFixture) enterVerification(t *testing.T) {
	t.Helper()
	f.completePlan(t)
	if _, err := f.db.TransitionPlan(f.ctx, Transition{Ref: f.ticket.Ref, ExpectedVersion: f.ticket.Version, From: domain.StatePlanning, To: domain.StateVerifying, Trigger: "phase_pass", Fence: f.fence, EventPayload: "{}"}); err != nil {
		t.Fatal(err)
	}
	f.reload(t)
}

func (f *providerRetryWorktreeFixture) enterBuilding(t *testing.T) {
	f.enterBuildingWithCommitParent(t, f.registeredHead)
}

func (f *providerRetryWorktreeFixture) enterBuildingWithCommitParent(t *testing.T, confirmedParent string) {
	t.Helper()
	if f.ticket.State == domain.StatePlanning {
		f.enterVerification(t)
	}
	planID, err := workflowprompt.NewPlanIdentity(f.plan)
	if err != nil {
		t.Fatal(err)
	}
	f.verification = phaseartifact.Verification{Schema: "sf.verification/v1", AcceptanceDigest: planID.Digest, ProofKind: phaseartifact.ProofReport, OwnedFiles: []string{"internal/retry.go"}, Command: []string{"go", "test", "./..."}, PrebuildOutcome: "report_ready", EvidenceDigest: sha256Digest([]byte("provider-retry-verification"))}
	raw, err := json.Marshal(f.verification)
	if err != nil {
		t.Fatal(err)
	}
	claim := f.complete(t, domain.PhaseVerification, "reviewer", runtime(f.reviewer), raw, phaseartifact.Validation{TicketType: domain.TicketSpike, AcceptanceDigest: planID.Digest})
	intent, err := workflowprompt.CanonicalVerificationIntentBytes(f.verification)
	if err != nil {
		t.Fatal(err)
	}
	proofBytes, err := workflowprompt.CanonicalVerificationProofBytes(f.verification)
	if err != nil {
		t.Fatal(err)
	}
	f.recordConfirmedCommit(t, confirmedParent, f.verificationHead, f.verificationTree, "1")
	key := ProviderAttemptResultKey{AttemptID: claim.ID, Ref: f.ticket.Ref, Phase: domain.PhaseVerification, Attempt: claim.Attempt}
	command := completeEvidenceRepositoryCommand(t, f.db, f.ctx, RepositoryCommandPurposePrebuildVerification, f.ticket.Ref, f.ticket.Version, f.fence, key, sha256Digest(intent), sha256Digest(proofBytes), "", "", 0)
	if _, err := f.db.RecordVerification(f.ctx, VerificationArtifact{Ref: f.ticket.Ref, ExpectedVersion: f.ticket.Version, Fence: f.fence, Intent: intent, Proof: proofBytes, OwnedFiles: f.verification.OwnedFiles, CheckpointID: f.verificationHead, ProviderResult: &key, Checkpoint: CommitObservation{CommitOID: f.verificationHead, ParentOID: f.registeredHead, TreeOID: f.verificationTree}, CommandResult: command}); err != nil {
		t.Fatal(err)
	}
	if _, err := f.db.TransitionVerification(f.ctx, Transition{Ref: f.ticket.Ref, ExpectedVersion: f.ticket.Version, From: domain.StateVerifying, To: domain.StateBuilding, Trigger: "phase_pass", Fence: f.fence, EventPayload: "{}"}); err != nil {
		t.Fatal(err)
	}
	f.reload(t)
}

func (f *providerRetryWorktreeFixture) enterReviewing(t *testing.T) {
	t.Helper()
	if f.ticket.State != domain.StateBuilding {
		f.enterBuilding(t)
	}
	raw := []byte(`{"schema":"sf.builder/v1","summary":"provider retry candidate","changed_files":["internal/retry.go"],"commands":[["go","test"]]}`)
	claim := f.complete(t, domain.PhaseBuild, "builder", runtime(f.builder), raw, phaseartifact.Validation{TicketType: domain.TicketSpike})
	_, parsed, err := f.db.LoadHistoricalProviderAttemptResult(f.ctx, ProviderAttemptResultKey{AttemptID: claim.ID, Ref: f.ticket.Ref, Phase: domain.PhaseBuild, Attempt: claim.Attempt})
	if err != nil || parsed.Builder == nil {
		t.Fatalf("builder result parsed=%+v err=%v", parsed, err)
	}
	builderDigest, err := phaseartifact.BuilderEvidenceDigest(*parsed.Builder)
	if err != nil {
		t.Fatal(err)
	}
	intent, _ := workflowprompt.CanonicalVerificationIntentBytes(f.verification)
	proofBytes, _ := workflowprompt.CanonicalVerificationProofBytes(f.verification)
	policy := sha256Digest([]byte("provider-retry-policy"))
	f.recordConfirmedCommit(t, f.verificationHead, f.candidateHead, f.candidateTree, "2")
	key := ProviderAttemptResultKey{AttemptID: claim.ID, Ref: f.ticket.Ref, Phase: domain.PhaseBuild, Attempt: claim.Attempt}
	command := completeEvidenceRepositoryCommand(t, f.db, f.ctx, RepositoryCommandPurposePostbuildCandidate, f.ticket.Ref, f.ticket.Version, f.fence, key, sha256Digest(intent), sha256Digest(proofBytes), f.verificationHead, "sha256:"+policy, 0)
	snapshot := domain.CandidateSnapshot{Generation: 1, BaseSHA: f.base, HeadSHA: f.candidateHead, TreeSHA: f.candidateTree, SourceDigest: f.sourceDigest, VerificationIntentDigest: sha256Digest(intent), ProofDigest: sha256Digest(proofBytes), CommandPolicyDigest: policy, BuilderEvidenceDigest: builderDigest}
	if _, err := f.db.RecordCandidate(f.ctx, CandidateEvidence{Ref: f.ticket.Ref, ExpectedVersion: f.ticket.Version, Fence: f.fence, Snapshot: snapshot, BuilderResult: key, Commit: CommitObservation{CommitOID: f.candidateHead, ParentOID: f.verificationHead, TreeOID: f.candidateTree}, Reason: "provider retry", CommandResult: command}); err != nil {
		t.Fatal(err)
	}
	stored, err := f.db.RecoverableCandidate(f.ctx, f.ticket.Ref)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.db.TransitionCandidate(f.ctx, Transition{Ref: f.ticket.Ref, ExpectedVersion: f.ticket.Version, From: domain.StateBuilding, To: domain.StateReviewing, Trigger: "phase_pass", Fence: f.fence, EventPayload: "{}"}, stored.Snapshot); err != nil {
		t.Fatal(err)
	}
	f.reload(t)
}

func (f *providerRetryWorktreeFixture) enterPendingAmendmentVerification(t *testing.T) {
	t.Helper()
	f.enterBuilding(t)
	priorProof, err := workflowprompt.CanonicalVerificationProofBytes(f.verification)
	if err != nil {
		t.Fatal(err)
	}
	proposal := f.verification
	proposal.EvidenceDigest = sha256Digest([]byte("provider-retry-rejected-amendment"))
	proposalProof, err := workflowprompt.CanonicalVerificationProofBytes(proposal)
	if err != nil {
		t.Fatal(err)
	}
	builderArtifact := phaseartifact.Builder{
		Schema:       "sf.builder/v1",
		Summary:      "propose a verification amendment",
		ChangedFiles: []string{"internal/retry.go"},
		Commands:     [][]string{{"go", "test"}},
		AmendmentRequest: &phaseartifact.AmendmentRequest{
			OldProofDigest:  sha256Digest(priorProof),
			ProposedDigest:  sha256Digest(proposalProof),
			ProposedCommand: proposal.Command,
			Reason:          "exercise the rejected amendment endpoint",
		},
	}
	raw, err := json.Marshal(builderArtifact)
	if err != nil {
		t.Fatal(err)
	}
	builderClaim := f.complete(t, domain.PhaseBuild, "builder", runtime(f.builder), raw, phaseartifact.Validation{TicketType: f.ticket.Type, ProtectedVerification: f.verification.OwnedFiles})
	builderKey := ProviderAttemptResultKey{AttemptID: builderClaim.ID, Ref: f.ticket.Ref, Phase: domain.PhaseBuild, Attempt: builderClaim.Attempt}
	// A discarded Builder generation is a real Store-owned commit, but it is
	// not the semantic checkpoint selected by amendment_rejected.
	discarded := strings.Repeat("8", len(f.verificationHead))
	f.recordConfirmedCommit(t, f.verificationHead, discarded, strings.Repeat("9", len(f.verificationTree)), "8")
	if _, err := f.db.TransitionVerificationAmendmentRequest(f.ctx, Transition{Ref: f.ticket.Ref, ExpectedVersion: f.ticket.Version, From: domain.StateBuilding, To: domain.StateVerifying, Trigger: "verification_amendment_requested", Fence: f.fence, EventPayload: "{}"}, builderKey); err != nil {
		t.Fatalf("transition amendment request: %v", err)
	}
	f.reload(t)
}

func (f *providerRetryWorktreeFixture) enterRejectedAmendmentBuilding(t *testing.T) {
	t.Helper()
	f.enterPendingAmendmentVerification(t)
	reviewerRaw, err := json.Marshal(f.verification)
	if err != nil {
		t.Fatal(err)
	}
	reviewerClaim := f.complete(t, domain.PhaseVerification, "reviewer", runtime(f.reviewer), reviewerRaw, phaseartifact.Validation{TicketType: f.ticket.Type, AcceptanceDigest: f.verification.AcceptanceDigest})
	reviewerKey := ProviderAttemptResultKey{AttemptID: reviewerClaim.ID, Ref: f.ticket.Ref, Phase: domain.PhaseVerification, Attempt: reviewerClaim.Attempt}
	if decision, err := f.db.VerificationAmendmentDecision(f.ctx, f.ticket.Ref, f.ticket.Version, f.fence, reviewerKey); err != nil || decision != VerificationAmendmentRejected {
		t.Fatalf("amendment decision=%q err=%v", decision, err)
	}
	if _, err := f.db.TransitionVerificationAmendmentRejected(f.ctx, Transition{Ref: f.ticket.Ref, ExpectedVersion: f.ticket.Version, From: domain.StateVerifying, To: domain.StateBuilding, Trigger: "amendment_rejected", Fence: f.fence, EventPayload: "{}"}, reviewerKey); err != nil {
		t.Fatalf("transition amendment rejected: %v", err)
	}
	f.reload(t)
}

func (f *providerRetryWorktreeFixture) reload(t *testing.T) {
	t.Helper()
	ticket, err := f.db.Ticket(f.ctx, f.ticket.Ref)
	if err != nil {
		t.Fatal(err)
	}
	f.ticket = ticket
	f.fence = domain.Fence{LeaderEpoch: f.fence.LeaderEpoch, RunnerEpoch: ticket.RunnerEpoch}
}

func (f *providerRetryWorktreeFixture) exhaust(t *testing.T, outcome string) Ticket {
	t.Helper()
	phase, ok := providerPhaseForState(f.ticket.State)
	if !ok {
		t.Fatalf("cannot exhaust state %s", f.ticket.State)
	}
	role := "reviewer"
	binding := runtime(f.reviewer)
	if phase == domain.PhasePlanning || phase == domain.PhaseBuild {
		role, binding = map[domain.Phase]string{domain.PhasePlanning: "planner", domain.PhaseBuild: "builder"}[phase], runtime(f.builder)
	}
	for attempt := 0; attempt < 2; attempt++ {
		claim, err := f.db.BeginProviderAttempt(f.ctx, f.request(t, phase, role, binding))
		if err != nil {
			t.Fatalf("begin %s retry pair attempt %d: %v", phase, attempt+1, err)
		}
		if err := f.db.FinishProviderAttempt(f.ctx, claim, proof(t, claim), f.ticket.Version, f.fence, "failed", outcome, 1, time.Now().UTC()); err != nil {
			t.Fatalf("finish %s retry pair attempt %d: %v", phase, attempt+1, err)
		}
	}
	if _, err := f.db.TransitionProviderExhausted(f.ctx, Transition{Ref: f.ticket.Ref, ExpectedVersion: f.ticket.Version, From: f.ticket.State, To: domain.StatePaused, ResumeState: f.ticket.State, Trigger: "retry_or_correction_exhausted", Fence: f.fence}); err != nil {
		t.Fatalf("pause exhausted %s: %v", phase, err)
	}
	f.reload(t)
	return f.ticket
}

func TestProviderRetryWorktreeProofReturnsSemanticHeadsAndReplays(t *testing.T) {
	tests := []struct {
		name     string
		phase    domain.Phase
		oidWidth int
		advance  func(*providerRetryWorktreeFixture, *testing.T)
		expected func(*providerRetryWorktreeFixture) string
	}{
		{name: "planning", phase: domain.PhasePlanning, oidWidth: 40, expected: func(f *providerRetryWorktreeFixture) string { return f.registeredHead }},
		{name: "initial-verification", phase: domain.PhaseVerification, oidWidth: 40, advance: func(f *providerRetryWorktreeFixture, t *testing.T) { f.enterVerification(t) }, expected: func(f *providerRetryWorktreeFixture) string { return f.registeredHead }},
		{name: "building-sha256", phase: domain.PhaseBuild, oidWidth: 64, advance: func(f *providerRetryWorktreeFixture, t *testing.T) { f.enterBuilding(t) }, expected: func(f *providerRetryWorktreeFixture) string { return f.verificationHead }},
		{name: "reviewing", phase: domain.PhaseReview, oidWidth: 40, advance: func(f *providerRetryWorktreeFixture, t *testing.T) { f.enterReviewing(t) }, expected: func(f *providerRetryWorktreeFixture) string { return f.candidateHead }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newProviderRetryWorktreeFixture(t, "SF-provider-retry-worktree-"+test.name, test.oidWidth)
			if test.advance != nil {
				test.advance(fixture, t)
			}
			paused := fixture.exhaust(t, "invalid_artifact")
			proofValue, err := fixture.db.ProviderRetryWorktreeProof(fixture.ctx, paused.Ref, paused.Version, fixture.fence)
			if err != nil {
				t.Fatal(err)
			}
			if proofValue.Ref != paused.Ref || proofValue.Phase != test.phase || proofValue.Version != paused.Version || proofValue.Fence != fixture.fence || proofValue.Worktree.Path != fixture.worktree.Path || proofValue.ExpectedHead != test.expected(fixture) {
				t.Fatalf("proof=%+v", proofValue)
			}
			replayed, err := fixture.db.ProviderRetryWorktreeProof(fixture.ctx, paused.Ref, paused.Version, fixture.fence)
			if err != nil || !reflect.DeepEqual(replayed, proofValue) {
				t.Fatalf("replay=%+v err=%v", replayed, err)
			}
			if err := fixture.db.SealRuntimeControl(fixture.ctx, paused.Ref); err != nil {
				t.Fatal(err)
			}
			if _, err := fixture.db.TransitionProviderRetry(fixture.ctx, Transition{Ref: paused.Ref, ExpectedVersion: paused.Version, From: domain.StatePaused, To: paused.ResumeState, ResumeState: paused.ResumeState, Trigger: "operator_retry", Fence: fixture.fence}); err != nil {
				t.Fatalf("consume provider retry: %v", err)
			}
			fixture.reload(t)
			active, err := fixture.db.ProviderRetryWorktreeProof(fixture.ctx, paused.Ref, fixture.ticket.Version, fixture.fence)
			if err != nil || active.Phase != proofValue.Phase || active.ExpectedHead != proofValue.ExpectedHead || active.Worktree.Path != proofValue.Worktree.Path {
				t.Fatalf("active proof=%+v err=%v paused=%+v", active, err, proofValue)
			}
			if replay, err := fixture.db.ProviderRetryReplay(fixture.ctx, fixture.ticket); err != nil || !replay {
				t.Fatalf("sealed active replay=%v err=%v", replay, err)
			}
			var storePath string
			if err := fixture.db.db.QueryRowContext(fixture.ctx, `SELECT file FROM pragma_database_list WHERE name='main'`).Scan(&storePath); err != nil || storePath == "" {
				t.Fatalf("store path=%q err=%v", storePath, err)
			}
			if err := fixture.db.Close(); err != nil {
				t.Fatal(err)
			}
			reopened, err := Open(fixture.ctx, storePath)
			if err != nil {
				t.Fatalf("reopen provider retry store: %v", err)
			}
			defer reopened.Close()
			fixture.db = reopened
			newLeader, err := fixture.db.AcquireLeader(fixture.ctx, paused.Ref.Channel, "provider-retry-worktree-replay-"+test.name)
			if err != nil {
				t.Fatal(err)
			}
			if changed, err := fixture.db.FenceRecoveredRunners(fixture.ctx, paused.Ref.Channel, newLeader); err != nil || changed != 1 {
				t.Fatalf("fence retry runner changed=%d err=%v", changed, err)
			}
			fixture.reload(t)
			fixture.fence.LeaderEpoch = newLeader
			recovered, err := fixture.db.ProviderRetryWorktreeProof(fixture.ctx, paused.Ref, fixture.ticket.Version, fixture.fence)
			if err != nil || recovered.Phase != proofValue.Phase || recovered.ExpectedHead != proofValue.ExpectedHead || recovered.Worktree.Path != proofValue.Worktree.Path {
				t.Fatalf("recovered active proof=%+v err=%v paused=%+v", recovered, err, proofValue)
			}
			if replay, err := fixture.db.ProviderRetryReplay(fixture.ctx, fixture.ticket); err != nil || !replay {
				t.Fatalf("sealed recovered replay=%v err=%v", replay, err)
			}
			durableStopped, err := fixture.db.StoppedRuntimeTicket(fixture.ctx, fixture.ticket.Ref)
			if err != nil || durableStopped.State != "" || durableStopped.ResumeState != "" || durableStopped.Version != paused.Version || durableStopped.RunnerEpoch != paused.RunnerEpoch {
				t.Fatalf("durable stopped ticket=%+v err=%v", durableStopped, err)
			}
			capability, err := fixture.db.ProviderRetryRearmProof(fixture.ctx, fixture.ticket.Ref, durableStopped)
			if err != nil || capability == nil {
				t.Fatalf("provider retry rearm capability=%v err=%v", capability, err)
			}
		})
	}
}

func TestProviderRetryReplayDistinguishesInstalledAndRestartedRuntimeAdmission(t *testing.T) {
	fixture := newProviderRetryWorktreeFixture(t, "SF-provider-retry-runtime-replay", 40)
	paused := fixture.exhaust(t, "invalid_artifact")
	if err := fixture.db.SealRuntimeControl(fixture.ctx, paused.Ref); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.db.TransitionProviderRetry(fixture.ctx, Transition{Ref: paused.Ref, ExpectedVersion: paused.Version, From: domain.StatePaused, To: paused.ResumeState, ResumeState: paused.ResumeState, Trigger: "operator_retry", Fence: fixture.fence}); err != nil {
		t.Fatal(err)
	}
	fixture.reload(t)
	if state, err := fixture.db.ProviderRetryRuntimeReplay(fixture.ctx, fixture.ticket); err != nil || state != ProviderRetryNeedsRearm {
		t.Fatalf("initial sealed provider retry state=%v err=%v", state, err)
	}
	capability, err := fixture.db.ProviderRetryRearmProof(fixture.ctx, fixture.ticket.Ref, paused)
	if err != nil {
		t.Fatal(err)
	}
	var admission *RuntimeAdmissionCapability
	if err := fixture.db.ActivateRearm(fixture.ctx, capability, func(value *RuntimeAdmissionCapability) error {
		if _, version, fence, ok := value.ConsumeRuntimeAdmission(); !ok || version != fixture.ticket.Version || fence != fixture.fence {
			return fmt.Errorf("runtime admission version=%d fence=%+v issued=%v", version, fence, ok)
		}
		admission = value
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if replay, err := fixture.db.ProviderRetryReplay(fixture.ctx, fixture.ticket); err != nil || !replay {
		t.Fatalf("armed provider retry replay=%v err=%v", replay, err)
	}
	if state, err := fixture.db.ProviderRetryRuntimeReplay(fixture.ctx, fixture.ticket); err != nil || state != ProviderRetryAlreadyRearmed {
		t.Fatalf("armed provider retry state=%v err=%v", state, err)
	}
	if needed, err := fixture.db.RuntimeRearmNeeded(fixture.ctx, fixture.ticket.Ref); err != nil || needed {
		t.Fatalf("armed provider retry rearm needed=%v err=%v", needed, err)
	}
	if admission == nil {
		t.Fatal("runtime admission was not installed")
	}
	if err := admission.OpenStoreAdmission(fixture.ctx); err != nil {
		t.Fatal(err)
	}
	if replay, err := fixture.db.ProviderRetryReplay(fixture.ctx, fixture.ticket); err != nil || !replay {
		t.Fatalf("open provider retry replay=%v err=%v", replay, err)
	}
	if state, err := fixture.db.ProviderRetryRuntimeReplay(fixture.ctx, fixture.ticket); err != nil || state != ProviderRetryAlreadyRearmed {
		t.Fatalf("open provider retry state=%v err=%v", state, err)
	}
	if needed, err := fixture.db.RuntimeRearmNeeded(fixture.ctx, fixture.ticket.Ref); err != nil || needed {
		t.Fatalf("open provider retry rearm needed=%v err=%v", needed, err)
	}

	var storePath string
	if err := fixture.db.db.QueryRowContext(fixture.ctx, `SELECT file FROM pragma_database_list WHERE name='main'`).Scan(&storePath); err != nil || storePath == "" {
		t.Fatalf("store path=%q err=%v", storePath, err)
	}
	if err := fixture.db.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(fixture.ctx, storePath)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	fixture.db = reopened
	newLeader, err := fixture.db.AcquireLeader(fixture.ctx, paused.Ref.Channel, "provider-retry-runtime-replay-restart")
	if err != nil {
		t.Fatal(err)
	}
	if changed, err := fixture.db.FenceRecoveredRunners(fixture.ctx, paused.Ref.Channel, newLeader); err != nil || changed != 1 {
		t.Fatalf("fence restarted provider retry changed=%d err=%v", changed, err)
	}
	fixture.reload(t)
	fixture.fence.LeaderEpoch = newLeader
	if replay, err := fixture.db.ProviderRetryReplay(fixture.ctx, fixture.ticket); err != nil || !replay {
		t.Fatalf("restarted sealed provider retry replay=%v err=%v", replay, err)
	}
	if state, err := fixture.db.ProviderRetryRuntimeReplay(fixture.ctx, fixture.ticket); err != nil || state != ProviderRetryNeedsRearm {
		t.Fatalf("restarted sealed provider retry state=%v err=%v", state, err)
	}
	if needed, err := fixture.db.RuntimeRearmNeeded(fixture.ctx, fixture.ticket.Ref); err != nil || !needed {
		t.Fatalf("restarted provider retry rearm needed=%v err=%v", needed, err)
	}
	durableStopped, err := fixture.db.StoppedRuntimeTicket(fixture.ctx, fixture.ticket.Ref)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.db.ProviderRetryRearmProof(fixture.ctx, fixture.ticket.Ref, durableStopped); err != nil {
		t.Fatalf("restarted provider retry rearm proof: %v", err)
	}
}

func TestProviderRetryActivationFailurePreservesExhaustionStopForReplay(t *testing.T) {
	fixture := newProviderRetryWorktreeFixture(t, "SF-provider-retry-install-failure", 40)
	paused := fixture.exhaust(t, "invalid_artifact")
	if err := fixture.db.SealRuntimeControl(fixture.ctx, paused.Ref); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.db.TransitionProviderRetry(fixture.ctx, Transition{Ref: paused.Ref, ExpectedVersion: paused.Version, From: domain.StatePaused, To: paused.ResumeState, ResumeState: paused.ResumeState, Trigger: "operator_retry", Fence: fixture.fence}); err != nil {
		t.Fatal(err)
	}
	fixture.reload(t)
	capability, err := fixture.db.ProviderRetryRearmProof(fixture.ctx, fixture.ticket.Ref, paused)
	if err != nil {
		t.Fatal(err)
	}
	installErr := errors.New("injected provider retry runtime install failure")
	if err := fixture.db.ActivateRearm(fixture.ctx, capability, func(admission *RuntimeAdmissionCapability) error {
		if ref, version, fence, ok := admission.ConsumeRuntimeAdmission(); !ok || ref != fixture.ticket.Ref || version != fixture.ticket.Version || fence != fixture.fence {
			return fmt.Errorf("provider retry admission ref=%+v version=%d fence=%+v issued=%v", ref, version, fence, ok)
		}
		return installErr
	}); !errors.Is(err, installErr) {
		t.Fatalf("provider retry install failure=%v", err)
	}

	var state string
	var stopVersion, stopLeader, stopRunner uint64
	if err := fixture.db.db.QueryRowContext(fixture.ctx, `SELECT state,stop_version,stop_leader_epoch,stop_runner_epoch
		FROM runtime_ticket_controls WHERE channel=? AND project_id=? AND ticket_id=?`, fixture.ticket.Ref.Channel, fixture.ticket.Ref.Project, fixture.ticket.Ref.Ticket).Scan(&state, &stopVersion, &stopLeader, &stopRunner); err != nil {
		t.Fatal(err)
	}
	if state != "sealed" || stopVersion != paused.Version || stopLeader != fixture.fence.LeaderEpoch || stopRunner != paused.RunnerEpoch {
		t.Fatalf("failed activation control state=%q stop=(%d,%d,%d) want=(%d,%d,%d)", state, stopVersion, stopLeader, stopRunner, paused.Version, fixture.fence.LeaderEpoch, paused.RunnerEpoch)
	}
	if replay, err := fixture.db.ProviderRetryRuntimeReplay(fixture.ctx, fixture.ticket); err != nil || replay != ProviderRetryNeedsRearm {
		t.Fatalf("failed activation replay=%v err=%v", replay, err)
	}

	var storePath string
	if err := fixture.db.db.QueryRowContext(fixture.ctx, `SELECT file FROM pragma_database_list WHERE name='main'`).Scan(&storePath); err != nil || storePath == "" {
		t.Fatalf("store path=%q err=%v", storePath, err)
	}
	if err := fixture.db.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(fixture.ctx, storePath)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	fixture.db = reopened
	newLeader, err := fixture.db.AcquireLeader(fixture.ctx, paused.Ref.Channel, "provider-retry-install-failure-restart")
	if err != nil {
		t.Fatal(err)
	}
	if changed, err := fixture.db.FenceRecoveredRunners(fixture.ctx, paused.Ref.Channel, newLeader); err != nil || changed != 1 {
		t.Fatalf("fence failed provider retry activation changed=%d err=%v", changed, err)
	}
	fixture.reload(t)
	fixture.fence.LeaderEpoch = newLeader
	if replay, err := fixture.db.ProviderRetryRuntimeReplay(fixture.ctx, fixture.ticket); err != nil || replay != ProviderRetryNeedsRearm {
		t.Fatalf("restarted failed activation replay=%v err=%v", replay, err)
	}
	durableStopped, err := fixture.db.StoppedRuntimeTicket(fixture.ctx, fixture.ticket.Ref)
	if err != nil || durableStopped.Version != paused.Version || durableStopped.RunnerEpoch != paused.RunnerEpoch {
		t.Fatalf("restarted failed activation stop=%+v err=%v", durableStopped, err)
	}
	retryCapability, err := fixture.db.ProviderRetryRearmProof(fixture.ctx, fixture.ticket.Ref, durableStopped)
	if err != nil {
		t.Fatal(err)
	}
	if err := fixture.db.ActivateRearm(fixture.ctx, retryCapability, func(admission *RuntimeAdmissionCapability) error {
		if ref, version, fence, ok := admission.ConsumeRuntimeAdmission(); !ok || ref != fixture.ticket.Ref || version != fixture.ticket.Version || fence != fixture.fence {
			return fmt.Errorf("restarted provider retry admission ref=%+v version=%d fence=%+v issued=%v", ref, version, fence, ok)
		}
		return nil
	}); err != nil {
		t.Fatalf("restarted provider retry activation: %v", err)
	}
	if replay, err := fixture.db.ProviderRetryRuntimeReplay(fixture.ctx, fixture.ticket); err != nil || replay != ProviderRetryAlreadyRearmed {
		t.Fatalf("restarted installed activation replay=%v err=%v", replay, err)
	}
}

func TestProviderRetryWorktreeProofAcceptsAuthenticatedOrdinaryFailurePair(t *testing.T) {
	fixture := newProviderRetryWorktreeFixture(t, "SF-provider-retry-worktree-ordinary", 40)
	paused := fixture.exhaust(t, "failed")
	proofValue, err := fixture.db.ProviderRetryWorktreeProof(fixture.ctx, paused.Ref, paused.Version, fixture.fence)
	if err != nil || proofValue.Phase != domain.PhasePlanning || proofValue.ExpectedHead != fixture.registeredHead {
		t.Fatalf("ordinary proof=%+v err=%v", proofValue, err)
	}
}

func TestProviderRetryWorktreeProofDoesNotBlessUnrelatedConfirmedCommits(t *testing.T) {
	t.Run("clean orphan before initial verification", func(t *testing.T) {
		fixture := newProviderRetryWorktreeFixture(t, "SF-provider-retry-worktree-orphan", 40)
		fixture.completePlan(t)
		foreign := strings.Repeat("9", 40)
		fixture.recordConfirmedCommit(t, fixture.registeredHead, foreign, strings.Repeat("8", 40), "3")
		if _, err := fixture.db.TransitionPlan(fixture.ctx, Transition{Ref: fixture.ticket.Ref, ExpectedVersion: fixture.ticket.Version, From: domain.StatePlanning, To: domain.StateVerifying, Trigger: "phase_pass", Fence: fixture.fence, EventPayload: "{}"}); err != nil {
			t.Fatal(err)
		}
		fixture.reload(t)
		paused := fixture.exhaust(t, "invalid_artifact")
		proofValue, err := fixture.db.ProviderRetryWorktreeProof(fixture.ctx, paused.Ref, paused.Version, fixture.fence)
		if err != nil || proofValue.ExpectedHead != fixture.registeredHead || proofValue.ExpectedHead == foreign {
			t.Fatalf("foreign commit influenced proof=%+v err=%v", proofValue, err)
		}
	})

	t.Run("discarded amendment generation", func(t *testing.T) {
		fixture := newProviderRetryWorktreeFixture(t, "SF-provider-retry-worktree-rejected-amendment", 40)
		fixture.enterRejectedAmendmentBuilding(t)
		paused := fixture.exhaust(t, "invalid_artifact")
		proofValue, err := fixture.db.ProviderRetryWorktreeProof(fixture.ctx, paused.Ref, paused.Version, fixture.fence)
		if err != nil || proofValue.ExpectedHead != fixture.verificationHead {
			t.Fatalf("rejected amendment proof=%+v err=%v", proofValue, err)
		}
	})
}

func TestProviderRetryWorktreeProofRejectsMalformedSemanticCommitBinding(t *testing.T) {
	fixture := newProviderRetryWorktreeFixture(t, "SF-provider-retry-worktree-disconnected", 40)
	fixture.enterBuildingWithCommitParent(t, strings.Repeat("7", 40))
	paused := fixture.exhaust(t, "failed")
	if _, err := fixture.db.ProviderRetryWorktreeProof(fixture.ctx, paused.Ref, paused.Version, fixture.fence); !errors.Is(err, ErrEvidenceConflict) {
		t.Fatalf("disconnected semantic commit accepted: %v", err)
	}
}

func TestProviderRetryWorktreeProofRejectsRegistrationAndPhaseEntryTampering(t *testing.T) {
	t.Run("registered head is not self-authorizing", func(t *testing.T) {
		fixture := newProviderRetryWorktreeFixture(t, "SF-provider-retry-worktree-head-tamper", 40)
		paused := fixture.exhaust(t, "failed")
		if _, err := fixture.db.db.ExecContext(fixture.ctx, `UPDATE worktrees SET head_sha=? WHERE channel=? AND project_id=? AND ticket_id=?`, strings.Repeat("7", 40), paused.Ref.Channel, paused.Ref.Project, paused.Ref.Ticket); err != nil {
			t.Fatal(err)
		}
		if _, err := fixture.db.ProviderRetryWorktreeProof(fixture.ctx, paused.Ref, paused.Version, fixture.fence); !errors.Is(err, ErrEvidenceConflict) {
			t.Fatalf("tampered registered head accepted: %v", err)
		}
	})

	t.Run("phase entry digest", func(t *testing.T) {
		fixture := newProviderRetryWorktreeFixture(t, "SF-provider-retry-worktree-entry-tamper", 40)
		fixture.enterVerification(t)
		paused := fixture.exhaust(t, "failed")
		if _, err := fixture.db.db.ExecContext(fixture.ctx, `DROP TRIGGER provider_phase_entries_immutable_update`); err != nil {
			t.Fatal(err)
		}
		if _, err := fixture.db.db.ExecContext(fixture.ctx, `UPDATE provider_phase_entries SET entry_digest=? WHERE channel=? AND project_id=? AND ticket_id=? AND phase=?`, strings.Repeat("0", 64), paused.Ref.Channel, paused.Ref.Project, paused.Ref.Ticket, domain.PhaseVerification); err != nil {
			t.Fatal(err)
		}
		if _, err := fixture.db.ProviderRetryWorktreeProof(fixture.ctx, paused.Ref, paused.Version, fixture.fence); !errors.Is(err, ErrEvidenceConflict) {
			t.Fatalf("tampered phase entry accepted: %v", err)
		}
	})
}

func TestProviderRetryWorktreeProofFailsClosedForOperatorSourceResumeLineage(t *testing.T) {
	db, ctx, leader, resumed, _ := operatorSourceResumeResumedFixture(t)
	openExactRuntimeAdmission(t, db, resumed.Ref)
	worktree, err := db.Worktree(ctx, resumed.Ref)
	if err != nil {
		t.Fatal(err)
	}
	project, err := db.Project(ctx, resumed.Ref.Channel, resumed.Ref.Project)
	if err != nil {
		t.Fatal(err)
	}
	var configDigest string
	if err := db.db.QueryRowContext(ctx, `SELECT config_digest FROM tickets WHERE channel=? AND project_id=? AND id=?`, resumed.Ref.Channel, resumed.Ref.Project, resumed.Ref.Ticket).Scan(&configDigest); err != nil {
		t.Fatal(err)
	}
	_, reviewer := setupProviderPair(t, db, ctx)
	fence := domain.Fence{LeaderEpoch: leader, RunnerEpoch: resumed.RunnerEpoch}
	request := func() ProviderAttemptRequest {
		value := supervised(t, ProviderAttemptRequest{Ref: resumed.Ref, ExpectedVersion: resumed.Version, Fence: fence, Phase: domain.PhaseVerification, Role: "reviewer", Binding: runtime(reviewer), ConfigDigest: configDigest, Capacity: 1, At: time.Now().UTC()})
		value.Repository, value.Worktree, value.WorktreeIdentity, value.BaseSHA = project.Path, worktree.Path, string(worktree.IdentityJSON), worktree.BaseSHA
		value.Input.Repository, value.Input.Worktree, value.Input.WorktreeIdentity, value.Input.BaseSHA = value.Repository, value.Worktree, value.WorktreeIdentity, value.BaseSHA
		return value
	}
	for attempt := 0; attempt < 2; attempt++ {
		claim, err := db.BeginProviderAttempt(ctx, request())
		if err != nil {
			t.Fatal(err)
		}
		if err := db.FinishProviderAttempt(ctx, claim, proof(t, claim), resumed.Version, fence, "failed", "failed", 1, time.Now().UTC()); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.TransitionProviderExhausted(ctx, Transition{Ref: resumed.Ref, ExpectedVersion: resumed.Version, From: domain.StateVerifying, To: domain.StatePaused, ResumeState: domain.StateVerifying, Trigger: "retry_or_correction_exhausted", Fence: fence}); err != nil {
		t.Fatal(err)
	}
	paused, err := db.Ticket(ctx, resumed.Ref)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ProviderRetryWorktreeProof(ctx, paused.Ref, paused.Version, fence); !errors.Is(err, ErrProviderRetryRequiresResubmission) {
		t.Fatalf("operator-source lineage was guessed: %v", err)
	}
	if disposition, err := db.ProviderRetryDisposition(ctx, paused); err != nil || disposition != ProviderRetryResubmissionRequired {
		t.Fatalf("operator-source retry disposition=%v err=%v", disposition, err)
	}
	if err := db.SealRuntimeControl(ctx, paused.Ref); err != nil {
		t.Fatal(err)
	}
	if _, err := db.TransitionProviderRetry(ctx, Transition{Ref: paused.Ref, ExpectedVersion: paused.Version, From: domain.StatePaused, To: paused.ResumeState, ResumeState: paused.ResumeState, Trigger: "operator_retry", Fence: fence}); !errors.Is(err, ErrProviderRetryRequiresResubmission) {
		t.Fatalf("operator-source retry transition was admitted: %v", err)
	}
	legacyActive := forceLegacyUnsupportedProviderRetry(t, db, ctx, paused, fence)
	if state, err := db.ProviderRetryRuntimeReplay(ctx, legacyActive); state != ProviderRetryNotReplay || !errors.Is(err, ErrProviderRetryRequiresResubmission) {
		t.Fatalf("legacy active source-resume replay state=%v err=%v", state, err)
	}
}

// forceLegacyUnsupportedProviderRetry models the durable shape a pre-hardening
// binary could create after the pause. Production TransitionProviderRetry now
// refuses this lineage; the fixture exists solely to prove upgraded replay
// returns the same terminal resubmission classification.
func forceLegacyUnsupportedProviderRetry(t *testing.T, db *Store, ctx context.Context, paused Ticket, fence domain.Fence) Ticket {
	t.Helper()
	phase, ok := providerPhaseForState(paused.ResumeState)
	if !ok {
		t.Fatalf("unsupported retry phase for %s", paused.ResumeState)
	}
	entry, err := loadProviderPhaseEntryAt(ctx, db.db, paused.Ref, phase, paused.Version)
	if err != nil {
		t.Fatal(err)
	}
	var raw string
	if err := db.db.QueryRowContext(ctx, `SELECT payload FROM events WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version=? AND trigger='retry_or_correction_exhausted'`, paused.Ref.Channel, paused.Ref.Project, paused.Ref.Ticket, paused.Version).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	var exhaustion providerExhaustionPayload
	if err := json.Unmarshal([]byte(raw), &exhaustion); err != nil || len(exhaustion.Attempts) != 2 {
		t.Fatalf("exhaustion payload=%q err=%v", raw, err)
	}
	epoch := providerRetryEpoch{
		Phase: phase, EntryVersion: entry.Version,
		InitialFirst: exhaustion.Attempts[0], InitialLast: exhaustion.Attempts[1],
		RetryFirst: exhaustion.Attempts[1] + 1, RetryLast: exhaustion.Attempts[1] + 2,
		ExhaustionVersion: paused.Version, ExhaustionLeader: fence.LeaderEpoch, ExhaustionRunner: paused.RunnerEpoch,
		RetryVersion: paused.Version + 1, RetryLeader: fence.LeaderEpoch, RetryRunner: paused.RunnerEpoch,
	}
	epoch.Digest, err = providerRetryDigest(paused.Ref, epoch)
	if err != nil {
		t.Fatal(err)
	}
	eventPayload, err := json.Marshal(map[string]any{"schema": providerExhaustionSchema, "phase": phase, "entry_ticket_version": entry.Version, "retry_epoch": 1})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.write(ctx, func(conn *sql.Conn) error {
		if _, err := conn.ExecContext(ctx, `INSERT INTO provider_retry_epochs(channel,project_id,ticket_id,phase,entry_ticket_version,epoch,initial_first_attempt,initial_last_attempt,retry_first_attempt,retry_last_attempt,exhaustion_ticket_version,exhaustion_leader_epoch,exhaustion_runner_epoch,retry_ticket_version,retry_leader_epoch,retry_runner_epoch,retry_digest,created_at) VALUES(?,?,?,?,?,1,?,?,?,?,?,?,?,?,?,?,?,?)`, paused.Ref.Channel, paused.Ref.Project, paused.Ref.Ticket, phase, entry.Version, epoch.InitialFirst, epoch.InitialLast, epoch.RetryFirst, epoch.RetryLast, epoch.ExhaustionVersion, epoch.ExhaustionLeader, epoch.ExhaustionRunner, epoch.RetryVersion, epoch.RetryLeader, epoch.RetryRunner, epoch.Digest, time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
			return err
		}
		if _, err := conn.ExecContext(ctx, `UPDATE tickets SET state=?,resume_state=NULL,version=? WHERE channel=? AND project_id=? AND id=? AND state=? AND version=? AND runner_epoch=?`, paused.ResumeState, epoch.RetryVersion, paused.Ref.Channel, paused.Ref.Project, paused.Ref.Ticket, domain.StatePaused, paused.Version, paused.RunnerEpoch); err != nil {
			return err
		}
		_, err := conn.ExecContext(ctx, `INSERT INTO events(channel,project_id,ticket_id,ticket_version,trigger,from_state,to_state,payload,created_at) VALUES(?,?,?,?,?,?,?,?,?)`, paused.Ref.Channel, paused.Ref.Project, paused.Ref.Ticket, epoch.RetryVersion, "operator_retry", domain.StatePaused, paused.ResumeState, string(eventPayload), time.Now().UTC().Format(time.RFC3339Nano))
		return err
	}); err != nil {
		t.Fatal(err)
	}
	active, err := db.Ticket(ctx, paused.Ref)
	if err != nil {
		t.Fatal(err)
	}
	return active
}

func TestProviderRetryWorktreeProofRequiresResubmissionForPendingAmendmentLineage(t *testing.T) {
	fixture := newProviderRetryWorktreeFixture(t, "SF-provider-retry-pending-amendment", 40)
	fixture.enterPendingAmendmentVerification(t)
	paused := fixture.exhaust(t, "invalid_artifact")
	if _, err := fixture.db.ProviderRetryWorktreeProof(fixture.ctx, paused.Ref, paused.Version, fixture.fence); !errors.Is(err, ErrProviderRetryRequiresResubmission) {
		t.Fatalf("pending amendment lineage was guessed: %v", err)
	}
	if disposition, err := fixture.db.ProviderRetryDisposition(fixture.ctx, paused); err != nil || disposition != ProviderRetryResubmissionRequired {
		t.Fatalf("pending amendment retry disposition=%v err=%v", disposition, err)
	}
	if err := fixture.db.SealRuntimeControl(fixture.ctx, paused.Ref); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.db.TransitionProviderRetry(fixture.ctx, Transition{Ref: paused.Ref, ExpectedVersion: paused.Version, From: domain.StatePaused, To: paused.ResumeState, ResumeState: paused.ResumeState, Trigger: "operator_retry", Fence: fixture.fence}); !errors.Is(err, ErrProviderRetryRequiresResubmission) {
		t.Fatalf("pending amendment retry transition was admitted: %v", err)
	}
}
