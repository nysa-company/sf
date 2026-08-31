package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/phaseartifact"
	"github.com/nysa-company/sf/internal/workflowprompt"
)

func TestPublicationEvidenceSchemaAndNarrowAbsence(t *testing.T) {
	db, ctx := openTestStore(t)
	if err := db.validateSchema(ctx); err != nil {
		t.Fatal(err)
	}
	ref := domain.TicketRef{Channel: domain.ChannelDev, Project: "nysa", Ticket: "SF-publication-absence"}
	if ok, err := db.PublishedCandidateExists(ctx, ref); err != nil || ok { // missing row is not a publication identity
		t.Fatalf("absence query=%v err=%v", ok, err)
	}
	if got := CanonicalPublicationPushObservation("sf/dev/branch", strings.Repeat("a", 40)); got == "" || !strings.HasPrefix(got, "sha256:") || strings.ContainsRune(got, '\x00') {
		t.Fatalf("invalid bounded canonical push identity: %q", got)
	}
}

func TestPublicationObservationIdentitiesAreExactAndDistinct(t *testing.T) {
	pr := contracts.PullRequestIdentity{Repository: contracts.RepositoryIdentity{Host: "github.com", Owner: "o", Name: "r"}, Number: 1, HeadOwner: "o", HeadRepository: "r", HeadRef: "sf/dev/branch", HeadOID: strings.Repeat("a", 40), BaseRef: "main", BaseOID: strings.Repeat("b", 40), FactoryOwned: true}
	push := CanonicalPublicationPushObservation(pr.HeadRef, pr.HeadOID)
	prID := CanonicalPublicationPRObservation(pr, "OPEN", true)
	if push == prID || !strings.HasPrefix(prID, "sha256:") {
		t.Fatalf("push=%q pr=%q", push, prID)
	}
}

func TestPublicationRebindDigestIsCanonicalAndChainsVersions(t *testing.T) {
	ref := domain.TicketRef{Channel: domain.ChannelDev, Project: "nysa", Ticket: "SF-rebind"}
	head := strings.Repeat("a", 40)
	first := PublicationRebind{Ref: ref, CandidateGeneration: 1, CandidateHeadOID: head, PriorWitnessDigest: "sha256:" + strings.Repeat("b", 64), PriorTicketVersion: 5, PriorFence: domain.Fence{LeaderEpoch: 2, RunnerEpoch: 3}, TicketVersion: 6, Fence: domain.Fence{LeaderEpoch: 4, RunnerEpoch: 4}}
	firstPayload, err := publicationRebindPayload(first)
	if err != nil {
		t.Fatal(err)
	}
	first.RebindDigest = publicationIdentityDigest(firstPayload)
	second := PublicationRebind{Ref: ref, CandidateGeneration: 1, CandidateHeadOID: head, PriorWitnessDigest: first.RebindDigest, PriorTicketVersion: first.TicketVersion, PriorFence: first.Fence, TicketVersion: 7, Fence: domain.Fence{LeaderEpoch: 5, RunnerEpoch: 5}}
	secondPayload, err := publicationRebindPayload(second)
	if err != nil {
		t.Fatal(err)
	}
	if publicationIdentityDigest(secondPayload) == first.RebindDigest || second.PriorTicketVersion != first.TicketVersion || second.PriorFence != first.Fence {
		t.Fatal("successive recovery rebind did not preserve an exact predecessor")
	}
}

func TestPublicationEffectDigestSpellingsAreStrict(t *testing.T) {
	base := PublicationEffectEvidence{SemanticKey: "effect", Kind: PublicationPushEffectKind, RequestDigest: strings.Repeat("a", 64), ClaimEpoch: 1, ObservedIdentity: "sha256:" + strings.Repeat("b", 64)}
	if !validPublicationEffect(base) {
		t.Fatal("plain GitHub digest should be accepted")
	}
	base.RequestDigest = "sha256:" + strings.Repeat("a", 64)
	if !validPublicationEffect(base) {
		t.Fatal("typed Git digest should be accepted")
	}
	for _, malformed := range []string{"digest", "SHA256:" + strings.Repeat("a", 64), "sha256:" + strings.Repeat("a", 63), strings.Repeat("a", 63)} {
		base.RequestDigest = malformed
		if validPublicationEffect(base) {
			t.Fatalf("malformed request digest accepted: %q", malformed)
		}
	}
}

func TestPublishedCandidateValidationAcceptsCanonicalPushWitness(t *testing.T) {
	ref := domain.TicketRef{Channel: domain.ChannelDev, Project: "nysa", Ticket: "SF-published"}
	base, head, tree, parent := strings.Repeat("a", 40), strings.Repeat("b", 40), strings.Repeat("c", 40), strings.Repeat("d", 40)
	remoteBase := base
	branch := "sf/dev/nysa/SF-published"
	identity := []byte(`{"Origin":"https://github.com/acme/app.git","PushOrigin":"git@github.com:acme/app.git","BaseRef":"main","BaseHead":"` + remoteBase + `","HeadRef":"` + branch + `"}`)
	policy := strings.Repeat("1", 64)
	command := RepositoryCommandResultBinding{Key: contracts.RepositoryCommandResultKey{SemanticKey: "command", ClaimEpoch: 1}, TicketVersion: 5, LeaderEpoch: 1, RunnerEpoch: 1, CommandDigest: "sha256:" + strings.Repeat("2", 64), SpecDigest: "sha256:" + strings.Repeat("3", 64), PolicyDigest: "sha256:" + policy, ExecutablePath: "/usr/bin/true", ExecutableDigest: "sha256:" + strings.Repeat("4", 64)}
	value := PublishedCandidateEvidence{Ref: ref, TicketVersion: 6, Fence: domain.Fence{LeaderEpoch: 1, RunnerEpoch: 1}, Candidate: StoredCandidate{Snapshot: domain.CandidateSnapshot{Generation: 1, BaseSHA: base, HeadSHA: head, TreeSHA: tree, SourceDigest: strings.Repeat("5", 64), VerificationIntentDigest: strings.Repeat("6", 64), ProofDigest: strings.Repeat("7", 64), CommandPolicyDigest: policy, BuilderEvidenceDigest: strings.Repeat("8", 64)}, TicketVersion: 5, Fence: domain.Fence{LeaderEpoch: 1, RunnerEpoch: 1}, BuilderResult: ProviderAttemptResultKey{AttemptID: 1, Ref: ref, Phase: domain.PhaseBuild, Attempt: 1}, Commit: CommitObservation{CommitOID: head, ParentOID: parent, TreeOID: tree}, CommandBinding: command}, ConfigGeneration: 1, ConfigDigest: strings.Repeat("9", 64), ConfigSnapshotDigest: strings.Repeat("a", 64), Worktree: StoredWorktree{Path: "/tmp/SF-published", Branch: branch, State: "registered", IdentityJSON: identity, BaseSHA: base, HeadSHA: parent, TicketVersion: 2, Fence: domain.Fence{LeaderEpoch: 1, RunnerEpoch: 1}}, RemoteBranchRef: branch, RemoteBranchOID: head, RemoteBaseOID: remoteBase, PushEffect: PublicationEffectEvidence{SemanticKey: "push", Kind: PublicationPushEffectKind, RequestDigest: strings.Repeat("b", 64), ClaimEpoch: 1, ObservedIdentity: CanonicalPublicationPushObservation(branch, head)}, PullRequest: contracts.PullRequestIdentity{Repository: contracts.RepositoryIdentity{Host: "github.com", Owner: "acme", Name: "app"}, Number: 1, HeadOwner: "acme", HeadRepository: "app", HeadRef: branch, HeadOID: head, BaseRef: "main", BaseOID: remoteBase, FactoryOwned: true}, PullRequestState: "OPEN", PullRequestDraft: true, PullRequestObservedAt: time.Date(2026, 8, 30, 0, 0, 0, 0, time.UTC), PRCreateOrUpdateEffect: PublicationEffectEvidence{SemanticKey: "pr", Kind: PublicationPRCreateEffectKind, RequestDigest: "sha256:" + strings.Repeat("c", 64), ClaimEpoch: 1}, CreatedAt: time.Date(2026, 8, 30, 0, 0, 1, 0, time.UTC)}
	value.PRCreateOrUpdateEffect.ObservedIdentity = CanonicalPublicationPRObservation(value.PullRequest, value.PullRequestState, value.PullRequestDraft)
	if err := validPublishedCandidateEvidence(value); err != nil {
		t.Fatalf("canonical publication witness rejected: %v", err)
	}
	replayed := value
	replayed.CreatedAt = value.CreatedAt.Add(17 * time.Minute)
	replayed.PullRequestObservedAt = value.PullRequestObservedAt.Add(19 * time.Minute)
	if !publicationEqual(value, replayed) {
		t.Fatal("publication replay identity changed with observation metadata")
	}
	mismatchedBase := value
	mismatchedBase.RemoteBaseOID = strings.Repeat("e", 40)
	if err := validPublishedCandidateEvidence(mismatchedBase); err == nil {
		t.Fatal("mismatched remote base was accepted")
	}
}

// publicationLifecycleFixture exercises the real Store fences and immutable
// bindings used by publication recording. It intentionally does not call a
// GitHub/network adapter: the two confirmed effects below are Store witnesses
// supplied by a caller after its external reconciliation.
func publicationLifecycleFixture(t *testing.T) (*Store, context.Context, Ticket, domain.Fence) {
	return publicationLifecycleFixtureFor(t, domain.TicketFeature, domain.MergeGuarded)
}

func publicationLifecycleFixtureFor(t *testing.T, ticketType domain.TicketType, mergeMode domain.MergeMode) (*Store, context.Context, Ticket, domain.Fence) {
	t.Helper()
	db, ctx := openTestStore(t)
	configDigest := setupProviderProject(t, db, ctx)
	leader, err := db.AcquireLeader(ctx, domain.ChannelDev, "publication-lifecycle")
	if err != nil {
		t.Fatalf("publication fixture acquire leader: %v", err)
	}
	ref := domain.TicketRef{Channel: domain.ChannelDev, Project: "provider", Ticket: "SF-publication-lifecycle"}
	source := sha256Digest([]byte("publication-source"))
	if err := db.CreateTicket(ctx, Ticket{Ref: ref, SourceDigest: source, Type: ticketType, MergeMode: mergeMode, CreatedAt: time.Now().UTC(), MaxDuration: time.Hour, MaxCostMicroUSD: 100}); err != nil {
		t.Fatalf("publication fixture create ticket: %v", err)
	}
	ticket, err := db.StartOrAdopt(ctx, ref, 1, "dev/provider/SF-publication-lifecycle", domain.Fence{LeaderEpoch: leader, RunnerEpoch: 1})
	if err != nil {
		t.Fatalf("publication fixture start or adopt: %v", err)
	}
	branch := testAllocatedBranch(ref, strings.Repeat("ab", 16))
	base := strings.Repeat("a", 40)
	identity := []byte(strings.ReplaceAll(strings.ReplaceAll(repositoryCommandIdentity(t, "/tmp/provider", "/tmp/provider/SF-publication-lifecycle", branch, "main"), "git@example.test:nysa.git", "https://github.com/acme/app.git"), "/tmp/nysa-origin", "git@github.com:acme/app.git"))
	registrationFence := domain.Fence{LeaderEpoch: leader, RunnerEpoch: ticket.RunnerEpoch}
	branchKey := string(ref.Channel) + "\x00" + string(ref.Project) + "\x00" + string(ref.Ticket)
	if _, err := db.LoadOrStoreBranchUnderFence(ctx, branchKey, branch, ticket.Version, registrationFence); err != nil {
		t.Fatalf("publication fixture register branch: %v", err)
	}
	if err := db.RegisterWorktree(ctx, WorktreeRegistration{Ref: ref, ExpectedVersion: ticket.Version, Fence: registrationFence, Path: "/tmp/provider/SF-publication-lifecycle", Branch: branch, IdentityJSON: identity, BaseSHA: base, HeadSHA: base}); err != nil {
		t.Fatalf("publication fixture register worktree: %v", err)
	}
	builder, reviewerQual := setupProviderPair(t, db, ctx)
	fence := domain.Fence{LeaderEpoch: leader, RunnerEpoch: ticket.RunnerEpoch}
	launch := func(phase domain.Phase, role string, binding contracts.RuntimeBinding, raw []byte, validation phaseartifact.Validation) ProviderAttemptClaim {
		request := supervised(t, ProviderAttemptRequest{Ref: ticket.Ref, ExpectedVersion: ticket.Version, Fence: fence, Phase: phase, Role: role, Binding: binding, ConfigDigest: configDigest, Capacity: 1, At: time.Now().UTC()})
		request.WorktreeIdentity = string(identity)
		request.Input.WorktreeIdentity = string(identity)
		claim, err := db.BeginProviderAttempt(ctx, request)
		if err != nil {
			t.Fatalf("publication fixture begin %s/%s attempt: %v", phase, role, err)
		}
		if err := db.RecordProviderLaunch(ctx, claim, contracts.ProviderLaunch{PID: int(claim.ID), PGID: int(claim.ID), BootIdentity: "publication", ProcessStartIdentity: fmt.Sprintf("publication-%d", claim.ID), Worktree: claim.Worktree}); err != nil {
			t.Fatalf("publication fixture record %s/%s launch: %v", phase, role, err)
		}
		if _, err := db.CompleteProviderAttemptSuccess(ctx, claim, proof(t, claim), ticket.Version, fence, contracts.PhaseResult{Provider: claim.Binding.Identity, Artifact: raw, UsageTrusted: true, UsageUnits: 1}, validation, time.Now().UTC()); err != nil {
			t.Fatalf("publication fixture complete %s/%s attempt: %v", phase, role, err)
		}
		return claim
	}
	proofKind := phaseartifact.ProofAcceptance
	if ticket.Type == domain.TicketSpike {
		proofKind = phaseartifact.ProofReport
	}
	plan := phaseartifact.Planner{Schema: "sf.planner/v1", Acceptance: []string{"a"}, Proof: phaseartifact.ProofPlan{Kind: proofKind, Command: []string{"go", "test"}, Details: "d"}, Paths: []string{"internal"}, Commands: [][]string{{"go", "test"}}, Risks: []string{"r"}}
	plannerRaw, err := json.Marshal(plan)
	if err != nil {
		t.Fatal(err)
	}
	planner := launch(domain.PhasePlanning, "planner", runtime(builder), plannerRaw, phaseartifact.Validation{TicketType: ticket.Type})
	planKey := ProviderAttemptResultKey{AttemptID: planner.ID, Ref: ticket.Ref, Phase: domain.PhasePlanning, Attempt: planner.Attempt}
	if _, err := db.RecordPlan(ctx, PlanArtifact{Ref: ticket.Ref, ExpectedVersion: ticket.Version, Fence: fence, Document: PlanDocument{Planner: &plan, ProviderResult: &planKey, Acceptance: plan.Acceptance, ProofKind: string(plan.Proof.Kind), Paths: plan.Paths, Commands: plan.Commands, Risks: plan.Risks}}); err != nil {
		t.Fatalf("publication fixture record plan: %v", err)
	}
	if _, err := db.TransitionPlan(ctx, Transition{Ref: ticket.Ref, ExpectedVersion: ticket.Version, From: domain.StatePlanning, To: domain.StateVerifying, Trigger: "phase_pass", Fence: fence, EventPayload: "{}"}); err != nil {
		t.Fatalf("publication fixture transition plan: %v", err)
	}
	ticket, _ = db.Ticket(ctx, ticket.Ref)
	fence.RunnerEpoch = ticket.RunnerEpoch
	planID, _ := workflowprompt.NewPlanIdentity(plan)
	prebuildOutcome := "red"
	prebuildExit := 1
	if ticket.Type == domain.TicketSpike {
		prebuildOutcome = "report_ready"
		prebuildExit = 0
	}
	verification := phaseartifact.Verification{Schema: "sf.verification/v1", AcceptanceDigest: planID.Digest, ProofKind: proofKind, OwnedFiles: []string{"internal"}, Command: []string{"go", "test", "./..."}, PrebuildOutcome: prebuildOutcome, EvidenceDigest: sha256Digest([]byte("publication-verification"))}
	verificationRaw, _ := json.Marshal(verification)
	reviewerClaim := launch(domain.PhaseVerification, "reviewer", runtime(reviewerQual), verificationRaw, phaseartifact.Validation{TicketType: ticket.Type, AcceptanceDigest: planID.Digest})
	intent, _ := workflowprompt.CanonicalVerificationIntentBytes(verification)
	proofBytes, _ := workflowprompt.CanonicalVerificationProofBytes(verification)
	checkpoint := strings.Repeat("c", 40)
	verificationKey := ProviderAttemptResultKey{AttemptID: reviewerClaim.ID, Ref: ticket.Ref, Phase: domain.PhaseVerification, Attempt: reviewerClaim.Attempt}
	command := completeEvidenceRepositoryCommand(t, db, ctx, RepositoryCommandPurposePrebuildVerification, ticket.Ref, ticket.Version, fence, verificationKey, sha256Digest(intent), sha256Digest(proofBytes), "", "", prebuildExit)
	if _, err := db.RecordVerification(ctx, VerificationArtifact{Ref: ticket.Ref, ExpectedVersion: ticket.Version, Fence: fence, Intent: intent, Proof: proofBytes, OwnedFiles: verification.OwnedFiles, CheckpointID: checkpoint, ProviderResult: &verificationKey, Checkpoint: CommitObservation{CommitOID: checkpoint, ParentOID: base, TreeOID: strings.Repeat("d", 40)}, CommandResult: command}); err != nil {
		t.Fatalf("publication fixture record verification: %v", err)
	}
	if _, err := db.TransitionVerification(ctx, Transition{Ref: ticket.Ref, ExpectedVersion: ticket.Version, From: domain.StateVerifying, To: domain.StateBuilding, Trigger: "phase_pass", Fence: fence, EventPayload: "{}"}); err != nil {
		t.Fatalf("publication fixture transition verification: %v", err)
	}
	ticket, _ = db.Ticket(ctx, ticket.Ref)
	fence.RunnerEpoch = ticket.RunnerEpoch
	builderRaw := []byte(`{"schema":"sf.builder/v1","summary":"publication","changed_files":["internal/x.go"],"commands":[["go","test"]]}`)
	built := launch(domain.PhaseBuild, "builder", runtime(builder), builderRaw, phaseartifact.Validation{TicketType: ticket.Type})
	_, parsed, err := db.LoadHistoricalProviderAttemptResult(ctx, ProviderAttemptResultKey{AttemptID: built.ID, Ref: ticket.Ref, Phase: domain.PhaseBuild, Attempt: built.Attempt})
	if err != nil {
		t.Fatalf("publication fixture load builder result: %v", err)
	}
	builderDigest, _ := phaseartifact.BuilderEvidenceDigest(*parsed.Builder)
	policy := sha256Digest([]byte("publication-policy"))
	snapshot := domain.CandidateSnapshot{Generation: 1, BaseSHA: base, HeadSHA: strings.Repeat("e", 40), TreeSHA: strings.Repeat("f", 40), SourceDigest: source, VerificationIntentDigest: sha256Digest(intent), ProofDigest: sha256Digest(proofBytes), CommandPolicyDigest: policy, BuilderEvidenceDigest: builderDigest}
	builtKey := ProviderAttemptResultKey{AttemptID: built.ID, Ref: ticket.Ref, Phase: domain.PhaseBuild, Attempt: built.Attempt}
	candidateCommand := completeEvidenceRepositoryCommand(t, db, ctx, RepositoryCommandPurposePostbuildCandidate, ticket.Ref, ticket.Version, fence, builtKey, sha256Digest(intent), sha256Digest(proofBytes), checkpoint, "sha256:"+policy, 0)
	if _, err := db.RecordCandidate(ctx, CandidateEvidence{Ref: ticket.Ref, ExpectedVersion: ticket.Version, Fence: fence, Snapshot: snapshot, BuilderResult: builtKey, Commit: CommitObservation{CommitOID: snapshot.HeadSHA, ParentOID: checkpoint, TreeOID: snapshot.TreeSHA}, Reason: "publication", CommandResult: candidateCommand}); err != nil {
		t.Fatalf("publication fixture record candidate: %v", err)
	}
	stored, err := db.RecoverableCandidate(ctx, ticket.Ref)
	if err != nil {
		t.Fatalf("publication fixture load candidate: %v", err)
	}
	if _, err := db.TransitionCandidate(ctx, Transition{Ref: ticket.Ref, ExpectedVersion: ticket.Version, From: domain.StateBuilding, To: domain.StatePublishing, Trigger: "phase_pass", Fence: fence, EventPayload: "{}"}, stored.Snapshot); err != nil {
		t.Fatalf("publication fixture transition candidate: %v", err)
	}
	ticket, _ = db.Ticket(ctx, ticket.Ref)
	return db, ctx, ticket, fence
}

func TestCandidateOnlyPublishingRecoveryWithoutPublicationWitness(t *testing.T) {
	db, ctx, ticket, _ := publicationLifecycleFixture(t)
	leader, err := db.AcquireLeader(ctx, domain.ChannelDev, "candidate-only-restart")
	if err != nil {
		t.Fatal(err)
	}
	if changed, err := db.FenceRecoveredRunners(ctx, domain.ChannelDev, leader); err != nil || changed != 1 {
		t.Fatalf("candidate-only fence changed=%d err=%v", changed, err)
	}
	if err := db.RebindRecoveredPublishedCandidates(ctx, domain.ChannelDev, leader); err != nil {
		t.Fatalf("candidate-only startup rebind=%v", err)
	}
	if _, err := db.LoadPublishedCandidate(ctx, ticket.Ref); !errors.Is(err, ErrNotFound) {
		t.Fatalf("candidate-only recovery manufactured publication evidence: %v", err)
	}
	current, err := db.Ticket(ctx, ticket.Ref)
	if err != nil || current.State != domain.StatePublishing {
		t.Fatalf("candidate-only ticket=%+v err=%v", current, err)
	}
}

func recordFixturePublication(t *testing.T, db *Store, ctx context.Context, ticket Ticket, fence domain.Fence) {
	t.Helper()
	worktree, err := db.Worktree(ctx, ticket.Ref)
	if err != nil {
		t.Fatal(err)
	}
	candidate, err := db.RecoverableCandidate(ctx, ticket.Ref)
	if err != nil {
		t.Fatal(err)
	}
	pr := contracts.PullRequestIdentity{Repository: contracts.RepositoryIdentity{Host: "github.com", Owner: "acme", Name: "app"}, Number: 42, HeadOwner: "acme", HeadRepository: "app", HeadRef: worktree.Branch, HeadOID: candidate.Snapshot.HeadSHA, BaseRef: "main", BaseOID: candidate.Snapshot.BaseSHA, FactoryOwned: true}
	value := PublishedCandidateEvidence{Ref: ticket.Ref, TicketVersion: ticket.Version, Fence: fence, Candidate: candidate, ConfigGeneration: ticket.ConfigGeneration, ConfigDigest: ticket.ConfigDigest, ConfigSnapshotDigest: sha256Digest(ticket.ConfigSnapshot), Worktree: worktree, RemoteBranchRef: worktree.Branch, RemoteBranchOID: candidate.Snapshot.HeadSHA, RemoteBaseOID: candidate.Snapshot.BaseSHA, PullRequest: pr, PullRequestState: "OPEN", PullRequestDraft: true, PullRequestObservedAt: time.Now().UTC(), CreatedAt: time.Now().UTC()}
	value.PushEffect = PublicationEffectEvidence{SemanticKey: "fixture-push-" + string(ticket.Ref.Ticket), Kind: PublicationPushEffectKind, RequestDigest: strings.Repeat("1", 64), ClaimEpoch: 1, ObservedIdentity: CanonicalPublicationPushObservation(value.RemoteBranchRef, value.RemoteBranchOID)}
	value.PRCreateOrUpdateEffect = PublicationEffectEvidence{SemanticKey: "fixture-pr-" + string(ticket.Ref.Ticket), Kind: PublicationPRCreateEffectKind, RequestDigest: "sha256:" + strings.Repeat("2", 64), ClaimEpoch: 1, ObservedIdentity: CanonicalPublicationPRObservation(pr, "OPEN", true)}
	for _, effect := range []PublicationEffectEvidence{value.PushEffect, value.PRCreateOrUpdateEffect} {
		if _, err := db.PlanEffect(ctx, EffectPlan{SemanticKey: effect.SemanticKey, Ref: ticket.Ref, Kind: effect.Kind, TicketVersion: ticket.Version, Fence: fence, RequestDigest: effect.RequestDigest}); err != nil {
			t.Fatal(err)
		}
		claim, err := db.ClaimEffect(ctx, EffectFence{SemanticKey: effect.SemanticKey, Ref: ticket.Ref, TicketVersion: ticket.Version, Fence: fence})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := db.ConfirmEffect(ctx, EffectFence{SemanticKey: effect.SemanticKey, Ref: ticket.Ref, TicketVersion: ticket.Version, Fence: domain.Fence{LeaderEpoch: claim.Effect.LeaderEpoch, RunnerEpoch: claim.Effect.RunnerEpoch, ClaimEpoch: claim.Effect.ClaimEpoch}}, effect.ObservedIdentity); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.RecordPublishedCandidate(ctx, value); err != nil {
		t.Fatal(err)
	}
}

func TestPausedPublishingResumeToWaitingCIAuthenticatesControlLineage(t *testing.T) {
	db, ctx, ticket, fence := publicationLifecycleFixture(t)
	recordFixturePublication(t, db, ctx, ticket, fence)
	current, err := db.Ticket(ctx, ticket.Ref)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.TransitionPublishedCandidate(ctx, Transition{Ref: ticket.Ref, ExpectedVersion: current.Version, From: domain.StatePublishing, To: domain.StateWaitingCI, Trigger: "effects_confirmed", Fence: fence, EventPayload: `{}`}); err != nil {
		t.Fatal(err)
	}
	current, err = db.Ticket(ctx, ticket.Ref)
	if err != nil {
		t.Fatal(err)
	}
	waitingFence := domain.Fence{LeaderEpoch: fence.LeaderEpoch, RunnerEpoch: current.RunnerEpoch}
	stopping, err := db.TransitionAndInvalidateRunner(ctx, Transition{Ref: ticket.Ref, ExpectedVersion: current.Version, From: domain.StateWaitingCI, To: domain.StateStopping, ResumeState: domain.StateWaitingCI, Trigger: "operator_pause_or_take", Fence: waitingFence, EventPayload: `{}`})
	if err != nil {
		t.Fatal(err)
	}
	paused, err := db.CompleteControlTransition(ctx, Transition{Ref: ticket.Ref, ExpectedVersion: stopping.Version, From: domain.StateStopping, To: domain.StatePaused, ResumeState: domain.StateWaitingCI, Trigger: "process_and_effects_drained", Fence: domain.Fence{LeaderEpoch: fence.LeaderEpoch, RunnerEpoch: fence.RunnerEpoch + 1}, EventPayload: `{}`})
	if err != nil {
		t.Fatal(err)
	}
	resumed, err := db.TransitionPublishedResume(ctx, Transition{Ref: ticket.Ref, ExpectedVersion: paused.Version, From: domain.StatePaused, To: domain.StateWaitingCI, Trigger: "operator_resume", Fence: domain.Fence{LeaderEpoch: fence.LeaderEpoch, RunnerEpoch: fence.RunnerEpoch + 1}, EventPayload: `{}`})
	if err != nil || resumed.EventID == 0 {
		t.Fatalf("waiting-ci resume=%+v err=%v", resumed, err)
	}
	loaded, err := db.LoadPublishedCandidate(ctx, ticket.Ref)
	if err != nil || loaded.CurrentTicketVersion != ticket.Version {
		t.Fatalf("waiting-ci publication=%+v err=%v", loaded, err)
	}
}

func TestWaitingCIPublicationReaderRequiresFenceAfterLeaderAcquisition(t *testing.T) {
	db, ctx, ticket, fence := publicationLifecycleFixture(t)
	recordFixturePublication(t, db, ctx, ticket, fence)
	if _, err := db.TransitionPublishedCandidate(ctx, Transition{Ref: ticket.Ref, ExpectedVersion: ticket.Version, From: domain.StatePublishing, To: domain.StateWaitingCI, Trigger: "effects_confirmed", Fence: fence, EventPayload: `{}`}); err != nil {
		t.Fatal(err)
	}
	waiting, err := db.Ticket(ctx, ticket.Ref)
	if err != nil {
		t.Fatal(err)
	}
	newLeader, err := db.AcquireLeader(ctx, domain.ChannelDev, "waiting-ci-leader-only")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.LoadPublishedCandidate(ctx, ticket.Ref); !errors.Is(err, ErrStaleFence) {
		t.Fatalf("waiting-ci publication read survived leader-only takeover: %v", err)
	}
	if _, err := db.FenceRecoveredRunners(ctx, domain.ChannelDev, newLeader); err != nil {
		t.Fatalf("waiting-ci fence=%v", err)
	}
	if _, err := db.LoadPublishedCandidate(ctx, ticket.Ref); err != nil {
		t.Fatalf("fenced waiting-ci publication read=%v", err)
	}
	current, err := db.Ticket(ctx, ticket.Ref)
	if err != nil || current.Version != waiting.Version+1 || current.RunnerEpoch != waiting.RunnerEpoch+1 {
		t.Fatalf("waiting-ci fence did not append exact recovery: current=%+v waiting=%+v err=%v", current, waiting, err)
	}
}

func TestSemanticPublishingPauseResumeAuthenticatesExactPair(t *testing.T) {
	db, ctx, ticket, fence := publicationLifecycleFixture(t)
	recordFixturePublication(t, db, ctx, ticket, fence)
	paused, err := db.Transition(ctx, Transition{Ref: ticket.Ref, ExpectedVersion: ticket.Version, From: domain.StatePublishing, To: domain.StatePaused, ResumeState: domain.StatePublishing, Trigger: "retry_or_correction_exhausted", Fence: fence, EventPayload: `{"reason":"retry_budget"}`})
	if err != nil {
		t.Fatalf("semantic publishing pause=%v", err)
	}
	resumed, err := db.TransitionPublishedResume(ctx, Transition{Ref: ticket.Ref, ExpectedVersion: paused.Version, From: domain.StatePaused, To: domain.StatePublishing, Trigger: "operator_retry", Fence: fence, EventPayload: `{}`})
	if err != nil || resumed.EventID == 0 {
		t.Fatalf("semantic publishing resume=%+v err=%v", resumed, err)
	}
	loaded, err := db.LoadPublishedCandidate(ctx, ticket.Ref)
	if err != nil || loaded.CurrentTicketVersion != ticket.Version {
		t.Fatalf("semantic publishing publication=%+v err=%v", loaded, err)
	}
	var rebinds int
	if err := db.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM publication_evidence_rebinds WHERE channel=? AND project_id=? AND ticket_id=?`, ticket.Ref.Channel, ticket.Ref.Project, ticket.Ref.Ticket).Scan(&rebinds); err != nil || rebinds != 0 {
		t.Fatalf("semantic pause minted publication rebinds=%d err=%v", rebinds, err)
	}
}

func TestSemanticWaitingCIPauseResumeAuthenticatesExactPair(t *testing.T) {
	db, ctx, ticket, fence := publicationLifecycleFixture(t)
	recordFixturePublication(t, db, ctx, ticket, fence)
	if _, err := db.TransitionPublishedCandidate(ctx, Transition{Ref: ticket.Ref, ExpectedVersion: ticket.Version, From: domain.StatePublishing, To: domain.StateWaitingCI, Trigger: "effects_confirmed", Fence: fence, EventPayload: `{}`}); err != nil {
		t.Fatal(err)
	}
	waiting, err := db.Ticket(ctx, ticket.Ref)
	if err != nil {
		t.Fatal(err)
	}
	paused, err := db.Transition(ctx, Transition{Ref: ticket.Ref, ExpectedVersion: waiting.Version, From: domain.StateWaitingCI, To: domain.StatePaused, ResumeState: domain.StateWaitingCI, Trigger: "retry_or_correction_exhausted", Fence: fence, EventPayload: `{"reason":"ci_red_exhausted"}`})
	if err != nil {
		t.Fatalf("semantic waiting-ci pause=%v", err)
	}
	resumed, err := db.TransitionPublishedResume(ctx, Transition{Ref: ticket.Ref, ExpectedVersion: paused.Version, From: domain.StatePaused, To: domain.StateWaitingCI, Trigger: "operator_resume", Fence: fence, EventPayload: `{}`})
	if err != nil || resumed.EventID == 0 {
		t.Fatalf("semantic waiting-ci resume=%+v err=%v", resumed, err)
	}
	loaded, err := db.LoadPublishedCandidate(ctx, ticket.Ref)
	if err != nil || loaded.CurrentTicketVersion != ticket.Version {
		t.Fatalf("semantic waiting-ci publication=%+v err=%v", loaded, err)
	}
	mutantDir := t.TempDir()
	if err := os.Chmod(mutantDir, 0o700); err != nil {
		t.Fatal(err)
	}
	mutantPath := mutantDir + "/semantic-waiting-runner-bump.sqlite"
	if err := db.Backup(ctx, mutantPath); err != nil {
		t.Fatal(err)
	}
	mutant, err := Open(ctx, mutantPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := mutant.db.ExecContext(ctx, `UPDATE tickets SET runner_epoch=runner_epoch+1 WHERE channel=? AND project_id=? AND id=?`, ticket.Ref.Channel, ticket.Ref.Project, ticket.Ref.Ticket); err != nil {
		mutant.Close()
		t.Fatal(err)
	}
	if _, err := mutant.LoadPublishedCandidate(ctx, ticket.Ref); err == nil {
		mutant.Close()
		t.Fatal("semantic waiting-ci replay accepted an unexplained runner bump")
	}
	mutant.Close()
}

func TestBlockedPublishingRecoverRequiresTypedBlockerAndRebindsPublication(t *testing.T) {
	db, ctx, ticket, fence := publicationLifecycleFixture(t)
	recordFixturePublication(t, db, ctx, ticket, fence)
	current, err := db.Ticket(ctx, ticket.Ref)
	if err != nil {
		t.Fatal(err)
	}
	blocked, err := db.Transition(ctx, Transition{Ref: ticket.Ref, ExpectedVersion: current.Version, From: domain.StatePublishing, To: domain.StateBlocked, ResumeState: domain.StatePublishing, Trigger: "typed_blocker", Fence: fence, EventPayload: `{"code":"publication_retry_required"}`})
	if err != nil {
		t.Fatal(err)
	}
	blockedVersion := blocked.Version
	resumed, err := db.TransitionPublishedResume(ctx, Transition{Ref: ticket.Ref, ExpectedVersion: blockedVersion, From: domain.StateBlocked, To: domain.StatePublishing, Trigger: "operator_recover", Fence: fence, EventPayload: `{}`})
	if err != nil || resumed.EventID == 0 {
		t.Fatalf("blocked publication recover=%+v err=%v", resumed, err)
	}
	loaded, err := db.LoadPublishedCandidate(ctx, ticket.Ref)
	if err != nil || loaded.CurrentTicketVersion != ticket.Version {
		t.Fatalf("recovered publication=%+v err=%v", loaded, err)
	}
}

func TestBlockedWaitingCIRecoverAuthenticatesExactPair(t *testing.T) {
	db, ctx, ticket, fence := publicationLifecycleFixture(t)
	recordFixturePublication(t, db, ctx, ticket, fence)
	if _, err := db.TransitionPublishedCandidate(ctx, Transition{Ref: ticket.Ref, ExpectedVersion: ticket.Version, From: domain.StatePublishing, To: domain.StateWaitingCI, Trigger: "effects_confirmed", Fence: fence, EventPayload: `{}`}); err != nil {
		t.Fatal(err)
	}
	waiting, err := db.Ticket(ctx, ticket.Ref)
	if err != nil {
		t.Fatal(err)
	}
	blocked, err := db.Transition(ctx, Transition{Ref: ticket.Ref, ExpectedVersion: waiting.Version, From: domain.StateWaitingCI, To: domain.StateBlocked, ResumeState: domain.StateWaitingCI, Trigger: "typed_blocker", Fence: fence, EventPayload: `{"code":"ci_retry_required"}`})
	if err != nil {
		t.Fatalf("waiting-ci block=%v", err)
	}
	resumed, err := db.TransitionPublishedResume(ctx, Transition{Ref: ticket.Ref, ExpectedVersion: blocked.Version, From: domain.StateBlocked, To: domain.StateWaitingCI, Trigger: "operator_recover", Fence: fence, EventPayload: `{}`})
	if err != nil || resumed.EventID == 0 {
		t.Fatalf("waiting-ci recover=%+v err=%v", resumed, err)
	}
	if _, err := db.LoadPublishedCandidate(ctx, ticket.Ref); err != nil {
		t.Fatalf("waiting-ci recovered publication=%v", err)
	}

	mutantDir := t.TempDir()
	if err := os.Chmod(mutantDir, 0o700); err != nil {
		t.Fatal(err)
	}
	mutantPath := mutantDir + "/blocked-waiting-runner-bump.sqlite"
	if err := db.Backup(ctx, mutantPath); err != nil {
		t.Fatal(err)
	}
	mutant, err := Open(ctx, mutantPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := mutant.db.ExecContext(ctx, `UPDATE tickets SET runner_epoch=runner_epoch+1 WHERE channel=? AND project_id=? AND id=?`, ticket.Ref.Channel, ticket.Ref.Project, ticket.Ref.Ticket); err != nil {
		mutant.Close()
		t.Fatal(err)
	}
	if _, err := mutant.LoadPublishedCandidate(ctx, ticket.Ref); err == nil {
		mutant.Close()
		t.Fatal("blocked waiting-ci replay accepted an unexplained runner bump")
	}
	mutant.Close()

	tamperDir := t.TempDir()
	if err := os.Chmod(tamperDir, 0o700); err != nil {
		t.Fatal(err)
	}
	tamperPath := tamperDir + "/blocked-waiting-payload.sqlite"
	if err := db.Backup(ctx, tamperPath); err != nil {
		t.Fatal(err)
	}
	tamper, err := Open(ctx, tamperPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tamper.db.ExecContext(ctx, `UPDATE events SET payload='{"code":"tampered"}' WHERE channel=? AND project_id=? AND ticket_id=? AND trigger='typed_blocker'`, ticket.Ref.Channel, ticket.Ref.Project, ticket.Ref.Ticket); err != nil {
		tamper.Close()
		t.Fatal(err)
	}
	if _, err := tamper.LoadPublishedCandidate(ctx, ticket.Ref); err == nil {
		tamper.Close()
		t.Fatal("blocked waiting-ci replay accepted tampered blocker lineage")
	}
	tamper.Close()
}

func TestPublishingResumeTakeoverRequiresRecoveryRebind(t *testing.T) {
	for _, tc := range []struct {
		name    string
		blocked bool
		pauseTo domain.State
		trigger string
		payload string
		resume  string
	}{
		{name: "typed-blocker", blocked: true},
		{name: "semantic-pause", pauseTo: domain.StatePaused, trigger: "retry_or_correction_exhausted", payload: `{"reason":"retry_budget"}`, resume: "operator_retry"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, ctx, ticket, fence := publicationLifecycleFixture(t)
			recordFixturePublication(t, db, ctx, ticket, fence)
			current, err := db.Ticket(ctx, ticket.Ref)
			if err != nil {
				t.Fatal(err)
			}
			if tc.blocked {
				blocked, transitionErr := db.TransitionPublishedBlock(ctx, Transition{Ref: ticket.Ref, ExpectedVersion: current.Version, From: domain.StatePublishing, To: domain.StateBlocked, ResumeState: domain.StatePublishing, Trigger: "typed_blocker", Fence: fence, EventPayload: `{"code":"publication_retry_required"}`})
				if transitionErr != nil {
					t.Fatal(transitionErr)
				}
				_, err = db.TransitionPublishedResume(ctx, Transition{Ref: ticket.Ref, ExpectedVersion: blocked.Version, From: domain.StateBlocked, To: domain.StatePublishing, Trigger: "operator_recover", Fence: fence, EventPayload: `{}`})
			} else {
				paused, transitionErr := db.Transition(ctx, Transition{Ref: ticket.Ref, ExpectedVersion: current.Version, From: domain.StatePublishing, To: tc.pauseTo, ResumeState: domain.StatePublishing, Trigger: tc.trigger, Fence: fence, EventPayload: tc.payload})
				if transitionErr != nil {
					t.Fatal(transitionErr)
				}
				_, err = db.TransitionPublishedResume(ctx, Transition{Ref: ticket.Ref, ExpectedVersion: paused.Version, From: domain.StatePaused, To: domain.StatePublishing, Trigger: tc.resume, Fence: fence, EventPayload: `{}`})
			}
			if err != nil {
				t.Fatal(err)
			}
			current, err = db.Ticket(ctx, ticket.Ref)
			if err != nil {
				t.Fatal(err)
			}
			newLeader, err := db.AcquireLeader(ctx, domain.ChannelDev, "publishing-resume-takeover-"+tc.name)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := db.LoadPublishedCandidate(ctx, ticket.Ref); !errors.Is(err, ErrStaleFence) {
				t.Fatalf("pre-fence publishing resume accepted: %v", err)
			}
			if changed, err := db.FenceRecoveredRunners(ctx, domain.ChannelDev, newLeader); err != nil || changed != 1 {
				t.Fatalf("publishing resume fence changed=%d err=%v", changed, err)
			}
			fenced, err := db.Ticket(ctx, ticket.Ref)
			if err != nil || fenced.Version != current.Version+1 || fenced.RunnerEpoch != current.RunnerEpoch+1 {
				t.Fatalf("publishing resume recovery=%+v prior=%+v err=%v", fenced, current, err)
			}
			if err := db.RebindRecoveredPublishedCandidates(ctx, domain.ChannelDev, newLeader); err != nil {
				t.Fatalf("publishing resume rebind=%v", err)
			}
			if err := db.RebindPublishedCandidate(ctx, ticket.Ref, fenced.Version, domain.Fence{LeaderEpoch: newLeader, RunnerEpoch: fenced.RunnerEpoch}); err != nil {
				t.Fatalf("publishing resume lost-response rebind=%v", err)
			}
			if _, err := db.LoadPublishedCandidate(ctx, ticket.Ref); err != nil {
				t.Fatalf("publishing resume post-fence load=%v", err)
			}
		})
	}
}

func TestWaitingCIResumeTakeoverUsesResumedEndpointRecovery(t *testing.T) {
	for _, tc := range []struct {
		name    string
		blocked bool
		trigger string
		payload string
		resume  string
	}{
		{name: "typed-blocker", blocked: true},
		{name: "semantic-pause", trigger: "ci_red_exhausted", payload: `{"reason":"ci_red_exhausted"}`, resume: "operator_resume"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, ctx, ticket, fence := publicationLifecycleFixture(t)
			recordFixturePublication(t, db, ctx, ticket, fence)
			_, err := db.TransitionPublishedCandidate(ctx, Transition{Ref: ticket.Ref, ExpectedVersion: ticket.Version, From: domain.StatePublishing, To: domain.StateWaitingCI, Trigger: "effects_confirmed", Fence: fence, EventPayload: `{}`})
			if err != nil {
				t.Fatal(err)
			}
			waiting, err := db.Ticket(ctx, ticket.Ref)
			if err != nil {
				t.Fatal(err)
			}
			if tc.blocked {
				blocked, transitionErr := db.TransitionPublishedBlock(ctx, Transition{Ref: ticket.Ref, ExpectedVersion: waiting.Version, From: domain.StateWaitingCI, To: domain.StateBlocked, ResumeState: domain.StateWaitingCI, Trigger: "typed_blocker", Fence: fence, EventPayload: `{"code":"ci_retry_required"}`})
				if transitionErr != nil {
					t.Fatal(transitionErr)
				}
				_, err = db.TransitionPublishedResume(ctx, Transition{Ref: ticket.Ref, ExpectedVersion: blocked.Version, From: domain.StateBlocked, To: domain.StateWaitingCI, Trigger: "operator_recover", Fence: fence, EventPayload: `{}`})
			} else {
				paused, transitionErr := db.Transition(ctx, Transition{Ref: ticket.Ref, ExpectedVersion: waiting.Version, From: domain.StateWaitingCI, To: domain.StatePaused, ResumeState: domain.StateWaitingCI, Trigger: tc.trigger, Fence: fence, EventPayload: tc.payload})
				if transitionErr != nil {
					t.Fatal(transitionErr)
				}
				_, err = db.TransitionPublishedResume(ctx, Transition{Ref: ticket.Ref, ExpectedVersion: paused.Version, From: domain.StatePaused, To: domain.StateWaitingCI, Trigger: tc.resume, Fence: fence, EventPayload: `{}`})
			}
			if err != nil {
				t.Fatal(err)
			}
			resumed, err := db.Ticket(ctx, ticket.Ref)
			if err != nil {
				t.Fatal(err)
			}
			newLeader, err := db.AcquireLeader(ctx, domain.ChannelDev, "waiting-resume-takeover-"+tc.name)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := db.LoadPublishedCandidate(ctx, ticket.Ref); !errors.Is(err, ErrStaleFence) {
				t.Fatalf("pre-fence waiting resume accepted: %v", err)
			}
			if changed, err := db.FenceRecoveredRunners(ctx, domain.ChannelDev, newLeader); err != nil || changed != 1 {
				t.Fatalf("waiting resume fence changed=%d err=%v", changed, err)
			}
			fenced, err := db.Ticket(ctx, ticket.Ref)
			if err != nil || fenced.Version != resumed.Version+1 || fenced.RunnerEpoch != resumed.RunnerEpoch+1 {
				t.Fatalf("waiting resume recovery=%+v prior=%+v err=%v", fenced, resumed, err)
			}
			if err := db.RebindRecoveredPublishedCandidates(ctx, domain.ChannelDev, newLeader); err != nil {
				t.Fatalf("waiting resume rebind/load=%v", err)
			}
			if _, err := db.LoadPublishedCandidate(ctx, ticket.Ref); err != nil {
				t.Fatalf("waiting resume post-fence load=%v", err)
			}
		})
	}
}

func TestLatestCandidateRejectsLeaderOnlyTakeover(t *testing.T) {
	db, ctx, ticket, fence := publicationLifecycleFixture(t)
	candidate, err := db.RecoverableCandidate(ctx, ticket.Ref)
	if err != nil {
		t.Fatal(err)
	}
	// The fixture advances through the publication boundary. Put the isolated
	// current-reader check back at the candidate's authenticated build endpoint;
	// the only subsequent change is daemon leadership.
	if _, err := db.db.ExecContext(ctx, `UPDATE tickets SET state='building',version=?,runner_epoch=? WHERE channel=? AND project_id=? AND id=?`, candidate.TicketVersion, candidate.Fence.RunnerEpoch, ticket.Ref.Channel, ticket.Ref.Project, ticket.Ref.Ticket); err != nil {
		t.Fatal(err)
	}
	if _, err := db.LatestCandidate(ctx, ticket.Ref); err != nil {
		t.Fatalf("current candidate before takeover=%v", err)
	}
	newLeader, err := db.AcquireLeader(ctx, domain.ChannelDev, "latest-candidate-leader-only")
	if err != nil {
		t.Fatal(err)
	}
	_ = fence
	if _, err := db.LatestCandidate(ctx, ticket.Ref); !errors.Is(err, ErrStaleFence) {
		t.Fatalf("latest candidate survived leader-only takeover: %v", err)
	}
	if newLeader == 0 {
		t.Fatal("leader acquisition returned zero")
	}
}

func TestBuilderPublishingRecoveryRebindsOnlyTheLatestCandidate(t *testing.T) {
	db, ctx, ticket, _ := publicationLifecycleFixture(t)
	candidate, err := db.RecoverableCandidate(ctx, ticket.Ref)
	if err != nil {
		t.Fatal(err)
	}
	leader, err := db.AcquireLeader(ctx, domain.ChannelDev, "builder-publishing-recovery")
	if err != nil {
		t.Fatal(err)
	}
	if changed, err := db.FenceRecoveredRunners(ctx, domain.ChannelDev, leader); err != nil || changed != 1 {
		t.Fatalf("fence publishing candidate changed=%d err=%v", changed, err)
	}
	live, err := db.Ticket(ctx, ticket.Ref)
	if err != nil {
		t.Fatal(err)
	}
	fence := domain.Fence{LeaderEpoch: leader, RunnerEpoch: live.RunnerEpoch}
	reusable, err := db.LatestReusableProviderAttempt(ctx, LatestReusableProviderAttemptRequest{Ref: ticket.Ref, Phase: domain.PhaseBuild, Role: "builder", ExpectedVersion: live.Version, Fence: fence})
	if err != nil || !reusable.Recovered || reusable.Key != candidate.BuilderResult {
		t.Fatalf("reusable builder=%+v err=%v", reusable, err)
	}
	if _, err := db.RecordCandidate(ctx, CandidateEvidence{Ref: ticket.Ref, ExpectedVersion: live.Version, Fence: fence, Snapshot: candidate.Snapshot, BuilderResult: candidate.BuilderResult, Commit: candidate.Commit, Reason: "recover builder before publish", CommandResult: candidate.CommandBinding.Key}); err != nil {
		t.Fatalf("rebind candidate=%v", err)
	}
	newGeneration := candidate
	newGeneration.Snapshot.Generation++
	if _, err := db.RecordCandidate(ctx, CandidateEvidence{Ref: ticket.Ref, ExpectedVersion: live.Version, Fence: fence, Snapshot: newGeneration.Snapshot, BuilderResult: newGeneration.BuilderResult, Commit: newGeneration.Commit, Reason: "must not mint during publish", CommandResult: newGeneration.CommandBinding.Key}); !errors.Is(err, ErrEvidenceConflict) {
		t.Fatalf("publishing accepted a new candidate generation: %v", err)
	}
	current, err := db.LatestCandidate(ctx, ticket.Ref)
	if err != nil || current.Snapshot.Generation != candidate.Snapshot.Generation || current.BuilderResult != candidate.BuilderResult || current.TicketVersion != live.Version || current.Fence != fence {
		t.Fatalf("current candidate=%+v err=%v", current, err)
	}

	t.Run("wrong builder role is not bridged", func(t *testing.T) {
		result, _, err := db.LoadHistoricalProviderAttemptResult(ctx, candidate.BuilderResult)
		if err != nil {
			t.Fatal(err)
		}
		result.Claim.Role = "reviewer"
		if err := providerResultReachesFence(ctx, db.db, candidate.BuilderResult, result, live.Version, fence); !errors.Is(err, ErrStaleFence) {
			t.Fatalf("wrong-role bridge accepted: %v", err)
		}
	})

	t.Run("wrong transition is not bridged", func(t *testing.T) {
		other, otherCtx, otherTicket, _ := publicationLifecycleFixture(t)
		otherCandidate, err := other.RecoverableCandidate(otherCtx, otherTicket.Ref)
		if err != nil {
			t.Fatal(err)
		}
		otherLeader, err := other.AcquireLeader(otherCtx, domain.ChannelDev, "builder-publishing-wrong-transition")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := other.FenceRecoveredRunners(otherCtx, domain.ChannelDev, otherLeader); err != nil {
			t.Fatal(err)
		}
		otherLive, err := other.Ticket(otherCtx, otherTicket.Ref)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := other.db.ExecContext(otherCtx, `UPDATE events SET to_state='reviewing' WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version=? AND trigger='phase_pass' AND from_state='building' AND to_state='publishing'`, otherTicket.Ref.Channel, otherTicket.Ref.Project, otherTicket.Ref.Ticket, otherLive.Version-1); err != nil {
			t.Fatal(err)
		}
		result, _, err := other.LoadHistoricalProviderAttemptResult(otherCtx, otherCandidate.BuilderResult)
		if err != nil {
			t.Fatal(err)
		}
		if err := providerResultReachesFence(otherCtx, other.db, otherCandidate.BuilderResult, result, otherLive.Version, domain.Fence{LeaderEpoch: otherLeader, RunnerEpoch: otherLive.RunnerEpoch}); !errors.Is(err, ErrStaleFence) {
			t.Fatalf("wrong-transition bridge accepted: %v", err)
		}
	})
}

func TestPublicationEvidenceLifecycleReplayRecoveryAndBackup(t *testing.T) {
	db, ctx, ticket, fence := publicationLifecycleFixture(t)
	worktree, err := db.Worktree(ctx, ticket.Ref)
	if err != nil {
		t.Fatal(err)
	}
	candidate, err := db.RecoverableCandidate(ctx, ticket.Ref)
	if err != nil {
		t.Fatal(err)
	}
	remoteBase := candidate.Snapshot.BaseSHA
	pr := contracts.PullRequestIdentity{Repository: contracts.RepositoryIdentity{Host: "github.com", Owner: "acme", Name: "app"}, Number: 42, HeadOwner: "acme", HeadRepository: "app", HeadRef: worktree.Branch, HeadOID: candidate.Snapshot.HeadSHA, BaseRef: "main", BaseOID: remoteBase, FactoryOwned: true}
	value := PublishedCandidateEvidence{Ref: ticket.Ref, TicketVersion: ticket.Version, Fence: fence, Candidate: candidate, ConfigGeneration: ticket.ConfigGeneration, ConfigDigest: ticket.ConfigDigest, ConfigSnapshotDigest: sha256Digest(ticket.ConfigSnapshot), Worktree: worktree, RemoteBranchRef: worktree.Branch, RemoteBranchOID: candidate.Snapshot.HeadSHA, RemoteBaseOID: remoteBase, PullRequest: pr, PullRequestState: "OPEN", PullRequestDraft: true, PullRequestObservedAt: time.Now().UTC(), CreatedAt: time.Now().UTC()}
	value.PushEffect = PublicationEffectEvidence{SemanticKey: "publication-push", Kind: PublicationPushEffectKind, RequestDigest: strings.Repeat("1", 64), ClaimEpoch: 1, ObservedIdentity: CanonicalPublicationPushObservation(value.RemoteBranchRef, value.RemoteBranchOID)}
	value.PRCreateOrUpdateEffect = PublicationEffectEvidence{SemanticKey: "publication-pr", Kind: PublicationPRCreateEffectKind, RequestDigest: "sha256:" + strings.Repeat("2", 64), ClaimEpoch: 1, ObservedIdentity: CanonicalPublicationPRObservation(pr, "OPEN", true)}
	for _, effect := range []PublicationEffectEvidence{value.PushEffect, value.PRCreateOrUpdateEffect} {
		if _, err := db.PlanEffect(ctx, EffectPlan{SemanticKey: effect.SemanticKey, Ref: ticket.Ref, Kind: effect.Kind, TicketVersion: ticket.Version, Fence: fence, RequestDigest: effect.RequestDigest}); err != nil {
			t.Fatal(err)
		}
		claim, err := db.ClaimEffect(ctx, EffectFence{SemanticKey: effect.SemanticKey, Ref: ticket.Ref, TicketVersion: ticket.Version, Fence: domain.Fence{LeaderEpoch: fence.LeaderEpoch, RunnerEpoch: fence.RunnerEpoch}})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := db.ConfirmEffect(ctx, EffectFence{SemanticKey: effect.SemanticKey, Ref: ticket.Ref, TicketVersion: ticket.Version, Fence: domain.Fence{LeaderEpoch: claim.Effect.LeaderEpoch, RunnerEpoch: claim.Effect.RunnerEpoch, ClaimEpoch: claim.Effect.ClaimEpoch}}, effect.ObservedIdentity); err != nil {
			t.Fatal(err)
		}
	}
	if err := validPublishedCandidateEvidence(value); err != nil {
		t.Fatalf("fixture witness validation=%v", err)
	}
	if err := db.RecordPublishedCandidate(ctx, value); err != nil {
		t.Fatal(err)
	}
	if err := db.RecordPublishedCandidate(ctx, value); err != nil {
		t.Fatalf("lost-response replay: %v", err)
	}
	loaded, err := db.LoadPublishedCandidate(ctx, ticket.Ref)
	if err != nil || loaded.WitnessDigest == "" || loaded.CreatedAt != value.CreatedAt || loaded.BuildTransitionCreatedAt.IsZero() || loaded.CurrentTicketVersion != ticket.Version {
		t.Fatalf("publication load=%+v err=%v", loaded, err)
	}
	newLeader, err := db.AcquireLeader(ctx, domain.ChannelDev, "publication-recovery-1")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.FenceRecoveredRunners(ctx, domain.ChannelDev, newLeader); err != nil {
		t.Fatal(err)
	}
	current, _ := db.Ticket(ctx, ticket.Ref)
	if err := db.RebindPublishedCandidate(ctx, ticket.Ref, current.Version, domain.Fence{LeaderEpoch: newLeader, RunnerEpoch: current.RunnerEpoch}); err != nil {
		t.Fatal(err)
	}
	newLeader, err = db.AcquireLeader(ctx, domain.ChannelDev, "publication-recovery-2")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.FenceRecoveredRunners(ctx, domain.ChannelDev, newLeader); err != nil {
		t.Fatal(err)
	}
	current, _ = db.Ticket(ctx, ticket.Ref)
	if err := db.RebindPublishedCandidate(ctx, ticket.Ref, current.Version, domain.Fence{LeaderEpoch: newLeader, RunnerEpoch: current.RunnerEpoch}); err != nil {
		t.Fatal(err)
	}
	// Preserve a real publishing fixture with two authenticated recoveries.
	// Waiting-ci recovery must consume only the remaining global ledger budget,
	// rather than receiving a fresh 64-row allowance after the transition.
	waitingSeedDir := t.TempDir()
	if err := os.Chmod(waitingSeedDir, 0o700); err != nil {
		t.Fatal(err)
	}
	waitingSeed := waitingSeedDir + "/waiting-seed.sqlite"
	if err := db.Backup(ctx, waitingSeed); err != nil {
		t.Fatal(err)
	}
	for recovery := 3; recovery <= 64; recovery++ {
		newLeader, err = db.AcquireLeader(ctx, domain.ChannelDev, fmt.Sprintf("publication-recovery-%d", recovery))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := db.FenceRecoveredRunners(ctx, domain.ChannelDev, newLeader); err != nil {
			t.Fatal(err)
		}
		current, _ = db.Ticket(ctx, ticket.Ref)
		if err := db.RebindPublishedCandidate(ctx, ticket.Ref, current.Version, domain.Fence{LeaderEpoch: newLeader, RunnerEpoch: current.RunnerEpoch}); err != nil {
			t.Fatalf("rebind %d: %v", recovery, err)
		}
	}
	current, _ = db.Ticket(ctx, ticket.Ref)
	if err := db.RebindPublishedCandidate(ctx, ticket.Ref, current.Version, domain.Fence{LeaderEpoch: newLeader, RunnerEpoch: current.RunnerEpoch}); err != nil {
		t.Fatalf("64th rebind replay: %v", err)
	}
	if err := db.RebindPublishedCandidate(ctx, ticket.Ref, current.Version, domain.Fence{LeaderEpoch: newLeader, RunnerEpoch: current.RunnerEpoch + 1}); err == nil {
		t.Fatal("wrong-fence rebind replay was accepted")
	}
	var rebindRows int
	if err := db.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM publication_evidence_rebinds WHERE channel=? AND project_id=? AND ticket_id=?`, ticket.Ref.Channel, ticket.Ref.Project, ticket.Ref.Ticket).Scan(&rebindRows); err != nil || rebindRows != 64 {
		t.Fatalf("rebind cap residue rows=%d err=%v", rebindRows, err)
	}
	if err := db.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE channel=? AND project_id=? AND ticket_id=? AND trigger='publication_rebind'`, ticket.Ref.Channel, ticket.Ref.Project, ticket.Ref.Ticket).Scan(&rebindRows); err != nil || rebindRows != 64 {
		t.Fatalf("rebind cap residue events=%d err=%v", rebindRows, err)
	}
	loaded, err = db.LoadPublishedCandidate(ctx, ticket.Ref)
	if err != nil || loaded.CurrentTicketVersion != current.Version || loaded.CurrentFence.LeaderEpoch != newLeader {
		t.Fatalf("rebound publication load=%+v err=%v", loaded, err)
	}
	publicationLatestVersion := current.Version
	backupDir := t.TempDir()
	if err := os.Chmod(backupDir, 0o700); err != nil {
		t.Fatal(err)
	}
	backup := backupDir + "/publication.sqlite"
	if err := db.Backup(ctx, backup); err != nil {
		t.Fatal(err)
	}
	copyDB, err := OpenReadOnly(ctx, backup)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := copyDB.LoadPublishedCandidate(ctx, ticket.Ref); err != nil {
		copyDB.Close()
		t.Fatalf("backup publication load=%v", err)
	}
	copyDB.Close()
	controlGapDir := t.TempDir()
	if err := os.Chmod(controlGapDir, 0o700); err != nil {
		t.Fatal(err)
	}
	controlGapPath := controlGapDir + "/publication-control-gap.sqlite"
	if err := db.Backup(ctx, controlGapPath); err != nil {
		t.Fatal(err)
	}
	leftStateDir := t.TempDir()
	if err := os.Chmod(leftStateDir, 0o700); err != nil {
		t.Fatal(err)
	}
	leftStatePath := leftStateDir + "/publication-left-state.sqlite"
	if err := db.Backup(ctx, leftStatePath); err != nil {
		t.Fatal(err)
	}
	leftState, err := Open(ctx, leftStatePath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := leftState.TransitionAndInvalidateRunner(ctx, Transition{Ref: ticket.Ref, ExpectedVersion: current.Version, From: domain.StatePublishing, To: domain.StateStopping, ResumeState: domain.StatePublishing, Trigger: "operator_pause_or_take", Fence: domain.Fence{LeaderEpoch: newLeader, RunnerEpoch: current.RunnerEpoch}, EventPayload: "{}"}); err != nil {
		leftState.Close()
		t.Fatal(err)
	}
	if err := leftState.RebindPublishedCandidate(ctx, ticket.Ref, current.Version, domain.Fence{LeaderEpoch: newLeader, RunnerEpoch: current.RunnerEpoch}); err == nil {
		leftState.Close()
		t.Fatal("left-state rebind replay was accepted")
	}
	leftState.Close()
	capMutant, err := Open(ctx, backup)
	if err != nil {
		t.Fatal(err)
	}
	capLeader, err := capMutant.AcquireLeader(ctx, domain.ChannelDev, "publication-recovery-cap-65")
	if err != nil {
		capMutant.Close()
		t.Fatal(err)
	}
	capTicket, _ := capMutant.Ticket(ctx, ticket.Ref)
	if capTicket.State != domain.StatePublishing {
		capMutant.Close()
		t.Fatalf("cap fixture state=%s want publishing", capTicket.State)
	}
	var capRecoveryRows int
	if err := capMutant.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM runner_recovery_ledger WHERE channel=? AND project_id=? AND ticket_id=?`, ticket.Ref.Channel, ticket.Ref.Project, ticket.Ref.Ticket).Scan(&capRecoveryRows); err != nil {
		capMutant.Close()
		t.Fatal(err)
	}
	if _, err := capMutant.FenceRecoveredRunners(ctx, domain.ChannelDev, capLeader); err == nil {
		capMutant.Close()
		t.Fatal("startup fencing advanced a publication past the rebind cap")
	}
	capAfter, err := capMutant.Ticket(ctx, ticket.Ref)
	if err != nil || capAfter.Version != capTicket.Version || capAfter.RunnerEpoch != capTicket.RunnerEpoch {
		capMutant.Close()
		t.Fatalf("cap refusal mutated ticket before=%+v after=%+v err=%v", capTicket, capAfter, err)
	}
	if err := capMutant.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM publication_evidence_rebinds WHERE channel=? AND project_id=? AND ticket_id=?`, ticket.Ref.Channel, ticket.Ref.Project, ticket.Ref.Ticket).Scan(&rebindRows); err != nil || rebindRows != 64 {
		capMutant.Close()
		t.Fatalf("65th rebind row residue=%d err=%v", rebindRows, err)
	}
	if err := capMutant.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM runner_recovery_ledger WHERE channel=? AND project_id=? AND ticket_id=?`, ticket.Ref.Channel, ticket.Ref.Project, ticket.Ref.Ticket).Scan(&rebindRows); err != nil || rebindRows != capRecoveryRows {
		capMutant.Close()
		t.Fatalf("cap refusal runner ledger residue=%d want=%d err=%v", rebindRows, capRecoveryRows, err)
	}
	if err := capMutant.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE channel=? AND project_id=? AND ticket_id=? AND trigger='publication_rebind'`, ticket.Ref.Channel, ticket.Ref.Project, ticket.Ref.Ticket).Scan(&rebindRows); err != nil || rebindRows != 64 {
		capMutant.Close()
		t.Fatalf("65th rebind event residue=%d err=%v", rebindRows, err)
	}
	if _, err := capMutant.LoadPublishedCandidate(ctx, ticket.Ref); err == nil {
		capMutant.Close()
		t.Fatal("unrebound live publishing ticket was accepted after cap rejection")
	}
	capMutant.Close()
	controlGap, err := Open(ctx, controlGapPath)
	if err != nil {
		t.Fatal(err)
	}
	gapTicket, _ := controlGap.Ticket(ctx, ticket.Ref)
	var gapDBLeader uint64
	if err := controlGap.db.QueryRowContext(ctx, `SELECT leader_epoch FROM daemon_instances WHERE channel=?`, ticket.Ref.Channel).Scan(&gapDBLeader); err != nil {
		controlGap.Close()
		t.Fatal(err)
	}
	if _, err := controlGap.InvalidateRunner(ctx, ticket.Ref, gapTicket.Version, domain.Fence{LeaderEpoch: newLeader, RunnerEpoch: gapTicket.RunnerEpoch}); err != nil {
		controlGap.Close()
		t.Fatalf("invalidation leader=%d dbLeader=%d ticketVersion=%d runner=%d: %v", newLeader, gapDBLeader, gapTicket.Version, gapTicket.RunnerEpoch, err)
	}
	gapLeader, err := controlGap.AcquireLeader(ctx, domain.ChannelDev, "publication-control-gap-recovery")
	if err != nil {
		controlGap.Close()
		t.Fatal(err)
	}
	if _, err := controlGap.FenceRecoveredRunners(ctx, domain.ChannelDev, gapLeader); err == nil {
		controlGap.Close()
		t.Fatal("publication recovery cap allowed a control-invalidation gap to advance")
	}
	controlGap.Close()
	// The publishing/rebind-cap fixture above has consumed all 64 ledger rows.
	// Continue the waiting-ci path from the two-row snapshot instead, so this
	// branch proves that pre-publication rows consume the same lifetime budget.
	waitingDB, err := Open(ctx, waitingSeed)
	if err != nil {
		t.Fatal(err)
	}
	defer waitingDB.Close()
	db = waitingDB
	current, err = db.Ticket(ctx, ticket.Ref)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.db.QueryRowContext(ctx, `SELECT leader_epoch FROM daemon_instances WHERE channel=?`, ticket.Ref.Channel).Scan(&newLeader); err != nil {
		t.Fatal(err)
	}
	loaded, err = db.LoadPublishedCandidate(ctx, ticket.Ref)
	if err != nil {
		t.Fatal(err)
	}
	publicationLatestVersion = current.Version
	publicationTransition := Transition{Ref: ticket.Ref, ExpectedVersion: current.Version, From: domain.StatePublishing, To: domain.StateWaitingCI, Trigger: "effects_confirmed", Fence: domain.Fence{LeaderEpoch: newLeader, RunnerEpoch: current.RunnerEpoch}}
	if _, err := db.Transition(ctx, publicationTransition); err == nil {
		t.Fatal("generic publishing transition bypass was accepted")
	}
	if _, err := db.TransitionPublishedCandidate(ctx, publicationTransition); err != nil {
		t.Fatal(err)
	}
	if _, err := db.TransitionPublishedCandidate(ctx, publicationTransition); err != nil {
		t.Fatalf("lost-response publication transition replay: %v", err)
	}
	wrongPublicationFence := publicationTransition
	wrongPublicationFence.Fence.RunnerEpoch++
	if _, err := db.TransitionPublishedCandidate(ctx, wrongPublicationFence); err == nil {
		t.Fatal("wrong-fence publication transition replay was accepted")
	}
	if _, err := db.LoadPublishedCandidate(ctx, ticket.Ref); err != nil {
		t.Fatalf("waiting_ci replay=%v", err)
	}
	waitingBaseDir := t.TempDir()
	if err := os.Chmod(waitingBaseDir, 0o700); err != nil {
		t.Fatal(err)
	}
	waitingBase := waitingBaseDir + "/waiting-base.sqlite"
	if err := db.Backup(ctx, waitingBase); err != nil {
		t.Fatal(err)
	}
	selfBaseDir := t.TempDir()
	if err := os.Chmod(selfBaseDir, 0o700); err != nil {
		t.Fatal(err)
	}
	selfBase := selfBaseDir + "/waiting-self.sqlite"
	if err := db.Backup(ctx, selfBase); err != nil {
		t.Fatal(err)
	}
	waitingCapDir := t.TempDir()
	if err := os.Chmod(waitingCapDir, 0o700); err != nil {
		t.Fatal(err)
	}
	waitingCapPath := waitingCapDir + "/waiting-cap.sqlite"
	if err := db.Backup(ctx, waitingCapPath); err != nil {
		t.Fatal(err)
	}
	waitingCap, err := Open(ctx, waitingCapPath)
	if err != nil {
		t.Fatal(err)
	}
	var capReplayLeader uint64
	for recovery := 1; recovery <= 62; recovery++ {
		leader, err := waitingCap.AcquireLeader(ctx, domain.ChannelDev, fmt.Sprintf("waiting-cap-%d", recovery))
		if err != nil {
			waitingCap.Close()
			t.Fatal(err)
		}
		if _, err := waitingCap.FenceRecoveredRunners(ctx, domain.ChannelDev, leader); err != nil {
			waitingCap.Close()
			t.Fatalf("waiting cap recovery %d=%v", recovery, err)
		}
		capReplayLeader = leader
	}
	// The final recorded row is replayable under the same leader even at the
	// cap. It is observation-only and does not consume another row.
	if changed, err := waitingCap.FenceRecoveredRunners(ctx, domain.ChannelDev, capReplayLeader); err != nil || changed != 0 {
		waitingCap.Close()
		t.Fatalf("row-64 same-leader replay changed=%d err=%v", changed, err)
	}
	waitingCapBefore, err := waitingCap.Ticket(ctx, ticket.Ref)
	if err != nil {
		waitingCap.Close()
		t.Fatal(err)
	}
	var waitingLedgerRows int
	if err := waitingCap.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM runner_recovery_ledger WHERE channel=? AND project_id=? AND ticket_id=?`, ticket.Ref.Channel, ticket.Ref.Project, ticket.Ref.Ticket).Scan(&waitingLedgerRows); err != nil || waitingLedgerRows != 64 {
		waitingCap.Close()
		t.Fatalf("global waiting cap ledger rows=%d err=%v", waitingLedgerRows, err)
	}
	var waitingEventRows, waitingRebindRows int
	if err := waitingCap.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE channel=? AND project_id=? AND ticket_id=?`, ticket.Ref.Channel, ticket.Ref.Project, ticket.Ref.Ticket).Scan(&waitingEventRows); err != nil {
		waitingCap.Close()
		t.Fatal(err)
	}
	if err := waitingCap.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM publication_evidence_rebinds WHERE channel=? AND project_id=? AND ticket_id=?`, ticket.Ref.Channel, ticket.Ref.Project, ticket.Ref.Ticket).Scan(&waitingRebindRows); err != nil {
		waitingCap.Close()
		t.Fatal(err)
	}
	leader, err := waitingCap.AcquireLeader(ctx, domain.ChannelDev, "waiting-cap-65")
	if err != nil {
		waitingCap.Close()
		t.Fatal(err)
	}
	if _, err := waitingCap.FenceRecoveredRunners(ctx, domain.ChannelDev, leader); err == nil {
		waitingCap.Close()
		t.Fatal("65th waiting recovery was accepted")
	}
	waitingCapAfter, err := waitingCap.Ticket(ctx, ticket.Ref)
	if err != nil || waitingCapAfter.Version != waitingCapBefore.Version || waitingCapAfter.RunnerEpoch != waitingCapBefore.RunnerEpoch {
		waitingCap.Close()
		t.Fatalf("waiting cap refusal mutated ticket before=%+v after=%+v err=%v", waitingCapBefore, waitingCapAfter, err)
	}
	if err := waitingCap.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM runner_recovery_ledger WHERE channel=? AND project_id=? AND ticket_id=?`, ticket.Ref.Channel, ticket.Ref.Project, ticket.Ref.Ticket).Scan(&waitingLedgerRows); err != nil || waitingLedgerRows != 64 {
		waitingCap.Close()
		t.Fatalf("waiting cap refusal ledger rows=%d err=%v", waitingLedgerRows, err)
	}
	var waitingEventsAfter, waitingRebindsAfter int
	if err := waitingCap.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE channel=? AND project_id=? AND ticket_id=?`, ticket.Ref.Channel, ticket.Ref.Project, ticket.Ref.Ticket).Scan(&waitingEventsAfter); err != nil || waitingEventsAfter != waitingEventRows {
		waitingCap.Close()
		t.Fatalf("waiting cap refusal events=%d want=%d err=%v", waitingEventsAfter, waitingEventRows, err)
	}
	if err := waitingCap.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM publication_evidence_rebinds WHERE channel=? AND project_id=? AND ticket_id=?`, ticket.Ref.Channel, ticket.Ref.Project, ticket.Ref.Ticket).Scan(&waitingRebindsAfter); err != nil || waitingRebindsAfter != waitingRebindRows {
		waitingCap.Close()
		t.Fatalf("waiting cap refusal rebinds=%d want=%d err=%v", waitingRebindsAfter, waitingRebindRows, err)
	}
	// A corrupted database cannot smuggle in a second phase-local budget:
	// every recovery loader and the waiting-chain validator reject row 65.
	cappedStep, found, err := loadLatestRunnerRecovery(ctx, waitingCap.db, ticket.Ref)
	if err != nil || !found {
		waitingCap.Close()
		t.Fatalf("cap latest recovery found=%v err=%v", found, err)
	}
	overCap := RunnerRecoveryLedger{Ref: ticket.Ref, PriorTicketVersion: cappedStep.TicketVersion, PriorRunnerEpoch: cappedStep.RunnerEpoch, PriorLeaderEpoch: cappedStep.LeaderEpoch, TicketVersion: cappedStep.TicketVersion + 1, RunnerEpoch: cappedStep.RunnerEpoch + 1, LeaderEpoch: leader, CreatedAt: time.Now().UTC()}
	overCap.RecoveryDigest = runnerRecoveryDigest(overCap)
	if _, err := waitingCap.db.ExecContext(ctx, `INSERT INTO runner_recovery_ledger(channel,project_id,ticket_id,prior_ticket_version,prior_runner_epoch,prior_leader_epoch,ticket_version,runner_epoch,leader_epoch,recovery_digest,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?)`, overCap.Ref.Channel, overCap.Ref.Project, overCap.Ref.Ticket, overCap.PriorTicketVersion, overCap.PriorRunnerEpoch, overCap.PriorLeaderEpoch, overCap.TicketVersion, overCap.RunnerEpoch, overCap.LeaderEpoch, overCap.RecoveryDigest, overCap.CreatedAt.Format(time.RFC3339Nano)); err != nil {
		waitingCap.Close()
		t.Fatal(err)
	}
	if _, _, err := loadLatestRunnerRecovery(ctx, waitingCap.db, ticket.Ref); err == nil {
		waitingCap.Close()
		t.Fatal("latest runner recovery loader accepted row 65")
	}
	if _, _, err := loadRunnerRecoveryAt(ctx, waitingCap.db, ticket.Ref, overCap.TicketVersion); err == nil {
		waitingCap.Close()
		t.Fatal("point runner recovery loader accepted row 65")
	}
	if _, err := waitingCap.LoadPublishedCandidate(ctx, ticket.Ref); err == nil {
		waitingCap.Close()
		t.Fatal("waiting recovery validator accepted row 65")
	}
	waitingCap.Close()
	waitingBaseLeader := newLeader
	for recovery := 1; recovery <= 2; recovery++ {
		newLeader, err = db.AcquireLeader(ctx, domain.ChannelDev, fmt.Sprintf("waiting-recovery-%d", recovery))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := db.FenceRecoveredRunners(ctx, domain.ChannelDev, newLeader); err != nil {
			t.Fatal(err)
		}
		if _, err := db.LoadPublishedCandidate(ctx, ticket.Ref); err != nil {
			t.Fatalf("waiting_ci recovery %d=%v", recovery, err)
		}
	}
	// A direct runner invalidation has no recovery-ledger or runtime-control
	// proof. Neither startup fencing nor publication replay may treat its bare
	// counter advance as an authenticated predecessor.
	invalidated, err := Open(ctx, waitingBase)
	if err != nil {
		t.Fatal(err)
	}
	invalidTicket, _ := invalidated.Ticket(ctx, ticket.Ref)
	if _, err := invalidated.InvalidateRunner(ctx, ticket.Ref, invalidTicket.Version, domain.Fence{LeaderEpoch: waitingBaseLeader, RunnerEpoch: invalidTicket.RunnerEpoch}); err != nil {
		invalidated.Close()
		t.Fatal(err)
	}
	invalidLeader, err := invalidated.AcquireLeader(ctx, domain.ChannelDev, "waiting-invalidation-recovery")
	if err != nil {
		invalidated.Close()
		t.Fatal(err)
	}
	invalidatedBeforeFence, err := invalidated.Ticket(ctx, ticket.Ref)
	if err != nil {
		invalidated.Close()
		t.Fatal(err)
	}
	var invalidatedLedgerRows int
	if err := invalidated.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM runner_recovery_ledger WHERE channel=? AND project_id=? AND ticket_id=?`, ticket.Ref.Channel, ticket.Ref.Project, ticket.Ref.Ticket).Scan(&invalidatedLedgerRows); err != nil {
		invalidated.Close()
		t.Fatal(err)
	}
	if _, err := invalidated.FenceRecoveredRunners(ctx, domain.ChannelDev, invalidLeader); !errors.Is(err, ErrPublicationEvidence) {
		invalidated.Close()
		t.Fatalf("bare control invalidation fence=%v", err)
	}
	invalidatedAfterFence, err := invalidated.Ticket(ctx, ticket.Ref)
	if err != nil || invalidatedAfterFence.Version != invalidatedBeforeFence.Version || invalidatedAfterFence.RunnerEpoch != invalidatedBeforeFence.RunnerEpoch {
		invalidated.Close()
		t.Fatalf("bare control invalidation fence mutated ticket before=%+v after=%+v err=%v", invalidatedBeforeFence, invalidatedAfterFence, err)
	}
	var invalidatedLedgerRowsAfter int
	if err := invalidated.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM runner_recovery_ledger WHERE channel=? AND project_id=? AND ticket_id=?`, ticket.Ref.Channel, ticket.Ref.Project, ticket.Ref.Ticket).Scan(&invalidatedLedgerRowsAfter); err != nil || invalidatedLedgerRowsAfter != invalidatedLedgerRows {
		invalidated.Close()
		t.Fatalf("bare control invalidation ledger rows=%d want=%d err=%v", invalidatedLedgerRowsAfter, invalidatedLedgerRows, err)
	}
	if _, err := invalidated.LoadPublishedCandidate(ctx, ticket.Ref); err == nil {
		invalidated.Close()
		t.Fatal("waiting replay accepted a direct invalidation gap")
	}
	invalidated.Close()
	selfTransition, err := Open(ctx, selfBase)
	if err != nil {
		t.Fatal(err)
	}
	selfTicket, _ := selfTransition.Ticket(ctx, ticket.Ref)
	if _, err := selfTransition.Transition(ctx, Transition{Ref: ticket.Ref, ExpectedVersion: selfTicket.Version, From: domain.StateWaitingCI, To: domain.StateWaitingCI, Trigger: "recovery_self_transition", Fence: domain.Fence{LeaderEpoch: waitingBaseLeader, RunnerEpoch: selfTicket.RunnerEpoch}, EventPayload: "{}"}); err == nil {
		selfTransition.Close()
		t.Fatal("generic publication recovery transition was accepted")
	}
	selfTransition.Close()
	// Every recovery row is immutable and part of the exact waiting chain.
	// Mutating, deleting, or relocating one row therefore invalidates replay.
	waitingVersion := publicationLatestVersion + 1
	var firstRecoveryVersion, latestRecoveryVersion uint64
	if err := db.db.QueryRowContext(ctx, `SELECT MIN(ticket_version),MAX(ticket_version) FROM runner_recovery_ledger WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version>?`, ticket.Ref.Channel, ticket.Ref.Project, ticket.Ref.Ticket, waitingVersion).Scan(&firstRecoveryVersion, &latestRecoveryVersion); err != nil || firstRecoveryVersion == 0 || latestRecoveryVersion == 0 {
		t.Fatalf("recovery ledger bounds first=%d latest=%d err=%v", firstRecoveryVersion, latestRecoveryVersion, err)
	}
	ledgerTamper := map[string]struct {
		statement string
		trigger   string
		version   uint64
	}{
		"ledger-latest-digest": {statement: `UPDATE runner_recovery_ledger SET recovery_digest='sha256:` + strings.Repeat("f", 64) + `' WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version=?`, trigger: "update", version: latestRecoveryVersion},
		"ledger-latest-leader": {statement: `UPDATE runner_recovery_ledger SET leader_epoch=leader_epoch+1 WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version=?`, trigger: "update", version: latestRecoveryVersion},
		"ledger-middle-delete": {statement: `DELETE FROM runner_recovery_ledger WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version=?`, trigger: "delete", version: firstRecoveryVersion},
	}
	for name, tamper := range ledgerTamper {
		dir := t.TempDir()
		if err := os.Chmod(dir, 0o700); err != nil {
			t.Fatal(err)
		}
		path := dir + "/" + name + ".sqlite"
		if err := db.Backup(ctx, path); err != nil {
			t.Fatal(err)
		}
		mutant, err := Open(ctx, path)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := mutant.db.ExecContext(ctx, `DROP TRIGGER runner_recovery_ledger_immutable_`+tamper.trigger); err != nil {
			mutant.Close()
			t.Fatal(err)
		}
		result, err := mutant.db.ExecContext(ctx, tamper.statement, ticket.Ref.Channel, ticket.Ref.Project, ticket.Ref.Ticket, tamper.version)
		if err != nil {
			mutant.Close()
			t.Fatal(err)
		}
		if changed, err := result.RowsAffected(); err != nil || changed != 1 {
			mutant.Close()
			t.Fatalf("tampered %s affected=%d err=%v", name, changed, err)
		}
		if _, err := mutant.LoadPublishedCandidate(ctx, ticket.Ref); err == nil {
			mutant.Close()
			t.Fatalf("tampered %s ledger was accepted", name)
		}
		mutant.Close()
	}
	duplicateDir := t.TempDir()
	if err := os.Chmod(duplicateDir, 0o700); err != nil {
		t.Fatal(err)
	}
	duplicatePath := duplicateDir + "/duplicate.sqlite"
	if err := db.Backup(ctx, duplicatePath); err != nil {
		t.Fatal(err)
	}
	duplicate, err := Open(ctx, duplicatePath)
	if err != nil {
		t.Fatal(err)
	}
	var duplicateInsertErr error
	_, duplicateInsertErr = duplicate.db.ExecContext(ctx, `INSERT INTO runner_recovery_ledger SELECT channel,project_id,ticket_id,prior_ticket_version,prior_runner_epoch,prior_leader_epoch,ticket_version,runner_epoch,leader_epoch,recovery_digest,created_at FROM runner_recovery_ledger WHERE channel=? AND project_id=? AND ticket_id=? LIMIT 1`, ticket.Ref.Channel, ticket.Ref.Project, ticket.Ref.Ticket)
	if duplicateInsertErr == nil {
		duplicate.Close()
		t.Fatal("duplicate recovery ledger row was accepted")
	}
	duplicate.Close()
	wrongDir := t.TempDir()
	if err := os.Chmod(wrongDir, 0o700); err != nil {
		t.Fatal(err)
	}
	wrongPath := wrongDir + "/wrong-predecessor.sqlite"
	if err := db.Backup(ctx, wrongPath); err != nil {
		t.Fatal(err)
	}
	wrong, err := Open(ctx, wrongPath)
	if err != nil {
		t.Fatal(err)
	}
	latest, found, err := loadLatestRunnerRecovery(ctx, wrong.db, ticket.Ref)
	if err != nil || !found {
		wrong.Close()
		t.Fatalf("latest recovery row found=%v err=%v", found, err)
	}
	wrongStep := RunnerRecoveryLedger{Ref: ticket.Ref, PriorTicketVersion: latest.TicketVersion + 5, PriorRunnerEpoch: latest.RunnerEpoch + 5, PriorLeaderEpoch: latest.LeaderEpoch, TicketVersion: latest.TicketVersion + 6, RunnerEpoch: latest.RunnerEpoch + 6, LeaderEpoch: latest.LeaderEpoch + 1, CreatedAt: time.Now().UTC()}
	wrongStep.RecoveryDigest = runnerRecoveryDigest(wrongStep)
	if _, err := wrong.db.ExecContext(ctx, `INSERT INTO runner_recovery_ledger(channel,project_id,ticket_id,prior_ticket_version,prior_runner_epoch,prior_leader_epoch,ticket_version,runner_epoch,leader_epoch,recovery_digest,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?)`, ticket.Ref.Channel, ticket.Ref.Project, ticket.Ref.Ticket, wrongStep.PriorTicketVersion, wrongStep.PriorRunnerEpoch, wrongStep.PriorLeaderEpoch, wrongStep.TicketVersion, wrongStep.RunnerEpoch, wrongStep.LeaderEpoch, wrongStep.RecoveryDigest, wrongStep.CreatedAt.Format(time.RFC3339Nano)); err != nil {
		wrong.Close()
		t.Fatal(err)
	}
	if _, err := wrong.LoadPublishedCandidate(ctx, ticket.Ref); err == nil {
		wrong.Close()
		t.Fatal("wrong-predecessor recovery row was accepted")
	}
	wrong.Close()
	for name, statement := range map[string]string{
		"waiting-wrong-transition":   fmt.Sprintf(`UPDATE events SET trigger='wrong_transition' WHERE channel='%s' AND project_id='%s' AND ticket_id='%s' AND ticket_version=%d AND trigger='effects_confirmed'`, ticket.Ref.Channel, ticket.Ref.Project, ticket.Ref.Ticket, waitingVersion),
		"waiting-missing-transition": fmt.Sprintf(`DELETE FROM events WHERE channel='%s' AND project_id='%s' AND ticket_id='%s' AND ticket_version=%d AND trigger='effects_confirmed'`, ticket.Ref.Channel, ticket.Ref.Project, ticket.Ref.Ticket, waitingVersion),
		"waiting-event-timestamp":    fmt.Sprintf(`UPDATE events SET created_at='2026-08-30T00:00:00Z' WHERE channel='%s' AND project_id='%s' AND ticket_id='%s' AND ticket_version=%d AND trigger='effects_confirmed'`, ticket.Ref.Channel, ticket.Ref.Project, ticket.Ref.Ticket, waitingVersion),
	} {
		dir := t.TempDir()
		if err := os.Chmod(dir, 0o700); err != nil {
			t.Fatal(err)
		}
		path := dir + "/" + name + ".sqlite"
		if err := db.Backup(ctx, path); err != nil {
			t.Fatal(err)
		}
		mutant, err := Open(ctx, path)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := mutant.db.ExecContext(ctx, statement); err != nil {
			mutant.Close()
			t.Fatal(err)
		}
		if _, err := mutant.LoadPublishedCandidate(ctx, ticket.Ref); err == nil {
			mutant.Close()
			t.Fatalf("tampered %s waiting transition was accepted", name)
		}
		mutant.Close()
	}
	// A prior generation's state transition is historical context, not the
	// transition that consumed this witness. It must not poison current replay.
	dir := t.TempDir()
	if err := os.Chmod(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	historicalPath := dir + "/waiting-historical.sqlite"
	if err := db.Backup(ctx, historicalPath); err != nil {
		t.Fatal(err)
	}
	historical, err := Open(ctx, historicalPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := historical.db.ExecContext(ctx, `INSERT INTO events(channel,project_id,ticket_id,ticket_version,trigger,from_state,to_state,payload,created_at) VALUES(?,?,?,?,?,?,?,?,?)`, ticket.Ref.Channel, ticket.Ref.Project, ticket.Ref.Ticket, loaded.TicketVersion, "effects_confirmed", "publishing", "waiting_ci", "{}", time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
		historical.Close()
		t.Fatal(err)
	}
	if _, err := historical.LoadPublishedCandidate(ctx, ticket.Ref); err != nil {
		historical.Close()
		t.Fatalf("historical extra transition poisoned current replay: %v", err)
	}
	historical.Close()
	// Tamper tests use independent backups so the good lifecycle fixture remains
	// available for the waiting_ci assertion above.
	for name, statement := range map[string]string{"witness": `UPDATE publication_evidence SET witness_digest='sha256:` + strings.Repeat("f", 64) + `'`, "witness-timestamp": `UPDATE publication_evidence SET created_at='2026-08-30T00:00:00Z'`, "effect": `UPDATE effects SET observed_identity='tampered' WHERE semantic_key='publication-push'`} {
		dir := t.TempDir()
		if err := os.Chmod(dir, 0o700); err != nil {
			t.Fatal(err)
		}
		path := dir + "/" + name + ".sqlite"
		if err := db.Backup(ctx, path); err != nil {
			t.Fatal(err)
		}
		mutant, err := Open(ctx, path)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := mutant.db.ExecContext(ctx, `DROP TRIGGER publication_evidence_immutable_update; DROP TRIGGER publication_evidence_immutable_delete;`); err != nil {
			mutant.Close()
			t.Fatal(err)
		}
		if _, err := mutant.db.ExecContext(ctx, statement); err != nil {
			mutant.Close()
			t.Fatal(err)
		}
		if _, err := mutant.LoadPublishedCandidate(ctx, ticket.Ref); err == nil {
			mutant.Close()
			t.Fatalf("tampered %s witness was accepted", name)
		}
		mutant.Close()
	}
	// The rebind history is append-only and chained. Exercise corruption of a
	// middle row, deletion of that row (a gap), and a duplicated event claim;
	// each is performed on an independent backup after dropping only the
	// relevant immutability triggers.
	middleVersion := ticket.Version + 1
	latestVersion := publicationLatestVersion
	rebindTamper := map[string]func(*Store) error{
		"rebind-middle-digest": func(mutant *Store) error {
			_, err := mutant.db.ExecContext(ctx, `UPDATE publication_evidence_rebinds SET rebind_digest=? WHERE channel=? AND project_id=? AND ticket_id=? AND candidate_generation=? AND candidate_head_sha=? AND ticket_version=?`, "sha256:"+strings.Repeat("e", 64), ticket.Ref.Channel, ticket.Ref.Project, ticket.Ref.Ticket, candidate.Snapshot.Generation, candidate.Snapshot.HeadSHA, middleVersion)
			return err
		},
		"rebind-middle-prior-fence": func(mutant *Store) error {
			_, err := mutant.db.ExecContext(ctx, `UPDATE publication_evidence_rebinds SET prior_leader_epoch=prior_leader_epoch+1 WHERE channel=? AND project_id=? AND ticket_id=? AND candidate_generation=? AND candidate_head_sha=? AND ticket_version=?`, ticket.Ref.Channel, ticket.Ref.Project, ticket.Ref.Ticket, candidate.Snapshot.Generation, candidate.Snapshot.HeadSHA, middleVersion)
			return err
		},
		"rebind-middle-timestamp": func(mutant *Store) error {
			_, err := mutant.db.ExecContext(ctx, `UPDATE publication_evidence_rebinds SET created_at='2026-08-30T00:00:00Z' WHERE channel=? AND project_id=? AND ticket_id=? AND candidate_generation=? AND candidate_head_sha=? AND ticket_version=?`, ticket.Ref.Channel, ticket.Ref.Project, ticket.Ref.Ticket, candidate.Snapshot.Generation, candidate.Snapshot.HeadSHA, middleVersion)
			return err
		},
		"rebind-middle-delete": func(mutant *Store) error {
			_, err := mutant.db.ExecContext(ctx, `DELETE FROM publication_evidence_rebinds WHERE channel=? AND project_id=? AND ticket_id=? AND candidate_generation=? AND candidate_head_sha=? AND ticket_version=?`, ticket.Ref.Channel, ticket.Ref.Project, ticket.Ref.Ticket, candidate.Snapshot.Generation, candidate.Snapshot.HeadSHA, middleVersion)
			return err
		},
		"rebind-latest-gap": func(mutant *Store) error {
			_, err := mutant.db.ExecContext(ctx, `UPDATE publication_evidence_rebinds SET ticket_version=ticket_version+1 WHERE channel=? AND project_id=? AND ticket_id=? AND candidate_generation=? AND candidate_head_sha=? AND ticket_version=?`, ticket.Ref.Channel, ticket.Ref.Project, ticket.Ref.Ticket, candidate.Snapshot.Generation, candidate.Snapshot.HeadSHA, latestVersion)
			return err
		},
		"rebind-duplicate-event": func(mutant *Store) error {
			var payload string
			if err := mutant.db.QueryRowContext(ctx, `SELECT payload FROM events WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version=? AND trigger='publication_rebind'`, ticket.Ref.Channel, ticket.Ref.Project, ticket.Ref.Ticket, latestVersion).Scan(&payload); err != nil {
				return err
			}
			_, err := mutant.db.ExecContext(ctx, `INSERT INTO events(channel,project_id,ticket_id,ticket_version,trigger,from_state,to_state,payload,created_at) VALUES(?,?,?,?,?,?,?,?,?)`, ticket.Ref.Channel, ticket.Ref.Project, ticket.Ref.Ticket, latestVersion, "publication_rebind", "publishing", "publishing", payload, time.Now().UTC().Format(time.RFC3339Nano))
			return err
		},
	}
	for name, mutate := range rebindTamper {
		dir := t.TempDir()
		if err := os.Chmod(dir, 0o700); err != nil {
			t.Fatal(err)
		}
		path := dir + "/" + name + ".sqlite"
		if err := db.Backup(ctx, path); err != nil {
			t.Fatal(err)
		}
		mutant, err := Open(ctx, path)
		if err != nil {
			t.Fatal(err)
		}
		if name != "rebind-duplicate-event" {
			if _, err := mutant.db.ExecContext(ctx, `DROP TRIGGER publication_evidence_rebinds_immutable_update`); err != nil {
				mutant.Close()
				t.Fatal(err)
			}
			if name == "rebind-middle-delete" {
				if _, err := mutant.db.ExecContext(ctx, `DROP TRIGGER publication_evidence_rebinds_immutable_delete`); err != nil {
					mutant.Close()
					t.Fatal(err)
				}
			}
		}
		if err := mutate(mutant); err != nil {
			mutant.Close()
			t.Fatal(err)
		}
		if _, err := mutant.LoadPublishedCandidate(ctx, ticket.Ref); err == nil {
			mutant.Close()
			t.Fatalf("tampered %s rebind chain was accepted", name)
		}
		mutant.Close()
	}
}
