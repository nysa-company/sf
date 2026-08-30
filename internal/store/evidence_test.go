package store

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/nysa-company/sf/internal/domain"
)

func evidenceFixture(t *testing.T) (*Store, context.Context, domain.TicketRef, uint64, domain.Fence) {
	t.Helper()
	database, ctx := openTestStore(t)
	ref := domain.TicketRef{Channel: domain.ChannelDev, Project: "nysa", Ticket: "SF-evidence"}
	if err := database.CreateTicket(ctx, ticket(ref, "evidence-digest")); err != nil {
		t.Fatal(err)
	}
	leader, err := database.AcquireLeader(ctx, domain.ChannelDev, "evidence-daemon")
	if err != nil {
		t.Fatal(err)
	}
	loaded, err := database.Ticket(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	return database, ctx, ref, loaded.Version, domain.Fence{LeaderEpoch: leader, RunnerEpoch: loaded.RunnerEpoch}
}

func evidenceDigest(value string) string { return sha256Digest([]byte(value)) }
func evidenceOID(value string) string    { return strings.Repeat(value, 40) }

func TestEvidencePlanVerificationCandidateAndApprovalFences(t *testing.T) {
	database, ctx, ref, version, fence := evidenceFixture(t)
	if _, err := database.RecordVerification(ctx, VerificationArtifact{Ref: ref, ExpectedVersion: version, Fence: fence, Intent: []byte("intent"), Proof: []byte("proof"), OwnedFiles: []string{"verify_test.go"}, CheckpointID: "legacy-checkpoint"}); err == nil {
		t.Fatal("legacy non-OID checkpoint was accepted")
	}
	plan := PlanArtifact{Ref: ref, ExpectedVersion: version, Fence: fence, Document: PlanDocument{Acceptance: []string{"works"}, ProofKind: "regression", Paths: []string{"src"}, Commands: [][]string{{"go", "test", "./..."}}, Risks: []string{"migration"}}}
	digest, err := database.RecordPlan(ctx, plan)
	if err != nil || digest == "" {
		t.Fatalf("plan digest=%q err=%v", digest, err)
	}
	if replay, err := database.RecordPlan(ctx, plan); err != nil || replay != digest {
		t.Fatalf("plan replay=%q err=%v", replay, err)
	}
	plan.Document.ProofKind = "changed"
	if _, err := database.RecordPlan(ctx, plan); !errors.Is(err, ErrEvidenceConflict) {
		t.Fatalf("plan mutation=%v", err)
	}

	if _, err := database.RecordVerification(ctx, VerificationArtifact{Ref: ref, ExpectedVersion: version, Fence: fence, Intent: []byte("intent one"), Proof: []byte("proof one"), OwnedFiles: []string{"verify_test.go"}, CheckpointID: evidenceOID("a")}); !errors.Is(err, ErrEvidenceConflict) {
		t.Fatalf("verification without immutable provider/result authority=%v", err)
	}

	// Direct snapshots cannot be adopted after v31: CandidateEvidence must
	// carry authenticated Builder completion evidence.
	if _, err := database.RecordPlan(ctx, PlanArtifact{Ref: ref, ExpectedVersion: version + 1, Fence: fence, Document: plan.Document}); !errors.Is(err, ErrStaleFence) {
		t.Fatalf("stale fence=%v", err)
	}
}

func TestTransitionCandidateRefusesReplacementGenerationAtomically(t *testing.T) {
	database, ctx, ref, version, fence := evidenceFixture(t)
	if _, err := database.db.ExecContext(ctx, `UPDATE tickets SET state='building' WHERE channel=? AND project_id=? AND id=?`, ref.Channel, ref.Project, ref.Ticket); err != nil {
		t.Fatal(err)
	}
	if err := database.RegisterWorktree(ctx, WorktreeRegistration{Ref: ref, ExpectedVersion: version, Fence: fence, Path: "/tmp/candidate-race", Branch: "dev/candidate-race", IdentityJSON: []byte(`{"repository":"/tmp/candidate-race"}`), BaseSHA: evidenceOID("a"), HeadSHA: evidenceOID("b")}); err != nil {
		t.Fatal(err)
	}
	// The old snapshot is deliberately injected without a v36 result binding;
	// it must never authorize a transition, even if its projections look valid.
	base := domain.CandidateSnapshot{Generation: 1, BaseSHA: evidenceOID("a"), HeadSHA: evidenceOID("d"), TreeSHA: evidenceOID("e"), SourceDigest: "evidence-digest", VerificationIntentDigest: evidenceDigest("intent"), ProofDigest: evidenceDigest("proof"), CommandPolicyDigest: evidenceDigest("policy"), BuilderEvidenceDigest: evidenceDigest("builder")}
	for _, candidate := range []domain.CandidateSnapshot{base, func() domain.CandidateSnapshot {
		v := base
		v.Generation = 2
		v.HeadSHA = evidenceOID("f")
		v.TreeSHA = evidenceOID("0")
		return v
	}()} {
		if _, err := database.db.ExecContext(ctx, `INSERT INTO candidate_snapshots(channel,project_id,ticket_id,generation,ticket_version,leader_epoch,runner_epoch,base_sha,head_sha,tree_sha,source_digest,verification_intent_digest,proof_digest,command_policy_digest,builder_evidence_digest,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, ref.Channel, ref.Project, ref.Ticket, candidate.Generation, version, fence.LeaderEpoch, fence.RunnerEpoch, candidate.BaseSHA, candidate.HeadSHA, candidate.TreeSHA, candidate.SourceDigest, candidate.VerificationIntentDigest, candidate.ProofDigest, candidate.CommandPolicyDigest, candidate.BuilderEvidenceDigest, now()); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := database.TransitionCandidate(ctx, Transition{Ref: ref, ExpectedVersion: version, From: domain.StateBuilding, To: domain.StatePublishing, Trigger: "phase_pass", Fence: fence, EventPayload: "{}"}, base); !errors.Is(err, ErrEvidenceConflict) {
		t.Fatalf("candidate replacement err=%v", err)
	}
	current, err := database.Ticket(ctx, ref)
	if err != nil || current.State != domain.StateBuilding || current.Version != version {
		t.Fatalf("ticket=%+v err=%v", current, err)
	}
	// Removing the competing generation leaves an otherwise valid immutable
	// snapshot, but no v34 current-fence result binding.  Transition authority
	// must fail closed rather than infer recovery from the snapshot's old claim.
	if _, err := database.db.ExecContext(ctx, `DELETE FROM candidate_snapshots WHERE channel=? AND project_id=? AND ticket_id=? AND generation=2`, ref.Channel, ref.Project, ref.Ticket); err != nil {
		t.Fatal(err)
	}
	if _, err := database.TransitionCandidate(ctx, Transition{Ref: ref, ExpectedVersion: version, From: domain.StateBuilding, To: domain.StatePublishing, Trigger: "phase_pass", Fence: fence, EventPayload: "{}"}, base); !errors.Is(err, ErrEvidenceConflict) {
		t.Fatalf("legacy unbound candidate transition=%v", err)
	}
}

func TestPlanEvidencePreservesArgvAndRejectsFlattenedCommands(t *testing.T) {
	database, ctx, ref, version, fence := evidenceFixture(t)
	document := PlanDocument{
		Acceptance: []string{"argv remains exact"},
		ProofKind:  "regression",
		Paths:      []string{"internal"},
		Commands:   [][]string{{"go", "test", "./internal/example", "-run", "Test one; still one argument"}},
		Risks:      []string{"command policy"},
	}
	if _, err := database.RecordPlan(ctx, PlanArtifact{Ref: ref, ExpectedVersion: version, Fence: fence, Document: document}); err != nil {
		t.Fatal(err)
	}
	stored, err := database.Plan(ctx, ref)
	if err != nil || len(stored.Document.Commands) != 1 || len(stored.Document.Commands[0]) != 5 || stored.Document.Commands[0][4] != "Test one; still one argument" {
		t.Fatalf("commands=%v err=%v", stored.Document.Commands, err)
	}

	var legacy PlanDocument
	if err := json.Unmarshal([]byte(`{"acceptance":["one"],"proof_kind":"regression","paths":["internal"],"commands":["go test ./..."],"risks":["one"]}`), &legacy); err == nil {
		t.Fatal("flattened legacy command unexpectedly decoded as argv")
	}
}

func TestEvidencePhaseWorktreeAndBoundedBudgets(t *testing.T) {
	database, ctx, ref, version, fence := evidenceFixture(t)
	attempt := PhaseAttempt{Ref: ref, Phase: domain.PhasePlanning, Attempt: 1, ExpectedVersion: version, Fence: fence, Provider: domain.ProviderIdentity{Provider: "cursor", Model: "m", Family: "cursor", Version: "1"}, WorktreeID: "sha256:worktree", BaseSHA: evidenceOID("a")}
	if err := database.StartPhaseAttempt(ctx, attempt); err != nil {
		t.Fatal(err)
	}
	attempt.Outcome, attempt.UsageJSON = "completed", []byte(`{"tokens":12}`)
	if err := database.CompletePhaseAttempt(ctx, attempt); err != nil {
		t.Fatal(err)
	}
	if err := database.CompletePhaseAttempt(ctx, attempt); err != nil {
		t.Fatalf("completion replay=%v", err)
	}
	changed := attempt
	changed.Provider.Model = "other"
	if err := database.CompletePhaseAttempt(ctx, changed); !errors.Is(err, ErrEvidenceConflict) {
		t.Fatalf("provider identity mutation=%v", err)
	}
	registration := WorktreeRegistration{Ref: ref, ExpectedVersion: version, Fence: fence, Path: "/tmp/sf-wt", Branch: "sf/dev/a/b", IdentityJSON: []byte(`{"repository":"/tmp/nysa"}`), BaseSHA: evidenceOID("a"), HeadSHA: evidenceOID("b")}
	if err := database.RegisterWorktree(ctx, registration); err != nil {
		t.Fatal(err)
	}
	registration.HeadSHA = evidenceOID("c")
	if err := database.RegisterWorktree(ctx, registration); !errors.Is(err, ErrEvidenceConflict) {
		t.Fatalf("worktree identity mutation=%v", err)
	}
	for _, id := range []string{"one", "two"} {
		used, err := database.ConsumeBudget(ctx, BudgetUse{Ref: ref, ExpectedVersion: version, Fence: fence, Kind: "correction", RequestID: id})
		if err != nil || used != len(strings.Split("one,two", ",")) && id == "two" {
			t.Fatalf("budget id=%s used=%d err=%v", id, used, err)
		}
	}
	if _, err := database.ConsumeBudget(ctx, BudgetUse{Ref: ref, ExpectedVersion: version, Fence: fence, Kind: "correction", RequestID: "three"}); !errors.Is(err, ErrBudgetExhausted) {
		t.Fatalf("budget overflow=%v", err)
	}
	if used, err := database.ConsumeBudget(ctx, BudgetUse{Ref: ref, ExpectedVersion: version, Fence: fence, Kind: "correction", RequestID: "two"}); err != nil || used != 2 {
		t.Fatalf("budget replay used=%d err=%v", used, err)
	}
}

func TestEvidenceBudgetConcurrentWritersRemainBounded(t *testing.T) {
	database, ctx, ref, version, fence := evidenceFixture(t)
	start := make(chan struct{})
	results := make(chan error, 3)
	var wait sync.WaitGroup
	for _, id := range []string{"a", "b", "c"} {
		wait.Add(1)
		go func(id string) {
			defer wait.Done()
			<-start
			_, err := database.ConsumeBudget(ctx, BudgetUse{Ref: ref, ExpectedVersion: version, Fence: fence, Kind: "fallback", RequestID: id})
			results <- err
		}(id)
	}
	close(start)
	wait.Wait()
	close(results)
	success := 0
	for err := range results {
		if err == nil {
			success++
		} else if !errors.Is(err, ErrBudgetExhausted) {
			t.Fatalf("unexpected concurrent error=%v", err)
		}
	}
	if success != 1 {
		t.Fatalf("fallback budget admitted %d uses", success)
	}
}
