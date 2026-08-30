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

	first, err := database.RecordVerification(ctx, VerificationArtifact{Ref: ref, ExpectedVersion: version, Fence: fence, Intent: []byte("intent one"), Proof: []byte("proof one"), OwnedFiles: []string{"verify_test.go"}, CheckpointID: evidenceOID("a")})
	if err != nil || first.Revision != 1 {
		t.Fatalf("first=%+v err=%v", first, err)
	}
	second, err := database.RecordVerification(ctx, VerificationArtifact{Ref: ref, ExpectedVersion: version, Fence: fence, Intent: []byte("intent two"), Proof: []byte("proof two"), OwnedFiles: []string{"verify_test.go"}, CheckpointID: evidenceOID("b"), AmendsRevision: first.Revision, Reason: "fix assertion", Requester: "reviewer"})
	if err != nil || second.Revision != 2 {
		t.Fatalf("second=%+v err=%v", second, err)
	}
	history, err := database.VerificationRevisions(ctx, ref)
	if err != nil || len(history) != 2 || history[1].Amends != 1 {
		t.Fatalf("history=%+v err=%v", history, err)
	}

	candidate := CandidateEvidence{Ref: ref, ExpectedVersion: version, Fence: fence, Reason: "candidate head changed", Snapshot: domain.CandidateSnapshot{BaseSHA: evidenceOID("c"), HeadSHA: evidenceOID("d"), TreeSHA: evidenceOID("e"), SourceDigest: evidenceDigest("source"), VerificationIntentDigest: second.IntentDigest, ProofDigest: second.ProofDigest, CommandPolicyDigest: evidenceDigest("policy")}}
	receipts, err := database.RecordCandidate(ctx, candidate)
	if err != nil || strings.Join(sortedReceiptKinds(receipts), ",") != "approval,final_review,github_checks,proof_result" {
		t.Fatalf("receipts=%+v err=%v", receipts, err)
	}
	if err := database.RecordOperatorDecision(ctx, OperatorDecision{Ref: ref, ExpectedVersion: version, Fence: fence, ReviewedHead: candidate.Snapshot.HeadSHA, OperatorUID: 501, Decision: "approved"}); err != nil {
		t.Fatal(err)
	}
	candidate.Snapshot.HeadSHA = evidenceOID("f")
	candidate.Snapshot.TreeSHA = evidenceOID("f")
	if _, err := database.RecordCandidate(ctx, candidate); err != nil {
		t.Fatal(err)
	}
	var invalidated int
	if err := database.db.QueryRow(`SELECT invalidated FROM approvals WHERE channel='dev' AND ticket_id='SF-evidence'`).Scan(&invalidated); err != nil || invalidated != 1 {
		t.Fatalf("approval invalidated=%d err=%v", invalidated, err)
	}
	if _, err := database.RecordPlan(ctx, PlanArtifact{Ref: ref, ExpectedVersion: version + 1, Fence: fence, Document: plan.Document}); !errors.Is(err, ErrStaleFence) {
		t.Fatalf("stale fence=%v", err)
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
