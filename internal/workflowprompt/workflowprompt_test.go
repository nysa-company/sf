package workflowprompt

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/git"
	"github.com/nysa-company/sf/internal/phaseartifact"
)

const (
	testOID    = "0123456789abcdef0123456789abcdef01234567"
	testDigest = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
)

type noopMutationAuthority struct{}
type noopMutationLease struct{}

func (noopMutationAuthority) AcquireGitMutation(context.Context, contracts.GitMutationClaim) (contracts.GitMutationLease, error) {
	return noopMutationLease{}, nil
}
func (noopMutationLease) Check(context.Context) error { return nil }
func (noopMutationLease) Release() error              { return nil }

func testTicket() Ticket {
	return Ticket{
		Channel: domain.ChannelDev, Project: "nysa", ID: "SF-123",
		Type: domain.TicketBug, SourceDigest: testDigest,
		Body: "Prevent duplicate reminders.",
	}
}

func testWorkspace() Workspace {
	identity, err := MarshalCanonicalWorktreeIdentity(CanonicalWorktreeIdentity{
		Repository: "/Users/sofia/nysa", RepositoryDev: 1, RepositoryIno: 2,
		Worktree: "/Users/sofia/.sf/worktree", WorktreeDev: 1, WorktreeIno: 3,
		GitFile: "gitdir: /Users/sofia/nysa/.git/worktrees/SF-123\n", GitFileDev: 1, GitFileIno: 4,
		CommonDir: "/Users/sofia/nysa/.git", CommonDirDev: 1, CommonDirIno: 5,
		Origin: "git@github.com:nysa-company/nysa.git", PushOrigin: "git@github.com:nysa-company/nysa.git",
		BaseRef: "main", BaseHead: testOID, HeadRef: "sf/dev/nysa/SF-123", ConfigHash: "sha256:" + testDigest, HooksHash: "sha256:" + testDigest,
	})
	if err != nil {
		panic(err)
	}
	return Workspace{
		Repository: "/Users/sofia/nysa", Worktree: "/Users/sofia/.sf/worktree",
		WorktreeIdentity: string(identity),
		BaseSHA:          testOID, AllowedPaths: []string{"internal/reminder", "internal/reminder/regression_test.go"},
	}
}

func testPlan() PlanIdentity {
	plan := phaseartifact.Planner{Schema: "sf.planner/v1", Acceptance: []string{"duplicate is prevented"},
		Proof: phaseartifact.ProofPlan{Kind: phaseartifact.ProofRegression, Command: []string{"go", "test", "./..."}, Details: "red regression"},
		Paths: []string{"internal/reminder"}, Commands: [][]string{{"go", "test", "./..."}}, Risks: []string{"idempotency"}, Questions: []phaseartifact.Question{}}
	identity, err := NewPlanIdentity(plan)
	if err != nil {
		panic(err)
	}
	return identity
}

func testRuntime() Runtime {
	return Runtime{Timeout: 2 * time.Minute, Profile: contracts.ProfileGuarded}
}

func testVerification() VerificationIdentity {
	plan := testPlan()
	artifact := phaseartifact.Verification{Schema: "sf.verification/v1", AcceptanceDigest: plan.Digest, ProofKind: phaseartifact.ProofRegression,
		OwnedFiles: []string{"internal/reminder/regression_test.go"}, Command: []string{"go", "test", "./internal/reminder"}, PrebuildOutcome: "red", EvidenceDigest: testDigest}
	intentDigest, _, err := canonicalVerificationIntent(artifact)
	if err != nil {
		panic(err)
	}
	proofDigest, _, err := canonicalVerificationProof(artifact)
	if err != nil {
		panic(err)
	}
	identity, err := NewVerificationIdentity(artifact, intentDigest, proofDigest, "checkpoint-1")
	if err != nil {
		panic(err)
	}
	return identity
}

func testCandidate() CandidateIdentity {
	verification := testVerification()
	evidence := phaseartifact.Builder{Schema: "sf.builder/v1", Summary: "implementation candidate", ChangedFiles: []string{"internal/reminder/reminder.go"}, Commands: [][]string{{"go", "test", "./..."}}}
	identity, err := NewCandidateIdentity(testOID, testOID, testOID, testDigest, verification.IntentDigest, verification.ProofDigest, testDigest, evidence, "")
	if err != nil {
		panic(err)
	}
	return identity
}

func testChecks() ChecksIdentity {
	identity, err := NewChecksIdentity("observation-1", testOID, []Check{{Name: "unit", ExternalID: "workflow-1", Status: "success"}})
	if err != nil {
		panic(err)
	}
	return identity
}

func TestSchemasAreStrictBoundedJSONAndMatchArtifacts(t *testing.T) {
	tests := []struct {
		role Role
		id   string
		keys []string
	}{
		{RolePlanner, "sf.planner/v1", []string{"schema", "acceptance", "proof", "paths", "commands", "risks", "questions"}},
		{RoleVerification, "sf.verification/v1", []string{"schema", "acceptance_digest", "proof_kind", "owned_files", "command", "prebuild_outcome", "evidence_digest", "rollback_command", "characterization_ref"}},
		{RoleBuilder, "sf.builder/v1", []string{"schema", "summary", "changed_files", "commands", "amendment_request"}},
		{RoleFinalReviewer, "sf.reviewer/v1", []string{"schema", "decision", "repair_owner", "findings", "reviewed_head", "proof_digest"}},
	}
	for _, tc := range tests {
		t.Run(string(tc.role), func(t *testing.T) {
			data, err := Schema(tc.role)
			if err != nil || !json.Valid(data) {
				t.Fatalf("schema err=%v valid=%v", err, json.Valid(data))
			}
			var root map[string]any
			if err := json.Unmarshal(data, &root); err != nil {
				t.Fatal(err)
			}
			if root["$id"] != tc.id || root["type"] != "object" || root["additionalProperties"] != false {
				t.Fatalf("root=%v", root)
			}
			properties, ok := root["properties"].(map[string]any)
			if !ok {
				t.Fatal("schema properties missing")
			}
			for _, key := range tc.keys {
				if _, ok := properties[key]; !ok {
					t.Errorf("artifact field %q missing from schema", key)
				}
			}
			if err := strictObjects(root); err != nil {
				t.Fatal(err)
			}
			if bytes.Equal(data, nil) {
				t.Fatal("empty schema")
			}
		})
	}
	if err := ValidateSchemas(); err != nil {
		t.Fatal(err)
	}
}

func strictObjects(value any) error {
	switch value := value.(type) {
	case map[string]any:
		if value["type"] == "object" && value["additionalProperties"] != false {
			return errors.New("strict object missing additionalProperties=false")
		}
		for _, child := range value {
			if err := strictObjects(child); err != nil {
				return err
			}
		}
	case []any:
		for _, child := range value {
			if err := strictObjects(child); err != nil {
				return err
			}
		}
	}
	return nil
}

func TestPromptsAreDeterministicAndRoleBound(t *testing.T) {
	ticket, workspace, runtime := testTicket(), testWorkspace(), testRuntime()
	plan := testPlan()
	verification, candidate, checks := testVerification(), testCandidate(), testChecks()
	inputs := []struct {
		name string
		make func() (contracts.PhaseInput, error)
		want []string
	}{
		{"planner", func() (contracts.PhaseInput, error) { return Planner(PlannerInput{ticket, workspace, runtime}) }, []string{"read-only", "Planner", "workflow states"}},
		{"verification", func() (contracts.PhaseInput, error) {
			return Verification(VerificationInput{ticket, workspace, plan, runtime})
		}, []string{"writes the tests or proof", "red", "missing", "baseline", "canonical_artifact"}},
		{"builder", func() (contracts.PhaseInput, error) {
			return Builder(BuilderInput{ticket, workspace, plan, verification, runtime})
		}, []string{"Preserve every verification-owned file", "amendment_request", "canonical_artifact"}},
		{"final-reviewer", func() (contracts.PhaseInput, error) {
			return FinalReviewer(FinalReviewerInput{ticket, workspace, plan, verification, candidate, checks, runtime})
		}, []string{"read-only review", "exact candidate head", "exact proof digest", "required-check set", "canonical_artifact"}},
	}
	for _, tc := range inputs {
		t.Run(tc.name, func(t *testing.T) {
			one, err := tc.make()
			if err != nil {
				t.Fatal(err)
			}
			two, err := tc.make()
			if err != nil {
				t.Fatal(err)
			}
			if one.Prompt != two.Prompt || !bytes.Equal(one.Schema, two.Schema) {
				t.Fatal("rendering is not deterministic")
			}
			for _, phrase := range tc.want {
				if !strings.Contains(one.Prompt, phrase) {
					t.Errorf("prompt missing %q", phrase)
				}
			}
			if strings.Contains(one.Prompt, "state:") || strings.Contains(one.Prompt, "merge_mode:") || strings.Contains(one.Prompt, "effect:") {
				t.Error("prompt exposes model-selectable lifecycle fields")
			}
		})
	}
}

func TestValidationRejectsOversizedUntrustedAndMismatchedIdentities(t *testing.T) {
	ticket, workspace, runtime := testTicket(), testWorkspace(), testRuntime()
	ticket.Body += "\n"
	if _, err := Planner(PlannerInput{Ticket: ticket, Workspace: workspace, Runtime: runtime}); err != nil {
		t.Fatalf("normal Markdown trailing newline rejected: %v", err)
	}
	ticket = testTicket()
	if _, err := Planner(PlannerInput{Ticket: ticket, Workspace: Workspace{Repository: "/tmp/repo", Worktree: "/tmp/wt", WorktreeIdentity: "id", BaseSHA: testOID, AllowedPaths: []string{"../escape"}}, Runtime: runtime}); err == nil {
		t.Fatal("path traversal accepted")
	}
	ticket.Body = strings.Repeat("x", MaxTicketBodyBytes+1)
	if _, err := Planner(PlannerInput{Ticket: ticket, Workspace: workspace, Runtime: runtime}); err == nil {
		t.Fatal("oversized ticket accepted")
	}
	ticket = testTicket()
	if _, err := Verification(VerificationInput{Ticket: ticket, Workspace: workspace, Plan: PlanIdentity{Digest: "untrusted model text"}, Runtime: runtime}); err == nil {
		t.Fatal("untrusted plan identity accepted")
	}
	candidate := testCandidate()
	candidate.HeadSHA = "fedcba9876543210fedcba9876543210fedcba98"
	if _, err := FinalReviewer(FinalReviewerInput{Ticket: ticket, Workspace: workspace, Plan: testPlan(), Verification: testVerification(), Candidate: candidate, Checks: testChecks(), Runtime: runtime}); err == nil {
		t.Fatal("checks/candidate mismatch accepted")
	}
	runtime.Profile = contracts.ProfileAutonomous
	if _, err := Planner(PlannerInput{Ticket: ticket, Workspace: workspace, Runtime: runtime}); err == nil {
		t.Fatal("autonomous profile accepted in local v1")
	}
}

func TestCanonicalIdentityRequiresShapeAndNonzeroFilesystemIdentity(t *testing.T) {
	workspace := testWorkspace()
	if _, err := ValidateCanonicalWorktreeIdentity([]byte(workspace.WorktreeIdentity)); err != nil {
		t.Fatal(err)
	}
	httpsIdentity := CanonicalWorktreeIdentity{}
	if err := json.Unmarshal([]byte(workspace.WorktreeIdentity), &httpsIdentity); err != nil {
		t.Fatal(err)
	}
	httpsIdentity.Origin = "https://github.com/nysa-company/nysa.git"
	httpsIdentity.PushOrigin = httpsIdentity.Origin
	httpsIdentity.PushOriginDev, httpsIdentity.PushOriginIno = 0, 0
	httpsData, err := json.Marshal(httpsIdentity)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ValidateCanonicalWorktreeIdentity(httpsData); err != nil {
		t.Fatalf("HTTPS origin identity rejected: %v", err)
	}
	for _, bad := range []string{
		`{"repository":"/tmp/repo","repository_dev":1,"repository_ino":2,"worktree":"/tmp/wt","worktree_dev":1,"worktree_ino":3,"git_dir":"/tmp/repo/.git","git_dir_dev":1,"git_dir_ino":4,"common_dir":"/tmp/repo/.git","common_dir_dev":1,"common_dir_ino":0}`,
		`{"repository":"/tmp/../repo","repository_dev":1,"repository_ino":2,"worktree":"/tmp/wt","worktree_dev":1,"worktree_ino":3,"git_dir":"/tmp/repo/.git","git_dir_dev":1,"git_dir_ino":4,"common_dir":"/tmp/repo/.git","common_dir_dev":1,"common_dir_ino":5}`,
	} {
		if _, err := ValidateCanonicalWorktreeIdentity([]byte(bad)); err == nil {
			t.Errorf("accepted invalid identity %s", bad)
		}
	}
}

func TestStructurallyValidOutputStillRequiresPhaseartifactValidation(t *testing.T) {
	bad := phaseartifact.Planner{Schema: "sf.planner/v1", Acceptance: []string{"acceptance"},
		Proof: phaseartifact.ProofPlan{Kind: phaseartifact.ProofAcceptance, Command: []string{"go", "test"}, Details: "looks structurally valid"},
		Paths: []string{"internal/reminder"}, Commands: [][]string{{"go", "test"}}, Risks: []string{"risk"}, Questions: []phaseartifact.Question{}}
	data, err := json.Marshal(bad)
	if err != nil {
		t.Fatal(err)
	}
	if !json.Valid(data) {
		t.Fatal("fixture is not structural JSON")
	}
	if _, err := phaseartifact.Parse(domain.PhasePlanning, contracts.PhaseResult{Artifact: data, Provider: validationProvider}, phaseartifact.Validation{TicketType: domain.TicketBug}); err == nil {
		t.Fatal("structurally valid but ticket-incompatible planner output bypassed phaseartifact.Parse")
	}
}

func TestCanonicalTypedArtifactsRoundTripAndRejectDigestAssertions(t *testing.T) {
	plan := testPlan()
	planBytes, err := json.Marshal(plan.Plan)
	if err != nil {
		t.Fatal(err)
	}
	planDigest, _, err := canonicalDigest(plan.Plan)
	if err != nil || plan.Digest != planDigest {
		t.Fatalf("plan canonical digest=%q stored=%q err=%v", planDigest, plan.Digest, err)
	}
	if !bytes.Equal(planBytes, canonicalBytes(plan.Plan)) {
		t.Fatal("plan canonical bytes are not deterministic")
	}

	verification := testVerification()
	intentBytes, err := CanonicalVerificationIntentBytes(verification.Artifact)
	if err != nil {
		t.Fatal(err)
	}
	intentDigest, err := VerificationIntentDigest(verification.Artifact)
	if err != nil || verification.IntentDigest != intentDigest {
		t.Fatalf("verification intent digest=%q stored=%q err=%v", intentDigest, verification.IntentDigest, err)
	}
	proofBytes, err := CanonicalVerificationProofBytes(verification.Artifact)
	if err != nil {
		t.Fatal(err)
	}
	proofDigest, err := VerificationProofDigest(verification.Artifact)
	if err != nil || verification.ProofDigest != proofDigest {
		t.Fatalf("verification proof digest=%q stored=%q err=%v", proofDigest, verification.ProofDigest, err)
	}
	if len(intentBytes) == 0 || len(proofBytes) == 0 || !bytes.Equal(intentBytes, mustCanonicalVerificationIntent(t, verification.Artifact)) {
		t.Fatal("verification canonical helper bytes are not deterministic")
	}
	artifactDigest, _, err := canonicalDigest(verification.Artifact)
	if err != nil || verification.ArtifactDigest != artifactDigest {
		t.Fatal("verification artifact identity is not the canonical artifact digest")
	}
	badVerification := verification
	badVerification.ProofDigest = testDigest
	if _, err := Builder(BuilderInput{Ticket: testTicket(), Workspace: testWorkspace(), Plan: plan, Verification: badVerification, Runtime: testRuntime()}); err == nil {
		t.Fatal("caller-supplied proof digest assertion bypassed canonical verification digest")
	}

	candidate := testCandidate()
	badCandidate := candidate
	badCandidate.Evidence.Summary = "tampered"
	if _, err := FinalReviewer(FinalReviewerInput{Ticket: testTicket(), Workspace: testWorkspace(), Plan: plan, Verification: verification, Candidate: badCandidate, Checks: testChecks(), Runtime: testRuntime()}); err == nil {
		t.Fatal("tampered canonical candidate evidence accepted")
	}
}

func mustCanonicalVerificationIntent(t *testing.T, artifact phaseartifact.Verification) []byte {
	t.Helper()
	data, err := CanonicalVerificationIntentBytes(artifact)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func TestChecksIdentityRequiresExactSuccessfulObservedSet(t *testing.T) {
	checks := testChecks()
	if checks.ObservationID == "" || checks.SetDigest == "" {
		t.Fatal("server-required observation identity is incomplete")
	}
	bad := checks
	bad.Required = append([]Check(nil), checks.Required...)
	bad.Required[0].ExternalID = "caller-invented"
	if _, err := FinalReviewer(FinalReviewerInput{Ticket: testTicket(), Workspace: testWorkspace(), Plan: testPlan(), Verification: testVerification(), Candidate: testCandidate(), Checks: bad, Runtime: testRuntime()}); err == nil {
		t.Fatal("tampered required-check observation accepted")
	}
	bad = checks
	bad.Required = []Check{{Name: "unit", ExternalID: "workflow-1", Status: "pending"}}
	if _, err := FinalReviewer(FinalReviewerInput{Ticket: testTicket(), Workspace: testWorkspace(), Plan: testPlan(), Verification: testVerification(), Candidate: testCandidate(), Checks: bad, Runtime: testRuntime()}); err == nil {
		t.Fatal("non-success required check accepted")
	}
}

func TestPromptBoundIncludesFinalNewline(t *testing.T) {
	if rendered, err := render(strings.Repeat("x", MaxPromptBytes-1)); err != nil || len(rendered) != MaxPromptBytes || rendered[len(rendered)-1] != '\n' {
		t.Fatalf("max-sized prompt err=%v len=%d", err, len(rendered))
	}
	if _, err := render(strings.Repeat("x", MaxPromptBytes)); err == nil {
		t.Fatal("prompt exceeding newline-inclusive bound accepted")
	} else {
		var bound *BoundError
		if !errors.As(err, &bound) || bound.Actual != MaxPromptBytes+1 {
			t.Fatalf("prompt bound error=%T %v", err, err)
		}
	}
}

func TestCanonicalIdentityRoundTripsExactGitRunnerSnapshot(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	repo := filepath.Join(root, "repo")
	remote := filepath.Join(root, "remote.git")
	worktree := filepath.Join(root, "worktree")
	runGit := func(dir string, args ...string) {
		cmd := exec.Command("git", append([]string{"-C", dir}, args...)...)
		if output, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v (%s)", args, err, output)
		}
	}
	if err := os.MkdirAll(repo, 0o700); err != nil {
		t.Fatal(err)
	}
	runGit(repo, "init", "-b", "main")
	runGit(repo, "config", "user.name", "fixture")
	runGit(repo, "config", "user.email", "fixture@example.test")
	if err := os.WriteFile(filepath.Join(repo, "README.md"), []byte("base\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	runGit(repo, "add", ".")
	runGit(repo, "commit", "-m", "base")
	if output, err := exec.Command("git", "init", "--bare", remote).CombinedOutput(); err != nil {
		t.Fatalf("bare: %v (%s)", err, output)
	}
	runGit(repo, "remote", "add", "origin", remote)
	runGit(repo, "push", "origin", "main:refs/heads/main")
	runner := git.Runner{Home: filepath.Join(root, "home"), TestLocalTransport: true, Run: func(ctx context.Context, binary string, argv, env []string) ([]byte, error) {
		cmd := exec.CommandContext(ctx, binary, argv...)
		cmd.Env = env
		return cmd.CombinedOutput()
	}, MutationAuthority: noopMutationAuthority{}}
	base, err := runner.Snapshot(ctx, repo, "main")
	if err == nil {
		t.Fatal("primary checkout unexpectedly accepted as linked worktree")
	}
	baseOID := ""
	if output, err := exec.Command("git", "-C", repo, "rev-parse", "main^{commit}").Output(); err != nil {
		t.Fatal(err)
	} else {
		baseOID = strings.TrimSpace(string(output))
	}
	repo, err = filepath.EvalSymlinks(repo)
	if err != nil {
		t.Fatal(err)
	}
	parent, err := filepath.EvalSymlinks(filepath.Dir(worktree))
	if err != nil {
		t.Fatal(err)
	}
	worktree = filepath.Join(parent, filepath.Base(worktree))
	branch := "sf/dev/0123456789abcdef/0123456789abcdef-0123456789abcdef0123456789abcdef"
	claim := contracts.GitMutationClaim{TicketRef: domain.TicketRef{Channel: domain.ChannelDev, Project: "nysa", Ticket: "SF-123"}, SemanticKey: "worktree/SF-123", RequestDigest: "sha256:" + testDigest, TicketVersion: 1, LeaderEpoch: 1, RunnerEpoch: 1, ClaimEpoch: 1, Repository: repo, Worktree: worktree, Branch: branch, Operation: "create-worktree", BaseRef: "main", ExpectedBaseOID: baseOID}
	claim.ExpectedHeadOID = baseOID
	created, err := runner.CreateWorktree(ctx, repo, worktree, branch, "main", claim)
	if err != nil {
		t.Fatal(err)
	}
	identity := created.Identity
	if base == identity {
		t.Fatal("unexpected primary identity match")
	}
	identity, err = runner.Snapshot(ctx, worktree, "main")
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(identity)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := ValidateCanonicalWorktreeIdentity(data)
	if err != nil {
		t.Fatal(err)
	}
	if decoded != identity {
		t.Fatal("identity round trip changed exact git.Identity")
	}
	tampered := append([]byte(nil), data...)
	tampered = bytes.Replace(tampered, []byte(`"WorktreeIno":`), []byte(`"WorktreeIno":0,"ignored":`), 1)
	if _, err := ValidateCanonicalWorktreeIdentity(tampered); err == nil {
		t.Fatal("tampered identity accepted")
	}
	zero := identity
	zero.WorktreeIno = 0
	zeroData, err := json.Marshal(zero)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ValidateCanonicalWorktreeIdentity(zeroData); err == nil {
		t.Fatal("zero-inode identity accepted")
	}
	if _, err := ValidateCanonicalWorktreeIdentity(append([]byte(" "), data...)); err == nil {
		t.Fatal("noncanonical identity whitespace accepted")
	}
}
