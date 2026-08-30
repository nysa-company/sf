package workflowprompt

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/phaseartifact"
)

const (
	testOID    = "0123456789abcdef0123456789abcdef01234567"
	testDigest = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
)

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
		GitDir: "/Users/sofia/nysa/.git/worktrees/SF-123", GitDirDev: 1, GitDirIno: 4,
		CommonDir: "/Users/sofia/nysa/.git", CommonDirDev: 1, CommonDirIno: 5,
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

func artifactDigest(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func testPlan() PlanIdentity {
	plan := phaseartifact.Planner{Schema: "sf.planner/v1", Acceptance: []string{"duplicate is prevented"},
		Proof: phaseartifact.ProofPlan{Kind: phaseartifact.ProofRegression, Command: []string{"go", "test", "./..."}, Details: "red regression"},
		Paths: []string{"internal/reminder"}, Commands: [][]string{{"go", "test", "./..."}}, Risks: []string{"idempotency"}, Questions: []phaseartifact.Question{}}
	data, err := json.Marshal(plan)
	if err != nil {
		panic(err)
	}
	return PlanIdentity{Digest: artifactDigest(data), Bytes: data, Plan: plan}
}

func testRuntime() Runtime {
	return Runtime{Timeout: 2 * time.Minute, Profile: contracts.ProfileGuarded}
}

func testVerification() VerificationIdentity {
	plan := testPlan()
	artifact := phaseartifact.Verification{Schema: "sf.verification/v1", AcceptanceDigest: plan.Digest, ProofKind: phaseartifact.ProofRegression,
		OwnedFiles: []string{"internal/reminder/regression_test.go"}, Command: []string{"go", "test", "./internal/reminder"}, PrebuildOutcome: "red", EvidenceDigest: testDigest}
	data, err := json.Marshal(artifact)
	if err != nil {
		panic(err)
	}
	return VerificationIdentity{IntentDigest: testDigest, ProofDigest: testDigest,
		OwnedFiles: artifact.OwnedFiles, CheckpointID: "checkpoint-1", Bytes: data, Artifact: artifact}
}

func testCandidate() CandidateIdentity {
	details := CandidateEvidence{Summary: "implementation candidate", ChangedFiles: []string{"internal/reminder/reminder.go"}, Commands: [][]string{{"go", "test", "./..."}}}
	evidence, err := json.Marshal(details)
	if err != nil {
		panic(err)
	}
	return CandidateIdentity{BaseSHA: testOID, HeadSHA: testOID, TreeSHA: testOID,
		SourceDigest: testDigest, VerificationIntentDigest: testDigest,
		ProofDigest: testDigest, CommandPolicyDigest: testDigest, Evidence: evidence, Details: details}
}

func testChecks() ChecksIdentity {
	return ChecksIdentity{HeadSHA: testOID, SetDigest: testDigest,
		Required: []Check{{Name: "unit", ExternalID: "workflow-1", Status: "success"}}}
}

func testCheckPolicy() CheckPolicy {
	return CheckPolicy{Digest: testDigest, Required: []CheckIdentity{{Name: "unit", ExternalID: "workflow-1"}}}
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
		}, []string{"writes the tests or proof", "red", "missing", "baseline", "typed_plan", "artifact"}},
		{"builder", func() (contracts.PhaseInput, error) {
			return Builder(BuilderInput{ticket, workspace, plan, verification, runtime})
		}, []string{"Preserve every verification-owned file", "amendment_request", "typed_plan", "typed_artifact"}},
		{"final-reviewer", func() (contracts.PhaseInput, error) {
			return FinalReviewer(FinalReviewerInput{ticket, workspace, plan, verification, candidate, checks, testCheckPolicy(), runtime})
		}, []string{"read-only review", "exact candidate head", "exact proof digest", "required-check set", "typed_plan", "typed_artifact", "CHECK_POLICY"}},
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
	if _, err := FinalReviewer(FinalReviewerInput{Ticket: ticket, Workspace: workspace, Plan: testPlan(), Verification: testVerification(), Candidate: candidate, Checks: testChecks(), CheckPolicy: testCheckPolicy(), Runtime: runtime}); err == nil {
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
