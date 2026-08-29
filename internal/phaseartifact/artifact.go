// Package phaseartifact parses the bounded structured output produced by role
// invocations. The schemas contain evidence only: no provider-controlled field
// can name a workflow state, transition, merge mode, or effect.
package phaseartifact

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
)

const MaxBytes = 1 << 20

type ProofKind string

const (
	ProofRegression       ProofKind = "regression"
	ProofAcceptance       ProofKind = "acceptance"
	ProofCharacterization ProofKind = "characterization"
	ProofValidation       ProofKind = "validation"
	ProofDocumentation    ProofKind = "documentation"
	ProofReport           ProofKind = "report"
)

type ProofPlan struct {
	Kind    ProofKind `json:"kind"`
	Command []string  `json:"command"`
	Details string    `json:"details"`
}

type Question struct {
	Prompt  string   `json:"prompt"`
	Options []string `json:"options"`
}

type Planner struct {
	Schema     string     `json:"schema"`
	Acceptance []string   `json:"acceptance"`
	Proof      ProofPlan  `json:"proof"`
	Paths      []string   `json:"paths"`
	Commands   [][]string `json:"commands"`
	Risks      []string   `json:"risks"`
	Questions  []Question `json:"questions"`
}

type Verification struct {
	Schema              string    `json:"schema"`
	AcceptanceDigest    string    `json:"acceptance_digest"`
	ProofKind           ProofKind `json:"proof_kind"`
	OwnedFiles          []string  `json:"owned_files"`
	Command             []string  `json:"command"`
	PrebuildOutcome     string    `json:"prebuild_outcome"`
	EvidenceDigest      string    `json:"evidence_digest"`
	RollbackCommand     []string  `json:"rollback_command,omitempty"`
	CharacterizationRef string    `json:"characterization_ref,omitempty"`
}

type AmendmentRequest struct {
	OldProofDigest string `json:"old_proof_digest"`
	ProposedDigest string `json:"proposed_digest"`
	Reason         string `json:"reason"`
}

type Builder struct {
	Schema           string            `json:"schema"`
	Summary          string            `json:"summary"`
	ChangedFiles     []string          `json:"changed_files"`
	Commands         [][]string        `json:"commands"`
	AmendmentRequest *AmendmentRequest `json:"amendment_request,omitempty"`
}

type ReviewDecision string

const (
	ReviewPass          ReviewDecision = "pass"
	ReviewRepair        ReviewDecision = "repair"
	ReviewNeedsOperator ReviewDecision = "needs_operator"
)

type Reviewer struct {
	Schema       string         `json:"schema"`
	Decision     ReviewDecision `json:"decision"`
	RepairOwner  string         `json:"repair_owner,omitempty"`
	Findings     []string       `json:"findings"`
	ReviewedHead string         `json:"reviewed_head"`
	ProofDigest  string         `json:"proof_digest"`
}

type Parsed struct {
	Phase    domain.Phase
	Digest   string
	Provider domain.ProviderIdentity
	Planner  *Planner
	Verify   *Verification
	Builder  *Builder
	Reviewer *Reviewer
}

type Validation struct {
	TicketType              domain.TicketType
	AcceptanceDigest        string
	ProtectedVerification   []string
	ApprovedAmendmentDigest string
	ExpectedReviewedHead    string
	ExpectedProofDigest     string
}

func Parse(phase domain.Phase, result contracts.PhaseResult, validation Validation) (Parsed, error) {
	if err := validateProvider(result.Provider); err != nil {
		return Parsed{}, err
	}
	if len(result.Artifact) == 0 {
		return Parsed{}, errors.New("phase artifact is required")
	}
	if len(result.Artifact) > MaxBytes {
		return Parsed{}, fmt.Errorf("phase artifact exceeds %d bytes", MaxBytes)
	}
	if !utf8.Valid(result.Artifact) {
		return Parsed{}, errors.New("phase artifact is not valid UTF-8")
	}
	parsed := Parsed{Phase: phase, Digest: digest(result.Artifact), Provider: result.Provider}
	var err error
	switch phase {
	case domain.PhasePlanning:
		var value Planner
		err = decodeStrict(result.Artifact, &value)
		if err == nil {
			err = validatePlanner(&value, validation.TicketType)
		}
		parsed.Planner = &value
	case domain.PhaseVerification:
		var value Verification
		err = decodeStrict(result.Artifact, &value)
		if err == nil {
			err = validateVerification(&value, validation)
		}
		parsed.Verify = &value
	case domain.PhaseBuild:
		var value Builder
		err = decodeStrict(result.Artifact, &value)
		if err == nil {
			err = validateBuilder(&value, validation)
		}
		parsed.Builder = &value
	case domain.PhaseReview:
		var value Reviewer
		err = decodeStrict(result.Artifact, &value)
		if err == nil {
			err = validateReviewer(&value, validation)
		}
		parsed.Reviewer = &value
	default:
		return Parsed{}, fmt.Errorf("phase %q has no provider artifact schema", phase)
	}
	if err != nil {
		return Parsed{}, fmt.Errorf("validate %s artifact: %w", phase, err)
	}
	return parsed, nil
}

func decodeStrict(data []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("multiple JSON values are not allowed")
		}
		return err
	}
	return nil
}

func validatePlanner(value *Planner, ticketType domain.TicketType) error {
	if value.Schema != "sf.planner/v1" {
		return fmt.Errorf("unsupported schema %q", value.Schema)
	}
	if !ticketType.Valid() {
		return errors.New("valid ticket type is required")
	}
	if err := stringsList("acceptance", value.Acceptance, 1, 50); err != nil {
		return err
	}
	if value.Proof.Kind != proofFor(ticketType) {
		return fmt.Errorf("proof kind %q does not satisfy ticket type %q", value.Proof.Kind, ticketType)
	}
	if err := argv("proof command", value.Proof.Command); err != nil {
		return err
	}
	if strings.TrimSpace(value.Proof.Details) == "" {
		return errors.New("proof details are required")
	}
	if _, err := paths(value.Paths); err != nil {
		return fmt.Errorf("planner paths: %w", err)
	}
	if len(value.Paths) == 0 {
		return errors.New("planner must name at least one affected path")
	}
	if len(value.Commands) == 0 || len(value.Commands) > 20 {
		return errors.New("planner must name 1 to 20 commands")
	}
	for _, command := range value.Commands {
		if err := argv("planner command", command); err != nil {
			return err
		}
	}
	if err := stringsList("risks", value.Risks, 1, 20); err != nil {
		return err
	}
	if len(value.Questions) > 5 {
		return errors.New("planner may ask at most 5 questions")
	}
	for _, question := range value.Questions {
		if strings.TrimSpace(question.Prompt) == "" || len(question.Options) < 2 || len(question.Options) > 4 {
			return errors.New("each question requires a prompt and 2 to 4 options")
		}
		if err := stringsList("question options", question.Options, 2, 4); err != nil {
			return err
		}
	}
	return nil
}

func validateVerification(value *Verification, validation Validation) error {
	if value.Schema != "sf.verification/v1" {
		return fmt.Errorf("unsupported schema %q", value.Schema)
	}
	if validation.AcceptanceDigest == "" || value.AcceptanceDigest != validation.AcceptanceDigest {
		return errors.New("verification acceptance digest does not match the accepted plan")
	}
	if !validation.TicketType.Valid() {
		return errors.New("valid ticket type is required")
	}
	if value.ProofKind != proofFor(validation.TicketType) {
		return fmt.Errorf("verification proof kind %q does not satisfy ticket type %q", value.ProofKind, validation.TicketType)
	}
	if _, err := paths(value.OwnedFiles); err != nil {
		return fmt.Errorf("verification owned files: %w", err)
	}
	if len(value.OwnedFiles) == 0 {
		return errors.New("verification must own at least one proof file")
	}
	if err := argv("verification command", value.Command); err != nil {
		return err
	}
	if value.EvidenceDigest == "" {
		return errors.New("verification evidence digest is required")
	}
	switch validation.TicketType {
	case domain.TicketBug:
		if value.PrebuildOutcome != "red" {
			return errors.New("bug proof must demonstrate a red regression before build")
		}
	case domain.TicketFeature:
		if value.PrebuildOutcome != "red" && value.PrebuildOutcome != "missing" {
			return errors.New("feature proof must demonstrate a red or missing behavior")
		}
	case domain.TicketRefactor:
		if value.PrebuildOutcome != "baseline" || value.CharacterizationRef == "" {
			return errors.New("refactor proof must record a characterization baseline")
		}
	case domain.TicketInfrastructure:
		if value.PrebuildOutcome != "dry_run" || len(value.RollbackCommand) == 0 {
			return errors.New("infrastructure proof requires dry-run evidence and a rollback command")
		}
		if err := argv("rollback command", value.RollbackCommand); err != nil {
			return err
		}
	case domain.TicketDocumentation:
		if value.PrebuildOutcome != "check_failed" {
			return errors.New("documentation proof must record a failing executable check")
		}
	case domain.TicketSpike:
		if value.PrebuildOutcome != "report_ready" {
			return errors.New("spike proof must produce a bounded report")
		}
	}
	return nil
}

func validateBuilder(value *Builder, validation Validation) error {
	if value.Schema != "sf.builder/v1" {
		return fmt.Errorf("unsupported schema %q", value.Schema)
	}
	if strings.TrimSpace(value.Summary) == "" {
		return errors.New("builder summary is required")
	}
	changed, err := paths(value.ChangedFiles)
	if err != nil || len(changed) == 0 {
		if err != nil {
			return fmt.Errorf("builder changed files: %w", err)
		}
		return errors.New("builder changed files are required")
	}
	for _, command := range value.Commands {
		if err := argv("builder command", command); err != nil {
			return err
		}
	}
	if len(value.Commands) == 0 {
		return errors.New("builder command evidence is required")
	}
	protected, err := paths(validation.ProtectedVerification)
	if err != nil {
		return fmt.Errorf("protected verification files: %w", err)
	}
	if intersects(changed, protected) {
		if value.AmendmentRequest == nil {
			return errors.New("builder changed protected verification without an amendment request")
		}
		if value.AmendmentRequest.OldProofDigest == "" || value.AmendmentRequest.ProposedDigest == "" || strings.TrimSpace(value.AmendmentRequest.Reason) == "" {
			return errors.New("verification amendment request is incomplete")
		}
		if validation.ApprovedAmendmentDigest == "" || value.AmendmentRequest.ProposedDigest != validation.ApprovedAmendmentDigest {
			return errors.New("verification amendment is not freshly approved")
		}
	}
	return nil
}

func validateReviewer(value *Reviewer, validation Validation) error {
	if value.Schema != "sf.reviewer/v1" {
		return fmt.Errorf("unsupported schema %q", value.Schema)
	}
	if value.ReviewedHead == "" || value.ReviewedHead != validation.ExpectedReviewedHead {
		return errors.New("reviewer did not review the exact candidate head")
	}
	if value.ProofDigest == "" || value.ProofDigest != validation.ExpectedProofDigest {
		return errors.New("reviewer proof digest is stale")
	}
	switch value.Decision {
	case ReviewPass:
		if value.RepairOwner != "" {
			return errors.New("passing review cannot name a repair owner")
		}
	case ReviewRepair:
		if value.RepairOwner != "builder" && value.RepairOwner != "reviewer" {
			return errors.New("repair review must name builder or reviewer")
		}
		if len(value.Findings) == 0 {
			return errors.New("repair review requires findings")
		}
	case ReviewNeedsOperator:
		if value.RepairOwner != "operator" || len(value.Findings) == 0 {
			return errors.New("needs-operator review requires operator ownership and findings")
		}
	default:
		return fmt.Errorf("invalid review decision %q", value.Decision)
	}
	return stringsList("review findings", value.Findings, 0, 50)
}

func proofFor(ticketType domain.TicketType) ProofKind {
	switch ticketType {
	case domain.TicketBug:
		return ProofRegression
	case domain.TicketFeature:
		return ProofAcceptance
	case domain.TicketRefactor:
		return ProofCharacterization
	case domain.TicketInfrastructure:
		return ProofValidation
	case domain.TicketDocumentation:
		return ProofDocumentation
	case domain.TicketSpike:
		return ProofReport
	default:
		return ""
	}
}

func paths(input []string) ([]string, error) {
	result := make([]string, 0, len(input))
	seen := make(map[string]struct{}, len(input))
	for _, value := range input {
		if value == "" || filepath.IsAbs(value) || strings.ContainsRune(value, '\x00') {
			return nil, fmt.Errorf("unsafe path %q", value)
		}
		clean := filepath.ToSlash(filepath.Clean(value))
		if clean == "." || clean == ".." || strings.HasPrefix(clean, "../") {
			return nil, fmt.Errorf("unsafe path %q", value)
		}
		if clean != value {
			return nil, fmt.Errorf("path %q is not normalized", value)
		}
		if _, exists := seen[clean]; exists {
			return nil, fmt.Errorf("duplicate path %q", clean)
		}
		seen[clean] = struct{}{}
		result = append(result, clean)
	}
	sort.Strings(result)
	return result, nil
}

func argv(name string, value []string) error {
	if len(value) == 0 || strings.TrimSpace(value[0]) == "" {
		return fmt.Errorf("%s requires an executable", name)
	}
	if len(value) > 64 {
		return fmt.Errorf("%s has too many arguments", name)
	}
	for _, argument := range value {
		if strings.ContainsRune(argument, '\x00') {
			return fmt.Errorf("%s contains NUL", name)
		}
	}
	return nil
}

func stringsList(name string, values []string, minimum, maximum int) error {
	if len(values) < minimum || len(values) > maximum {
		return fmt.Errorf("%s requires %d to %d values", name, minimum, maximum)
	}
	for _, value := range values {
		if strings.TrimSpace(value) == "" || len(value) > 4096 {
			return fmt.Errorf("%s contains an empty or oversized value", name)
		}
	}
	return nil
}

func validateProvider(identity domain.ProviderIdentity) error {
	if identity.Provider == "" || identity.Model == "" || identity.Family == "" || identity.Version == "" {
		return errors.New("complete provider, model, family, and version identity is required")
	}
	return nil
}

func intersects(left, right []string) bool {
	set := make(map[string]struct{}, len(left))
	for _, value := range left {
		set[value] = struct{}{}
	}
	for _, value := range right {
		if _, ok := set[value]; ok {
			return true
		}
	}
	return false
}

func digest(value []byte) string {
	sum := sha256.Sum256(value)
	return "sha256:" + hex.EncodeToString(sum[:])
}
