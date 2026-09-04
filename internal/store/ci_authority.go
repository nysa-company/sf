package store

// This file is the Store-side CI authority.  It accepts an already-authenticated
// GitHub observation, but does not poll GitHub and does not perform a repair.
// The only mutable operation here is the SQLite observation/transition
// boundary.  In particular, a red result may enter building only when the
// caller presents a separately persisted, exact correction-budget authority.

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
)

const (
	maxCIDiagnosticText             = 16 << 10
	maxCIDiagnosticJSON             = 64 << 10
	maxCIChecks                     = 512
	maxCandidateRepairPromptChecks  = 16
	maxCandidateRepairCIHistorySpan = 128
	maxCIAggregateDiag              = 256 << 10
	maxCIClockSkew                  = 24 * time.Hour
)

// CIObservationCheck is one member of the exact required check set. Values
// stored in SQLite are canonical (trimmed names/IDs and lower-case states).
type CIObservationCheck struct {
	CanonicalName           string
	ExternalID              string
	NormalizedState         string
	FailingDiagnosticText   string
	FailingDiagnosticDigest string
}

// CIObservation is an authenticated observation of the currently published
// candidate. Digests are output fields: callers may leave them empty, but a
// non-empty value must equal the Store's recomputation.
type CIObservation struct {
	ObservationID            int64
	Ref                      domain.TicketRef
	CandidateGeneration      uint64
	CandidateHeadSHA         string
	CandidateTreeSHA         string
	PublicationWitnessDigest string
	PolicyWitnessDigest      string
	PullRequest              contracts.PullRequestIdentity
	ObservedTicketVersion    uint64
	ObservedFence            domain.Fence
	ObservedAt               time.Time
	RequiredChecks           []CIObservationCheck
	RequiredSetDigest        string
	Classification           string
	DiagnosticText           string
	DiagnosticJSON           string
	DiagnosticDigest         string
	ObservationDigest        string
}

// CIRequiredCheckPolicy is the durable, candidate-bound required set against
// which every later CI observation is reduced. Its checks carry no state: the
// server-defined names/IDs are the policy witness, while observations carry
// the changing state.
type CIRequiredCheckPolicy struct {
	PolicyID                 int64
	Ref                      domain.TicketRef
	CandidateGeneration      uint64
	CandidateHeadSHA         string
	CandidateTreeSHA         string
	PublicationWitnessDigest string
	ProtectedBranchRef       string
	ProtectedBranchOID       string
	PolicySourceDigest       string
	AuthenticatedPrincipal   string
	PolicyWitnessDigest      string
	RequiredChecks           []CIObservationCheck
	RequiredSetDigest        string
	CreatedAt                time.Time
	authenticated            bool
}

// CorrectionBudgetAuthority identifies the exact correction request the Store
// may allocate atomically with its red CI transition.
type CorrectionBudgetAuthority struct {
	Ref           domain.TicketRef
	RequestID     string
	TicketVersion uint64
	Fence         domain.Fence
}

// CIObservationTransition consumes one exact observation into immutable
// transition evidence, its event, and the ticket CAS.
type CIObservationTransition struct {
	Ref               domain.TicketRef
	ObservationDigest string
	ExpectedVersion   uint64
	Fence             domain.Fence
	CorrectionBudget  *CorrectionBudgetAuthority
}

// CITransitionEvidence is the durable transition result returned by reads and
// useful to callers that need to prove a lost response was replayed.
type CITransitionEvidence struct {
	TransitionDigest string
	Observation      CIObservation
	Result           TransitionResult
	ResultingState   domain.State
	ResultingTrigger string
}

// CandidateRepairCompletion is the immutable Builder completion witness for
// the successor generation created by a red CI repair transition.
type CandidateRepairCompletion struct {
	Ref                         domain.TicketRef
	TargetGeneration            uint64
	BuilderResultAttemptID      int64
	BuilderResultAttempt        int
	BuilderBindingTicketVersion uint64
	BuilderBindingFence         domain.Fence
	FinalCandidateHeadSHA       string
	FinalCandidateTreeSHA       string
	CompletionDigest            string
	CompletedAt                 time.Time
}

// CandidateRepairBuildContext is the Store-authenticated correction input for
// a fresh Builder generation. The verification remains immutable at the
// predecessor generation, while PredecessorHeadSHA is the only admissible Git
// parent for the corrected candidate.
type CandidateRepairBuildContext struct {
	Ref                      domain.TicketRef
	TargetGeneration         uint64
	PredecessorGeneration    uint64
	PredecessorHeadSHA       string
	PredecessorTreeSHA       string
	PublicationWitnessDigest string
	EntryTicketVersion       uint64
	EntryFence               domain.Fence
	Verification             StoredVerification
	Diagnostic               CandidateRepairDiagnostic
}

// CandidateRepairDiagnostic is the bounded projection of the exact red CI
// observation supplied to a correction Builder. It deliberately carries no
// diagnostic body or JSON: the model receives only authenticated identities,
// states, and digests. TotalFailingChecks makes deterministic truncation
// visible when an observation contains more failures than fit safely in a
// provider prompt.
type CandidateRepairDiagnostic struct {
	ObservationDigest  string
	RequiredSetDigest  string
	DiagnosticDigest   string
	TotalFailingChecks int
	FailingChecks      []CandidateRepairDiagnosticCheck
}

// CandidateRepairDiagnosticCheck identifies one failing required check. An
// empty FailingDiagnosticDigest means that the authenticated observation did
// not carry a per-check diagnostic body.
type CandidateRepairDiagnosticCheck struct {
	CanonicalName           string
	ExternalID              string
	NormalizedState         string
	FailingDiagnosticDigest string
}

func ciDigest(value []byte) string {
	sum := sha256.Sum256(value)
	return hex.EncodeToString(sum[:])
}

func ciAuthorityDigest(value []byte) string {
	sum := sha256.Sum256(value)
	return "sha256:" + hex.EncodeToString(sum[:])
}

func validCIAuthorityDigest(value string) bool {
	return len(value) == 71 && strings.HasPrefix(value, "sha256:") && validDigest(value[len("sha256:"):])
}

func canonicalCIState(value string) (string, bool) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "pending", "queued", "in_progress", "in-progress", "requested", "waiting":
		return "pending", true
	case "success":
		return "success", true
	case "skipped":
		return "skipped", true
	case "neutral":
		return "neutral", true
	case "failure", "failed", "error", "timed_out", "timed-out", "action_required":
		return "failure", true
	case "cancelled", "canceled", "cancelled_failure":
		return "cancelled", true
	default:
		return "", false
	}
}

func canonicalCIText(value string, max int) (string, bool) {
	if strings.IndexByte(value, 0) >= 0 || len(value) > max {
		return "", false
	}
	return strings.TrimSpace(value), true
}

func canonicalCIObservationChecks(checks []CIObservationCheck) ([]CIObservationCheck, error) {
	if len(checks) == 0 || len(checks) > maxCIChecks {
		return nil, ErrCIObservation
	}
	out := make([]CIObservationCheck, len(checks))
	seen := make(map[string]struct{}, len(checks))
	seenNames := make(map[string]struct{}, len(checks))
	seenIDs := make(map[string]struct{}, len(checks))
	for i, check := range checks {
		name, ok := canonicalCIText(check.CanonicalName, 255)
		if !ok || name == "" {
			return nil, ErrCIObservation
		}
		external, ok := canonicalCIText(check.ExternalID, 255)
		if !ok || external == "" {
			return nil, ErrCIObservation
		}
		state, ok := canonicalCIState(check.NormalizedState)
		if !ok {
			return nil, ErrCIObservation
		}
		diagnostic, ok := canonicalCIText(check.FailingDiagnosticText, maxCIDiagnosticText)
		if !ok || (diagnostic != "" && state != "failure" && state != "cancelled") {
			return nil, ErrCIObservation
		}
		key := name + "\x00" + external
		if _, exists := seen[key]; exists {
			return nil, ErrCIObservation
		}
		if _, exists := seenNames[name]; exists {
			return nil, ErrCIObservation
		}
		if _, exists := seenIDs[external]; exists {
			return nil, ErrCIObservation
		}
		seen[key] = struct{}{}
		seenNames[name] = struct{}{}
		seenIDs[external] = struct{}{}
		item := CIObservationCheck{CanonicalName: name, ExternalID: external, NormalizedState: state, FailingDiagnosticText: diagnostic}
		if diagnostic != "" {
			item.FailingDiagnosticDigest = ciDigest([]byte(diagnostic))
		}
		if check.FailingDiagnosticDigest != "" && check.FailingDiagnosticDigest != item.FailingDiagnosticDigest && check.FailingDiagnosticDigest != "sha256:"+item.FailingDiagnosticDigest {
			return nil, ErrCIObservation
		}
		out[i] = item
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].CanonicalName == out[j].CanonicalName {
			return out[i].ExternalID < out[j].ExternalID
		}
		return out[i].CanonicalName < out[j].CanonicalName
	})
	return out, nil
}

func canonicalCIRequiredSetDigest(checks []CIObservationCheck) string {
	type identity struct{ Name, ExternalID string }
	items := make([]identity, len(checks))
	for i, check := range checks {
		items[i] = identity{check.CanonicalName, check.ExternalID}
	}
	body, _ := json.Marshal(items)
	return ciDigest(body)
}

// canonicalCIPolicySetDigest deliberately contains only protected-branch
// context names. A check run's URL/external id changes on rerun and belongs to
// the immutable observation, never the policy identity.
func canonicalCIPolicySetDigest(checks []CIObservationCheck) string {
	names := make([]string, len(checks))
	for i, check := range checks {
		names[i] = check.CanonicalName
	}
	body, _ := json.Marshal(names)
	return ciDigest(body)
}

func canonicalCIPolicyChecks(checks []CIObservationCheck) ([]CIObservationCheck, error) {
	if len(checks) == 0 || len(checks) > maxCIChecks {
		return nil, ErrCIObservation
	}
	out := make([]CIObservationCheck, len(checks))
	seenNames := make(map[string]struct{}, len(checks))
	for i, check := range checks {
		name, ok := canonicalCIText(check.CanonicalName, 255)
		if !ok || name == "" {
			return nil, ErrCIObservation
		}
		if _, exists := seenNames[name]; exists {
			return nil, ErrCIObservation
		}
		seenNames[name] = struct{}{}
		out[i] = CIObservationCheck{CanonicalName: name}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CanonicalName < out[j].CanonicalName })
	return out, nil
}

func canonicalCIPolicy(value CIRequiredCheckPolicy) (CIRequiredCheckPolicy, error) {
	if value.Ref.Validate() != nil || value.CandidateGeneration == 0 || !validOID(value.CandidateHeadSHA) || !validOID(value.CandidateTreeSHA) || !validCIAuthorityDigest(value.PublicationWitnessDigest) {
		return CIRequiredCheckPolicy{}, ErrCIObservation
	}
	claimedWitness := value.PolicyWitnessDigest
	value.ProtectedBranchRef = strings.TrimSpace(value.ProtectedBranchRef)
	value.AuthenticatedPrincipal = strings.TrimSpace(value.AuthenticatedPrincipal)
	if !boundedText(value.ProtectedBranchRef, 255) || value.ProtectedBranchRef == "" || !validOID(value.ProtectedBranchOID) || !validPolicySourceDigest(value.PolicySourceDigest) || !boundedText(value.AuthenticatedPrincipal, 300) || value.AuthenticatedPrincipal == "" {
		return CIRequiredCheckPolicy{}, ErrCIObservation
	}
	checks, err := canonicalCIPolicyChecks(value.RequiredChecks)
	if err != nil {
		return CIRequiredCheckPolicy{}, err
	}
	value.RequiredChecks = checks
	if len(canonicalCIPolicyJSON(checks)) > maxCIDiagnosticJSON {
		return CIRequiredCheckPolicy{}, ErrCIObservation
	}
	digest := canonicalCIPolicySetDigest(checks)
	if value.RequiredSetDigest != "" && value.RequiredSetDigest != digest {
		return CIRequiredCheckPolicy{}, ErrCIObservation
	}
	value.RequiredSetDigest = digest
	value.PolicyWitnessDigest = canonicalCIPolicyWitnessDigest(value)
	if value.PolicyWitnessDigest == "" {
		return CIRequiredCheckPolicy{}, ErrCIObservation
	}
	if claimedWitness != "" && claimedWitness != value.PolicyWitnessDigest {
		return CIRequiredCheckPolicy{}, ErrCIObservation
	}
	if value.CreatedAt.IsZero() {
		value.CreatedAt = time.Now().UTC()
	}
	if _, ok := canonicalPublicationTime(value.CreatedAt); !ok {
		return CIRequiredCheckPolicy{}, ErrCIObservation
	}
	return value, nil
}

func validPolicySourceDigest(value string) bool {
	return validDigest(value) || validCIAuthorityDigest(value)
}

func canonicalCIPolicyWitnessDigest(value CIRequiredCheckPolicy) string {
	body, err := json.Marshal(struct {
		Ref                     domain.TicketRef
		Generation              uint64
		Head, Tree, Publication string
		ProtectedBranchRef      string
		ProtectedBranchOID      string
		PolicySourceDigest      string
		AuthenticatedPrincipal  string
		RequiredSetDigest       string
		RequiredChecks          []CIObservationCheck
	}{value.Ref, value.CandidateGeneration, value.CandidateHeadSHA, value.CandidateTreeSHA, value.PublicationWitnessDigest, value.ProtectedBranchRef, value.ProtectedBranchOID, value.PolicySourceDigest, value.AuthenticatedPrincipal, value.RequiredSetDigest, value.RequiredChecks})
	if err != nil {
		return ""
	}
	return ciAuthorityDigest(body)
}

func canonicalCIDiagnosticDigest(text, diagnosticJSON string) string {
	body, _ := json.Marshal(struct {
		Text string `json:"text"`
		JSON string `json:"json"`
	}{text, diagnosticJSON})
	return ciDigest(body)
}

func canonicalCIObservationDigest(value CIObservation, checks []CIObservationCheck) string {
	// Do not include database identity or caller-provided digests. The explicit
	// field order supplied by encoding/json is part of this stable contract.
	body, _ := json.Marshal(struct {
		Ref                                                              domain.TicketRef
		Generation                                                       uint64
		Head, Tree, Witness                                              string
		PR                                                               contracts.PullRequestIdentity
		TicketVersion, LeaderEpoch, RunnerEpoch                          uint64
		ObservedAt, PolicyWitnessDigest                                  string
		RequiredSetDigest                                                string
		Checks                                                           []CIObservationCheck
		Classification, DiagnosticDigest, DiagnosticText, DiagnosticJSON string
	}{value.Ref, value.CandidateGeneration, value.CandidateHeadSHA, value.CandidateTreeSHA, value.PublicationWitnessDigest, value.PullRequest, value.ObservedTicketVersion, value.ObservedFence.LeaderEpoch, value.ObservedFence.RunnerEpoch, value.ObservedAt.UTC().Format(time.RFC3339Nano), value.PolicyWitnessDigest, value.RequiredSetDigest, checks, value.Classification, value.DiagnosticDigest, value.DiagnosticText, value.DiagnosticJSON})
	return ciAuthorityDigest(body)
}

func canonicalCIObservation(value CIObservation) (CIObservation, error) {
	if value.Ref.Validate() != nil || value.CandidateGeneration == 0 || !validOID(value.CandidateHeadSHA) || !validOID(value.CandidateTreeSHA) || !validCIAuthorityDigest(value.PublicationWitnessDigest) || (value.PolicyWitnessDigest != "" && !validCIAuthorityDigest(value.PolicyWitnessDigest)) || !validPublicationPR(value.PullRequest, "OPEN", true, value.ObservedAt) || value.PullRequest.BaseOID == "" || value.ObservedTicketVersion == 0 || value.ObservedFence.LeaderEpoch == 0 || value.ObservedFence.RunnerEpoch == 0 || value.ObservedFence.ClaimEpoch != 0 {
		return CIObservation{}, ErrCIObservation
	}
	observed, ok := canonicalPublicationTime(value.ObservedAt)
	if !ok {
		return CIObservation{}, ErrCIObservation
	}
	value.ObservedAt = observed
	claimedRequiredSetDigest := value.RequiredSetDigest
	claimedDiagnosticDigest := value.DiagnosticDigest
	claimedObservationDigest := value.ObservationDigest
	text, ok := canonicalCIText(value.DiagnosticText, maxCIDiagnosticText)
	if !ok {
		return CIObservation{}, ErrCIObservation
	}
	value.DiagnosticText = text
	if value.DiagnosticJSON != "" {
		if len(value.DiagnosticJSON) > maxCIDiagnosticJSON || !json.Valid([]byte(value.DiagnosticJSON)) || strings.IndexByte(value.DiagnosticJSON, 0) >= 0 {
			return CIObservation{}, ErrCIObservation
		}
		var diagnostic any
		if err := json.Unmarshal([]byte(value.DiagnosticJSON), &diagnostic); err != nil {
			return CIObservation{}, ErrCIObservation
		}
		canonicalJSON, err := json.Marshal(diagnostic)
		if err != nil {
			return CIObservation{}, ErrCIObservation
		}
		value.DiagnosticJSON = string(canonicalJSON)
	}
	checks, err := canonicalCIObservationChecks(value.RequiredChecks)
	if err != nil {
		return CIObservation{}, err
	}
	value.RequiredChecks = checks
	diagnosticBytes := len(value.DiagnosticText) + len(value.DiagnosticJSON)
	for _, check := range checks {
		diagnosticBytes += len(check.FailingDiagnosticText)
	}
	if diagnosticBytes > maxCIAggregateDiag {
		return CIObservation{}, ErrCIObservation
	}
	value.RequiredSetDigest = canonicalCIRequiredSetDigest(checks)
	value.DiagnosticDigest = canonicalCIDiagnosticDigest(value.DiagnosticText, value.DiagnosticJSON)
	if claimedRequiredSetDigest != "" && claimedRequiredSetDigest != value.RequiredSetDigest {
		return CIObservation{}, ErrCIObservation
	}
	if claimedDiagnosticDigest != "" && claimedDiagnosticDigest != value.DiagnosticDigest && claimedDiagnosticDigest != "sha256:"+value.DiagnosticDigest {
		return CIObservation{}, ErrCIObservation
	}
	if value.Classification != "pending" && value.Classification != "green" && value.Classification != "red" {
		return CIObservation{}, ErrCIObservation
	}
	var pending, red bool
	for _, check := range checks {
		pending = pending || check.NormalizedState == "pending"
		red = red || check.NormalizedState == "failure" || check.NormalizedState == "cancelled"
	}
	want := "green"
	if red {
		want = "red"
	} else if pending {
		want = "pending"
	}
	if value.Classification != want {
		return CIObservation{}, ErrCIObservation
	}
	value.ObservationDigest = canonicalCIObservationDigest(value, checks)
	if claimedObservationDigest != "" && claimedObservationDigest != value.ObservationDigest {
		return CIObservation{}, ErrCIObservation
	}
	return value, nil
}

func candidateRepairDiagnosticForObservation(value CIObservation) (CandidateRepairDiagnostic, error) {
	if value.Classification != "red" || len(value.RequiredChecks) == 0 || len(value.RequiredChecks) > maxCIChecks || !validCIAuthorityDigest(value.ObservationDigest) || !validDigest(value.RequiredSetDigest) || !validDigest(value.DiagnosticDigest) {
		return CandidateRepairDiagnostic{}, ErrEvidenceConflict
	}
	checks := make([]CandidateRepairDiagnosticCheck, 0, len(value.RequiredChecks))
	for _, check := range value.RequiredChecks {
		if check.NormalizedState != "failure" && check.NormalizedState != "cancelled" {
			continue
		}
		name, nameOK := canonicalCIText(check.CanonicalName, 255)
		externalID, externalOK := canonicalCIText(check.ExternalID, 255)
		if !nameOK || !externalOK || name == "" || externalID == "" || name != check.CanonicalName || externalID != check.ExternalID || check.FailingDiagnosticDigest != "" && !validDigest(check.FailingDiagnosticDigest) {
			return CandidateRepairDiagnostic{}, ErrEvidenceConflict
		}
		checks = append(checks, CandidateRepairDiagnosticCheck{
			CanonicalName:           name,
			ExternalID:              externalID,
			NormalizedState:         check.NormalizedState,
			FailingDiagnosticDigest: check.FailingDiagnosticDigest,
		})
	}
	if len(checks) == 0 {
		return CandidateRepairDiagnostic{}, ErrEvidenceConflict
	}
	sort.Slice(checks, func(i, j int) bool {
		return checks[i].CanonicalName+"\x00"+checks[i].ExternalID < checks[j].CanonicalName+"\x00"+checks[j].ExternalID
	})
	total := len(checks)
	if len(checks) > maxCandidateRepairPromptChecks {
		checks = checks[:maxCandidateRepairPromptChecks]
	}
	return CandidateRepairDiagnostic{
		ObservationDigest:  value.ObservationDigest,
		RequiredSetDigest:  value.RequiredSetDigest,
		DiagnosticDigest:   value.DiagnosticDigest,
		TotalFailingChecks: total,
		FailingChecks:      checks,
	}, nil
}

func ciObservationEqual(left, right CIObservation) bool {
	return left.Ref == right.Ref && left.CandidateGeneration == right.CandidateGeneration && left.CandidateHeadSHA == right.CandidateHeadSHA && left.CandidateTreeSHA == right.CandidateTreeSHA && left.PublicationWitnessDigest == right.PublicationWitnessDigest && left.PolicyWitnessDigest == right.PolicyWitnessDigest && left.PullRequest == right.PullRequest && left.ObservedTicketVersion == right.ObservedTicketVersion && left.ObservedFence == right.ObservedFence && left.ObservedAt.Equal(right.ObservedAt) && left.RequiredSetDigest == right.RequiredSetDigest && left.Classification == right.Classification && left.DiagnosticText == right.DiagnosticText && left.DiagnosticJSON == right.DiagnosticJSON && left.DiagnosticDigest == right.DiagnosticDigest && left.ObservationDigest == right.ObservationDigest && bytes.Equal(mustJSON(left.RequiredChecks), mustJSON(right.RequiredChecks))
}

func mustJSON(value any) []byte { encoded, _ := json.Marshal(value); return encoded }

func ciObservationMatchesPublication(value CIObservation, publication PublishedCandidateEvidence) bool {
	return value.Ref == publication.Ref && value.CandidateGeneration == publication.Candidate.Snapshot.Generation && value.CandidateHeadSHA == publication.Candidate.Snapshot.HeadSHA && value.CandidateTreeSHA == publication.Candidate.Snapshot.TreeSHA && value.PublicationWitnessDigest == publication.WitnessDigest && value.PullRequest == publication.PullRequest
}

type ciQuery interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

// ciWrite serializes the authority operation with supervised external
// mutations. The transaction still performs the durable runtime-control and
// fence check; taking the gate here prevents a process handoff from occurring
// between that check and the append-only write.
func (s *Store) ciWrite(ctx context.Context, ref domain.TicketRef, fn func(*sql.Conn) error) error {
	if s == nil || s.mutations == nil {
		return ErrStaleFence
	}
	if err := s.mutations.lock(ctx); err != nil {
		return err
	}
	defer s.mutations.unlock()
	return s.write(ctx, fn)
}

func scanCIObservation(ctx context.Context, q ciQuery, latest bool, ref domain.TicketRef, digest string) (CIObservation, bool, error) {
	query := `SELECT observation_id,channel,project_id,ticket_id,candidate_generation,candidate_head_sha,candidate_tree_sha,publication_witness_digest,policy_witness_digest,pr_host,pr_owner,pr_repo,pr_number,pr_head_owner,pr_head_repo,pr_head_ref,pr_head_oid,pr_base_ref,pr_base_oid,observed_ticket_version,observed_leader_epoch,observed_runner_epoch,observed_at,required_set_digest,required_check_count,classification,diagnostic_digest,diagnostic_text,diagnostic_json,observation_digest FROM ci_observations WHERE channel=? AND project_id=? AND ticket_id=?`
	args := []any{ref.Channel, ref.Project, ref.Ticket}
	if digest != "" {
		query += ` AND observation_digest=?`
		args = append(args, digest)
	} else if latest {
		query += ` ORDER BY observation_id DESC LIMIT 1`
	}
	var value CIObservation
	var channel domain.Channel
	var project, ticket, host, owner, repo, headOwner, headRepo, headRef, headOID, baseRef, baseOID, observed, policyWitness, diagnosticDigest, diagnosticText, diagnosticJSON, observationDigest string
	var prNumber, generation, version, leader, runner, count int64
	var err error
	err = q.QueryRowContext(ctx, query, args...).Scan(&value.ObservationID, &channel, &project, &ticket, &generation, &value.CandidateHeadSHA, &value.CandidateTreeSHA, &value.PublicationWitnessDigest, &policyWitness, &host, &owner, &repo, &prNumber, &headOwner, &headRepo, &headRef, &headOID, &baseRef, &baseOID, &version, &leader, &runner, &observed, &value.RequiredSetDigest, &count, &value.Classification, &diagnosticDigest, &diagnosticText, &diagnosticJSON, &observationDigest)
	if errors.Is(err, sql.ErrNoRows) {
		return CIObservation{}, false, nil
	}
	if err != nil {
		return CIObservation{}, false, normalizeBusy(ctx, err)
	}
	value.Ref = domain.TicketRef{Channel: channel, Project: domain.ProjectID(project), Ticket: domain.TicketID(ticket)}
	value.CandidateGeneration, value.ObservedTicketVersion = uint64(generation), uint64(version)
	value.ObservedFence = domain.Fence{LeaderEpoch: uint64(leader), RunnerEpoch: uint64(runner)}
	value.PullRequest = contracts.PullRequestIdentity{Repository: contracts.RepositoryIdentity{Host: host, Owner: owner, Name: repo}, Number: int(prNumber), HeadOwner: headOwner, HeadRepository: headRepo, HeadRef: headRef, HeadOID: headOID, BaseRef: baseRef, BaseOID: baseOID, FactoryOwned: true}
	value.PolicyWitnessDigest, value.DiagnosticDigest, value.DiagnosticText, value.DiagnosticJSON, value.ObservationDigest = policyWitness, diagnosticDigest, diagnosticText, diagnosticJSON, observationDigest
	if !validCIAuthorityDigest(value.PolicyWitnessDigest) {
		return CIObservation{}, false, ErrCIObservation
	}
	value.ObservedAt, err = parsePublicationTime(observed)
	if err != nil {
		return CIObservation{}, false, ErrCIObservation
	}
	rows, err := q.QueryContext(ctx, `SELECT canonical_name,external_id,normalized_state,failing_diagnostic_digest,failing_diagnostic_text FROM ci_observation_checks WHERE observation_id=? AND observation_digest=? ORDER BY canonical_name,external_id`, value.ObservationID, value.ObservationDigest)
	if err != nil {
		return CIObservation{}, false, normalizeBusy(ctx, err)
	}
	for rows.Next() {
		var check CIObservationCheck
		if err := rows.Scan(&check.CanonicalName, &check.ExternalID, &check.NormalizedState, &check.FailingDiagnosticDigest, &check.FailingDiagnosticText); err != nil {
			rows.Close()
			return CIObservation{}, false, err
		}
		value.RequiredChecks = append(value.RequiredChecks, check)
	}
	if err := rows.Close(); err != nil {
		return CIObservation{}, false, err
	}
	if int64(len(value.RequiredChecks)) != count {
		return CIObservation{}, false, ErrCIObservation
	}
	canonical, err := canonicalCIObservation(value)
	if err != nil || !ciObservationEqual(canonical, value) {
		return CIObservation{}, false, ErrCIObservation
	}
	return value, true, nil
}

func (s *Store) authenticateCurrentCIObservation(ctx context.Context, q ciQuery, ref domain.TicketRef, digest string, requireCurrent bool) (CIObservation, error) {
	value, found, err := scanCIObservation(ctx, q, digest == "", ref, digest)
	if err != nil {
		return CIObservation{}, err
	}
	if !found {
		return CIObservation{}, ErrNotFound
	}
	publication, err := loadCIPublicationBase(ctx, q, ref)
	if err != nil || !publicationFound(publication) || !ciObservationMatchesPublication(value, publication) {
		return CIObservation{}, ErrCIObservation
	}
	if requireCurrent {
		publication, err = loadCICurrentPublication(ctx, q, ref)
		if err != nil || !ciObservationMatchesPublication(value, publication) {
			return CIObservation{}, ErrCIObservation
		}
	}
	policy, err := scanCurrentCIPolicy(ctx, q, ref, publication)
	if err != nil || !policyMatchesObservation(policy, value) {
		return CIObservation{}, ErrCIObservation
	}
	var state string
	var version, runner, leader uint64
	if err := q.QueryRowContext(ctx, `SELECT t.state,t.version,t.runner_epoch,d.leader_epoch FROM tickets t JOIN daemon_instances d ON d.channel=t.channel WHERE t.channel=? AND t.project_id=? AND t.id=?`, ref.Channel, ref.Project, ref.Ticket).Scan(&state, &version, &runner, &leader); err != nil {
		return CIObservation{}, normalizeBusy(ctx, err)
	}
	if requireCurrent && (state != string(domain.StateWaitingCI) || version != value.ObservedTicketVersion || runner != value.ObservedFence.RunnerEpoch || leader != value.ObservedFence.LeaderEpoch) {
		return CIObservation{}, ErrStaleFence
	}
	return value, nil
}

// loadCICurrentPublication authenticates the publication witness and the
// complete waiting_ci self-transition chain without using the strict
// publishing/waiting reader. CI pending observations may advance the ticket
// version while preserving the same candidate, publication witness, and
// leader/runner fence; every such advance must be an exact contiguous,
// authenticated checks_pending evidence row.
func loadCICurrentPublication(ctx context.Context, q ciQuery, ref domain.TicketRef) (PublishedCandidateEvidence, error) {
	return loadCICurrentPublicationAt(ctx, q, ref, 0)
}

// loadCICurrentPublicationAt validates the complete CI chain at a particular
// ticket-local leader when nonzero. Startup fencing uses that form after the
// daemon leader has advanced but before it appends the next signed recovery
// row; ordinary readers pass zero and require the durable daemon leader.
func loadCICurrentPublicationAt(ctx context.Context, q ciQuery, ref domain.TicketRef, ticketLeader uint64) (PublishedCandidateEvidence, error) {
	publication, err := loadCIPublicationBase(ctx, q, ref)
	if err != nil {
		return PublishedCandidateEvidence{}, err
	}
	var state string
	var version, runner, leader uint64
	if err := q.QueryRowContext(ctx, `SELECT t.state,t.version,t.runner_epoch,d.leader_epoch FROM tickets t JOIN daemon_instances d ON d.channel=t.channel WHERE t.channel=? AND t.project_id=? AND t.id=?`, ref.Channel, ref.Project, ref.Ticket).Scan(&state, &version, &runner, &leader); err != nil {
		return PublishedCandidateEvidence{}, normalizeBusy(ctx, err)
	}
	if ticketLeader != 0 {
		leader = ticketLeader
	}
	if state != string(domain.StateWaitingCI) || runner < publication.CurrentFence.RunnerEpoch || (runner == publication.CurrentFence.RunnerEpoch && leader != publication.CurrentFence.LeaderEpoch) {
		return PublishedCandidateEvidence{}, ErrPublicationEvidence
	}
	// The publication->waiting_ci transition owns the first successor version.
	// Recovery starts from that durable waiting-ci version; using the
	// publication version here would incorrectly demand CI evidence for the
	// publication transition itself.
	waitingVersion := publication.CurrentTicketVersion + 1
	if version == waitingVersion && runner == publication.CurrentFence.RunnerEpoch && leader == publication.CurrentFence.LeaderEpoch {
		// The shared validator proves the untouched publication->waiting
		// baseline, including rejection of a recovery row at that version.
		if err := validateWaitingRecoveryLedger(ctx, q, ref, waitingVersion, publication.CurrentFence.RunnerEpoch, publication.CurrentFence.LeaderEpoch, version, runner, leader); err != nil {
			return PublishedCandidateEvidence{}, errors.Join(ciPublicationFailure("waiting recovery baseline"), err)
		}
	} else if runner == publication.CurrentFence.RunnerEpoch && leader == publication.CurrentFence.LeaderEpoch {
		// Once a pending CI self-transition exists, the generic recovery helper
		// quite correctly sees a live version beyond its recovery baseline. The
		// CI-specific validator accounts for those exact evidence rows.
		if err := validateCIRecoveryLedger(ctx, q, ref, waitingVersion, publication.CurrentFence.RunnerEpoch, publication.CurrentFence.LeaderEpoch, version, runner, leader); err != nil {
			return PublishedCandidateEvidence{}, errors.Join(ciPublicationFailure("pending CI chain"), err)
		}
	} else if err := validateWaitingRecoveryLedger(ctx, q, ref, waitingVersion, publication.CurrentFence.RunnerEpoch, publication.CurrentFence.LeaderEpoch, version, runner, leader); err != nil {
		// A legitimate recovery may have pending CI evidence between ledger
		// rows; the CI-specific validator supplies that contiguous-gap proof.
		if err2 := validateCIRecoveryLedger(ctx, q, ref, waitingVersion, publication.CurrentFence.RunnerEpoch, publication.CurrentFence.LeaderEpoch, version, runner, leader); err2 != nil {
			return PublishedCandidateEvidence{}, errors.Join(ciPublicationFailure("recovery with pending CI chain"), err2)
		}
	}
	if version < waitingVersion {
		return PublishedCandidateEvidence{}, ErrPublicationEvidence
	}
	payloadBytes, err := json.Marshal(struct {
		WitnessDigest    string `json:"witness_digest"`
		WitnessCreatedAt string `json:"witness_created_at"`
	}{publication.WitnessDigest, publication.CreatedAt.Format(time.RFC3339Nano)})
	if err != nil {
		return PublishedCandidateEvidence{}, ErrPublicationEvidence
	}
	var count int
	if err := q.QueryRowContext(ctx, `SELECT COUNT(*) FROM events e JOIN publication_transition_evidence p ON p.channel=e.channel AND p.project_id=e.project_id AND p.ticket_id=e.ticket_id AND p.ticket_version=e.ticket_version AND p.event_created_at=e.created_at WHERE e.channel=? AND e.project_id=? AND e.ticket_id=? AND e.ticket_version=? AND e.trigger='effects_confirmed' AND e.from_state='publishing' AND e.to_state='waiting_ci' AND e.payload=? AND p.witness_digest=? AND p.witness_created_at=?`, ref.Channel, ref.Project, ref.Ticket, waitingVersion, string(payloadBytes), publication.WitnessDigest, publication.CreatedAt.Format(time.RFC3339Nano)).Scan(&count); err != nil || count != 1 {
		return PublishedCandidateEvidence{}, ErrPublicationEvidence
	}
	if err := validateCIWaitingVersionEvents(ctx, q, ref, waitingVersion, string(payloadBytes), publication); err != nil {
		return PublishedCandidateEvidence{}, ciPublicationFailure("publication waiting event")
	}
	// A single exhausted-poll -> operator-resume pair consumes two ticket
	// versions without an external CI observation. It is distinct from a
	// pending observation and is accepted only by its exact immutable events.
	pollPair, hasPollPair, err := findCIPollResumePair(ctx, q, ref, waitingVersion, version)
	if err != nil {
		return PublishedCandidateEvidence{}, err
	}
	pollControlVersions := uint64(0)
	if hasPollPair {
		pollControlVersions = 2
	}
	rows, err := q.QueryContext(ctx, `SELECT c.ticket_version,c.event_id,c.event_created_at,c.candidate_generation,c.candidate_head_sha,c.candidate_tree_sha,c.observation_classification,c.observation_digest,c.observation_ticket_version,c.observation_leader_epoch,c.observation_runner_epoch,c.prior_publication_witness_digest,c.prior_state,c.resulting_state,c.resulting_trigger,c.transition_digest,e.id,e.created_at,e.from_state,e.to_state,e.trigger,e.payload FROM ci_transition_evidence c JOIN events e ON e.channel=c.channel AND e.project_id=c.project_id AND e.ticket_id=c.ticket_id AND e.ticket_version=c.ticket_version AND e.id=c.event_id WHERE c.channel=? AND c.project_id=? AND c.ticket_id=? AND c.ticket_version>? ORDER BY c.ticket_version`, ref.Channel, ref.Project, ref.Ticket, waitingVersion)
	if err != nil {
		return PublishedCandidateEvidence{}, normalizeBusy(ctx, err)
	}
	defer rows.Close()
	expectedVersion := waitingVersion + 1
	chainRunner, chainLeader := publication.CurrentFence.RunnerEpoch, publication.CurrentFence.LeaderEpoch
	chainCount := 0
	for rows.Next() {
		if hasPollPair && expectedVersion == pollPair.exhaustedVersion {
			expectedVersion = pollPair.resumeVersion + 1
		}
		chainCount++
		var evidenceVersion, eventID, generation, observationVersion, observationLeader, observationRunner int64
		var eventCreated, head, tree, classification, observationDigest, witness, priorState, resultingState, trigger, transitionDigest string
		var joinedEventID int64
		var joinedCreated, fromState, toState, eventTrigger, eventPayload string
		if err := rows.Scan(&evidenceVersion, &eventID, &eventCreated, &generation, &head, &tree, &classification, &observationDigest, &observationVersion, &observationLeader, &observationRunner, &witness, &priorState, &resultingState, &trigger, &transitionDigest, &joinedEventID, &joinedCreated, &fromState, &toState, &eventTrigger, &eventPayload); err != nil {
			return PublishedCandidateEvidence{}, ErrPublicationEvidence
		}
		// Recovery ledger rows occupy otherwise empty versions and carry the
		// only legal fence change in this append-only waiting-ci chain.
		if evidenceVersion > int64(expectedVersion) {
			var recoveryCount, recoveryEvents int
			if err := q.QueryRowContext(ctx, `SELECT COUNT(*) FROM runner_recovery_ledger WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version>=? AND ticket_version<?`, ref.Channel, ref.Project, ref.Ticket, expectedVersion, evidenceVersion).Scan(&recoveryCount); err != nil || recoveryCount != int(evidenceVersion-int64(expectedVersion)) {
				return PublishedCandidateEvidence{}, ErrPublicationEvidence
			}
			if err := q.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version>=? AND ticket_version<?`, ref.Channel, ref.Project, ref.Ticket, expectedVersion, evidenceVersion).Scan(&recoveryEvents); err != nil || recoveryEvents != 0 {
				return PublishedCandidateEvidence{}, ErrPublicationEvidence
			}
			expectedVersion = uint64(evidenceVersion)
		}
		if evidenceVersion == int64(expectedVersion) && evidenceVersion > int64(waitingVersion+1) {
			var recoveredRunner, recoveredLeader uint64
			err := q.QueryRowContext(ctx, `SELECT runner_epoch,leader_epoch FROM runner_recovery_ledger WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version>=? AND ticket_version<? ORDER BY ticket_version DESC LIMIT 1`, ref.Channel, ref.Project, ref.Ticket, waitingVersion, evidenceVersion).Scan(&recoveredRunner, &recoveredLeader)
			if err == nil {
				chainRunner, chainLeader = recoveredRunner, recoveredLeader
			} else if !errors.Is(err, sql.ErrNoRows) {
				return PublishedCandidateEvidence{}, normalizeBusy(ctx, err)
			}
		}
		if evidenceVersion != int64(expectedVersion) || eventID != joinedEventID || eventCreated != joinedCreated || generation != int64(publication.Candidate.Snapshot.Generation) || head != publication.Candidate.Snapshot.HeadSHA || tree != publication.Candidate.Snapshot.TreeSHA || classification != "pending" || observationDigest == "" || observationVersion != evidenceVersion-1 || observationLeader != int64(chainLeader) || observationRunner != int64(chainRunner) || witness != publication.WitnessDigest || priorState != string(domain.StateWaitingCI) || resultingState != string(domain.StateWaitingCI) || trigger != "checks_pending" || fromState != string(domain.StateWaitingCI) || toState != string(domain.StateWaitingCI) || eventTrigger != "checks_pending" {
			return PublishedCandidateEvidence{}, ciPublicationFailure("pending CI evidence identity")
		}
		observation, found, err := scanCIObservation(ctx, q, false, ref, observationDigest)
		if err != nil || !found || !ciObservationMatchesPublication(observation, publication) || observation.Classification != "pending" || observation.ObservedTicketVersion != uint64(observationVersion) || observation.ObservedFence.LeaderEpoch != uint64(observationLeader) || observation.ObservedFence.RunnerEpoch != uint64(observationRunner) {
			return PublishedCandidateEvidence{}, ciPublicationFailure("pending CI observation")
		}
		if transitionDigest != ciTransitionDigest(ref, observation, uint64(evidenceVersion), eventID, eventCreated, resultingState, trigger, eventPayload) {
			return PublishedCandidateEvidence{}, ciPublicationFailure("pending CI transition digest")
		}
		if err := q.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version=?`, ref.Channel, ref.Project, ref.Ticket, evidenceVersion).Scan(&count); err != nil || count != 1 {
			return PublishedCandidateEvidence{}, ErrPublicationEvidence
		}
		expectedVersion++
	}
	if err := rows.Err(); err != nil {
		return PublishedCandidateEvidence{}, ErrPublicationEvidence
	}
	var recoveryCount int
	if hasPollPair && (pollPair.exhaustedVersion < waitingVersion+1 || pollPair.resumeVersion > version) {
		return PublishedCandidateEvidence{}, ciPublicationFailure("poll resume placement")
	}
	if err := q.QueryRowContext(ctx, `SELECT COUNT(*) FROM runner_recovery_ledger WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version>? AND ticket_version<=?`, ref.Channel, ref.Project, ref.Ticket, waitingVersion, version).Scan(&recoveryCount); err != nil || chainCount+recoveryCount+int(pollControlVersions) != int(version-waitingVersion) {
		return PublishedCandidateEvidence{}, ciPublicationFailure("pending CI chain cardinality")
	}
	return publication, nil
}

// authenticateCandidateRepairCIHistory authenticates the bounded historical
// waiting-CI history consumed by one Store-owned repair binding. Unlike the
// current publication reader, it intentionally ignores rows after consumed;
// validateRunnerRecoveryAuthority separately audits the complete live ledger.
// The returned map names the exact fence at every authenticated publication,
// pending-CI, poll-control, and recovery endpoint through consumed.
func authenticateCandidateRepairCIHistory(ctx context.Context, q ciQuery, ref domain.TicketRef, publication PublishedCandidateEvidence, consumed uint64, consumedFence domain.Fence, budgetID string) (map[uint64]domain.Fence, error) {
	if ref.Validate() != nil || !publicationFound(publication) || publication.CurrentTicketVersion == 0 || publication.CurrentTicketVersion == ^uint64(0) || publication.CurrentFence.LeaderEpoch == 0 || publication.CurrentFence.RunnerEpoch == 0 || publication.CurrentFence.ClaimEpoch != 0 || consumedFence.LeaderEpoch == 0 || consumedFence.RunnerEpoch == 0 || consumedFence.ClaimEpoch != 0 || !boundedText(budgetID, 300) {
		return nil, ErrPublicationEvidence
	}
	waitingVersion := publication.CurrentTicketVersion + 1
	if consumed < waitingVersion || consumed-waitingVersion > maxCandidateRepairCIHistorySpan {
		return nil, ErrPublicationEvidence
	}

	publicationPayload, err := json.Marshal(struct {
		WitnessDigest    string `json:"witness_digest"`
		WitnessCreatedAt string `json:"witness_created_at"`
	}{publication.WitnessDigest, publication.CreatedAt.Format(time.RFC3339Nano)})
	if err != nil {
		return nil, ErrPublicationEvidence
	}
	var publicationRows int
	if err := q.QueryRowContext(ctx, `SELECT COUNT(*) FROM events e JOIN publication_transition_evidence p ON p.channel=e.channel AND p.project_id=e.project_id AND p.ticket_id=e.ticket_id AND p.ticket_version=e.ticket_version AND p.event_created_at=e.created_at WHERE e.channel=? AND e.project_id=? AND e.ticket_id=? AND e.ticket_version=? AND e.trigger='effects_confirmed' AND e.from_state='publishing' AND e.to_state='waiting_ci' AND e.payload=? AND p.witness_digest=? AND p.witness_created_at=?`, ref.Channel, ref.Project, ref.Ticket, waitingVersion, string(publicationPayload), publication.WitnessDigest, publication.CreatedAt.Format(time.RFC3339Nano)).Scan(&publicationRows); err != nil || publicationRows != 1 {
		return nil, ErrPublicationEvidence
	}
	if err := validateCIWaitingVersionEvents(ctx, q, ref, waitingVersion, string(publicationPayload), publication); err != nil {
		return nil, ErrPublicationEvidence
	}
	if _, found, err := loadRunnerRecoveryAt(ctx, q, ref, waitingVersion); err != nil || found {
		return nil, ErrPublicationEvidence
	}
	var baselineTransitions int
	if err := q.QueryRowContext(ctx, `SELECT COUNT(*) FROM ci_transition_evidence WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version=?`, ref.Channel, ref.Project, ref.Ticket, waitingVersion).Scan(&baselineTransitions); err != nil || baselineTransitions != 0 {
		return nil, ErrPublicationEvidence
	}

	budgetPayload, err := json.Marshal(struct {
		RequestID string `json:"request_id"`
	}{budgetID})
	if err != nil {
		return nil, ErrPublicationEvidence
	}
	var budgetEvents, exactBudgetEvents, budgetRows int
	if err := q.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version>=? AND ticket_version<=? AND trigger='budget_correction'`, ref.Channel, ref.Project, ref.Ticket, waitingVersion, consumed).Scan(&budgetEvents); err != nil || budgetEvents != 1 {
		return nil, ErrPublicationEvidence
	}
	if err := q.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version=? AND trigger='budget_correction' AND from_state='waiting_ci' AND to_state='waiting_ci' AND payload=?`, ref.Channel, ref.Project, ref.Ticket, consumed, string(budgetPayload)).Scan(&exactBudgetEvents); err != nil || exactBudgetEvents != 1 {
		return nil, ErrPublicationEvidence
	}
	if err := q.QueryRowContext(ctx, `SELECT COUNT(*) FROM ticket_budget_uses WHERE channel=? AND project_id=? AND ticket_id=? AND kind='correction' AND request_id=? AND ticket_version=? AND leader_epoch=? AND runner_epoch=?`, ref.Channel, ref.Project, ref.Ticket, budgetID, consumed, consumedFence.LeaderEpoch, consumedFence.RunnerEpoch).Scan(&budgetRows); err != nil || budgetRows != 1 {
		return nil, ErrPublicationEvidence
	}

	pollPair, hasPollPair, err := findCandidateRepairCIPollResumePair(ctx, q, ref, waitingVersion, consumed, string(budgetPayload))
	if err != nil {
		return nil, err
	}
	retry, hasRetry, err := loadCIPollRetryEpoch(ctx, q, ref, publication)
	if err != nil {
		return nil, err
	}
	if hasPollPair {
		if !hasRetry || retry.initialAttempts < 1 || retry.initialAttempts > ciPollMaxAttempts || retry.exhaustedVersion != pollPair.exhaustedVersion || retry.resumeVersion != pollPair.resumeVersion || retry.leader != publication.CurrentFence.LeaderEpoch || retry.runner != publication.CurrentFence.RunnerEpoch || !retry.resumedAt.Equal(pollPair.resumedAt) || !retry.deadline.Equal(retry.resumedAt.Add(ciPollDeadline)) || retry.digest != ciPollRetryEpochDigest(ref, publication, retry.initialAttempts, retry.exhaustedVersion, retry.resumeVersion, retry.leader, retry.runner, retry.resumedAt, retry.deadline) {
			return nil, ErrPublicationEvidence
		}
	} else if hasRetry {
		return nil, ErrPublicationEvidence
	}
	policy, err := scanCurrentCIPolicy(ctx, q, ref, publication)
	if err != nil {
		return nil, ErrPublicationEvidence
	}
	endpoints := map[uint64]domain.Fence{
		publication.CurrentTicketVersion: publication.CurrentFence,
		waitingVersion:                   publication.CurrentFence,
	}
	currentFence := publication.CurrentFence
	for version := waitingVersion + 1; version <= consumed; {
		if hasPollPair && version == pollPair.exhaustedVersion {
			endpoints[pollPair.exhaustedVersion] = currentFence
			endpoints[pollPair.resumeVersion] = currentFence
			version = pollPair.resumeVersion + 1
			continue
		}

		recovery, recovered, err := loadRunnerRecoveryAt(ctx, q, ref, version)
		if err != nil {
			return nil, err
		}
		var transitionCount int
		if err := q.QueryRowContext(ctx, `SELECT COUNT(*) FROM ci_transition_evidence WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version=?`, ref.Channel, ref.Project, ref.Ticket, version).Scan(&transitionCount); err != nil || transitionCount > 1 {
			return nil, ErrPublicationEvidence
		}
		expectedEvents := 0
		if version == consumed {
			expectedEvents++
		}
		if recovered {
			if transitionCount != 0 || !validRunnerRecovery(recovery) || recovery.PriorTicketVersion+1 != version || recovery.PriorTicketVersion != version-1 || recovery.PriorRunnerEpoch != currentFence.RunnerEpoch || recovery.PriorLeaderEpoch != currentFence.LeaderEpoch {
				return nil, ErrPublicationEvidence
			}
			var events int
			if err := q.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version=?`, ref.Channel, ref.Project, ref.Ticket, version).Scan(&events); err != nil || events != expectedEvents {
				return nil, ErrPublicationEvidence
			}
			currentFence = domain.Fence{LeaderEpoch: recovery.LeaderEpoch, RunnerEpoch: recovery.RunnerEpoch}
			endpoints[version] = currentFence
			version++
			continue
		}
		if transitionCount != 1 {
			return nil, ErrPublicationEvidence
		}
		var evidenceVersion, observationVersion uint64
		var eventID, joinedEventID int64
		var generation uint64
		var eventCreated, head, tree, classification, observationDigest, witness, priorState, resultingState, trigger, transitionDigest string
		var observationLeader, observationRunner uint64
		var joinedCreated, fromState, toState, eventTrigger, eventPayload string
		if err := q.QueryRowContext(ctx, `SELECT c.ticket_version,c.event_id,c.event_created_at,c.candidate_generation,c.candidate_head_sha,c.candidate_tree_sha,c.observation_classification,c.observation_digest,c.observation_ticket_version,c.observation_leader_epoch,c.observation_runner_epoch,c.prior_publication_witness_digest,c.prior_state,c.resulting_state,c.resulting_trigger,c.transition_digest,e.id,e.created_at,e.from_state,e.to_state,e.trigger,e.payload FROM ci_transition_evidence c JOIN events e ON e.channel=c.channel AND e.project_id=c.project_id AND e.ticket_id=c.ticket_id AND e.ticket_version=c.ticket_version AND e.id=c.event_id AND e.created_at=c.event_created_at WHERE c.channel=? AND c.project_id=? AND c.ticket_id=? AND c.ticket_version=?`, ref.Channel, ref.Project, ref.Ticket, version).Scan(&evidenceVersion, &eventID, &eventCreated, &generation, &head, &tree, &classification, &observationDigest, &observationVersion, &observationLeader, &observationRunner, &witness, &priorState, &resultingState, &trigger, &transitionDigest, &joinedEventID, &joinedCreated, &fromState, &toState, &eventTrigger, &eventPayload); err != nil {
			return nil, ErrPublicationEvidence
		}
		if evidenceVersion != version || eventID != joinedEventID || eventCreated != joinedCreated || generation != publication.Candidate.Snapshot.Generation || head != publication.Candidate.Snapshot.HeadSHA || tree != publication.Candidate.Snapshot.TreeSHA || classification != "pending" || observationVersion+1 != version || observationLeader != currentFence.LeaderEpoch || observationRunner != currentFence.RunnerEpoch || witness != publication.WitnessDigest || priorState != string(domain.StateWaitingCI) || resultingState != string(domain.StateWaitingCI) || trigger != "checks_pending" || fromState != string(domain.StateWaitingCI) || toState != string(domain.StateWaitingCI) || eventTrigger != "checks_pending" {
			return nil, ErrPublicationEvidence
		}
		observation, found, err := scanCIObservation(ctx, q, false, ref, observationDigest)
		if err != nil || !found || !ciObservationMatchesPublication(observation, publication) || !policyMatchesObservation(policy, observation) || observation.Classification != "pending" || observation.ObservedTicketVersion != observationVersion || observation.ObservedFence != currentFence || transitionDigest != ciTransitionDigest(ref, observation, version, eventID, eventCreated, resultingState, trigger, eventPayload) {
			return nil, ErrPublicationEvidence
		}
		expectedEvents++
		var events int
		if err := q.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version=?`, ref.Channel, ref.Project, ref.Ticket, version).Scan(&events); err != nil || events != expectedEvents {
			return nil, ErrPublicationEvidence
		}
		endpoints[version] = currentFence
		version++
	}
	if endpoints[consumed] != consumedFence {
		return nil, ErrPublicationEvidence
	}
	return endpoints, nil
}

// findCandidateRepairCIPollResumePair is the bounded historical form of the
// ordinary CI poll-pair reader. A red observation may append its one canonical
// budget event at the resume version, so that row is excluded explicitly from
// the otherwise exact two-event pair.
func findCandidateRepairCIPollResumePair(ctx context.Context, q ciQuery, ref domain.TicketRef, baseline, consumed uint64, budgetPayload string) (ciPollResumePair, bool, error) {
	rows, err := q.QueryContext(ctx, `SELECT ticket_version,payload FROM events WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version>? AND ticket_version<=? AND trigger='ci_poll_exhausted' AND from_state='waiting_ci' AND to_state='paused' ORDER BY ticket_version`, ref.Channel, ref.Project, ref.Ticket, baseline, consumed)
	if err != nil {
		return ciPollResumePair{}, false, normalizeBusy(ctx, err)
	}
	defer rows.Close()
	var pair ciPollResumePair
	found := false
	for rows.Next() {
		var exhausted uint64
		var payload string
		if err := rows.Scan(&exhausted, &payload); err != nil {
			return ciPollResumePair{}, false, err
		}
		var value struct {
			Code string `json:"code"`
		}
		if found || json.Unmarshal([]byte(payload), &value) != nil || (value.Code != "ci_poll_attempts_exhausted" && value.Code != "ci_poll_deadline_exhausted") || exhausted == ^uint64(0) || exhausted+1 > consumed {
			return ciPollResumePair{}, false, ErrPublicationEvidence
		}
		resumed := exhausted + 1
		var exhaustedCount, resumedCount, exhaustedEvents, resumedEvents, recoveryRows, transitionRows int
		if err := q.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version=? AND trigger='ci_poll_exhausted' AND from_state='waiting_ci' AND to_state='paused'`, ref.Channel, ref.Project, ref.Ticket, exhausted).Scan(&exhaustedCount); err != nil || exhaustedCount != 1 {
			return ciPollResumePair{}, false, ErrPublicationEvidence
		}
		if err := q.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version=? AND trigger='operator_resume' AND from_state='paused' AND to_state='waiting_ci'`, ref.Channel, ref.Project, ref.Ticket, resumed).Scan(&resumedCount); err != nil || resumedCount != 1 {
			return ciPollResumePair{}, false, ErrPublicationEvidence
		}
		if err := q.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version=?`, ref.Channel, ref.Project, ref.Ticket, exhausted).Scan(&exhaustedEvents); err != nil || exhaustedEvents != 1 {
			return ciPollResumePair{}, false, ErrPublicationEvidence
		}
		expectedResumeEvents := 1
		if resumed == consumed {
			expectedResumeEvents++
		}
		if err := q.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version=?`, ref.Channel, ref.Project, ref.Ticket, resumed).Scan(&resumedEvents); err != nil || resumedEvents != expectedResumeEvents {
			return ciPollResumePair{}, false, ErrPublicationEvidence
		}
		if resumed == consumed {
			var budget int
			if err := q.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version=? AND trigger='budget_correction' AND from_state='waiting_ci' AND to_state='waiting_ci' AND payload=?`, ref.Channel, ref.Project, ref.Ticket, resumed, budgetPayload).Scan(&budget); err != nil || budget != 1 {
				return ciPollResumePair{}, false, ErrPublicationEvidence
			}
		}
		if err := q.QueryRowContext(ctx, `SELECT COUNT(*) FROM runner_recovery_ledger WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version IN (?,?)`, ref.Channel, ref.Project, ref.Ticket, exhausted, resumed).Scan(&recoveryRows); err != nil || recoveryRows != 0 {
			return ciPollResumePair{}, false, ErrPublicationEvidence
		}
		if err := q.QueryRowContext(ctx, `SELECT COUNT(*) FROM ci_transition_evidence WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version IN (?,?)`, ref.Channel, ref.Project, ref.Ticket, exhausted, resumed).Scan(&transitionRows); err != nil || transitionRows != 0 {
			return ciPollResumePair{}, false, ErrPublicationEvidence
		}
		var resumedAt string
		if err := q.QueryRowContext(ctx, `SELECT created_at FROM events WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version=? AND trigger='operator_resume' AND from_state='paused' AND to_state='waiting_ci'`, ref.Channel, ref.Project, ref.Ticket, resumed).Scan(&resumedAt); err != nil {
			return ciPollResumePair{}, false, ErrPublicationEvidence
		}
		parsed, err := parsePublicationTime(resumedAt)
		if err != nil {
			return ciPollResumePair{}, false, ErrPublicationEvidence
		}
		pair = ciPollResumePair{exhaustedVersion: exhausted, resumeVersion: resumed, resumedAt: parsed}
		found = true
	}
	if err := rows.Err(); err != nil {
		return ciPollResumePair{}, false, err
	}
	return pair, found, nil
}

// validateCIRecoveryLedger authenticates a runner takeover after the
// publication boundary. CI self-transitions may advance the ticket version
// between recovery rows, but every such version must already be an authenticated
// waiting-ci evidence row; no counter gap is treated as a recovery proof.
func validateCIRecoveryLedger(ctx context.Context, q ciQuery, ref domain.TicketRef, baselineVersion, baselineRunner, baselineLeader, liveVersion, liveRunner, liveLeader uint64) error {
	if baselineVersion == 0 || baselineRunner == 0 || baselineLeader == 0 {
		return ciPublicationFailure("CI recovery baseline fence")
	}
	if _, found, err := loadRunnerRecoveryAt(ctx, q, ref, baselineVersion); err != nil || found {
		return ciPublicationFailure("CI recovery row at baseline")
	}
	if err := validateRunnerRecoveryCardinality(ctx, q, ref); err != nil {
		return err
	}
	rows, err := q.QueryContext(ctx, `SELECT prior_ticket_version,prior_runner_epoch,prior_leader_epoch,ticket_version,runner_epoch,leader_epoch,recovery_digest,created_at FROM runner_recovery_ledger WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version>? ORDER BY ticket_version`, ref.Channel, ref.Project, ref.Ticket, baselineVersion)
	if err != nil {
		return err
	}
	defer rows.Close()
	expectedVersion, expectedRunner, expectedLeader := baselineVersion, baselineRunner, baselineLeader
	pollPair, hasPollPair, err := findCIPollResumePair(ctx, q, ref, baselineVersion, liveVersion)
	if err != nil {
		return err
	}
	for rows.Next() {
		if hasPollPair && expectedVersion == pollPair.exhaustedVersion {
			expectedVersion = pollPair.resumeVersion
		}
		var step RunnerRecoveryLedger
		var created string
		if err := rows.Scan(&step.PriorTicketVersion, &step.PriorRunnerEpoch, &step.PriorLeaderEpoch, &step.TicketVersion, &step.RunnerEpoch, &step.LeaderEpoch, &step.RecoveryDigest, &created); err != nil {
			return ciPublicationFailure("CI recovery row decode")
		}
		step.Ref = ref
		step.CreatedAt, err = parseRunnerRecoveryTime(created)
		if err != nil || !validRunnerRecovery(step) || step.PriorRunnerEpoch != expectedRunner || step.PriorLeaderEpoch != expectedLeader || step.PriorTicketVersion < expectedVersion || step.TicketVersion != step.PriorTicketVersion+1 || step.RunnerEpoch != expectedRunner+1 || step.LeaderEpoch <= expectedLeader {
			return ciPublicationFailure("CI recovery row chain")
		}
		if step.PriorTicketVersion > expectedVersion {
			var evidence, events int
			if err := q.QueryRowContext(ctx, `SELECT COUNT(*) FROM ci_transition_evidence WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version>? AND ticket_version<=? AND resulting_state='waiting_ci'`, ref.Channel, ref.Project, ref.Ticket, expectedVersion, step.PriorTicketVersion).Scan(&evidence); err != nil {
				return ciPublicationFailure("CI recovery gap evidence")
			}
			if err := q.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version>? AND ticket_version<=?`, ref.Channel, ref.Project, ref.Ticket, expectedVersion, step.PriorTicketVersion).Scan(&events); err != nil {
				return ciPublicationFailure("CI recovery gap events")
			}
			controlCount := 0
			if hasPollPair && pollPair.exhaustedVersion > expectedVersion && pollPair.resumeVersion <= step.PriorTicketVersion {
				controlCount = 2
			}
			if evidence != int(step.PriorTicketVersion-expectedVersion)-controlCount || events != evidence+controlCount {
				return ciPublicationFailure("CI recovery gap cardinality")
			}
		}
		var events int
		if err := q.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version=? AND from_state<>to_state`, ref.Channel, ref.Project, ref.Ticket, step.TicketVersion).Scan(&events); err != nil || events != 0 {
			return ciPublicationFailure("CI recovery event cardinality")
		}
		expectedVersion, expectedRunner, expectedLeader = step.TicketVersion, step.RunnerEpoch, step.LeaderEpoch
	}
	if err := rows.Err(); err != nil {
		return ErrPublicationEvidence
	}
	// Between recovery rows (and after the last one), ordinary pending CI
	// evidence may advance the ticket while retaining the current fence. The
	// CI chain below authenticates those rows individually; this check only
	// proves that no unaccounted version was smuggled into a recovery gap.
	if hasPollPair && expectedVersion <= pollPair.exhaustedVersion {
		// The control pair itself has no CI evidence rows. Any versions before
		// exhaustion must already be accounted for by pending evidence or signed
		// recovery rows; the pair then advances the contiguous expectation to the
		// resumed waiting-ci version.
		var evidence, events, recovery int
		predecessorStart := expectedVersion + 1
		if err := q.QueryRowContext(ctx, `SELECT COUNT(*) FROM ci_transition_evidence WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version>=? AND ticket_version<? AND resulting_state='waiting_ci'`, ref.Channel, ref.Project, ref.Ticket, predecessorStart, pollPair.exhaustedVersion).Scan(&evidence); err != nil {
			return ciPublicationFailure("CI poll pair predecessor evidence")
		}
		if err := q.QueryRowContext(ctx, `SELECT COUNT(*) FROM runner_recovery_ledger WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version>=? AND ticket_version<?`, ref.Channel, ref.Project, ref.Ticket, predecessorStart, pollPair.exhaustedVersion).Scan(&recovery); err != nil {
			return ciPublicationFailure("CI poll pair predecessor recovery")
		}
		if err := q.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version>=? AND ticket_version<?`, ref.Channel, ref.Project, ref.Ticket, predecessorStart, pollPair.exhaustedVersion).Scan(&events); err != nil || events != evidence {
			return ciPublicationFailure("CI poll pair predecessor events")
		}
		if pollPair.exhaustedVersion > predecessorStart && evidence+recovery != int(pollPair.exhaustedVersion-predecessorStart) {
			return ciPublicationFailure("CI poll pair predecessor cardinality")
		}
		expectedVersion = pollPair.resumeVersion
	}
	if liveVersion > expectedVersion {
		var evidence, events int
		if err := q.QueryRowContext(ctx, `SELECT COUNT(*) FROM ci_transition_evidence WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version>? AND ticket_version<=? AND resulting_state='waiting_ci'`, ref.Channel, ref.Project, ref.Ticket, expectedVersion, liveVersion).Scan(&evidence); err != nil || evidence != int(liveVersion-expectedVersion) {
			return ciPublicationFailure("CI trailing evidence")
		}
		if err := q.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version>? AND ticket_version<=?`, ref.Channel, ref.Project, ref.Ticket, expectedVersion, liveVersion).Scan(&events); err != nil || events != evidence {
			return ciPublicationFailure("CI trailing events")
		}
		expectedVersion = liveVersion
	}
	if liveVersion != expectedVersion || liveRunner != expectedRunner || liveLeader != expectedLeader {
		return ciPublicationFailure("CI recovery final fence")
	}
	return nil
}

// validateCIWaitingVersionEvents authenticates the publication boundary while
// allowing the existing budget API to append its canonical self-event at the
// same ticket version. No other event may share that version: otherwise an
// unrelated self-transition could masquerade as a contiguous CI chain.
func validateCIWaitingVersionEvents(ctx context.Context, q ciQuery, ref domain.TicketRef, version uint64, publicationPayload string, publication PublishedCandidateEvidence) error {
	rows, err := q.QueryContext(ctx, `SELECT from_state,to_state,trigger,payload FROM events WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version=? ORDER BY id`, ref.Channel, ref.Project, ref.Ticket, version)
	if err != nil {
		return normalizeBusy(ctx, err)
	}
	defer rows.Close()
	publicationCount := 0
	total := 0
	for rows.Next() {
		total++
		var fromState, toState, trigger, payload string
		if err := rows.Scan(&fromState, &toState, &trigger, &payload); err != nil {
			return err
		}
		if fromState == string(domain.StatePublishing) && toState == string(domain.StateWaitingCI) && trigger == "effects_confirmed" && payload == publicationPayload {
			publicationCount++
			continue
		}
		if fromState != string(domain.StateWaitingCI) || toState != string(domain.StateWaitingCI) || trigger != "budget_correction" {
			return ErrPublicationEvidence
		}
		var budget struct {
			RequestID string `json:"request_id"`
		}
		if json.Unmarshal([]byte(payload), &budget) != nil || !boundedText(budget.RequestID, 300) {
			return ErrPublicationEvidence
		}
		canonicalBudgetPayload, err := json.Marshal(budget)
		if err != nil || payload != string(canonicalBudgetPayload) {
			return ErrPublicationEvidence
		}
		var found int
		if err := q.QueryRowContext(ctx, `SELECT COUNT(*) FROM ticket_budget_uses WHERE channel=? AND project_id=? AND ticket_id=? AND kind='correction' AND request_id=? AND ticket_version=? AND leader_epoch=? AND runner_epoch=?`, ref.Channel, ref.Project, ref.Ticket, budget.RequestID, version, publication.CurrentFence.LeaderEpoch, publication.CurrentFence.RunnerEpoch).Scan(&found); err != nil {
			return normalizeBusy(ctx, err)
		}
		if found != 1 {
			return ErrPublicationEvidence
		}
	}
	if err := rows.Err(); err != nil || publicationCount != 1 || total < 1 {
		return ErrPublicationEvidence
	}
	return nil
}

func loadCIPublicationBase(ctx context.Context, q ciQuery, ref domain.TicketRef) (PublishedCandidateEvidence, error) {
	publication, found, err := loadPublicationEvidenceRow(ctx, q, ref)
	if err != nil || !found || !publicationFound(publication) {
		return PublishedCandidateEvidence{}, ErrPublicationEvidence
	}
	if err := loadLatestPublicationRebind(ctx, q, &publication); err != nil {
		return PublishedCandidateEvidence{}, ErrPublicationEvidence
	}
	return publication, nil
}

func publicationFound(value PublishedCandidateEvidence) bool {
	return value.Ref.Validate() == nil && value.WitnessDigest != ""
}

func ciPublicationFailure(stage string) error {
	return fmt.Errorf("%w: %s", ErrPublicationEvidence, stage)
}

func canonicalCIPolicyJSON(checks []CIObservationCheck) string {
	type identity struct {
		Name       string `json:"name"`
		ExternalID string `json:"external_id"`
	}
	items := make([]identity, len(checks))
	for i, check := range checks {
		items[i] = identity{check.CanonicalName, ""}
	}
	body, _ := json.Marshal(items)
	return string(body)
}

func scanCurrentCIPolicy(ctx context.Context, q ciQuery, ref domain.TicketRef, publication PublishedCandidateEvidence) (CIRequiredCheckPolicy, error) {
	var policy CIRequiredCheckPolicy
	var channel domain.Channel
	var project, ticket, witness, protectedRef, protectedOID, sourceDigest, principal, policyWitness, checksJSON, created string
	var generation, policyID, count int64
	err := q.QueryRowContext(ctx, `SELECT policy_id,channel,project_id,ticket_id,candidate_generation,candidate_head_sha,candidate_tree_sha,publication_witness_digest,protected_branch_ref,protected_branch_oid,policy_source_digest,authenticated_principal,policy_witness_digest,required_set_digest,required_check_count,required_checks_json,created_at FROM ci_required_check_policies WHERE channel=? AND project_id=? AND ticket_id=? AND candidate_generation=? AND candidate_head_sha=? AND candidate_tree_sha=? AND publication_witness_digest=? ORDER BY policy_id DESC LIMIT 1`, ref.Channel, ref.Project, ref.Ticket, publication.Candidate.Snapshot.Generation, publication.Candidate.Snapshot.HeadSHA, publication.Candidate.Snapshot.TreeSHA, publication.WitnessDigest).Scan(&policyID, &channel, &project, &ticket, &generation, &policy.CandidateHeadSHA, &policy.CandidateTreeSHA, &witness, &protectedRef, &protectedOID, &sourceDigest, &principal, &policyWitness, &policy.RequiredSetDigest, &count, &checksJSON, &created)
	if errors.Is(err, sql.ErrNoRows) {
		return CIRequiredCheckPolicy{}, ErrCIObservation
	}
	if err != nil {
		return CIRequiredCheckPolicy{}, normalizeBusy(ctx, err)
	}
	policy.PolicyID, policy.Ref = policyID, domain.TicketRef{Channel: channel, Project: domain.ProjectID(project), Ticket: domain.TicketID(ticket)}
	policy.CandidateGeneration, policy.PublicationWitnessDigest = uint64(generation), witness
	policy.ProtectedBranchRef, policy.ProtectedBranchOID, policy.PolicySourceDigest, policy.AuthenticatedPrincipal, policy.PolicyWitnessDigest = protectedRef, protectedOID, sourceDigest, principal, policyWitness
	if policy.Ref != ref || policy.CandidateGeneration != publication.Candidate.Snapshot.Generation || policy.CandidateHeadSHA != publication.Candidate.Snapshot.HeadSHA || policy.CandidateTreeSHA != publication.Candidate.Snapshot.TreeSHA || policy.PublicationWitnessDigest != publication.WitnessDigest {
		return CIRequiredCheckPolicy{}, ErrCIObservation
	}
	var storedChecks []struct {
		Name       string `json:"name"`
		ExternalID string `json:"external_id"`
	}
	if err := json.Unmarshal([]byte(checksJSON), &storedChecks); err != nil || int64(len(storedChecks)) != count {
		return CIRequiredCheckPolicy{}, ErrCIObservation
	}
	policy.RequiredChecks = make([]CIObservationCheck, len(storedChecks))
	for i, check := range storedChecks {
		policy.RequiredChecks[i] = CIObservationCheck{CanonicalName: check.Name, ExternalID: check.ExternalID}
	}
	policy.CreatedAt, err = parsePublicationTime(created)
	if err != nil {
		return CIRequiredCheckPolicy{}, ErrCIObservation
	}
	canonical, err := canonicalCIPolicy(policy)
	if err != nil || canonicalCIPolicyJSON(canonical.RequiredChecks) != checksJSON || canonical.RequiredSetDigest != policy.RequiredSetDigest || canonical.PolicyWitnessDigest != policy.PolicyWitnessDigest {
		return CIRequiredCheckPolicy{}, ErrCIObservation
	}
	return policy, nil
}

func policyMatchesObservation(policy CIRequiredCheckPolicy, observation CIObservation) bool {
	if policy.Ref != observation.Ref || policy.CandidateGeneration != observation.CandidateGeneration || policy.CandidateHeadSHA != observation.CandidateHeadSHA || policy.CandidateTreeSHA != observation.CandidateTreeSHA || policy.PublicationWitnessDigest != observation.PublicationWitnessDigest || policy.PolicyWitnessDigest != observation.PolicyWitnessDigest || len(policy.RequiredChecks) != len(observation.RequiredChecks) {
		return false
	}
	for i := range policy.RequiredChecks {
		if policy.RequiredChecks[i].CanonicalName != observation.RequiredChecks[i].CanonicalName {
			return false
		}
	}
	return true
}

// NormalizeCIObservationChecks converts the GitHub adapter's untrusted check
// rows into the Store's typed observation contract.
func NormalizeCIObservationChecks(checks []contracts.RequiredCheck) ([]CIObservationCheck, error) {
	out := make([]CIObservationCheck, len(checks))
	for i, check := range checks {
		out[i] = CIObservationCheck{CanonicalName: check.Name, ExternalID: check.ExternalID, NormalizedState: check.State}
	}
	return canonicalCIObservationChecks(out)
}

func ciObservationClassification(checks []CIObservationCheck) string {
	pending, red := false, false
	for _, check := range checks {
		pending = pending || check.NormalizedState == "pending"
		red = red || check.NormalizedState == "failure" || check.NormalizedState == "cancelled"
	}
	if red {
		return "red"
	}
	if pending {
		return "pending"
	}
	return "green"
}

// RecordCIObservation refuses raw caller-assembled GitHub facts. Production
// callers must use RecordCIObservationFromObserver so the Store owns the
// observer handoff and reauthenticates the result before persistence.
func (s *Store) RecordCIObservation(ctx context.Context, input CIObservation) error {
	return ErrCIObservation
}

// RecordAuthenticatedCIObservation is retained as an explicit fail-closed
// compatibility surface. A descriptive method name is not an authentication
// capability; only RecordCIObservationFromObserver may append remote facts.
func (s *Store) RecordAuthenticatedCIObservation(ctx context.Context, input CIObservation) error {
	return ErrCIObservation
}

// RecordCIObservationFromObserver loads the exact current publication before
// invoking the read-only external boundary. The observer runs without a
// SQLite write transaction. recordCIObservation then reloads the publication,
// ticket fence, and required-check policy on one connection before inserting.
func (s *Store) RecordCIObservationFromObserver(ctx context.Context, ref domain.TicketRef, expectedVersion uint64, fence domain.Fence, observer contracts.CIRequiredChecksObserver) error {
	if observer == nil || ref.Validate() != nil || expectedVersion == 0 || fence.LeaderEpoch == 0 || fence.RunnerEpoch == 0 || fence.ClaimEpoch != 0 {
		return ErrCIObservation
	}
	if err := s.AssertTicketFence(ctx, ref, expectedVersion, fence); err != nil {
		return err
	}
	publication, err := loadCICurrentPublication(ctx, s.db, ref)
	if err != nil {
		return errors.Join(ErrCIObservation, fmt.Errorf("load current CI publication: %w", err))
	}
	checks, err := observer.RequiredChecks(ctx, publication.PullRequest)
	if err != nil {
		return errors.Join(ErrCIObservation, err)
	}
	normalized, err := NormalizeCIObservationChecks(checks)
	if err != nil {
		return err
	}
	return s.recordCIObservation(ctx, CIObservation{
		Ref:                      ref,
		CandidateGeneration:      publication.Candidate.Snapshot.Generation,
		CandidateHeadSHA:         publication.Candidate.Snapshot.HeadSHA,
		CandidateTreeSHA:         publication.Candidate.Snapshot.TreeSHA,
		PublicationWitnessDigest: publication.WitnessDigest,
		PullRequest:              publication.PullRequest,
		ObservedTicketVersion:    expectedVersion,
		ObservedFence:            fence,
		ObservedAt:               time.Now().UTC(),
		RequiredChecks:           normalized,
		Classification:           ciObservationClassification(normalized),
	})
}

// recordCIObservation is the same-package fixture seam and the sole internal
// append implementation. Exact replay is a no-op; any digest or field conflict
// fails closed.
func (s *Store) recordCIObservation(ctx context.Context, input CIObservation) error {
	claimedObservationDigest := input.ObservationDigest
	input.ObservationDigest = ""
	canonical, err := canonicalCIObservation(input)
	if err != nil {
		return err
	}
	if delta := time.Since(canonical.ObservedAt); delta > maxCIClockSkew || delta < -maxCIClockSkew {
		return ErrCIObservation
	}
	err = s.ciWrite(ctx, canonical.Ref, func(conn *sql.Conn) error {
		publication, err := loadCICurrentPublication(ctx, conn, canonical.Ref)
		if err != nil {
			return errors.Join(ErrCIObservation, fmt.Errorf("load current CI publication: %w", err))
		}
		if !ciObservationMatchesPublication(canonical, publication) {
			return fmt.Errorf("%w: observation does not match current publication", ErrCIObservation)
		}
		var state string
		var version, runner, leader uint64
		if err := conn.QueryRowContext(ctx, `SELECT t.state,t.version,t.runner_epoch,d.leader_epoch FROM tickets t JOIN daemon_instances d ON d.channel=t.channel WHERE t.channel=? AND t.project_id=? AND t.id=?`, canonical.Ref.Channel, canonical.Ref.Project, canonical.Ref.Ticket).Scan(&state, &version, &runner, &leader); err != nil {
			return normalizeBusy(ctx, err)
		}
		if err := s.assertTicketFence(ctx, conn, canonical.Ref, canonical.ObservedTicketVersion, canonical.ObservedFence); err != nil {
			return err
		}
		if state != string(domain.StateWaitingCI) || version != canonical.ObservedTicketVersion || runner != canonical.ObservedFence.RunnerEpoch || leader != canonical.ObservedFence.LeaderEpoch {
			return ErrStaleFence
		}
		policy, err := scanCurrentCIPolicy(ctx, conn, canonical.Ref, publication)
		if err != nil {
			return ErrCIObservation
		}
		canonical.PolicyWitnessDigest = policy.PolicyWitnessDigest
		if !policyMatchesObservation(policy, canonical) {
			return ErrCIObservation
		}
		canonical.ObservationDigest = ""
		canonical, err = canonicalCIObservation(canonical)
		if err != nil || (claimedObservationDigest != "" && claimedObservationDigest != canonical.ObservationDigest) {
			return ErrCIObservation
		}
		var existing CIObservation
		existing, found, err := scanCIObservation(ctx, conn, false, canonical.Ref, canonical.ObservationDigest)
		if err != nil {
			return err
		}
		if found {
			if !ciObservationEqual(existing, canonical) {
				return ErrEvidenceConflict
			}
			return nil
		}
		var predecessorGeneration uint64
		if err := conn.QueryRowContext(ctx, `SELECT COALESCE(MAX(generation),0) FROM candidate_snapshots WHERE channel=? AND project_id=? AND ticket_id=?`, canonical.Ref.Channel, canonical.Ref.Project, canonical.Ref.Ticket).Scan(&predecessorGeneration); err != nil {
			return err
		}
		var pendingRepair int
		if err := conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM candidate_repair_bindings WHERE channel=? AND project_id=? AND ticket_id=? AND predecessor_generation=?`, canonical.Ref.Channel, canonical.Ref.Project, canonical.Ref.Ticket, predecessorGeneration).Scan(&pendingRepair); err != nil {
			return err
		}
		if pendingRepair != 0 {
			return ErrEvidenceConflict
		}
		result, err := conn.ExecContext(ctx, `INSERT INTO ci_observations(channel,project_id,ticket_id,candidate_generation,candidate_head_sha,candidate_tree_sha,publication_witness_digest,policy_witness_digest,pr_host,pr_owner,pr_repo,pr_number,pr_head_owner,pr_head_repo,pr_head_ref,pr_head_oid,pr_base_ref,pr_base_oid,pr_factory_owned,pr_open,pr_draft,observed_ticket_version,observed_leader_epoch,observed_runner_epoch,observed_at,required_set_digest,required_check_count,classification,diagnostic_digest,diagnostic_text,diagnostic_json,observation_digest) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, canonical.Ref.Channel, canonical.Ref.Project, canonical.Ref.Ticket, canonical.CandidateGeneration, canonical.CandidateHeadSHA, canonical.CandidateTreeSHA, canonical.PublicationWitnessDigest, canonical.PolicyWitnessDigest, canonical.PullRequest.Repository.Host, canonical.PullRequest.Repository.Owner, canonical.PullRequest.Repository.Name, canonical.PullRequest.Number, canonical.PullRequest.HeadOwner, canonical.PullRequest.HeadRepository, canonical.PullRequest.HeadRef, canonical.PullRequest.HeadOID, canonical.PullRequest.BaseRef, canonical.PullRequest.BaseOID, 1, 1, 1, canonical.ObservedTicketVersion, canonical.ObservedFence.LeaderEpoch, canonical.ObservedFence.RunnerEpoch, canonical.ObservedAt.Format(time.RFC3339Nano), canonical.RequiredSetDigest, len(canonical.RequiredChecks), canonical.Classification, canonical.DiagnosticDigest, canonical.DiagnosticText, canonical.DiagnosticJSON, canonical.ObservationDigest)
		if err != nil {
			return err
		}
		id, err := result.LastInsertId()
		if err != nil || id <= 0 {
			return ErrCIObservation
		}
		for _, check := range canonical.RequiredChecks {
			if _, err := conn.ExecContext(ctx, `INSERT INTO ci_observation_checks(observation_id,observation_digest,canonical_name,external_id,normalized_state,failing_diagnostic_digest,failing_diagnostic_text) VALUES(?,?,?,?,?,?,?)`, id, canonical.ObservationDigest, check.CanonicalName, check.ExternalID, check.NormalizedState, check.FailingDiagnosticDigest, check.FailingDiagnosticText); err != nil {
				return err
			}
		}
		return nil
	})
	return err
}

// RecordCIRequiredCheckPolicy stores the exact server-defined required set for
// the currently published candidate. The timestamp is assigned by Store so an
// external clock cannot poison policy ordering.
func (s *Store) RecordCIRequiredCheckPolicy(ctx context.Context, input CIRequiredCheckPolicy) error {
	if !input.authenticated {
		return ErrCIObservation
	}
	return s.recordCIRequiredCheckPolicy(ctx, input)
}

func (s *Store) recordCIRequiredCheckPolicy(ctx context.Context, input CIRequiredCheckPolicy) error {
	policy, err := canonicalCIPolicy(input)
	if err != nil {
		return err
	}
	publication, err := loadCICurrentPublication(ctx, s.db, policy.Ref)
	if err != nil {
		return errors.Join(ErrCIObservation, err)
	}
	if policy.CandidateGeneration != publication.Candidate.Snapshot.Generation || policy.CandidateHeadSHA != publication.Candidate.Snapshot.HeadSHA || policy.CandidateTreeSHA != publication.Candidate.Snapshot.TreeSHA || policy.PublicationWitnessDigest != publication.WitnessDigest || policy.ProtectedBranchRef != publication.PullRequest.BaseRef || policy.ProtectedBranchOID != publication.PullRequest.BaseOID {
		return ErrCIObservation
	}
	policy.CreatedAt = time.Now().UTC()
	return s.ciWrite(ctx, policy.Ref, func(conn *sql.Conn) error {
		var state string
		var version, runner, leader uint64
		if err := conn.QueryRowContext(ctx, `SELECT t.state,t.version,t.runner_epoch,d.leader_epoch FROM tickets t JOIN daemon_instances d ON d.channel=t.channel WHERE t.channel=? AND t.project_id=? AND t.id=?`, policy.Ref.Channel, policy.Ref.Project, policy.Ref.Ticket).Scan(&state, &version, &runner, &leader); err != nil {
			return normalizeBusy(ctx, err)
		}
		if state != string(domain.StateWaitingCI) {
			return ErrStaleFence
		}
		if err := s.assertTicketFence(ctx, conn, policy.Ref, version, domain.Fence{LeaderEpoch: leader, RunnerEpoch: runner}); err != nil {
			return err
		}
		result, err := conn.ExecContext(ctx, `INSERT INTO ci_required_check_policies(channel,project_id,ticket_id,candidate_generation,candidate_head_sha,candidate_tree_sha,publication_witness_digest,protected_branch_ref,protected_branch_oid,policy_source_digest,authenticated_principal,policy_witness_digest,required_set_digest,required_check_count,required_checks_json,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(channel,project_id,ticket_id,candidate_generation,candidate_head_sha,candidate_tree_sha,publication_witness_digest) DO NOTHING`, policy.Ref.Channel, policy.Ref.Project, policy.Ref.Ticket, policy.CandidateGeneration, policy.CandidateHeadSHA, policy.CandidateTreeSHA, policy.PublicationWitnessDigest, policy.ProtectedBranchRef, policy.ProtectedBranchOID, policy.PolicySourceDigest, policy.AuthenticatedPrincipal, policy.PolicyWitnessDigest, policy.RequiredSetDigest, len(policy.RequiredChecks), canonicalCIPolicyJSON(policy.RequiredChecks), policy.CreatedAt.Format(time.RFC3339Nano))
		if err != nil {
			return err
		}
		if count, _ := result.RowsAffected(); count > 1 {
			return ErrCIObservation
		}
		stored, err := scanCurrentCIPolicy(ctx, conn, policy.Ref, publication)
		if err != nil {
			return err
		}
		if stored.RequiredSetDigest != policy.RequiredSetDigest || stored.PolicyWitnessDigest != policy.PolicyWitnessDigest || stored.ProtectedBranchRef != policy.ProtectedBranchRef || stored.ProtectedBranchOID != policy.ProtectedBranchOID || stored.PolicySourceDigest != policy.PolicySourceDigest || stored.AuthenticatedPrincipal != policy.AuthenticatedPrincipal || !policyMatchesObservation(stored, CIObservation{Ref: policy.Ref, CandidateGeneration: policy.CandidateGeneration, CandidateHeadSHA: policy.CandidateHeadSHA, CandidateTreeSHA: policy.CandidateTreeSHA, PublicationWitnessDigest: policy.PublicationWitnessDigest, PolicyWitnessDigest: policy.PolicyWitnessDigest, RequiredChecks: policy.RequiredChecks, RequiredSetDigest: policy.RequiredSetDigest}) {
			return ErrEvidenceConflict
		}
		return nil
	})
}

// RecordCIRequiredCheckPolicyFromObserver is the production composition
// boundary. The Store derives candidate/publication identity from its own
// authenticated row and accepts the check set only from an observer that also
// supplies the protected-ref/source/principal witness.
func (s *Store) RecordCIRequiredCheckPolicyFromObserver(ctx context.Context, ref domain.TicketRef, observer contracts.CIRequiredCheckPolicyObserver) error {
	if observer == nil || ref.Validate() != nil {
		return ErrCIObservation
	}
	publication, err := s.LoadPublishedCandidate(ctx, ref)
	if err != nil {
		return err
	}
	observed, err := observer.ObserveCIRequiredCheckPolicy(ctx, publication.PullRequest)
	if err != nil || observed.PullRequest != publication.PullRequest || observed.ProtectedBranchRef != publication.PullRequest.BaseRef || observed.ProtectedBranchOID != publication.PullRequest.BaseOID {
		return ErrCIObservation
	}
	checks, err := NormalizeCIObservationChecks(observed.RequiredChecks)
	if err != nil {
		return err
	}
	input := CIRequiredCheckPolicy{
		Ref: ref, CandidateGeneration: publication.Candidate.Snapshot.Generation,
		CandidateHeadSHA: publication.Candidate.Snapshot.HeadSHA, CandidateTreeSHA: publication.Candidate.Snapshot.TreeSHA,
		PublicationWitnessDigest: publication.WitnessDigest, ProtectedBranchRef: observed.ProtectedBranchRef,
		ProtectedBranchOID: observed.ProtectedBranchOID, PolicySourceDigest: observed.PolicySourceDigest,
		AuthenticatedPrincipal: observed.AuthenticatedPrincipal, PolicyWitnessDigest: observed.PolicyWitnessDigest,
		RequiredChecks: checks, CreatedAt: observed.ObservedAt, authenticated: true,
	}
	canonical, err := canonicalCIPolicy(input)
	if err != nil {
		return err
	}
	return s.recordCIRequiredCheckPolicy(ctx, canonical)
}

func (s *Store) RecordRequiredCheckPolicy(ctx context.Context, input CIRequiredCheckPolicy) error {
	return s.RecordCIRequiredCheckPolicy(ctx, input)
}

type candidateRepairBindingAuthority struct {
	context                 CandidateRepairBuildContext
	PRHost, PROwner, PRRepo string
	PRNumber                int64
	BranchRef, RemoteHead   string
	BaseRef, RemoteBase     string
	ObservationDigest       string
	ObservationClass        string
	TransitionDigest        string
	BudgetKind, BudgetID    string
	ConsumedVersion         uint64
	ConsumedFence           domain.Fence
	RecoveryPrefixDigest    string
	ContextDigest           string
	CreatedAt               string
	// publicationVersion/fence and ciEndpoints are derived, read-only
	// authentication results. They are deliberately not persisted in the
	// repair binding: the immutable publication, CI transition, observation,
	// and recovery rows remain the source of truth.
	publicationVersion uint64
	publicationFence   domain.Fence
	ciEndpoints        map[uint64]domain.Fence
}

// candidateRepairRecoveryPrefixDigest seals the exact recovery rows which
// existed when a red-CI observation was consumed. The repair binding is a
// historical authority after that point, so later rows are intentionally
// excluded while deletion, insertion, or mutation anywhere at or before the
// consumed endpoint changes this digest. The live Consume path has already
// authenticated the CI/publication chain which admitted these rows; historical
// readers recompute this snapshot instead of circularly treating checks_red as
// a new recovery root.
func candidateRepairRecoveryPrefixDigest(ctx context.Context, q ciQuery, ref domain.TicketRef, through uint64) (string, error) {
	if ref.Validate() != nil || through == 0 {
		return "", ErrEvidenceConflict
	}
	if err := validateRunnerRecoveryCardinality(ctx, q, ref); err != nil {
		return "", err
	}
	type prefixRow struct {
		PriorTicketVersion uint64 `json:"prior_ticket_version"`
		PriorRunnerEpoch   uint64 `json:"prior_runner_epoch"`
		PriorLeaderEpoch   uint64 `json:"prior_leader_epoch"`
		TicketVersion      uint64 `json:"ticket_version"`
		RunnerEpoch        uint64 `json:"runner_epoch"`
		LeaderEpoch        uint64 `json:"leader_epoch"`
		RecoveryDigest     string `json:"recovery_digest"`
		CreatedAt          string `json:"created_at"`
	}
	rows, err := q.QueryContext(ctx, `SELECT prior_ticket_version,prior_runner_epoch,prior_leader_epoch,ticket_version,runner_epoch,leader_epoch,recovery_digest,created_at FROM runner_recovery_ledger WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version<=? ORDER BY ticket_version`, ref.Channel, ref.Project, ref.Ticket, through)
	if err != nil {
		return "", err
	}
	defer rows.Close()
	values := make([]prefixRow, 0, 8)
	for rows.Next() {
		if len(values) >= 64 {
			return "", ErrPublicationEvidence
		}
		var row prefixRow
		if err := rows.Scan(&row.PriorTicketVersion, &row.PriorRunnerEpoch, &row.PriorLeaderEpoch, &row.TicketVersion, &row.RunnerEpoch, &row.LeaderEpoch, &row.RecoveryDigest, &row.CreatedAt); err != nil {
			return "", err
		}
		created, err := parseRunnerRecoveryTime(row.CreatedAt)
		value := RunnerRecoveryLedger{Ref: ref, PriorTicketVersion: row.PriorTicketVersion, PriorRunnerEpoch: row.PriorRunnerEpoch, PriorLeaderEpoch: row.PriorLeaderEpoch, TicketVersion: row.TicketVersion, RunnerEpoch: row.RunnerEpoch, LeaderEpoch: row.LeaderEpoch, RecoveryDigest: row.RecoveryDigest, CreatedAt: created}
		if err != nil || row.TicketVersion > through || !validRunnerRecovery(value) {
			return "", ErrPublicationEvidence
		}
		values = append(values, row)
	}
	if err := rows.Err(); err != nil {
		return "", err
	}
	payload, err := json.Marshal(struct {
		Ref     domain.TicketRef `json:"ref"`
		Through uint64           `json:"through"`
		Rows    []prefixRow      `json:"rows"`
	}{Ref: ref, Through: through, Rows: values})
	if err != nil {
		return "", err
	}
	return ciAuthorityDigest(payload), nil
}

// CandidateRepairBuildContext authenticates the exact red-CI correction
// lineage at the caller's live Building fence. It is deliberately separate
// from ordinary phase recovery: the prior publication remains durable context
// while only a fresh Builder result may create the successor generation.
func (s *Store) CandidateRepairBuildContext(ctx context.Context, ref domain.TicketRef, expected uint64, fence domain.Fence) (CandidateRepairBuildContext, error) {
	if ref.Validate() != nil || expected == 0 || fence.LeaderEpoch == 0 || fence.RunnerEpoch == 0 || fence.ClaimEpoch != 0 {
		return CandidateRepairBuildContext{}, ErrEvidenceConflict
	}
	var state domain.State
	var version, runner, leader uint64
	if err := s.db.QueryRowContext(ctx, `SELECT t.state,t.version,t.runner_epoch,d.leader_epoch FROM tickets t JOIN daemon_instances d ON d.channel=t.channel WHERE t.channel=? AND t.project_id=? AND t.id=?`, ref.Channel, ref.Project, ref.Ticket).Scan(&state, &version, &runner, &leader); err != nil {
		return CandidateRepairBuildContext{}, normalizeBusy(ctx, err)
	}
	if state != domain.StateBuilding || version != expected || runner != fence.RunnerEpoch || leader != fence.LeaderEpoch {
		return CandidateRepairBuildContext{}, ErrStaleFence
	}
	return s.candidateRepairBuildContextAt(ctx, s.db, ref, expected, fence)
}

// authenticateCandidateRepairBuildContextAt is the mutation-side proof used
// by runtime rearm. If a target snapshot already exists, its complete
// candidate/result/command/completion tuple must also exist; partial target
// evidence is never treated as a pre-target repair.
func (s *Store) authenticateCandidateRepairBuildContextAt(ctx context.Context, q ciQuery, ref domain.TicketRef, expected uint64, fence domain.Fence) error {
	// Runtime rearm authenticates the retained pre-stop endpoint after startup
	// may already have appended a later recovery row. Keep this source proof
	// historical; the caller separately audits the complete live ledger.
	authority, err := s.candidateRepairBuildAuthorityHistoricalAt(ctx, q, ref, expected, fence)
	if err != nil {
		return err
	}
	var latest uint64
	if err := q.QueryRowContext(ctx, `SELECT COALESCE(MAX(generation),0) FROM candidate_snapshots WHERE channel=? AND project_id=? AND ticket_id=?`, ref.Channel, ref.Project, ref.Ticket).Scan(&latest); err != nil {
		return normalizeBusy(ctx, err)
	}
	if latest == authority.context.PredecessorGeneration {
		var targetRows int
		if err := q.QueryRowContext(ctx, `SELECT COUNT(*) FROM candidate_repair_completions WHERE channel=? AND project_id=? AND ticket_id=? AND target_generation=?`, ref.Channel, ref.Project, ref.Ticket, authority.context.TargetGeneration).Scan(&targetRows); err != nil {
			return normalizeBusy(ctx, err)
		}
		if targetRows != 0 {
			return ErrEvidenceConflict
		}
		return nil
	}
	if latest != authority.context.TargetGeneration {
		return ErrEvidenceConflict
	}
	candidate, err := s.latestCandidateFrom(ctx, q, ref, false)
	if err != nil || candidate.Snapshot.Generation != authority.context.TargetGeneration || candidate.Commit.ParentOID != authority.context.PredecessorHeadSHA || candidate.Snapshot.VerificationIntentDigest != authority.context.Verification.Revision.IntentDigest || candidate.Snapshot.ProofDigest != authority.context.Verification.Revision.ProofDigest {
		return ErrEvidenceConflict
	}
	result, _, err := s.loadHistoricalProviderAttemptResult(ctx, q, candidate.BuilderResult)
	// Runtime rearm authenticates this immutable repair completion at the
	// pre-stop baseline. The caller separately proves the complete sealed
	// control and recovery suffix from that baseline to the live owner, so
	// later signed rows must not make the historical endpoint unverifiable.
	if err != nil || candidateRepairBuilderResultReachesHistoricalFence(ctx, q, candidate.BuilderResult, result, expected, fence) != nil {
		return ErrEvidenceConflict
	}
	if err := s.reauthenticateStoredCandidateCommandHistoricalFrom(ctx, q, ref, candidate); err != nil {
		return ErrEvidenceConflict
	}
	return nil
}

func (s *Store) candidateRepairBuildContextAt(ctx context.Context, q ciQuery, ref domain.TicketRef, expected uint64, fence domain.Fence) (CandidateRepairBuildContext, error) {
	authority, err := s.candidateRepairBuildAuthorityAt(ctx, q, ref, expected, fence)
	if err != nil {
		return CandidateRepairBuildContext{}, err
	}
	return authority.context, nil
}

// candidateRepairBuildAuthorityAt authenticates the immutable repair binding
// for the exact Build phase entry which owns the requested endpoint. A retained
// earlier CI repair must not shadow a later review-repair or provider-retry
// Build entry; those unrelated entries return ErrNotFound.
func (s *Store) candidateRepairBuildAuthorityAt(ctx context.Context, q ciQuery, ref domain.TicketRef, expected uint64, fence domain.Fence) (candidateRepairBindingAuthority, error) {
	return s.candidateRepairBuildAuthorityAtMode(ctx, q, ref, expected, fence, false)
}

// candidateRepairBuildAuthorityHistoricalAt authenticates an exact retained
// repair endpoint while permitting independently authenticated later recovery
// rows. Only startup/rearm/source-proof paths may use this mode; live readers
// must additionally run validateRunnerRecoveryAuthority.
func (s *Store) candidateRepairBuildAuthorityHistoricalAt(ctx context.Context, q ciQuery, ref domain.TicketRef, expected uint64, fence domain.Fence) (candidateRepairBindingAuthority, error) {
	return s.candidateRepairBuildAuthorityAtMode(ctx, q, ref, expected, fence, true)
}

func (s *Store) candidateRepairBuildAuthorityAtMode(ctx context.Context, q ciQuery, ref domain.TicketRef, expected uint64, fence domain.Fence, allowFutureRecovery bool) (candidateRepairBindingAuthority, error) {
	if ref.Validate() != nil || expected == 0 || fence.LeaderEpoch == 0 || fence.RunnerEpoch == 0 || fence.ClaimEpoch != 0 {
		return candidateRepairBindingAuthority{}, ErrEvidenceConflict
	}
	entry, entryErr := loadProviderPhaseEntryAt(ctx, q, ref, domain.PhaseBuild, expected)
	if entryErr != nil {
		return candidateRepairBindingAuthority{}, entryErr
	}
	var totalBindings, entryBindings int
	if err := q.QueryRowContext(ctx, `SELECT COUNT(*),COALESCE(SUM(CASE WHEN red_transition_ticket_version=? THEN 1 ELSE 0 END),0) FROM candidate_repair_bindings WHERE channel=? AND project_id=? AND ticket_id=?`, entry.Version, ref.Channel, ref.Project, ref.Ticket).Scan(&totalBindings, &entryBindings); err != nil {
		return candidateRepairBindingAuthority{}, normalizeBusy(ctx, err)
	}
	if totalBindings > 1 || entryBindings > 1 {
		return candidateRepairBindingAuthority{}, ErrEvidenceConflict
	}
	if entryBindings == 0 {
		if entry.From == domain.StateWaitingCI && entry.State == domain.StateBuilding && entry.Trigger == "checks_red" {
			return candidateRepairBindingAuthority{}, ErrEvidenceConflict
		}
		return candidateRepairBindingAuthority{}, ErrNotFound
	}
	if totalBindings != 1 || entry.From != domain.StateWaitingCI || entry.State != domain.StateBuilding || entry.Trigger != "checks_red" {
		return candidateRepairBindingAuthority{}, ErrEvidenceConflict
	}
	var value candidateRepairBindingAuthority
	value.context.Ref = ref
	err := q.QueryRowContext(ctx, `SELECT target_generation,predecessor_generation,predecessor_head_sha,predecessor_tree_sha,predecessor_publication_witness_digest,
		pr_host,pr_owner,pr_repo,pr_number,branch_ref,remote_head_oid,base_ref,remote_base_oid,red_observation_digest,red_observation_classification,
		red_transition_ticket_version,red_transition_digest,correction_budget_kind,correction_budget_request_id,consumed_ticket_version,consumed_leader_epoch,consumed_runner_epoch,consumed_recovery_prefix_digest,repair_context_digest,created_at
		FROM candidate_repair_bindings WHERE channel=? AND project_id=? AND ticket_id=? AND red_transition_ticket_version=?`, ref.Channel, ref.Project, ref.Ticket, entry.Version).Scan(
		&value.context.TargetGeneration, &value.context.PredecessorGeneration, &value.context.PredecessorHeadSHA, &value.context.PredecessorTreeSHA, &value.context.PublicationWitnessDigest,
		&value.PRHost, &value.PROwner, &value.PRRepo, &value.PRNumber, &value.BranchRef, &value.RemoteHead, &value.BaseRef, &value.RemoteBase,
		&value.ObservationDigest, &value.ObservationClass, &value.context.EntryTicketVersion, &value.TransitionDigest, &value.BudgetKind, &value.BudgetID,
		&value.ConsumedVersion, &value.ConsumedFence.LeaderEpoch, &value.ConsumedFence.RunnerEpoch, &value.RecoveryPrefixDigest, &value.ContextDigest, &value.CreatedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return candidateRepairBindingAuthority{}, ErrNotFound
	}
	if err != nil {
		return candidateRepairBindingAuthority{}, normalizeBusy(ctx, err)
	}
	if value.context.TargetGeneration == 0 || value.context.PredecessorGeneration == ^uint64(0) || value.context.TargetGeneration != value.context.PredecessorGeneration+1 || !validOID(value.context.PredecessorHeadSHA) || !validOID(value.context.PredecessorTreeSHA) || !validCIAuthorityDigest(value.context.PublicationWitnessDigest) || value.PRNumber <= 0 || !boundedText(value.PRHost, 300) || !boundedText(value.PROwner, 300) || !boundedText(value.PRRepo, 300) || !boundedText(value.BranchRef, 300) || !validOID(value.RemoteHead) || !boundedText(value.BaseRef, 300) || !validOID(value.RemoteBase) || !validCIAuthorityDigest(value.ObservationDigest) || value.ObservationClass != "red" || value.context.EntryTicketVersion == 0 || value.ConsumedVersion == 0 || value.context.EntryTicketVersion != value.ConsumedVersion+1 || value.ConsumedFence.LeaderEpoch == 0 || value.ConsumedFence.RunnerEpoch == 0 || !validCIAuthorityDigest(value.RecoveryPrefixDigest) || !validCIAuthorityDigest(value.TransitionDigest) || value.BudgetKind != "correction" || !boundedText(value.BudgetID, 300) || !validCIAuthorityDigest(value.ContextDigest) {
		return candidateRepairBindingAuthority{}, ErrEvidenceConflict
	}
	if _, err := time.Parse(time.RFC3339Nano, value.CreatedAt); err != nil {
		return candidateRepairBindingAuthority{}, ErrEvidenceConflict
	}
	err = entryErr
	if err == nil && (entry.Version != expected || entry.Runner != fence.RunnerEpoch || entry.Leader != fence.LeaderEpoch) {
		if allowFutureRecovery {
			err = validateRunnerRecoveryLedgerPrefix(ctx, q, ref, entry.Version, entry.Runner, entry.Leader, expected, fence.RunnerEpoch, fence.LeaderEpoch)
		} else {
			_, err = loadCurrentProviderPhaseEntry(ctx, q, ref, domain.PhaseBuild, expected, fence.RunnerEpoch, fence.LeaderEpoch)
		}
	}
	if err != nil || entry.Version != value.context.EntryTicketVersion || entry.Leader != value.ConsumedFence.LeaderEpoch || entry.Runner != value.ConsumedFence.RunnerEpoch || entry.From != domain.StateWaitingCI || entry.State != domain.StateBuilding || entry.Trigger != "checks_red" {
		return candidateRepairBindingAuthority{}, ErrEvidenceConflict
	}
	value.context.EntryFence = domain.Fence{LeaderEpoch: entry.Leader, RunnerEpoch: entry.Runner}
	var eventCount int
	if err := q.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE channel=? AND project_id=? AND ticket_id=? AND id=? AND ticket_version=? AND created_at=? AND trigger='checks_red' AND from_state='waiting_ci' AND to_state='building'`, ref.Channel, ref.Project, ref.Ticket, entry.EventID, entry.Version, entry.EventCreated).Scan(&eventCount); err != nil || eventCount != 1 {
		return candidateRepairBindingAuthority{}, ErrEvidenceConflict
	}
	prefixDigest, err := candidateRepairRecoveryPrefixDigest(ctx, q, ref, value.ConsumedVersion)
	if err != nil || prefixDigest != value.RecoveryPrefixDigest {
		return candidateRepairBindingAuthority{}, ErrEvidenceConflict
	}
	publication, found, err := loadPublicationEvidenceRowMatching(ctx, q, ref, value.context.PredecessorGeneration, value.context.PredecessorHeadSHA, value.context.PredecessorTreeSHA, value.context.PublicationWitnessDigest)
	if err == nil && found {
		err = loadLatestPublicationRebind(ctx, q, &publication)
	}
	if err != nil || publication.Candidate.Snapshot.Generation != value.context.PredecessorGeneration || publication.Candidate.Snapshot.HeadSHA != value.context.PredecessorHeadSHA || publication.Candidate.Snapshot.TreeSHA != value.context.PredecessorTreeSHA || publication.WitnessDigest != value.context.PublicationWitnessDigest || publication.PullRequest.Repository.Host != value.PRHost || publication.PullRequest.Repository.Owner != value.PROwner || publication.PullRequest.Repository.Name != value.PRRepo || int64(publication.PullRequest.Number) != value.PRNumber || publication.RemoteBranchRef != value.BranchRef || publication.RemoteBranchOID != value.RemoteHead || publication.PullRequest.BaseRef != value.BaseRef || publication.RemoteBaseOID != value.RemoteBase {
		return candidateRepairBindingAuthority{}, ErrEvidenceConflict
	}
	if err := s.reauthenticateStoredCandidateCheckpointFrom(ctx, q, ref, publication.Candidate); err != nil {
		return candidateRepairBindingAuthority{}, ErrEvidenceConflict
	}
	observation, observationFound, err := scanCIObservation(ctx, q, false, ref, value.ObservationDigest)
	if err != nil || !observationFound || observation.Classification != "red" || observation.ObservedTicketVersion != value.ConsumedVersion || observation.ObservedFence != value.ConsumedFence || !ciObservationMatchesPublication(observation, publication) {
		return candidateRepairBindingAuthority{}, ErrEvidenceConflict
	}
	policy, err := scanCurrentCIPolicy(ctx, q, ref, publication)
	if err != nil || !policyMatchesObservation(policy, observation) {
		return candidateRepairBindingAuthority{}, ErrEvidenceConflict
	}
	diagnostic, err := candidateRepairDiagnosticForObservation(observation)
	if err != nil {
		return candidateRepairBindingAuthority{}, ErrEvidenceConflict
	}
	var transitionVersion uint64
	var transitionEventID int64
	var transitionCreated, priorState, resultingState, trigger, eventPayload, storedDigest string
	err = q.QueryRowContext(ctx, `SELECT c.ticket_version,c.event_id,c.event_created_at,c.prior_state,c.resulting_state,c.resulting_trigger,c.transition_digest,e.payload
		FROM ci_transition_evidence c JOIN events e ON e.channel=c.channel AND e.project_id=c.project_id AND e.ticket_id=c.ticket_id AND e.id=c.event_id AND e.ticket_version=c.ticket_version AND e.created_at=c.event_created_at
		WHERE c.channel=? AND c.project_id=? AND c.ticket_id=? AND c.candidate_generation=? AND c.candidate_head_sha=? AND c.candidate_tree_sha=? AND c.observation_classification='red' AND c.observation_digest=? AND c.prior_publication_witness_digest=?`,
		ref.Channel, ref.Project, ref.Ticket, value.context.PredecessorGeneration, value.context.PredecessorHeadSHA, value.context.PredecessorTreeSHA, value.ObservationDigest, value.context.PublicationWitnessDigest).Scan(
		&transitionVersion, &transitionEventID, &transitionCreated, &priorState, &resultingState, &trigger, &storedDigest, &eventPayload)
	if err != nil || transitionVersion != value.context.EntryTicketVersion || priorState != string(domain.StateWaitingCI) || resultingState != string(domain.StateBuilding) || trigger != "checks_red" || storedDigest != value.TransitionDigest || storedDigest != ciTransitionDigest(ref, observation, transitionVersion, transitionEventID, transitionCreated, resultingState, trigger, eventPayload) {
		return candidateRepairBindingAuthority{}, ErrEvidenceConflict
	}
	if err := validateCorrectionBudgetLedger(ctx, q, ref); err != nil {
		return candidateRepairBindingAuthority{}, ErrEvidenceConflict
	}
	var budgetRows int
	if err := q.QueryRowContext(ctx, `SELECT COUNT(*) FROM ticket_budget_uses WHERE channel=? AND project_id=? AND ticket_id=? AND kind='correction' AND request_id=? AND ticket_version=? AND leader_epoch=? AND runner_epoch=?`, ref.Channel, ref.Project, ref.Ticket, value.BudgetID, value.ConsumedVersion, value.ConsumedFence.LeaderEpoch, value.ConsumedFence.RunnerEpoch).Scan(&budgetRows); err != nil || budgetRows != 1 {
		return candidateRepairBindingAuthority{}, ErrEvidenceConflict
	}
	ciEndpoints, err := authenticateCandidateRepairCIHistory(ctx, q, ref, publication, value.ConsumedVersion, value.ConsumedFence, value.BudgetID)
	if err != nil {
		return candidateRepairBindingAuthority{}, ErrEvidenceConflict
	}
	value.publicationVersion = publication.CurrentTicketVersion
	value.publicationFence = publication.CurrentFence
	value.ciEndpoints = ciEndpoints
	budget := CorrectionBudgetAuthority{Ref: ref, RequestID: value.BudgetID, TicketVersion: value.ConsumedVersion, Fence: value.ConsumedFence}
	if value.ContextDigest != candidateRepairContextDigest(ref, observation, publication, budget, value.context.TargetGeneration, value.TransitionDigest, value.RecoveryPrefixDigest) {
		return candidateRepairBindingAuthority{}, ErrEvidenceConflict
	}
	verification, err := s.verificationEvidenceForIdentityFrom(ctx, q, ref,
		publication.Candidate.Snapshot.VerificationIntentDigest,
		publication.Candidate.Snapshot.ProofDigest,
		publication.Candidate.Commit.ParentOID)
	if err != nil || publication.Candidate.Snapshot.VerificationIntentDigest != verification.Revision.IntentDigest || publication.Candidate.Snapshot.ProofDigest != verification.Revision.ProofDigest || publication.Candidate.Commit.ParentOID != verification.Checkpoint.CommitOID {
		return candidateRepairBindingAuthority{}, ErrEvidenceConflict
	}
	value.context.Verification = verification
	value.context.Diagnostic = diagnostic
	if !allowFutureRecovery {
		// Every live consumer of the repair authority must audit the entire
		// ticket ledger, not merely the suffix from the checks_red entry. The
		// historical mode above is used by that audit itself, preventing
		// recursion while preserving later legitimate recovery rows.
		if err := validateRunnerRecoveryAuthority(ctx, q, ref, expected, fence); err != nil {
			return candidateRepairBindingAuthority{}, ErrStaleFence
		}
	}
	return value, nil
}

func candidateRepairCompletionDigest(value CandidateRepairCompletion) string {
	body, _ := json.Marshal(struct {
		Ref                            domain.TicketRef
		TargetGeneration               uint64
		AttemptID                      int64
		Attempt                        int
		BindingVersion, Leader, Runner uint64
		Head, Tree                     string
		CompletedAt                    string
	}{value.Ref, value.TargetGeneration, value.BuilderResultAttemptID, value.BuilderResultAttempt, value.BuilderBindingTicketVersion, value.BuilderBindingFence.LeaderEpoch, value.BuilderBindingFence.RunnerEpoch, value.FinalCandidateHeadSHA, value.FinalCandidateTreeSHA, value.CompletedAt.UTC().Format(time.RFC3339Nano)})
	return ciAuthorityDigest(body)
}

func ensureCandidateRepairCompletionAt(ctx context.Context, conn *sql.Conn, evidence CandidateEvidence, generation uint64, builder ProviderAttemptResult, authority candidateRepairBindingAuthority) error {
	if generation != authority.context.TargetGeneration || evidence.BuilderResult.AttemptID != builder.Claim.ID || evidence.BuilderResult.Attempt != builder.Claim.Attempt || builder.Claim.Ref != evidence.Ref || builder.Claim.Phase != domain.PhaseBuild || builder.Claim.Role != "builder" || builder.Claim.ExpectedVersion < authority.context.EntryTicketVersion || builder.Claim.LeaderEpoch == 0 || builder.Claim.RunnerEpoch == 0 || evidence.Snapshot.HeadSHA != evidence.Commit.CommitOID || evidence.Snapshot.TreeSHA != evidence.Commit.TreeOID || evidence.Commit.ParentOID != authority.context.PredecessorHeadSHA {
		return ErrEvidenceConflict
	}
	sourceFence := domain.Fence{LeaderEpoch: builder.Claim.LeaderEpoch, RunnerEpoch: builder.Claim.RunnerEpoch}
	if err := ensureCandidateResultBindingRow(ctx, conn, evidence, generation, builder.Claim.ExpectedVersion, sourceFence); err != nil {
		return err
	}
	var phase, role, state, outcome string
	if err := conn.QueryRowContext(ctx, `SELECT a.phase,a.role,a.state,a.outcome FROM provider_attempts a JOIN provider_attempt_results r ON r.provider_attempt_id=a.id WHERE a.id=? AND a.attempt=?`, builder.Claim.ID, builder.Claim.Attempt).Scan(&phase, &role, &state, &outcome); err != nil || phase != "build" || role != "builder" || state != "completed" || outcome != "completed" {
		return ErrEvidenceConflict
	}
	var stored CandidateRepairCompletion
	var completedAt string
	err := conn.QueryRowContext(ctx, `SELECT builder_result_attempt_id,builder_result_attempt,builder_binding_ticket_version,builder_binding_leader_epoch,builder_binding_runner_epoch,final_candidate_head_sha,final_candidate_tree_sha,completion_digest,completed_at FROM candidate_repair_completions WHERE channel=? AND project_id=? AND ticket_id=? AND target_generation=?`, evidence.Ref.Channel, evidence.Ref.Project, evidence.Ref.Ticket, generation).Scan(&stored.BuilderResultAttemptID, &stored.BuilderResultAttempt, &stored.BuilderBindingTicketVersion, &stored.BuilderBindingFence.LeaderEpoch, &stored.BuilderBindingFence.RunnerEpoch, &stored.FinalCandidateHeadSHA, &stored.FinalCandidateTreeSHA, &stored.CompletionDigest, &completedAt)
	if err == nil {
		stored.Ref, stored.TargetGeneration = evidence.Ref, generation
		stored.CompletedAt, err = time.Parse(time.RFC3339Nano, completedAt)
		if err != nil || stored.BuilderResultAttemptID != builder.Claim.ID || stored.BuilderResultAttempt != builder.Claim.Attempt || stored.BuilderBindingTicketVersion != builder.Claim.ExpectedVersion || stored.BuilderBindingFence != sourceFence || stored.FinalCandidateHeadSHA != evidence.Snapshot.HeadSHA || stored.FinalCandidateTreeSHA != evidence.Snapshot.TreeSHA || stored.CompletionDigest != candidateRepairCompletionDigest(stored) {
			return ErrEvidenceConflict
		}
		return nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	completed := time.Now().UTC()
	input := CandidateRepairCompletion{
		Ref: evidence.Ref, TargetGeneration: generation,
		BuilderResultAttemptID: builder.Claim.ID, BuilderResultAttempt: builder.Claim.Attempt,
		BuilderBindingTicketVersion: builder.Claim.ExpectedVersion, BuilderBindingFence: sourceFence,
		FinalCandidateHeadSHA: evidence.Snapshot.HeadSHA, FinalCandidateTreeSHA: evidence.Snapshot.TreeSHA,
		CompletedAt: completed,
	}
	input.CompletionDigest = candidateRepairCompletionDigest(input)
	_, err = conn.ExecContext(ctx, `INSERT INTO candidate_repair_completions(channel,project_id,ticket_id,target_generation,builder_result_attempt_id,builder_result_attempt,builder_result_phase,builder_result_role,builder_binding_ticket_version,builder_binding_leader_epoch,builder_binding_runner_epoch,final_candidate_head_sha,final_candidate_tree_sha,completion_digest,completed_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, evidence.Ref.Channel, evidence.Ref.Project, evidence.Ref.Ticket, generation, builder.Claim.ID, builder.Claim.Attempt, "build", "builder", builder.Claim.ExpectedVersion, builder.Claim.LeaderEpoch, builder.Claim.RunnerEpoch, evidence.Snapshot.HeadSHA, evidence.Snapshot.TreeSHA, input.CompletionDigest, completed.Format(time.RFC3339Nano))
	return err
}

// candidateRepairBuilderEntryResultReachesFence authenticates a completed
// Builder result from the Store-owned red-CI repair phase entry before a
// successor candidate/completion exists. The phase-entry binding prevents the
// predecessor generation's Builder from being mistaken for fresh repair work.
func candidateRepairBuilderEntryResultReachesFence(ctx context.Context, q candidateEvidenceQuerier, key ProviderAttemptResultKey, result ProviderAttemptResult, expected uint64, fence domain.Fence) error {
	if key.Ref.Validate() != nil || key.AttemptID <= 0 || key.Attempt <= 0 || key.Phase != domain.PhaseBuild || result.Claim.ID != key.AttemptID || result.Claim.Ref != key.Ref || result.Claim.Phase != domain.PhaseBuild || result.Claim.Role != "builder" || result.Claim.Attempt != key.Attempt || expected == 0 || fence.LeaderEpoch == 0 || fence.RunnerEpoch == 0 || fence.ClaimEpoch != 0 {
		return ErrEvidenceConflict
	}
	authority, err := (&Store{}).candidateRepairBuildAuthorityAt(ctx, q, key.Ref, expected, fence)
	if err != nil {
		return err
	}
	if result.Claim.ExpectedVersion < authority.context.EntryTicketVersion {
		return ErrNotFound
	}
	entry, err := loadCurrentProviderPhaseEntry(ctx, q, key.Ref, domain.PhaseBuild, result.Claim.ExpectedVersion, result.Claim.RunnerEpoch, result.Claim.LeaderEpoch)
	if err != nil || entry.Version != authority.context.EntryTicketVersion || entry.Leader != authority.context.EntryFence.LeaderEpoch || entry.Runner != authority.context.EntryFence.RunnerEpoch || entry.From != domain.StateWaitingCI || entry.State != domain.StateBuilding || entry.Trigger != "checks_red" || entry.Digest == "" {
		return ErrEvidenceConflict
	}
	var bindings int
	if err := q.QueryRowContext(ctx, `SELECT COUNT(*) FROM provider_phase_attempt_entries WHERE provider_attempt_id=? AND channel=? AND project_id=? AND ticket_id=? AND phase='build' AND role='builder' AND attempt=? AND entry_ticket_version=?`, key.AttemptID, key.Ref.Channel, key.Ref.Project, key.Ref.Ticket, key.Attempt, authority.context.EntryTicketVersion).Scan(&bindings); err != nil || bindings != 1 {
		return ErrEvidenceConflict
	}
	return nil
}

// completedCandidateRepairContextAt authenticates a repaired candidate after
// its Building lifecycle may already have advanced. It starts from the exact
// immutable checks_red entry stored in the repair binding and then requires
// the candidate/result completion to reach the candidate's recorded fence.
func completedCandidateRepairContextAt(ctx context.Context, q candidateEvidenceQuerier, stored StoredCandidate, builder ProviderAttemptResult) (CandidateRepairBuildContext, error) {
	if stored.Snapshot.Generation == 0 || stored.BuilderResult.AttemptID <= 0 || stored.BuilderResult.Ref.Validate() != nil || stored.BuilderResult.Ref != builder.Claim.Ref || stored.BuilderResult.AttemptID != builder.Claim.ID || stored.BuilderResult.Attempt != builder.Claim.Attempt {
		return CandidateRepairBuildContext{}, ErrEvidenceConflict
	}
	var entryVersion, consumedLeader, consumedRunner uint64
	err := q.QueryRowContext(ctx, `SELECT red_transition_ticket_version,consumed_leader_epoch,consumed_runner_epoch FROM candidate_repair_bindings WHERE channel=? AND project_id=? AND ticket_id=? AND target_generation=?`, stored.BuilderResult.Ref.Channel, stored.BuilderResult.Ref.Project, stored.BuilderResult.Ref.Ticket, stored.Snapshot.Generation).Scan(&entryVersion, &consumedLeader, &consumedRunner)
	if errors.Is(err, sql.ErrNoRows) {
		return CandidateRepairBuildContext{}, ErrNotFound
	}
	if err != nil || entryVersion == 0 || consumedLeader == 0 || consumedRunner == 0 {
		return CandidateRepairBuildContext{}, ErrEvidenceConflict
	}
	authority, err := (&Store{}).candidateRepairBuildAuthorityHistoricalAt(ctx, q, stored.BuilderResult.Ref, entryVersion, domain.Fence{LeaderEpoch: consumedLeader, RunnerEpoch: consumedRunner})
	if err != nil || authority.context.TargetGeneration != stored.Snapshot.Generation || stored.Commit.ParentOID != authority.context.PredecessorHeadSHA || stored.Snapshot.VerificationIntentDigest != authority.context.Verification.Revision.IntentDigest || stored.Snapshot.ProofDigest != authority.context.Verification.Revision.ProofDigest {
		return CandidateRepairBuildContext{}, ErrEvidenceConflict
	}
	if candidateRepairBuilderResultReachesHistoricalFence(ctx, q, stored.BuilderResult, builder, stored.TicketVersion, stored.Fence) != nil {
		return CandidateRepairBuildContext{}, ErrEvidenceConflict
	}
	return authority.context, nil
}

// candidateRepairBuilderResultReachesFence authenticates the one correction
// path whose Builder result cannot be replayed through the generic ticket-wide
// provider authority. A red-CI repair deliberately retains the prior
// publication generation, so its immutable completion is the narrow source
// anchor for the successor Builder result and the signed recovery/control
// suffix to the current Building owner.
func candidateRepairBuilderResultReachesFence(ctx context.Context, q candidateEvidenceQuerier, key ProviderAttemptResultKey, result ProviderAttemptResult, expected uint64, fence domain.Fence) error {
	return candidateRepairBuilderResultReachesFenceAtMode(ctx, q, key, result, expected, fence, false)
}

// candidateRepairBuilderResultReachesHistoricalFence authenticates an
// immutable repair completion at its recorded endpoint while permitting a
// later, independently signed recovery suffix. It is intentionally narrower
// than the live helper above: callers must separately authenticate the suffix
// from this stored endpoint to their current owner.
func candidateRepairBuilderResultReachesHistoricalFence(ctx context.Context, q candidateEvidenceQuerier, key ProviderAttemptResultKey, result ProviderAttemptResult, expected uint64, fence domain.Fence) error {
	return candidateRepairBuilderResultReachesFenceAtMode(ctx, q, key, result, expected, fence, true)
}

func candidateRepairBuilderResultReachesFenceAtMode(ctx context.Context, q candidateEvidenceQuerier, key ProviderAttemptResultKey, result ProviderAttemptResult, expected uint64, fence domain.Fence, allowFutureRecovery bool) error {
	if key.Ref.Validate() != nil || key.AttemptID <= 0 || key.Attempt <= 0 || key.Phase != domain.PhaseBuild || result.Claim.ID != key.AttemptID || result.Claim.Ref != key.Ref || result.Claim.Phase != domain.PhaseBuild || result.Claim.Role != "builder" || result.Claim.Attempt != key.Attempt || expected == 0 || fence.LeaderEpoch == 0 || fence.RunnerEpoch == 0 || fence.ClaimEpoch != 0 {
		return ErrEvidenceConflict
	}
	var count int
	if err := q.QueryRowContext(ctx, `SELECT COUNT(*) FROM candidate_repair_completions WHERE channel=? AND project_id=? AND ticket_id=? AND builder_result_attempt_id=? AND builder_result_attempt=?`, key.Ref.Channel, key.Ref.Project, key.Ref.Ticket, key.AttemptID, key.Attempt).Scan(&count); err != nil {
		return err
	}
	if count == 0 {
		return ErrNotFound
	}
	if count != 1 {
		return ErrEvidenceConflict
	}
	var completion CandidateRepairCompletion
	var completedAt string
	if err := q.QueryRowContext(ctx, `SELECT target_generation,builder_binding_ticket_version,builder_binding_leader_epoch,builder_binding_runner_epoch,final_candidate_head_sha,final_candidate_tree_sha,completion_digest,completed_at
		FROM candidate_repair_completions WHERE channel=? AND project_id=? AND ticket_id=? AND builder_result_attempt_id=? AND builder_result_attempt=?`, key.Ref.Channel, key.Ref.Project, key.Ref.Ticket, key.AttemptID, key.Attempt).Scan(&completion.TargetGeneration, &completion.BuilderBindingTicketVersion, &completion.BuilderBindingFence.LeaderEpoch, &completion.BuilderBindingFence.RunnerEpoch, &completion.FinalCandidateHeadSHA, &completion.FinalCandidateTreeSHA, &completion.CompletionDigest, &completedAt); err != nil {
		return ErrEvidenceConflict
	}
	completion.Ref, completion.BuilderResultAttemptID, completion.BuilderResultAttempt = key.Ref, key.AttemptID, key.Attempt
	var err error
	completion.CompletedAt, err = time.Parse(time.RFC3339Nano, completedAt)
	if err != nil || completion.TargetGeneration == 0 || completion.BuilderBindingTicketVersion == 0 || completion.BuilderBindingFence.LeaderEpoch == 0 || completion.BuilderBindingFence.RunnerEpoch == 0 || !validOID(completion.FinalCandidateHeadSHA) || !validOID(completion.FinalCandidateTreeSHA) || completion.CompletionDigest != candidateRepairCompletionDigest(completion) {
		return ErrEvidenceConflict
	}
	if result.Claim.ExpectedVersion != completion.BuilderBindingTicketVersion || result.Claim.LeaderEpoch != completion.BuilderBindingFence.LeaderEpoch || result.Claim.RunnerEpoch != completion.BuilderBindingFence.RunnerEpoch {
		return ErrEvidenceConflict
	}
	var predecessor uint64
	var snapshotHead, snapshotTree string
	if err := q.QueryRowContext(ctx, `SELECT r.predecessor_generation,c.head_sha,c.tree_sha
		FROM candidate_repair_bindings r JOIN candidate_snapshots c ON c.channel=r.channel AND c.project_id=r.project_id AND c.ticket_id=r.ticket_id AND c.generation=r.target_generation
		WHERE r.channel=? AND r.project_id=? AND r.ticket_id=? AND r.target_generation=?`, key.Ref.Channel, key.Ref.Project, key.Ref.Ticket, completion.TargetGeneration).Scan(&predecessor, &snapshotHead, &snapshotTree); err != nil || predecessor == ^uint64(0) || predecessor+1 != completion.TargetGeneration || snapshotHead != completion.FinalCandidateHeadSHA || snapshotTree != completion.FinalCandidateTreeSHA {
		return ErrEvidenceConflict
	}
	var boundAttemptID int64
	var boundAttempt int
	if err := q.QueryRowContext(ctx, `SELECT provider_attempt_id,provider_attempt FROM candidate_result_bindings WHERE channel=? AND project_id=? AND ticket_id=? AND generation=? AND binding_ticket_version=? AND leader_epoch=? AND runner_epoch=?`, key.Ref.Channel, key.Ref.Project, key.Ref.Ticket, completion.TargetGeneration, completion.BuilderBindingTicketVersion, completion.BuilderBindingFence.LeaderEpoch, completion.BuilderBindingFence.RunnerEpoch).Scan(&boundAttemptID, &boundAttempt); err != nil || boundAttemptID != key.AttemptID || boundAttempt != key.Attempt {
		return ErrEvidenceConflict
	}
	if !allowFutureRecovery {
		if err := validateRunnerRecoveryAuthority(ctx, q, key.Ref, expected, fence); err != nil {
			return ErrStaleFence
		}
	}
	if expected == completion.BuilderBindingTicketVersion && fence == completion.BuilderBindingFence {
		return nil
	}
	var recoveryErr error
	if allowFutureRecovery {
		recoveryErr = validateRunnerRecoveryLedgerPrefix(ctx, q, key.Ref, completion.BuilderBindingTicketVersion, completion.BuilderBindingFence.RunnerEpoch, completion.BuilderBindingFence.LeaderEpoch, expected, fence.RunnerEpoch, fence.LeaderEpoch)
	} else {
		recoveryErr = validateRunnerRecoveryLedger(ctx, q, key.Ref, completion.BuilderBindingTicketVersion, completion.BuilderBindingFence.RunnerEpoch, completion.BuilderBindingFence.LeaderEpoch, expected, fence.RunnerEpoch, fence.LeaderEpoch)
	}
	if recoveryErr != nil {
		return ErrStaleFence
	}
	return nil
}

// RecordCandidateRepairCompletion authenticates the successor Builder result
// and candidate identity, then appends the completion required before that
// candidate can be published or reviewed.
func (s *Store) RecordCandidateRepairCompletion(ctx context.Context, input CandidateRepairCompletion) error {
	if input.Ref.Validate() != nil || input.TargetGeneration == 0 || input.BuilderResultAttemptID <= 0 || input.BuilderResultAttempt <= 0 || input.BuilderBindingTicketVersion == 0 || input.BuilderBindingFence.LeaderEpoch == 0 || input.BuilderBindingFence.RunnerEpoch == 0 || input.BuilderBindingFence.ClaimEpoch != 0 || !validOID(input.FinalCandidateHeadSHA) || !validOID(input.FinalCandidateTreeSHA) {
		return ErrEvidenceConflict
	}
	if input.CompletedAt.IsZero() {
		input.CompletedAt = time.Now().UTC()
	}
	if _, ok := canonicalPublicationTime(input.CompletedAt); !ok {
		return ErrEvidenceConflict
	}
	if delta := time.Since(input.CompletedAt); delta > maxCIClockSkew || delta < -maxCIClockSkew {
		return ErrEvidenceConflict
	}
	claimedDigest := input.CompletionDigest
	input.CompletionDigest = candidateRepairCompletionDigest(input)
	if claimedDigest != "" && claimedDigest != input.CompletionDigest {
		return ErrEvidenceConflict
	}
	return s.ciWrite(ctx, input.Ref, func(conn *sql.Conn) error {
		if err := s.assertTicketFence(ctx, conn, input.Ref, input.BuilderBindingTicketVersion, input.BuilderBindingFence); err != nil {
			// Builder completion may be durable before a daemon crash. The
			// successor binding remains immutable at its original builder fence,
			// but a signed runner-recovery ledger may prove the exact current
			// building owner derived from it. Do not accept a leader-only or
			// caller-reconstructed rebind.
			var state string
			var version, runner, leader uint64
			if scanErr := conn.QueryRowContext(ctx, `SELECT t.state,t.version,t.runner_epoch,d.leader_epoch FROM tickets t JOIN daemon_instances d ON d.channel=t.channel WHERE t.channel=? AND t.project_id=? AND t.id=?`, input.Ref.Channel, input.Ref.Project, input.Ref.Ticket).Scan(&state, &version, &runner, &leader); scanErr != nil || state != string(domain.StateBuilding) || version <= input.BuilderBindingTicketVersion || runner <= input.BuilderBindingFence.RunnerEpoch || validateRunnerRecoveryLedger(ctx, conn, input.Ref, input.BuilderBindingTicketVersion, input.BuilderBindingFence.RunnerEpoch, input.BuilderBindingFence.LeaderEpoch, version, runner, leader) != nil {
				return err
			}
		}
		var predecessor uint64
		if err := conn.QueryRowContext(ctx, `SELECT predecessor_generation FROM candidate_repair_bindings WHERE channel=? AND project_id=? AND ticket_id=? AND target_generation=?`, input.Ref.Channel, input.Ref.Project, input.Ref.Ticket, input.TargetGeneration).Scan(&predecessor); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return ErrEvidenceConflict
			}
			return err
		}
		var head, tree string
		if err := conn.QueryRowContext(ctx, `SELECT head_sha,tree_sha FROM candidate_snapshots WHERE channel=? AND project_id=? AND ticket_id=? AND generation=?`, input.Ref.Channel, input.Ref.Project, input.Ref.Ticket, input.TargetGeneration).Scan(&head, &tree); err != nil || head != input.FinalCandidateHeadSHA || tree != input.FinalCandidateTreeSHA || predecessor+1 != input.TargetGeneration {
			return ErrEvidenceConflict
		}
		var bindingAttemptID int64
		var bindingAttempt int
		if err := conn.QueryRowContext(ctx, `SELECT provider_attempt_id,provider_attempt FROM candidate_result_bindings WHERE channel=? AND project_id=? AND ticket_id=? AND generation=? AND binding_ticket_version=? AND leader_epoch=? AND runner_epoch=?`, input.Ref.Channel, input.Ref.Project, input.Ref.Ticket, input.TargetGeneration, input.BuilderBindingTicketVersion, input.BuilderBindingFence.LeaderEpoch, input.BuilderBindingFence.RunnerEpoch).Scan(&bindingAttemptID, &bindingAttempt); err != nil || bindingAttemptID != input.BuilderResultAttemptID || bindingAttempt != input.BuilderResultAttempt {
			return ErrEvidenceConflict
		}
		var phase, role, state, outcome string
		if err := conn.QueryRowContext(ctx, `SELECT a.phase,a.role,a.state,a.outcome FROM provider_attempts a JOIN provider_attempt_results r ON r.provider_attempt_id=a.id WHERE a.id=? AND a.attempt=?`, input.BuilderResultAttemptID, input.BuilderResultAttempt).Scan(&phase, &role, &state, &outcome); err != nil || phase != "build" || role != "builder" || state != "completed" || outcome != "completed" {
			return ErrEvidenceConflict
		}
		result, err := conn.ExecContext(ctx, `INSERT INTO candidate_repair_completions(channel,project_id,ticket_id,target_generation,builder_result_attempt_id,builder_result_attempt,builder_result_phase,builder_result_role,builder_binding_ticket_version,builder_binding_leader_epoch,builder_binding_runner_epoch,final_candidate_head_sha,final_candidate_tree_sha,completion_digest,completed_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(channel,project_id,ticket_id,target_generation) DO NOTHING`, input.Ref.Channel, input.Ref.Project, input.Ref.Ticket, input.TargetGeneration, input.BuilderResultAttemptID, input.BuilderResultAttempt, "build", "builder", input.BuilderBindingTicketVersion, input.BuilderBindingFence.LeaderEpoch, input.BuilderBindingFence.RunnerEpoch, input.FinalCandidateHeadSHA, input.FinalCandidateTreeSHA, input.CompletionDigest, input.CompletedAt.Format(time.RFC3339Nano))
		if err != nil {
			return err
		}
		if count, _ := result.RowsAffected(); count > 1 {
			return ErrEvidenceConflict
		}
		var storedDigest string
		if err := conn.QueryRowContext(ctx, `SELECT completion_digest FROM candidate_repair_completions WHERE channel=? AND project_id=? AND ticket_id=? AND target_generation=?`, input.Ref.Channel, input.Ref.Project, input.Ref.Ticket, input.TargetGeneration).Scan(&storedDigest); err != nil || storedDigest != input.CompletionDigest {
			return ErrEvidenceConflict
		}
		return nil
	})
}

func (s *Store) RecordRepairCompletion(ctx context.Context, input CandidateRepairCompletion) error {
	return s.RecordCandidateRepairCompletion(ctx, input)
}

// LoadCurrentCIObservation authenticates only the newest durable observation.
// If that newest row is malformed it returns an error and never falls back to
// an older row.
func (s *Store) LoadCurrentCIObservation(ctx context.Context, ref domain.TicketRef) (CIObservation, error) {
	if err := ref.Validate(); err != nil {
		return CIObservation{}, err
	}
	return s.authenticateCurrentCIObservation(ctx, s.db, ref, "", true)
}

func (s *Store) LoadLatestCIObservation(ctx context.Context, ref domain.TicketRef) (CIObservation, error) {
	return s.LoadCurrentCIObservation(ctx, ref)
}

// LoadCIObservation is retained as the concise Store boundary name.
func (s *Store) LoadCIObservation(ctx context.Context, ref domain.TicketRef) (CIObservation, error) {
	return s.LoadCurrentCIObservation(ctx, ref)
}

// validateCorrectionBudgetLedger rejects any correction use that is not joined
// to its exact red transition and repair binding. A legacy/crash orphan fails
// closed rather than silently authorizing a repair.
func validateCorrectionBudgetLedger(ctx context.Context, q ciQuery, ref domain.TicketRef) error {
	var orphanCount int
	if err := q.QueryRowContext(ctx, `SELECT COUNT(*) FROM ticket_budget_uses u WHERE u.channel=? AND u.project_id=? AND u.ticket_id=? AND u.kind='correction' AND substr(u.request_id,1,7)='ci-red/' AND NOT EXISTS (
		SELECT 1 FROM candidate_repair_bindings b
		JOIN ci_transition_evidence c ON c.channel=b.channel AND c.project_id=b.project_id AND c.ticket_id=b.ticket_id AND c.candidate_generation=b.predecessor_generation AND c.candidate_head_sha=b.predecessor_head_sha AND c.candidate_tree_sha=b.predecessor_tree_sha AND c.observation_classification=b.red_observation_classification AND c.observation_digest=b.red_observation_digest AND c.prior_publication_witness_digest=b.predecessor_publication_witness_digest AND c.observation_ticket_version=b.consumed_ticket_version AND c.observation_leader_epoch=b.consumed_leader_epoch AND c.observation_runner_epoch=b.consumed_runner_epoch AND c.ticket_version=b.red_transition_ticket_version AND c.transition_digest=b.red_transition_digest
		WHERE b.channel=u.channel AND b.project_id=u.project_id AND b.ticket_id=u.ticket_id AND b.correction_budget_kind=u.kind AND b.correction_budget_request_id=u.request_id AND b.consumed_ticket_version=u.ticket_version AND b.consumed_leader_epoch=u.leader_epoch AND b.consumed_runner_epoch=u.runner_epoch
		AND EXISTS (SELECT 1 FROM events e WHERE e.channel=u.channel AND e.project_id=u.project_id AND e.ticket_id=u.ticket_id AND e.ticket_version=u.ticket_version AND e.trigger='budget_correction' AND e.from_state='waiting_ci' AND e.to_state='waiting_ci' AND json_extract(e.payload,'$.request_id')=u.request_id)
	)`, ref.Channel, ref.Project, ref.Ticket).Scan(&orphanCount); err != nil {
		return normalizeBusy(ctx, err)
	}
	if orphanCount != 0 {
		return ErrCIObservation
	}
	return nil
}

func ciRedCorrectionRequestID(observationDigest string) (string, bool) {
	if !validCIAuthorityDigest(observationDigest) {
		return "", false
	}
	requestID := "ci-red/" + strings.TrimPrefix(observationDigest, "sha256:")
	return requestID, boundedText(requestID, 300)
}

// consumeCorrectionBudget allocates one correction budget use inside the
// caller's CI transaction.
func consumeCorrectionBudget(ctx context.Context, conn *sql.Conn, authority CorrectionBudgetAuthority, ref domain.TicketRef, version uint64, fence domain.Fence, observationDigest string) (bool, error) {
	if err := validateCorrectionBudgetLedger(ctx, conn, ref); err != nil {
		return false, err
	}
	expectedRequestID, validRequest := ciRedCorrectionRequestID(observationDigest)
	if !validRequest || authority.Ref != ref || authority.TicketVersion != version || authority.Fence != fence || authority.Fence.ClaimEpoch != 0 || authority.RequestID != expectedRequestID {
		return false, nil
	}
	var used, limit int
	err := conn.QueryRowContext(ctx, `SELECT used,limit_count FROM ticket_counters WHERE channel=? AND project_id=? AND ticket_id=? AND kind='correction'`, ref.Channel, ref.Project, ref.Ticket).Scan(&used, &limit)
	if errors.Is(err, sql.ErrNoRows) {
		if _, err := conn.ExecContext(ctx, `INSERT INTO ticket_counters(channel,project_id,ticket_id,kind,used,limit_count) VALUES(?,?,?,?,0,2)`, ref.Channel, ref.Project, ref.Ticket, "correction"); err != nil {
			return false, err
		}
		used, limit = 0, 2
	} else if err != nil {
		return false, normalizeBusy(ctx, err)
	}
	if limit != 2 || used < 0 || used > limit {
		return false, ErrCIObservation
	}
	var useCount int
	if err := conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM ticket_budget_uses WHERE channel=? AND project_id=? AND ticket_id=? AND kind='correction'`, ref.Channel, ref.Project, ref.Ticket).Scan(&useCount); err != nil {
		return false, normalizeBusy(ctx, err)
	}
	if useCount != used {
		return false, ErrCIObservation
	}
	var existingVersion, existingLeader, existingRunner uint64
	err = conn.QueryRowContext(ctx, `SELECT ticket_version,leader_epoch,runner_epoch FROM ticket_budget_uses WHERE channel=? AND project_id=? AND ticket_id=? AND kind='correction' AND request_id=?`, ref.Channel, ref.Project, ref.Ticket, authority.RequestID).Scan(&existingVersion, &existingLeader, &existingRunner)
	if err == nil {
		// A durable transition with this request is replayed above. Reaching
		// this branch means the request is an orphan or a mismatched attempt;
		// never reuse a budget row without its exact red transition/binding.
		if existingVersion != version || existingLeader != fence.LeaderEpoch || existingRunner != fence.RunnerEpoch {
			return false, ErrCIObservation
		}
		return false, ErrCIObservation
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return false, normalizeBusy(ctx, err)
	}
	if used >= limit {
		return false, nil
	}
	updated, err := conn.ExecContext(ctx, `UPDATE ticket_counters SET used=used+1 WHERE channel=? AND project_id=? AND ticket_id=? AND kind='correction' AND used<limit_count`, ref.Channel, ref.Project, ref.Ticket)
	if err != nil {
		return false, err
	}
	if count, _ := updated.RowsAffected(); count != 1 {
		return false, ErrCIObservation
	}
	if _, err := conn.ExecContext(ctx, `INSERT INTO ticket_budget_uses(channel,project_id,ticket_id,kind,request_id,ticket_version,leader_epoch,runner_epoch,created_at) VALUES(?,?,?,?,?,?,?,?,?)`, ref.Channel, ref.Project, ref.Ticket, "correction", authority.RequestID, version, fence.LeaderEpoch, fence.RunnerEpoch, now()); err != nil {
		return false, err
	}
	if err := evidenceEvent(ctx, conn, ref, version, "budget_correction", map[string]string{"request_id": authority.RequestID}); err != nil {
		return false, err
	}
	return true, nil
}

func ciTransitionDigest(ref domain.TicketRef, observation CIObservation, resultVersion uint64, eventID int64, eventCreated, resultingState, trigger, payload string) string {
	body, _ := json.Marshal(struct {
		Ref                                               domain.TicketRef
		ObservationDigest, Witness, Head, Tree            string
		Generation                                        uint64
		ObservationVersion, Leader, Runner, ResultVersion uint64
		EventID                                           int64
		EventCreated, ResultingState, Trigger, Payload    string
	}{ref, observation.ObservationDigest, observation.PublicationWitnessDigest, observation.CandidateHeadSHA, observation.CandidateTreeSHA, observation.CandidateGeneration, observation.ObservedTicketVersion, observation.ObservedFence.LeaderEpoch, observation.ObservedFence.RunnerEpoch, resultVersion, eventID, eventCreated, resultingState, trigger, payload})
	return ciAuthorityDigest(body)
}

// ConsumeCIObservation atomically appends ci_transition_evidence and its
// canonical event while advancing waiting_ci. Pending self-transitions and
// green transitions need no budget. Red transitions allocate at most one
// correction use in this same transaction; exhaustion pauses with no new use.
func (s *Store) ConsumeCIObservation(ctx context.Context, request CIObservationTransition) (TransitionResult, error) {
	if request.Ref.Validate() != nil || request.ObservationDigest == "" || request.ExpectedVersion == 0 || request.Fence.LeaderEpoch == 0 || request.Fence.RunnerEpoch == 0 {
		return TransitionResult{}, ErrCIObservation
	}
	var result TransitionResult
	err := s.ciWrite(ctx, request.Ref, func(conn *sql.Conn) error {
		// Authenticate the requested immutable observation first, without
		// requiring the ticket still to be in waiting_ci. A lost response is
		// replayed from this exact evidence even after a newer observation exists.
		observation, err := s.authenticateCurrentCIObservation(ctx, conn, request.Ref, request.ObservationDigest, false)
		if err != nil {
			return err
		}
		if request.ObservationDigest == "" || observation.ObservationDigest != request.ObservationDigest || observation.ObservedTicketVersion != request.ExpectedVersion || observation.ObservedFence != request.Fence {
			return ErrStaleFence
		}
		if err := validateCorrectionBudgetLedger(ctx, conn, request.Ref); err != nil {
			return err
		}
		var replayVersion, replayEventID, replayGeneration int64
		var replayHead, replayTree, replayWitness string
		var replayCreated, replayState, replayTrigger, replayDigest, replayPrior string
		var replayPayload string
		replayErr := conn.QueryRowContext(ctx, `SELECT c.candidate_generation,c.candidate_head_sha,c.candidate_tree_sha,c.prior_publication_witness_digest,e.ticket_version,e.id,e.created_at,e.from_state,e.to_state,e.trigger,e.payload,c.transition_digest FROM ci_transition_evidence c JOIN events e ON e.channel=c.channel AND e.project_id=c.project_id AND e.ticket_id=c.ticket_id AND e.ticket_version=c.ticket_version AND e.id=c.event_id AND e.created_at=c.event_created_at WHERE c.channel=? AND c.project_id=? AND c.ticket_id=? AND c.observation_classification=? AND c.observation_digest=? AND c.observation_ticket_version=? AND c.observation_leader_epoch=? AND c.observation_runner_epoch=?`, request.Ref.Channel, request.Ref.Project, request.Ref.Ticket, observation.Classification, observation.ObservationDigest, observation.ObservedTicketVersion, observation.ObservedFence.LeaderEpoch, observation.ObservedFence.RunnerEpoch).Scan(&replayGeneration, &replayHead, &replayTree, &replayWitness, &replayVersion, &replayEventID, &replayCreated, &replayPrior, &replayState, &replayTrigger, &replayPayload, &replayDigest)
		if replayErr == nil {
			if replayGeneration != int64(observation.CandidateGeneration) || replayHead != observation.CandidateHeadSHA || replayTree != observation.CandidateTreeSHA || replayWitness != observation.PublicationWitnessDigest || replayVersion != int64(observation.ObservedTicketVersion+1) || replayPrior != string(domain.StateWaitingCI) || replayTrigger != ciTrigger(observation.Classification) || !validCIResultingState(observation.Classification, domain.State(replayState)) || replayDigest != ciTransitionDigest(request.Ref, observation, uint64(replayVersion), replayEventID, replayCreated, replayState, replayTrigger, replayPayload) {
				return ErrCIObservation
			}
			result.Version, result.EventID = uint64(replayVersion), replayEventID
			return nil
		}
		if !errors.Is(replayErr, sql.ErrNoRows) {
			return normalizeBusy(ctx, replayErr)
		}
		// No immutable evidence exists yet, so this is a new transition and the
		// observation must still be the newest one at the current waiting_ci
		// fence. This check deliberately follows replay authentication.
		latest, err := s.authenticateCurrentCIObservation(ctx, conn, request.Ref, "", true)
		if err != nil {
			return err
		}
		if latest.ObservationDigest != observation.ObservationDigest {
			return ErrStaleFence
		}
		observation = latest
		resulting := domain.StateReviewing
		trigger := "checks_green"
		budgetAuthorized := false
		repairLoopExhausted := false
		if observation.Classification == "pending" {
			resulting, trigger = domain.StateWaitingCI, "checks_pending"
		} else if observation.Classification == "red" {
			resulting, trigger = domain.StatePaused, "checks_red"
			var repairBindings int
			if err := conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM candidate_repair_bindings WHERE channel=? AND project_id=? AND ticket_id=?`, request.Ref.Channel, request.Ref.Project, request.Ref.Ticket).Scan(&repairBindings); err != nil {
				return normalizeBusy(ctx, err)
			}
			if repairBindings > 1 {
				return ErrCIObservation
			}
			// v1 admits exactly one diagnosed CI repair loop. A second red
			// observation pauses without consuming the ticket's remaining
			// shared correction unit, which remains available to the final-review
			// amendment authority.
			if repairBindings == 0 {
				authority := CorrectionBudgetAuthority{}
				if request.CorrectionBudget != nil {
					authority = *request.CorrectionBudget
				}
				var budgetErr error
				budgetAuthorized, budgetErr = consumeCorrectionBudget(ctx, conn, authority, request.Ref, observation.ObservedTicketVersion, observation.ObservedFence, observation.ObservationDigest)
				if budgetErr != nil {
					return budgetErr
				}
			}
			repairLoopExhausted = repairBindings == 1
			if budgetAuthorized {
				resulting = domain.StateBuilding
			}
			if budgetAuthorized {
				if err := s.injectedCIConsumeFault("after_correction_budget"); err != nil {
					return err
				}
			}
		}
		payload := map[string]any{"observation_digest": observation.ObservationDigest, "publication_witness_digest": observation.PublicationWitnessDigest}
		if observation.Classification == "red" && resulting == domain.StatePaused {
			payload["code"] = "ci_red_exhausted"
			if repairLoopExhausted {
				payload["reason"] = "required CI checks remain red after the single diagnosed repair loop"
			} else {
				payload["reason"] = "required CI checks are red and no authenticated correction budget remains"
			}
		} else if request.CorrectionBudget != nil && observation.Classification == "red" {
			payload["correction_request_id"] = request.CorrectionBudget.RequestID
		}
		encoded, err := json.Marshal(payload)
		if err != nil || len(encoded) > maxEvidenceJSON {
			return ErrCIObservation
		}
		var state string
		var version, runner, leader uint64
		if err := conn.QueryRowContext(ctx, `SELECT t.state,t.version,t.runner_epoch,d.leader_epoch FROM tickets t JOIN daemon_instances d ON d.channel=t.channel WHERE t.channel=? AND t.project_id=? AND t.id=?`, request.Ref.Channel, request.Ref.Project, request.Ref.Ticket).Scan(&state, &version, &runner, &leader); err != nil {
			return normalizeBusy(ctx, err)
		}
		if err := s.assertTicketFence(ctx, conn, request.Ref, observation.ObservedTicketVersion, observation.ObservedFence); err != nil {
			return err
		}
		if state != string(domain.StateWaitingCI) || version != observation.ObservedTicketVersion || runner != request.Fence.RunnerEpoch || leader != request.Fence.LeaderEpoch {
			return ErrStaleFence
		}
		createdAt := now()
		updated, err := conn.ExecContext(ctx, `UPDATE tickets SET state=?,resume_state=?,version=version+1 WHERE channel=? AND project_id=? AND id=? AND state='waiting_ci' AND version=? AND runner_epoch=?`, resulting, nullableState(func() domain.State {
			if resulting == domain.StatePaused {
				return domain.StateWaitingCI
			}
			return ""
		}()), request.Ref.Channel, request.Ref.Project, request.Ref.Ticket, version, runner)
		if err != nil {
			return err
		}
		if count, _ := updated.RowsAffected(); count != 1 {
			return ErrStaleFence
		}
		event, err := conn.ExecContext(ctx, `INSERT INTO events(channel,project_id,ticket_id,ticket_version,trigger,from_state,to_state,payload,created_at) VALUES(?,?,?,?,?,?,?,?,?)`, request.Ref.Channel, request.Ref.Project, request.Ref.Ticket, version+1, trigger, domain.StateWaitingCI, resulting, string(encoded), createdAt)
		if err != nil {
			return err
		}
		if count, _ := event.RowsAffected(); count != 1 {
			return ErrCIObservation
		}
		eventID, err := event.LastInsertId()
		if err != nil || eventID <= 0 {
			return ErrCIObservation
		}
		transitionDigest := ciTransitionDigest(request.Ref, observation, version+1, eventID, createdAt, string(resulting), trigger, string(encoded))
		inserted, err := conn.ExecContext(ctx, `INSERT INTO ci_transition_evidence(channel,project_id,ticket_id,candidate_generation,candidate_head_sha,candidate_tree_sha,ticket_version,event_id,event_created_at,observation_classification,observation_digest,observation_ticket_version,observation_leader_epoch,observation_runner_epoch,prior_publication_witness_digest,prior_state,resulting_state,resulting_trigger,transition_digest,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, request.Ref.Channel, request.Ref.Project, request.Ref.Ticket, observation.CandidateGeneration, observation.CandidateHeadSHA, observation.CandidateTreeSHA, version+1, eventID, createdAt, observation.Classification, observation.ObservationDigest, observation.ObservedTicketVersion, observation.ObservedFence.LeaderEpoch, observation.ObservedFence.RunnerEpoch, observation.PublicationWitnessDigest, domain.StateWaitingCI, resulting, trigger, transitionDigest, createdAt)
		if err != nil {
			return err
		}
		if count, _ := inserted.RowsAffected(); count != 1 {
			return ErrCIObservation
		}
		if resulting == domain.StateBuilding {
			if err := recordProviderPhaseEntry(ctx, conn, request.Ref, domain.PhaseBuild, version+1, request.Fence.LeaderEpoch, runner, eventID, createdAt, domain.StateWaitingCI, domain.StateBuilding, trigger); err != nil {
				return err
			}
		} else if resulting == domain.StateReviewing {
			if err := recordProviderPhaseEntry(ctx, conn, request.Ref, domain.PhaseReview, version+1, request.Fence.LeaderEpoch, runner, eventID, createdAt, domain.StateWaitingCI, domain.StateReviewing, trigger); err != nil {
				return err
			}
		}
		if observation.Classification == "red" && resulting == domain.StateBuilding {
			publication, found, err := loadPublicationEvidenceRow(ctx, conn, request.Ref)
			if err != nil || !found || !ciObservationMatchesPublication(observation, publication) {
				return ErrCIObservation
			}
			if err := s.recordRepairBinding(ctx, conn, observation, publication, *request.CorrectionBudget, transitionDigest, createdAt); err != nil {
				return err
			}
		}
		result.Version, result.EventID = version+1, eventID
		return nil
	})
	return result, err
}

func ciTrigger(classification string) string {
	switch classification {
	case "pending":
		return "checks_pending"
	case "green":
		return "checks_green"
	default:
		return "checks_red"
	}
}

func candidateRepairContextDigest(ref domain.TicketRef, observation CIObservation, publication PublishedCandidateEvidence, authority CorrectionBudgetAuthority, target uint64, transitionDigest, recoveryPrefixDigest string) string {
	body, _ := json.Marshal(struct {
		Ref                                                                                   domain.TicketRef
		Target, Predecessor                                                                   uint64
		Head, Tree, Witness, PRHost, PROwner, PRRepo, Branch, RemoteHead, BaseRef, RemoteBase string
		ObservationDigest, TransitionDigest, RecoveryPrefixDigest, RequestID                  string
		TicketVersion, Leader, Runner                                                         uint64
	}{ref, target, observation.CandidateGeneration, observation.CandidateHeadSHA, observation.CandidateTreeSHA, observation.PublicationWitnessDigest, publication.PullRequest.Repository.Host, publication.PullRequest.Repository.Owner, publication.PullRequest.Repository.Name, publication.RemoteBranchRef, publication.RemoteBranchOID, publication.PullRequest.BaseRef, publication.RemoteBaseOID, observation.ObservationDigest, transitionDigest, recoveryPrefixDigest, authority.RequestID, observation.ObservedTicketVersion, observation.ObservedFence.LeaderEpoch, observation.ObservedFence.RunnerEpoch})
	return ciAuthorityDigest(body)
}

func (s *Store) recordRepairBinding(ctx context.Context, conn *sql.Conn, observation CIObservation, publication PublishedCandidateEvidence, authority CorrectionBudgetAuthority, transitionDigest string, createdAt string) error {
	target := observation.CandidateGeneration + 1
	var existing int
	if err := conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM candidate_repair_bindings WHERE channel=? AND project_id=? AND ticket_id=?`, observation.Ref.Channel, observation.Ref.Project, observation.Ref.Ticket).Scan(&existing); err != nil || existing != 0 {
		return ErrCIObservation
	}
	recoveryPrefixDigest, err := candidateRepairRecoveryPrefixDigest(ctx, conn, observation.Ref, observation.ObservedTicketVersion)
	if err != nil {
		return ErrCIObservation
	}
	contextDigest := candidateRepairContextDigest(observation.Ref, observation, publication, authority, target, transitionDigest, recoveryPrefixDigest)
	for _, kind := range []string{"github_checks", "final_review", "approval"} {
		result, err := conn.ExecContext(ctx, `INSERT INTO invalidation_receipts(channel,project_id,ticket_id,generation,kind,ticket_version,reason,created_at) VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(channel,project_id,ticket_id,generation,kind) DO NOTHING`, observation.Ref.Channel, observation.Ref.Project, observation.Ref.Ticket, observation.CandidateGeneration, kind, observation.ObservedTicketVersion, "required CI checks failed; candidate requires repair", createdAt)
		if err != nil {
			return err
		}
		if count, _ := result.RowsAffected(); count > 1 {
			return ErrCIObservation
		}
	}
	result, err := conn.ExecContext(ctx, `INSERT INTO candidate_repair_bindings(channel,project_id,ticket_id,target_generation,predecessor_generation,predecessor_head_sha,predecessor_tree_sha,predecessor_publication_witness_digest,pr_host,pr_owner,pr_repo,pr_number,branch_ref,remote_head_oid,base_ref,remote_base_oid,red_observation_digest,red_observation_classification,red_transition_ticket_version,red_transition_digest,correction_budget_kind,correction_budget_request_id,consumed_ticket_version,consumed_leader_epoch,consumed_runner_epoch,consumed_recovery_prefix_digest,repair_context_digest,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, observation.Ref.Channel, observation.Ref.Project, observation.Ref.Ticket, target, observation.CandidateGeneration, observation.CandidateHeadSHA, observation.CandidateTreeSHA, observation.PublicationWitnessDigest, publication.PullRequest.Repository.Host, publication.PullRequest.Repository.Owner, publication.PullRequest.Repository.Name, publication.PullRequest.Number, publication.RemoteBranchRef, publication.RemoteBranchOID, publication.PullRequest.BaseRef, publication.RemoteBaseOID, observation.ObservationDigest, "red", observation.ObservedTicketVersion+1, transitionDigest, "correction", authority.RequestID, observation.ObservedTicketVersion, observation.ObservedFence.LeaderEpoch, observation.ObservedFence.RunnerEpoch, recoveryPrefixDigest, contextDigest, createdAt)
	if err != nil {
		return err
	}
	if count, _ := result.RowsAffected(); count > 1 {
		return ErrCIObservation
	}
	var stored string
	if err := conn.QueryRowContext(ctx, `SELECT repair_context_digest FROM candidate_repair_bindings WHERE channel=? AND project_id=? AND ticket_id=? AND target_generation=?`, observation.Ref.Channel, observation.Ref.Project, observation.Ref.Ticket, target).Scan(&stored); err != nil || stored != contextDigest {
		return ErrCIObservation
	}
	return nil
}

func validCIResultingState(classification string, state domain.State) bool {
	switch classification {
	case "pending":
		return state == domain.StateWaitingCI
	case "green":
		return state == domain.StateReviewing
	case "red":
		return state == domain.StateBuilding || state == domain.StatePaused
	default:
		return false
	}
}

func (s *Store) ConsumeAuthenticatedCIObservation(ctx context.Context, request CIObservationTransition) (TransitionResult, error) {
	return s.ConsumeCIObservation(ctx, request)
}

func (s *Store) TransitionCIObservation(ctx context.Context, request CIObservationTransition) (TransitionResult, error) {
	return s.ConsumeCIObservation(ctx, request)
}
