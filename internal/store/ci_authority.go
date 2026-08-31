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
	maxCIDiagnosticText = 16 << 10
	maxCIDiagnosticJSON = 64 << 10
	maxCIChecks         = 512
	maxCIAggregateDiag  = 256 << 10
	maxCIClockSkew      = 24 * time.Hour
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

// CorrectionBudgetAuthority is not a request to consume a budget. It is the
// exact identity of a budget use that was authenticated and persisted by the
// existing budget API before this transition is attempted.
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
	pollControlVersions := uint64(0)
	// A single exhausted-poll -> operator-resume pair consumes two ticket
	// versions without an external CI observation. It is distinct from a
	// pending observation and is accepted only by its exact immutable events.
	if version >= waitingVersion+2 && authenticateCIPollResume(ctx, q, ref, waitingVersion+1, waitingVersion+2) {
		pollControlVersions = 2
	}
	rows, err := q.QueryContext(ctx, `SELECT c.ticket_version,c.event_id,c.event_created_at,c.candidate_generation,c.candidate_head_sha,c.candidate_tree_sha,c.observation_classification,c.observation_digest,c.observation_ticket_version,c.observation_leader_epoch,c.observation_runner_epoch,c.prior_publication_witness_digest,c.prior_state,c.resulting_state,c.resulting_trigger,c.transition_digest,e.id,e.created_at,e.from_state,e.to_state,e.trigger,e.payload FROM ci_transition_evidence c JOIN events e ON e.channel=c.channel AND e.project_id=c.project_id AND e.ticket_id=c.ticket_id AND e.ticket_version=c.ticket_version AND e.id=c.event_id WHERE c.channel=? AND c.project_id=? AND c.ticket_id=? AND c.ticket_version>? ORDER BY c.ticket_version`, ref.Channel, ref.Project, ref.Ticket, waitingVersion)
	if err != nil {
		return PublishedCandidateEvidence{}, normalizeBusy(ctx, err)
	}
	defer rows.Close()
	expectedVersion := waitingVersion + 1 + pollControlVersions
	chainRunner, chainLeader := publication.CurrentFence.RunnerEpoch, publication.CurrentFence.LeaderEpoch
	chainCount := 0
	for rows.Next() {
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
	if err := q.QueryRowContext(ctx, `SELECT COUNT(*) FROM runner_recovery_ledger WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version>? AND ticket_version<=?`, ref.Channel, ref.Project, ref.Ticket, waitingVersion, version).Scan(&recoveryCount); err != nil || chainCount+recoveryCount+int(pollControlVersions) != int(version-waitingVersion) {
		return PublishedCandidateEvidence{}, ciPublicationFailure("pending CI chain cardinality")
	}
	return publication, nil
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
	if liveVersion >= baselineVersion+2 && authenticateCIPollResume(ctx, q, ref, baselineVersion+1, baselineVersion+2) {
		expectedVersion = baselineVersion + 2
	}
	for rows.Next() {
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
			if err := q.QueryRowContext(ctx, `SELECT COUNT(*) FROM ci_transition_evidence WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version>? AND ticket_version<=? AND resulting_state='waiting_ci'`, ref.Channel, ref.Project, ref.Ticket, expectedVersion, step.PriorTicketVersion).Scan(&evidence); err != nil || evidence != int(step.PriorTicketVersion-expectedVersion) {
				return ciPublicationFailure("CI recovery gap evidence")
			}
			if err := q.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version>? AND ticket_version<=?`, ref.Channel, ref.Project, ref.Ticket, expectedVersion, step.PriorTicketVersion).Scan(&events); err != nil || events != evidence {
				return ciPublicationFailure("CI recovery gap events")
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

// RecordCIObservation atomically inserts one observation and its complete
// check set. Exact replay is a no-op; any digest or field conflict fails closed.
func (s *Store) RecordCIObservation(ctx context.Context, input CIObservation) error {
	claimedObservationDigest := input.ObservationDigest
	input.ObservationDigest = ""
	canonical, err := canonicalCIObservation(input)
	if err != nil {
		return err
	}
	if delta := time.Since(canonical.ObservedAt); delta > maxCIClockSkew || delta < -maxCIClockSkew {
		return ErrCIObservation
	}
	publication, err := loadCICurrentPublication(ctx, s.db, canonical.Ref)
	if err != nil {
		return errors.Join(ErrCIObservation, fmt.Errorf("load current CI publication: %w", err))
	}
	if !ciObservationMatchesPublication(canonical, publication) {
		return fmt.Errorf("%w: observation does not match current publication", ErrCIObservation)
	}
	err = s.ciWrite(ctx, canonical.Ref, func(conn *sql.Conn) error {
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

// RecordAuthenticatedCIObservation is the descriptive alias used by callers
// that want the authentication boundary visible in their code.
func (s *Store) RecordAuthenticatedCIObservation(ctx context.Context, input CIObservation) error {
	return s.RecordCIObservation(ctx, input)
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

func correctionBudgetPresent(ctx context.Context, q ciQuery, authority CorrectionBudgetAuthority, ref domain.TicketRef, version uint64, fence domain.Fence) (bool, error) {
	if authority.Ref != ref || authority.TicketVersion != version || authority.Fence != fence || authority.Fence.ClaimEpoch != 0 || authority.RequestID == "" || !boundedText(authority.RequestID, 300) {
		return false, ErrBudgetExhausted
	}
	var found, sameFence int
	err := q.QueryRowContext(ctx, `SELECT COUNT(*) FROM ticket_budget_uses u JOIN ticket_counters c ON c.channel=u.channel AND c.project_id=u.project_id AND c.ticket_id=u.ticket_id AND c.kind=u.kind WHERE u.channel=? AND u.project_id=? AND u.ticket_id=? AND u.kind='correction' AND u.request_id=? AND u.ticket_version=? AND u.leader_epoch=? AND u.runner_epoch=? AND c.used>0 AND c.used<=c.limit_count AND EXISTS (SELECT 1 FROM events e WHERE e.channel=u.channel AND e.project_id=u.project_id AND e.ticket_id=u.ticket_id AND e.ticket_version=u.ticket_version AND e.trigger='budget_correction' AND json_extract(e.payload,'$.request_id')=u.request_id)`, ref.Channel, ref.Project, ref.Ticket, authority.RequestID, version, fence.LeaderEpoch, fence.RunnerEpoch).Scan(&found)
	if err != nil {
		return false, normalizeBusy(ctx, err)
	}
	if err := q.QueryRowContext(ctx, `SELECT COUNT(*) FROM ticket_budget_uses u WHERE u.channel=? AND u.project_id=? AND u.ticket_id=? AND u.kind='correction' AND u.ticket_version=? AND u.leader_epoch=? AND u.runner_epoch=?`, ref.Channel, ref.Project, ref.Ticket, version, fence.LeaderEpoch, fence.RunnerEpoch).Scan(&sameFence); err != nil {
		return false, normalizeBusy(ctx, err)
	}
	return found == 1 && sameFence == 1, nil
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
// green transitions need no budget. Red transitions require an exact,
// separately persisted authority; otherwise the ticket is paused with the
// normative ci_red_exhausted reason.
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
		if observation.Classification == "pending" {
			resulting, trigger = domain.StateWaitingCI, "checks_pending"
		} else if observation.Classification == "red" {
			resulting, trigger = domain.StatePaused, "checks_red"
			if request.CorrectionBudget != nil {
				present, budgetErr := correctionBudgetPresent(ctx, conn, *request.CorrectionBudget, request.Ref, observation.ObservedTicketVersion, observation.ObservedFence)
				if budgetErr != nil && !errors.Is(budgetErr, ErrBudgetExhausted) {
					// A malformed/stale authority is the exhausted branch for a
					// known red observation. Actual SQLite failures remain fatal.
					return budgetErr
				}
				if budgetErr == nil && present {
					resulting = domain.StateBuilding
				}
			}
		}
		payload := map[string]any{"observation_digest": observation.ObservationDigest, "publication_witness_digest": observation.PublicationWitnessDigest}
		if observation.Classification == "red" && resulting == domain.StatePaused {
			payload["code"] = "ci_red_exhausted"
			payload["reason"] = "required CI checks are red and no authenticated correction budget remains"
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

func candidateRepairContextDigest(ref domain.TicketRef, observation CIObservation, publication PublishedCandidateEvidence, authority CorrectionBudgetAuthority, target uint64, transitionDigest string) string {
	body, _ := json.Marshal(struct {
		Ref                                                                                   domain.TicketRef
		Target, Predecessor                                                                   uint64
		Head, Tree, Witness, PRHost, PROwner, PRRepo, Branch, RemoteHead, BaseRef, RemoteBase string
		ObservationDigest, TransitionDigest, RequestID                                        string
		TicketVersion, Leader, Runner                                                         uint64
	}{ref, target, observation.CandidateGeneration, observation.CandidateHeadSHA, observation.CandidateTreeSHA, observation.PublicationWitnessDigest, publication.PullRequest.Repository.Host, publication.PullRequest.Repository.Owner, publication.PullRequest.Repository.Name, publication.RemoteBranchRef, publication.RemoteBranchOID, publication.PullRequest.BaseRef, publication.RemoteBaseOID, observation.ObservationDigest, transitionDigest, authority.RequestID, observation.ObservedTicketVersion, observation.ObservedFence.LeaderEpoch, observation.ObservedFence.RunnerEpoch})
	return ciAuthorityDigest(body)
}

func (s *Store) recordRepairBinding(ctx context.Context, conn *sql.Conn, observation CIObservation, publication PublishedCandidateEvidence, authority CorrectionBudgetAuthority, transitionDigest string, createdAt string) error {
	target := observation.CandidateGeneration + 1
	contextDigest := candidateRepairContextDigest(observation.Ref, observation, publication, authority, target, transitionDigest)
	for _, kind := range []string{"github_checks", "final_review", "approval"} {
		result, err := conn.ExecContext(ctx, `INSERT INTO invalidation_receipts(channel,project_id,ticket_id,generation,kind,ticket_version,reason,created_at) VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(channel,project_id,ticket_id,generation,kind) DO NOTHING`, observation.Ref.Channel, observation.Ref.Project, observation.Ref.Ticket, observation.CandidateGeneration, kind, observation.ObservedTicketVersion, "required CI checks failed; candidate requires repair", createdAt)
		if err != nil {
			return err
		}
		if count, _ := result.RowsAffected(); count > 1 {
			return ErrCIObservation
		}
	}
	result, err := conn.ExecContext(ctx, `INSERT INTO candidate_repair_bindings(channel,project_id,ticket_id,target_generation,predecessor_generation,predecessor_head_sha,predecessor_tree_sha,predecessor_publication_witness_digest,pr_host,pr_owner,pr_repo,pr_number,branch_ref,remote_head_oid,base_ref,remote_base_oid,red_observation_digest,red_observation_classification,red_transition_ticket_version,red_transition_digest,correction_budget_kind,correction_budget_request_id,consumed_ticket_version,consumed_leader_epoch,consumed_runner_epoch,repair_context_digest,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(channel,project_id,ticket_id,target_generation) DO NOTHING`, observation.Ref.Channel, observation.Ref.Project, observation.Ref.Ticket, target, observation.CandidateGeneration, observation.CandidateHeadSHA, observation.CandidateTreeSHA, observation.PublicationWitnessDigest, publication.PullRequest.Repository.Host, publication.PullRequest.Repository.Owner, publication.PullRequest.Repository.Name, publication.PullRequest.Number, publication.RemoteBranchRef, publication.RemoteBranchOID, publication.PullRequest.BaseRef, publication.RemoteBaseOID, observation.ObservationDigest, "red", observation.ObservedTicketVersion+1, transitionDigest, "correction", authority.RequestID, observation.ObservedTicketVersion, observation.ObservedFence.LeaderEpoch, observation.ObservedFence.RunnerEpoch, contextDigest, createdAt)
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
