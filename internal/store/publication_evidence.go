package store

// Publication evidence is the Store-side handoff between the immutable local
// candidate and the two public effects (push and draft PR).  This file does
// not advance a ticket.  It only records and authenticates a witness that a
// later worker may use while replaying the publishing -> waiting_ci boundary.

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"path"
	"strings"
	"time"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
)

type PublicationEffectEvidence struct {
	SemanticKey      string
	Kind             string
	RequestDigest    string
	ClaimEpoch       uint64
	ObservedIdentity string
}

// PublishedCandidateEvidence is a complete witness for one candidate that
// has reached an open draft PR. Candidate and worktree values are repeated on
// purpose: the loader can detect tampering or a later rebinding without
// trusting a caller's reconstructed object graph.
type PublishedCandidateEvidence struct {
	Ref                    domain.TicketRef
	TicketVersion          uint64
	Fence                  domain.Fence
	Candidate              StoredCandidate
	ConfigGeneration       uint64
	ConfigDigest           string
	ConfigSnapshotDigest   string
	Worktree               StoredWorktree
	RemoteBranchRef        string
	RemoteBranchOID        string
	RemoteBaseOID          string
	PushEffect             PublicationEffectEvidence
	PullRequest            contracts.PullRequestIdentity
	PullRequestState       string
	PullRequestDraft       bool
	PullRequestObservedAt  time.Time
	PRCreateOrUpdateEffect PublicationEffectEvidence
	WitnessDigest          string
	CreatedAt              time.Time
	// BuildTransitionCreatedAt is the exact creation time of the authoritative
	// building -> publishing event consumed by this witness. It is filled from
	// SQLite, never accepted as a caller reconstruction.
	BuildTransitionCreatedAt time.Time
	// CurrentTicketVersion/Fence are the live publication authority after an
	// explicit recovery rebind. TicketVersion/Fence remain the immutable
	// publication effect claim identity and are never rewritten.
	CurrentTicketVersion uint64
	CurrentFence         domain.Fence
}

// PublicationEvidence is retained as the concise public name for callers.
type PublicationEvidence = PublishedCandidateEvidence

// PublicationRebind is an append-only recovery proof. It advances only the
// live publication fence/version; the original effect claims and remote
// witness remain immutable.
type PublicationRebind struct {
	Ref                 domain.TicketRef
	CandidateGeneration uint64
	CandidateHeadOID    string
	PriorWitnessDigest  string
	PriorTicketVersion  uint64
	PriorFence          domain.Fence
	TicketVersion       uint64
	Fence               domain.Fence
	RebindDigest        string
	CreatedAt           time.Time
}

func publicationRebindPayload(value PublicationRebind) ([]byte, error) {
	return json.Marshal(struct {
		Ref                 domain.TicketRef
		CandidateGeneration uint64
		CandidateHeadOID    string
		PriorWitnessDigest  string
		PriorTicketVersion  uint64
		PriorFence          domain.Fence
		TicketVersion       uint64
		Fence               domain.Fence
		CreatedAt           string
	}{value.Ref, value.CandidateGeneration, value.CandidateHeadOID, value.PriorWitnessDigest, value.PriorTicketVersion, value.PriorFence, value.TicketVersion, value.Fence, value.CreatedAt.UTC().Format(time.RFC3339Nano)})
}

func canonicalPublicationTime(value time.Time) (time.Time, bool) {
	if value.IsZero() || value.Location() != time.UTC || value.Format(time.RFC3339Nano) != value.UTC().Format(time.RFC3339Nano) {
		return time.Time{}, false
	}
	return value, true
}

func parsePublicationTime(raw string) (time.Time, error) {
	value, err := time.Parse(time.RFC3339Nano, raw)
	if err != nil || value.Location() != time.UTC || value.Format(time.RFC3339Nano) != raw {
		return time.Time{}, ErrPublicationEvidence
	}
	return value, nil
}

func publicationEffectKind(kind string) bool {
	return kind == PublicationPushEffectKind || kind == PublicationPRCreateEffectKind || kind == PublicationPRUpdateEffectKind
}

const (
	PublicationPushEffectKind     = "git/push"
	PublicationPRCreateEffectKind = "draft_pr"
	// Keep the update category aligned with the GitHub adapter's exact effect
	// claim kind. It is deliberately distinct from draft_pr (create/adopt).
	PublicationPRUpdateEffectKind = "pr_edit"
)

func validPublicationEffect(value PublicationEffectEvidence) bool {
	return value.SemanticKey != "" && boundedText(value.SemanticKey, 512) && publicationEffectKind(value.Kind) && validPublicationRequestDigest(value.RequestDigest) && value.ClaimEpoch > 0 && boundedText(value.ObservedIdentity, 4096)
}

func validPublicationRequestDigest(value string) bool {
	// Git mutation claims use the typed sha256: spelling; the GitHub adapter's
	// requestDigest is the same 32-byte value in plain lowercase hex. No other
	// free-form effect digest is accepted at this boundary.
	return validDigest(value) || validClaimDigest(value)
}

// CanonicalPublicationPushObservation is the only accepted identity for the
// confirmed candidate-branch push effect.
func CanonicalPublicationPushObservation(branch, oid string) string {
	return publicationIdentityDigest([]byte("sf.publication.push.v1\x00" + branch + "\x00" + oid))
}

// CanonicalPublicationPushObservationDigest is the explicit digest spelling
// for callers that persist an observed effect identity.
func CanonicalPublicationPushObservationDigest(branch, oid string) string {
	return CanonicalPublicationPushObservation(branch, oid)
}

// CanonicalPublicationPRObservation authenticates every PR identity and its
// open/draft observation, rather than trusting a command response or number.
func CanonicalPublicationPRObservation(value contracts.PullRequestIdentity, state string, draft bool) string {
	payload, _ := json.Marshal(struct {
		PR    contracts.PullRequestIdentity
		State string
		Draft bool
	}{value, state, draft})
	return publicationIdentityDigest(payload)
}

// CanonicalPublicationPRObservationDigest names the digest contract explicitly
// for callers that persist only the digest; it is equivalent to the canonical
// PR observation identity because that identity is already a SHA-256 digest.
func CanonicalPublicationPRObservationDigest(value contracts.PullRequestIdentity, state string, draft bool) string {
	return CanonicalPublicationPRObservation(value, state, draft)
}

func validPublicationPR(value contracts.PullRequestIdentity, state string, draft bool, observed time.Time) bool {
	if value.Repository.Host != "github.com" || !validPRPart(value.Repository.Owner) || !validPRPart(value.Repository.Name) || value.Number <= 0 || !validPRPart(value.HeadOwner) || !validPRPart(value.HeadRepository) || !validPublicationRef(value.HeadRef) || !validOID(value.HeadOID) || !validPublicationRef(value.BaseRef) || !validOID(value.BaseOID) || !value.FactoryOwned || state != "OPEN" || !draft || observed.IsZero() || observed.Location() != time.UTC {
		return false
	}
	return true
}

func validPRPart(value string) bool {
	if !boundedText(value, 100) {
		return false
	}
	for _, r := range value {
		if !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '-' || r == '_' || r == '.') {
			return false
		}
	}
	return true
}

func validPublicationRef(value string) bool {
	return boundedText(value, 255) && value[0] != '/' && value[len(value)-1] != '/' && !strings.Contains(value, "..") && !strings.ContainsAny(value, " ~^:?*[\\\r\n")
}

type publicationGitIdentity struct {
	Repository string
	Origin     string
	PushOrigin string
	BaseRef    string
	BaseHead   string
	HeadRef    string
}

func publicationGitHubRepo(raw string) (string, string, bool) {
	if raw == "" || strings.HasPrefix(raw, "/") {
		return "", "", false
	}
	if strings.HasPrefix(raw, "git@github.com:") {
		raw = "https://github.com/" + strings.TrimPrefix(raw, "git@github.com:")
	} else if strings.HasPrefix(raw, "ssh://git@github.com/") {
		raw = "https://github.com/" + strings.TrimPrefix(raw, "ssh://git@github.com/")
	}
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "https" || u.Host != "github.com" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return "", "", false
	}
	parts := strings.Split(strings.TrimSuffix(strings.TrimPrefix(path.Clean(u.Path), "/"), ".git"), "/")
	if len(parts) != 2 || !validPRPart(parts[0]) || !validPRPart(parts[1]) {
		return "", "", false
	}
	return parts[0], parts[1], true
}

func publicationOriginsMatch(identity []byte, pr contracts.PullRequestIdentity, remoteBase string) bool {
	var git publicationGitIdentity
	if json.Unmarshal(identity, &git) != nil || git.BaseRef != pr.BaseRef || git.HeadRef != pr.HeadRef || git.BaseHead != remoteBase || !validOID(git.BaseHead) {
		return false
	}
	owner, name, ok := publicationGitHubRepo(git.Origin)
	if !ok || owner != pr.Repository.Owner || name != pr.Repository.Name {
		return false
	}
	pushOwner, pushName, ok := publicationGitHubRepo(git.PushOrigin)
	if !ok || pushOwner != pr.Repository.Owner || pushName != pr.Repository.Name {
		return false
	}
	return true
}

func publicationIdentityDigest(value []byte) string {
	sum := sha256.Sum256(value)
	return "sha256:" + fmt.Sprintf("%x", sum[:])
}

func publicationPayload(value PublishedCandidateEvidence) ([]byte, error) {
	// Keep this envelope explicit and stable. CreatedAt and WitnessDigest are
	// intentionally excluded so an exact lost-response replay hashes alike.
	candidate := value.Candidate
	candidate.CreatedAt = time.Time{}
	worktree := value.Worktree
	// HeadSHA is the registration projection omitted by this publication table;
	// all other historical registration fence/state fields are authenticated.
	worktree.HeadSHA = ""
	return json.Marshal(struct {
		Ref                                             domain.TicketRef
		TicketVersion                                   uint64
		Fence                                           domain.Fence
		Candidate                                       StoredCandidate
		ConfigGeneration                                uint64
		ConfigDigest, ConfigSnapshotDigest              string
		Worktree                                        StoredWorktree
		RemoteBranchRef, RemoteBranchOID, RemoteBaseOID string
		PushEffect                                      PublicationEffectEvidence
		PullRequest                                     contracts.PullRequestIdentity
		PullRequestState                                string
		PullRequestDraft                                bool
		PullRequestObservedAt                           string
		PRCreateOrUpdateEffect                          PublicationEffectEvidence
		CreatedAt                                       string
		BuildTransitionCreatedAt                        string
	}{value.Ref, value.TicketVersion, value.Fence, candidate, value.ConfigGeneration, value.ConfigDigest, value.ConfigSnapshotDigest, worktree, value.RemoteBranchRef, value.RemoteBranchOID, value.RemoteBaseOID, value.PushEffect, value.PullRequest, value.PullRequestState, value.PullRequestDraft, value.PullRequestObservedAt.Format(time.RFC3339Nano), value.PRCreateOrUpdateEffect, value.CreatedAt.UTC().Format(time.RFC3339Nano), value.BuildTransitionCreatedAt.UTC().Format(time.RFC3339Nano)})
}

func validPublishedCandidateEvidence(value PublishedCandidateEvidence) error {
	if _, ok := canonicalPublicationTime(value.CreatedAt); !ok {
		return ErrPublicationEvidence
	}
	if value.Ref.Validate() != nil || value.TicketVersion == 0 || value.Fence.LeaderEpoch == 0 || value.Fence.RunnerEpoch == 0 || value.Fence.ClaimEpoch != 0 || validateCandidate(value.Candidate.Snapshot) != nil || (value.Candidate.TicketVersion+1 != value.TicketVersion && (value.Candidate.TicketVersion == ^uint64(0) || value.Candidate.TicketVersion+2 != value.TicketVersion)) || value.Candidate.Fence.LeaderEpoch == 0 || value.Candidate.Fence.RunnerEpoch == 0 || value.Candidate.Fence.ClaimEpoch != 0 || value.Candidate.BuilderResult.AttemptID <= 0 || value.Candidate.BuilderResult.Attempt <= 0 || !validOID(value.Candidate.Commit.ParentOID) || value.Candidate.CommandBinding.Key.SemanticKey == "" || value.Candidate.CommandBinding.Key.ClaimEpoch == 0 || !validClaimDigest(value.Candidate.CommandBinding.CommandDigest) || !validClaimDigest(value.Candidate.CommandBinding.SpecDigest) || !validClaimDigest(value.Candidate.CommandBinding.PolicyDigest) || !validStorePath(value.Candidate.CommandBinding.ExecutablePath) || !validRepositoryExecutableDigest(value.Candidate.CommandBinding.ExecutablePath, value.Candidate.CommandBinding.ExecutableDigest) || value.ConfigGeneration == 0 || !validDigest(value.ConfigDigest) || !validDigest(value.ConfigSnapshotDigest) || !validStorePath(value.Worktree.Path) || !boundedText(value.Worktree.Branch, 300) || !boundedText(value.Worktree.State, 100) || !validJSON(value.Worktree.IdentityJSON) || !validOID(value.Worktree.BaseSHA) || value.Worktree.TicketVersion == 0 || value.Worktree.Fence.LeaderEpoch == 0 || value.Worktree.Fence.RunnerEpoch == 0 || value.Worktree.Fence.ClaimEpoch != 0 || !validPublicationRef(value.RemoteBranchRef) || !validOID(value.RemoteBranchOID) || !validOID(value.RemoteBaseOID) || value.RemoteBranchOID != value.Candidate.Snapshot.HeadSHA || value.RemoteBranchRef != value.Worktree.Branch || value.Worktree.BaseSHA != value.Candidate.Snapshot.BaseSHA || value.RemoteBaseOID != value.Worktree.BaseSHA || !validPublicationEffect(value.PushEffect) || !validPublicationEffect(value.PRCreateOrUpdateEffect) || value.PushEffect.Kind != PublicationPushEffectKind || (value.PRCreateOrUpdateEffect.Kind != PublicationPRCreateEffectKind && value.PRCreateOrUpdateEffect.Kind != PublicationPRUpdateEffectKind) || value.PushEffect.SemanticKey == value.PRCreateOrUpdateEffect.SemanticKey || validPublicationPR(value.PullRequest, value.PullRequestState, value.PullRequestDraft, value.PullRequestObservedAt) == false || value.PullRequest.HeadOwner != value.PullRequest.Repository.Owner || value.PullRequest.HeadRepository != value.PullRequest.Repository.Name || value.PullRequest.HeadOID != value.Candidate.Snapshot.HeadSHA || value.PullRequest.BaseOID != value.RemoteBaseOID || value.PullRequest.HeadRef != value.Worktree.Branch || value.PullRequest.BaseRef == "" || !publicationOriginsMatch(value.Worktree.IdentityJSON, value.PullRequest, value.RemoteBaseOID) {
		return ErrPublicationEvidence
	}
	if value.Candidate.CommandBinding.PolicyDigest != "sha256:"+value.Candidate.Snapshot.CommandPolicyDigest {
		return ErrPublicationEvidence
	}
	if value.PushEffect.ObservedIdentity != CanonicalPublicationPushObservation(value.RemoteBranchRef, value.RemoteBranchOID) || value.PRCreateOrUpdateEffect.ObservedIdentity != CanonicalPublicationPRObservation(value.PullRequest, value.PullRequestState, value.PullRequestDraft) {
		return ErrPublicationEvidence
	}
	return nil
}

func publicationEqual(left, right PublishedCandidateEvidence) bool {
	lp, lerr := publicationPayload(left)
	rp, rerr := publicationPayload(right)
	return lerr == nil && rerr == nil && bytes.Equal(lp, rp)
}

// RecordPublishedCandidate persists one exact witness. Repeating the exact
// request after a lost response is a no-op; every differing field is a hard
// conflict. All referenced effects must already be confirmed.
func (s *Store) RecordPublishedCandidate(ctx context.Context, value PublishedCandidateEvidence) error {
	if err := validPublishedCandidateEvidence(value); err != nil {
		return err
	}
	// Candidate snapshots, their provider result, and their command binding are
	// immutable authorities. Authenticate them before opening the publication
	// write; the transaction below then rechecks the current ticket/fence and
	// exact candidate generation before inserting the witness.
	authenticatedCandidate, err := s.RecoverableCandidate(ctx, value.Ref)
	if err != nil || !publicationCandidateEqual(authenticatedCandidate, value.Candidate) {
		return ErrPublicationEvidence
	}
	authenticatedWorktree, err := s.Worktree(ctx, value.Ref)
	if err != nil || authenticatedWorktree.Path != value.Worktree.Path || authenticatedWorktree.Branch != value.Worktree.Branch || authenticatedWorktree.State != value.Worktree.State || !bytes.Equal(authenticatedWorktree.IdentityJSON, value.Worktree.IdentityJSON) || authenticatedWorktree.BaseSHA != value.Worktree.BaseSHA || authenticatedWorktree.TicketVersion != value.Worktree.TicketVersion || authenticatedWorktree.Fence != value.Worktree.Fence {
		return ErrPublicationEvidence
	}
	return s.write(ctx, func(conn *sql.Conn) error {
		var state string
		var version, runner, configGeneration uint64
		var sourceDigest, configDigest, configSnapshot []byte
		var projectBaseRef string
		if err := conn.QueryRowContext(ctx, `SELECT t.state,t.version,t.runner_epoch,t.source_digest,t.config_generation,t.config_digest,t.config_snapshot_bytes,p.base_ref FROM tickets t JOIN projects p ON p.channel=t.channel AND p.id=t.project_id WHERE t.channel=? AND t.project_id=? AND t.id=?`, value.Ref.Channel, value.Ref.Project, value.Ref.Ticket).Scan(&state, &version, &runner, &sourceDigest, &configGeneration, &configDigest, &configSnapshot, &projectBaseRef); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return ErrNotFound
			}
			return err
		}
		if state != string(domain.StatePublishing) || version != value.TicketVersion || runner != value.Fence.RunnerEpoch || value.Candidate.Snapshot.SourceDigest != string(sourceDigest) || configGeneration != value.ConfigGeneration || string(configDigest) != value.ConfigDigest || sha256Digest(configSnapshot) != value.ConfigSnapshotDigest || value.PullRequest.BaseRef != projectBaseRef {
			return ErrStaleFence
		}
		if err := s.currentFence(ctx, conn, value.Ref.Channel, version, runner, value.Fence); err != nil {
			return err
		}
		// Candidate/worktree rows are immutable witnesses, but read them in this
		// transaction so a caller cannot combine a current ticket with stale data.
		var generation uint64
		var candidate domain.CandidateSnapshot
		if err := conn.QueryRowContext(ctx, `SELECT generation,base_sha,head_sha,tree_sha,source_digest,verification_intent_digest,proof_digest,command_policy_digest,builder_evidence_digest FROM candidate_snapshots WHERE channel=? AND project_id=? AND ticket_id=? ORDER BY generation DESC LIMIT 1`, value.Ref.Channel, value.Ref.Project, value.Ref.Ticket).Scan(&generation, &candidate.BaseSHA, &candidate.HeadSHA, &candidate.TreeSHA, &candidate.SourceDigest, &candidate.VerificationIntentDigest, &candidate.ProofDigest, &candidate.CommandPolicyDigest, &candidate.BuilderEvidenceDigest); err != nil {
			return ErrPublicationEvidence
		}
		candidate.Generation = generation
		if generation != value.Candidate.Snapshot.Generation || candidate != value.Candidate.Snapshot {
			return ErrPublicationEvidence
		}
		buildVersion := value.Candidate.TicketVersion + 1
		if value.TicketVersion == value.Candidate.TicketVersion+2 {
			// The candidate-only crash window is building->publishing at
			// candidate+1 followed by the first daemon takeover at candidate+2.
			// Authenticate both exact endpoints; do not infer publication from
			// the counter gap.
			if err := authenticateRunnerRecoveryStep(ctx, conn, value.Ref, value.Candidate.TicketVersion+1, value.Candidate.Fence, value.TicketVersion, value.Fence); err != nil {
				return ErrPublicationEvidence
			}
		} else if value.TicketVersion != buildVersion {
			return ErrPublicationEvidence
		}
		var consumed int
		if err := conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version=? AND trigger='phase_pass' AND from_state='building' AND to_state='publishing'`, value.Ref.Channel, value.Ref.Project, value.Ref.Ticket, buildVersion).Scan(&consumed); err != nil || consumed != 1 {
			return ErrPublicationEvidence
		}
		var buildEventCreated string
		if err := conn.QueryRowContext(ctx, `SELECT created_at FROM events WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version=? AND trigger='phase_pass' AND from_state='building' AND to_state='publishing'`, value.Ref.Channel, value.Ref.Project, value.Ref.Ticket, buildVersion).Scan(&buildEventCreated); err != nil {
			return ErrPublicationEvidence
		}
		var err error
		if value.BuildTransitionCreatedAt, err = parsePublicationTime(buildEventCreated); err != nil {
			return ErrPublicationEvidence
		}
		payload, err := publicationPayload(value)
		if err != nil {
			return ErrPublicationEvidence
		}
		digest := publicationIdentityDigest(payload)
		var path, branch, stateWT, identity string
		var base string
		var wtVersion, wtLeader, wtRunner uint64
		if err := conn.QueryRowContext(ctx, `SELECT path,branch_ref,state,identity_json,base_sha,ticket_version,leader_epoch,runner_epoch FROM worktrees WHERE channel=? AND project_id=? AND ticket_id=?`, value.Ref.Channel, value.Ref.Project, value.Ref.Ticket).Scan(&path, &branch, &stateWT, &identity, &base, &wtVersion, &wtLeader, &wtRunner); err != nil {
			return ErrPublicationEvidence
		}
		if path != value.Worktree.Path || branch != value.Worktree.Branch || stateWT != value.Worktree.State || identity != string(value.Worktree.IdentityJSON) || base != value.Worktree.BaseSHA || wtVersion != value.Worktree.TicketVersion || wtLeader != value.Worktree.Fence.LeaderEpoch || wtRunner != value.Worktree.Fence.RunnerEpoch {
			return ErrPublicationEvidence
		}
		var allocatedBranch string
		if err := conn.QueryRowContext(ctx, `SELECT branch_ref FROM branch_allocations WHERE channel=? AND project_id=? AND ticket_id=?`, value.Ref.Channel, value.Ref.Project, value.Ref.Ticket).Scan(&allocatedBranch); err != nil || allocatedBranch != value.Worktree.Branch {
			return ErrPublicationEvidence
		}
		if err := checkPublicationEffect(ctx, conn, value.Ref, value.TicketVersion, value.Fence, value.PushEffect); err != nil {
			return err
		}
		if err := checkPublicationEffect(ctx, conn, value.Ref, value.TicketVersion, value.Fence, value.PRCreateOrUpdateEffect); err != nil {
			return err
		}
		_, err = conn.ExecContext(ctx, `INSERT INTO publication_evidence(channel,project_id,ticket_id,ticket_version,leader_epoch,runner_epoch,candidate_generation,candidate_ticket_version,candidate_leader_epoch,candidate_runner_epoch,candidate_base_sha,candidate_head_sha,candidate_tree_sha,candidate_source_digest,candidate_verification_intent_digest,candidate_proof_digest,candidate_command_policy_digest,candidate_builder_evidence_digest,candidate_builder_attempt_id,candidate_builder_attempt,candidate_commit_parent_oid,candidate_command_semantic_key,candidate_command_claim_epoch,candidate_command_ticket_version,candidate_command_leader_epoch,candidate_command_runner_epoch,candidate_command_digest,candidate_command_spec_digest,candidate_command_policy_claim_digest,candidate_command_executable_path,candidate_command_executable_digest,config_generation,config_digest,config_snapshot_digest,worktree_path,worktree_branch_ref,worktree_state,worktree_ticket_version,worktree_leader_epoch,worktree_runner_epoch,worktree_identity_json,worktree_identity_digest,worktree_base_sha,remote_branch_ref,remote_branch_oid,remote_base_oid,push_effect_semantic_key,push_effect_kind,push_effect_request_digest,push_effect_claim_epoch,push_effect_observed_identity,github_host,github_owner,github_name,github_pr_number,github_head_owner,github_head_repository,github_head_ref,github_head_oid,github_base_ref,github_base_oid,github_state,github_draft,github_factory_owned,github_observed_at,pr_effect_semantic_key,pr_effect_kind,pr_effect_request_digest,pr_effect_claim_epoch,pr_effect_observed_identity,build_transition_created_at,witness_digest,created_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(channel,project_id,ticket_id,candidate_generation,candidate_head_sha) DO NOTHING`, value.Ref.Channel, value.Ref.Project, value.Ref.Ticket, value.TicketVersion, value.Fence.LeaderEpoch, value.Fence.RunnerEpoch, value.Candidate.Snapshot.Generation, value.Candidate.TicketVersion, value.Candidate.Fence.LeaderEpoch, value.Candidate.Fence.RunnerEpoch, value.Candidate.Snapshot.BaseSHA, value.Candidate.Snapshot.HeadSHA, value.Candidate.Snapshot.TreeSHA, value.Candidate.Snapshot.SourceDigest, value.Candidate.Snapshot.VerificationIntentDigest, value.Candidate.Snapshot.ProofDigest, value.Candidate.Snapshot.CommandPolicyDigest, value.Candidate.Snapshot.BuilderEvidenceDigest, value.Candidate.BuilderResult.AttemptID, value.Candidate.BuilderResult.Attempt, value.Candidate.Commit.ParentOID, value.Candidate.CommandBinding.Key.SemanticKey, value.Candidate.CommandBinding.Key.ClaimEpoch, value.Candidate.CommandBinding.TicketVersion, value.Candidate.CommandBinding.LeaderEpoch, value.Candidate.CommandBinding.RunnerEpoch, value.Candidate.CommandBinding.CommandDigest, value.Candidate.CommandBinding.SpecDigest, value.Candidate.CommandBinding.PolicyDigest, value.Candidate.CommandBinding.ExecutablePath, value.Candidate.CommandBinding.ExecutableDigest, value.ConfigGeneration, value.ConfigDigest, value.ConfigSnapshotDigest, value.Worktree.Path, value.Worktree.Branch, value.Worktree.State, value.Worktree.TicketVersion, value.Worktree.Fence.LeaderEpoch, value.Worktree.Fence.RunnerEpoch, value.Worktree.IdentityJSON, sha256Digest(value.Worktree.IdentityJSON), value.Worktree.BaseSHA, value.RemoteBranchRef, value.RemoteBranchOID, value.RemoteBaseOID, value.PushEffect.SemanticKey, value.PushEffect.Kind, value.PushEffect.RequestDigest, value.PushEffect.ClaimEpoch, value.PushEffect.ObservedIdentity, value.PullRequest.Repository.Host, value.PullRequest.Repository.Owner, value.PullRequest.Repository.Name, value.PullRequest.Number, value.PullRequest.HeadOwner, value.PullRequest.HeadRepository, value.PullRequest.HeadRef, value.PullRequest.HeadOID, value.PullRequest.BaseRef, value.PullRequest.BaseOID, value.PullRequestState, boolInt(value.PullRequestDraft), boolInt(value.PullRequest.FactoryOwned), value.PullRequestObservedAt.Format(time.RFC3339Nano), value.PRCreateOrUpdateEffect.SemanticKey, value.PRCreateOrUpdateEffect.Kind, value.PRCreateOrUpdateEffect.RequestDigest, value.PRCreateOrUpdateEffect.ClaimEpoch, value.PRCreateOrUpdateEffect.ObservedIdentity, value.BuildTransitionCreatedAt.Format(time.RFC3339Nano), digest, value.CreatedAt.Format(time.RFC3339Nano))
		if err != nil {
			return err
		}
		stored, found, err := loadPublicationEvidenceRow(ctx, conn, value.Ref)
		if err != nil || !found {
			return ErrPublicationEvidence
		}
		if !publicationEqual(stored, value) {
			return ErrPublicationEvidence
		}
		return nil
	})
}

func (s *Store) RecordPublicationEvidence(ctx context.Context, value PublishedCandidateEvidence) error {
	return s.RecordPublishedCandidate(ctx, value)
}

func (s *Store) RecordPublishedCandidateEvidence(ctx context.Context, value PublishedCandidateEvidence) error {
	return s.RecordPublishedCandidate(ctx, value)
}

func semanticPublicationPauseTransition(transition Transition) bool {
	if transition.To != domain.StatePaused || transition.ResumeState != transition.From {
		return false
	}
	if transition.Trigger == "retry_or_correction_exhausted" {
		return transition.From == domain.StatePublishing || transition.From == domain.StateWaitingCI
	}
	// CI exhaustion is a distinct semantic pause signal. It is admitted only
	// from waiting_ci; it must not become a generic escape from publishing.
	return transition.Trigger == "ci_red_exhausted" && transition.From == domain.StateWaitingCI
}

func semanticPublicationBlockTransition(transition Transition) bool {
	return transition.Trigger == "typed_blocker" && transition.To == domain.StateBlocked && transition.ResumeState == transition.From && (transition.From == domain.StatePublishing || transition.From == domain.StateWaitingCI)
}

// TransitionPublishedBlock records a typed publication blocker only after the
// current publication witness (and, for waiting_ci, its effects event) is
// authenticated. The blocked recovery boundary then proves this exact event.
func (s *Store) TransitionPublishedBlock(ctx context.Context, transition Transition) (TransitionResult, error) {
	if transition.Ref.Validate() != nil || !semanticPublicationBlockTransition(transition) || len(transition.EventPayload) > maxEvidenceJSON {
		return TransitionResult{}, ErrPublicationEvidence
	}
	if transition.EventPayload == "" {
		transition.EventPayload = "{}"
	}
	var blocker struct {
		Code string `json:"code"`
	}
	if json.Unmarshal([]byte(transition.EventPayload), &blocker) != nil || blocker.Code == "" || !boundedText(blocker.Code, 128) {
		return TransitionResult{}, ErrPublicationEvidence
	}
	if err := s.DrainExternalMutations(ctx, transition.Ref); err != nil {
		return TransitionResult{}, err
	}
	var result TransitionResult
	err := s.write(ctx, func(conn *sql.Conn) error {
		var state domain.State
		var version, runner uint64
		if err := conn.QueryRowContext(ctx, `SELECT state,version,runner_epoch FROM tickets WHERE channel=? AND project_id=? AND id=?`, transition.Ref.Channel, transition.Ref.Project, transition.Ref.Ticket).Scan(&state, &version, &runner); err != nil || state != transition.From || version != transition.ExpectedVersion {
			return ErrStaleFence
		}
		if err := s.currentFence(ctx, conn, transition.Ref.Channel, version, runner, transition.Fence); err != nil {
			return err
		}
		publication, found, err := loadPublicationEvidenceRow(ctx, conn, transition.Ref)
		if err != nil || !found {
			return ErrPublicationEvidence
		}
		if err := loadLatestPublicationRebind(ctx, conn, &publication); err != nil {
			return err
		}
		if publication.CurrentFence.RunnerEpoch != runner || publication.CurrentFence.LeaderEpoch != transition.Fence.LeaderEpoch {
			return ErrPublicationEvidence
		}
		if state == domain.StatePublishing {
			if version != publication.CurrentTicketVersion {
				return ErrPublicationEvidence
			}
		} else {
			if publication.CurrentTicketVersion == ^uint64(0) || version != publication.CurrentTicketVersion+1 {
				return ErrPublicationEvidence
			}
			if err := authenticatePublishedWaitingEvent(ctx, conn, transition.Ref, publication, version); err != nil {
				return err
			}
		}
		if version == ^uint64(0) {
			return ErrPublicationEvidence
		}
		newVersion := version + 1
		updated, err := conn.ExecContext(ctx, `UPDATE tickets SET state='blocked',resume_state=?,blocked_code=?,version=? WHERE channel=? AND project_id=? AND id=? AND state=? AND version=? AND runner_epoch=?`, transition.From, blocker.Code, newVersion, transition.Ref.Channel, transition.Ref.Project, transition.Ref.Ticket, transition.From, version, runner)
		if err != nil {
			return err
		}
		if changed, _ := updated.RowsAffected(); changed != 1 {
			return ErrStaleFence
		}
		created, err := conn.ExecContext(ctx, `INSERT INTO events(channel,project_id,ticket_id,ticket_version,trigger,from_state,to_state,payload,created_at) VALUES(?,?,?,?,?,?,?,?,?)`, transition.Ref.Channel, transition.Ref.Project, transition.Ref.Ticket, newVersion, transition.Trigger, transition.From, domain.StateBlocked, transition.EventPayload, time.Now().UTC().Format(time.RFC3339Nano))
		if err != nil {
			return err
		}
		result.Version = newVersion
		result.EventID, _ = created.LastInsertId()
		return nil
	})
	return result, err
}

// TransitionPublishedPause atomically records the semantic retry-exhaustion
// pause for a published candidate. Unlike operator pause/take, this path does
// not advance runner_epoch and therefore must never mint a publication rebind.
// The exact source->paused event is later consumed by the paired operator
// resume/retry authentication.
func (s *Store) TransitionPublishedPause(ctx context.Context, transition Transition) (TransitionResult, error) {
	if transition.Ref.Validate() != nil || !semanticPublicationPauseTransition(transition) {
		return TransitionResult{}, ErrPublicationEvidence
	}
	if transition.EventPayload == "" {
		transition.EventPayload = "{}"
	}
	if len(transition.EventPayload) > maxEvidenceJSON || !json.Valid([]byte(transition.EventPayload)) {
		return TransitionResult{}, ErrPublicationEvidence
	}
	if err := s.DrainExternalMutations(ctx, transition.Ref); err != nil {
		return TransitionResult{}, err
	}
	var result TransitionResult
	err := s.write(ctx, func(conn *sql.Conn) error {
		var state domain.State
		var version, runner uint64
		if err := conn.QueryRowContext(ctx, `SELECT state,version,runner_epoch FROM tickets WHERE channel=? AND project_id=? AND id=?`, transition.Ref.Channel, transition.Ref.Project, transition.Ref.Ticket).Scan(&state, &version, &runner); err != nil || state != transition.From || version != transition.ExpectedVersion {
			return ErrStaleFence
		}
		if err := s.currentFence(ctx, conn, transition.Ref.Channel, version, runner, transition.Fence); err != nil {
			return err
		}
		publication, found, err := loadPublicationEvidenceRow(ctx, conn, transition.Ref)
		if err != nil || !found {
			return ErrPublicationEvidence
		}
		if err := loadLatestPublicationRebind(ctx, conn, &publication); err != nil {
			return err
		}
		if publication.CurrentFence.RunnerEpoch != runner || publication.CurrentFence.LeaderEpoch != transition.Fence.LeaderEpoch {
			return ErrPublicationEvidence
		}
		if state == domain.StatePublishing {
			if version != publication.CurrentTicketVersion {
				return ErrPublicationEvidence
			}
		} else {
			if publication.CurrentTicketVersion == ^uint64(0) || version != publication.CurrentTicketVersion+1 {
				return ErrPublicationEvidence
			}
			if err := authenticatePublishedWaitingEvent(ctx, conn, transition.Ref, publication, version); err != nil {
				return err
			}
		}
		if version == ^uint64(0) {
			return ErrPublicationEvidence
		}
		newVersion := version + 1
		updated, err := conn.ExecContext(ctx, `UPDATE tickets SET state='paused',resume_state=?,blocked_code='',version=? WHERE channel=? AND project_id=? AND id=? AND state=? AND version=? AND runner_epoch=?`, transition.From, newVersion, transition.Ref.Channel, transition.Ref.Project, transition.Ref.Ticket, transition.From, version, runner)
		if err != nil {
			return err
		}
		if changed, _ := updated.RowsAffected(); changed != 1 {
			return ErrStaleFence
		}
		created, err := conn.ExecContext(ctx, `INSERT INTO events(channel,project_id,ticket_id,ticket_version,trigger,from_state,to_state,payload,created_at) VALUES(?,?,?,?,?,?,?,?,?)`, transition.Ref.Channel, transition.Ref.Project, transition.Ref.Ticket, newVersion, transition.Trigger, transition.From, domain.StatePaused, transition.EventPayload, time.Now().UTC().Format(time.RFC3339Nano))
		if err != nil {
			return err
		}
		result.Version = newVersion
		result.EventID, _ = created.LastInsertId()
		return nil
	})
	return result, err
}

// TransitionPublishedResume atomically reopens a paused or typed-blocked
// publication ticket and authenticates the control/blocker lineage that led to
// its publication state. A witness is rebound at a runner-advanced pause
// resume; candidate-only recovery remains observation-only until the external
// publication boundary records its witness.
func (s *Store) TransitionPublishedResume(ctx context.Context, transition Transition) (TransitionResult, error) {
	pausedResume := transition.From == domain.StatePaused && (transition.Trigger == "operator_resume" || transition.Trigger == "operator_retry")
	blockedRecover := transition.From == domain.StateBlocked && transition.Trigger == "operator_recover"
	if transition.Ref.Validate() != nil || (!pausedResume && !blockedRecover) || (transition.To != domain.StatePublishing && transition.To != domain.StateWaitingCI) {
		return TransitionResult{}, ErrPublicationEvidence
	}
	if transition.EventPayload == "" {
		transition.EventPayload = "{}"
	}
	if len(transition.EventPayload) > maxEvidenceJSON || !json.Valid([]byte(transition.EventPayload)) {
		return TransitionResult{}, ErrPublicationEvidence
	}
	if err := s.DrainExternalMutations(ctx, transition.Ref); err != nil {
		return TransitionResult{}, err
	}
	var result TransitionResult
	err := s.write(ctx, func(conn *sql.Conn) error {
		var state, resumeState domain.State
		var blockedCode string
		var version, runner uint64
		if err := conn.QueryRowContext(ctx, `SELECT state,COALESCE(resume_state,''),blocked_code,version,runner_epoch FROM tickets WHERE channel=? AND project_id=? AND id=?`, transition.Ref.Channel, transition.Ref.Project, transition.Ref.Ticket).Scan(&state, &resumeState, &blockedCode, &version, &runner); err != nil || state != transition.From || version != transition.ExpectedVersion {
			return ErrStaleFence
		}
		if resumeState != transition.To {
			return ErrPublicationEvidence
		}
		if blockedRecover {
			if blockedCode == "" || version == 0 || resumeState != transition.To {
				return ErrPublicationEvidence
			}
			if !boundedText(blockedCode, 128) {
				return ErrPublicationEvidence
			}
		}
		if err := s.currentFence(ctx, conn, transition.Ref.Channel, version, runner, transition.Fence); err != nil {
			return err
		}
		if version == ^uint64(0) {
			return ErrPublicationEvidence
		}
		newVersion := version + 1
		newFence := domain.Fence{LeaderEpoch: transition.Fence.LeaderEpoch, RunnerEpoch: runner}
		if blockedRecover {
			payload, err := blockedResumeEventPayload(blockedCode)
			if err != nil {
				return ErrPublicationEvidence
			}
			transition.EventPayload = payload
		}
		updated, err := conn.ExecContext(ctx, `UPDATE tickets SET state=?,resume_state=NULL,blocked_code='',version=? WHERE channel=? AND project_id=? AND id=? AND state=? AND version=? AND runner_epoch=?`, transition.To, newVersion, transition.Ref.Channel, transition.Ref.Project, transition.Ref.Ticket, transition.From, version, runner)
		if err != nil {
			return err
		}
		if changed, _ := updated.RowsAffected(); changed != 1 {
			return ErrStaleFence
		}
		stamp := time.Now().UTC().Format(time.RFC3339Nano)
		operatorEvent, err := conn.ExecContext(ctx, `INSERT INTO events(channel,project_id,ticket_id,ticket_version,trigger,from_state,to_state,payload,created_at) VALUES(?,?,?,?,?,?,?,?,?)`, transition.Ref.Channel, transition.Ref.Project, transition.Ref.Ticket, newVersion, transition.Trigger, transition.From, transition.To, transition.EventPayload, stamp)
		if err != nil {
			return err
		}
		result.EventID, _ = operatorEvent.LastInsertId()
		if blockedRecover {
			if err := authenticateBlockedResume(ctx, conn, transition.Ref, version, resumeState, blockedCode, transition.To); err != nil {
				return err
			}
		}
		publication, found, err := loadPublicationEvidenceRow(ctx, conn, transition.Ref)
		if err != nil {
			return ErrPublicationEvidence
		}
		semanticResume := pausedResume && authenticateSemanticPublicationResume(ctx, conn, transition.Ref, version, newVersion, transition.To) == nil
		if semanticResume {
			if !found {
				return ErrPublicationEvidence
			}
			if err := loadLatestPublicationRebind(ctx, conn, &publication); err != nil {
				return err
			}
			if publication.CurrentFence.RunnerEpoch != runner || publication.CurrentFence.LeaderEpoch != transition.Fence.LeaderEpoch {
				return ErrPublicationEvidence
			}
			if transition.To == domain.StatePublishing {
				if publication.CurrentTicketVersion+1 != version {
					return ErrPublicationEvidence
				}
			} else {
				if publication.CurrentTicketVersion > ^uint64(0)-3 || publication.CurrentTicketVersion+3 != newVersion {
					return ErrPublicationEvidence
				}
				if err := authenticatePublishedWaitingEvent(ctx, conn, transition.Ref, publication, publication.CurrentTicketVersion+1); err != nil {
					return err
				}
			}
			// Semantic pause/resume keeps the same runner and has no rebind row.
			result.Version = newVersion
			return nil
		}
		if blockedRecover {
			if !found {
				if transition.To != domain.StatePublishing {
					return ErrPublicationEvidence
				}
				candidate, candidateErr := s.latestCandidateFrom(ctx, conn, transition.Ref, false)
				if candidateErr != nil || candidate.TicketVersion == ^uint64(0) || candidate.TicketVersion+2 != version {
					return ErrPublicationEvidence
				}
				var transitions int
				if err := conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version=? AND trigger='phase_pass' AND from_state='building' AND to_state='publishing'`, transition.Ref.Channel, transition.Ref.Project, transition.Ref.Ticket, candidate.TicketVersion+1).Scan(&transitions); err != nil || transitions != 1 {
					return ErrPublicationEvidence
				}
				provider, _, providerErr := s.loadHistoricalProviderAttemptResult(ctx, conn, candidate.BuilderResult)
				if providerErr != nil || providerResultReachesFence(ctx, conn, candidate.BuilderResult, provider, version-1, domain.Fence{LeaderEpoch: transition.Fence.LeaderEpoch, RunnerEpoch: runner}) != nil {
					return ErrPublicationEvidence
				}
			} else {
				if err := loadLatestPublicationRebind(ctx, conn, &publication); err != nil {
					return err
				}
				if transition.To == domain.StatePublishing {
					if resumeState != domain.StatePublishing || publication.CurrentTicketVersion+1 != version || publication.CurrentFence.RunnerEpoch != runner {
						return ErrPublicationEvidence
					}
				} else {
					if resumeState != domain.StateWaitingCI || publication.CurrentTicketVersion+2 != version {
						return ErrPublicationEvidence
					}
					if err := authenticatePublishedWaitingEvent(ctx, conn, transition.Ref, publication, publication.CurrentTicketVersion+1); err != nil {
						return err
					}
				}
			}
		} else if transition.To == domain.StateWaitingCI {
			if !found {
				return ErrPublicationEvidence
			}
			if err := loadLatestPublicationRebind(ctx, conn, &publication); err != nil {
				return err
			}
			if publication.CurrentTicketVersion == ^uint64(0) || publication.CurrentFence.RunnerEpoch == 0 || publication.CurrentFence.LeaderEpoch == 0 || newVersion <= publication.CurrentTicketVersion+1 {
				return ErrPublicationEvidence
			}
			if err := validateRunnerControlAdvance(ctx, conn, transition.Ref, publication.CurrentTicketVersion+1, publication.CurrentFence.RunnerEpoch, publication.CurrentFence.LeaderEpoch, newVersion, newFence.RunnerEpoch, newFence.LeaderEpoch); err != nil {
				return ErrPublicationEvidence
			}
		} else if found {
			if err := loadLatestPublicationRebind(ctx, conn, &publication); err != nil {
				return err
			}
			priorVersion, priorFence, priorDigest := publication.TicketVersion, publication.Fence, publication.WitnessDigest
			if publication.CurrentTicketVersion != 0 && publication.CurrentTicketVersion != publication.TicketVersion {
				priorVersion, priorFence = publication.CurrentTicketVersion, publication.CurrentFence
				if err := conn.QueryRowContext(ctx, `SELECT rebind_digest FROM publication_evidence_rebinds WHERE channel=? AND project_id=? AND ticket_id=? AND candidate_generation=? AND candidate_head_sha=? AND ticket_version=?`, transition.Ref.Channel, transition.Ref.Project, transition.Ref.Ticket, publication.Candidate.Snapshot.Generation, publication.Candidate.Snapshot.HeadSHA, priorVersion).Scan(&priorDigest); err != nil {
					return ErrPublicationEvidence
				}
			}
			if err := validatePublicationAdvance(ctx, conn, transition.Ref, priorVersion, priorFence, newVersion, newFence); err != nil {
				return err
			}
			value := PublicationRebind{Ref: transition.Ref, CandidateGeneration: publication.Candidate.Snapshot.Generation, CandidateHeadOID: publication.Candidate.Snapshot.HeadSHA, PriorWitnessDigest: priorDigest, PriorTicketVersion: priorVersion, PriorFence: priorFence, TicketVersion: newVersion, Fence: newFence, CreatedAt: time.Now().UTC()}
			payload, err := publicationRebindPayload(value)
			if err != nil {
				return ErrPublicationEvidence
			}
			value.RebindDigest = publicationIdentityDigest(payload)
			var count int
			if err := conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM publication_evidence_rebinds WHERE channel=? AND project_id=? AND ticket_id=? AND candidate_generation=? AND candidate_head_sha=?`, transition.Ref.Channel, transition.Ref.Project, transition.Ref.Ticket, value.CandidateGeneration, value.CandidateHeadOID).Scan(&count); err != nil || count >= 64 {
				return ErrPublicationEvidence
			}
			if _, err := conn.ExecContext(ctx, `INSERT INTO publication_evidence_rebinds(channel,project_id,ticket_id,candidate_generation,candidate_head_sha,prior_witness_digest,prior_ticket_version,prior_leader_epoch,prior_runner_epoch,ticket_version,leader_epoch,runner_epoch,rebind_digest,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, transition.Ref.Channel, transition.Ref.Project, transition.Ref.Ticket, value.CandidateGeneration, value.CandidateHeadOID, value.PriorWitnessDigest, value.PriorTicketVersion, value.PriorFence.LeaderEpoch, value.PriorFence.RunnerEpoch, value.TicketVersion, value.Fence.LeaderEpoch, value.Fence.RunnerEpoch, value.RebindDigest, value.CreatedAt.Format(time.RFC3339Nano)); err != nil {
				return err
			}
			if _, err := conn.ExecContext(ctx, `INSERT INTO events(channel,project_id,ticket_id,ticket_version,trigger,from_state,to_state,payload,created_at) VALUES(?,?,?,?,?,?,?,?,?)`, transition.Ref.Channel, transition.Ref.Project, transition.Ref.Ticket, newVersion, "publication_rebind", domain.StatePublishing, domain.StatePublishing, string(payload), value.CreatedAt.Format(time.RFC3339Nano)); err != nil {
				return err
			}
		} else {
			candidate, candidateErr := s.latestCandidateFrom(ctx, conn, transition.Ref, false)
			if candidateErr != nil || candidate.TicketVersion == ^uint64(0) {
				return ErrPublicationEvidence
			}
			var transitions int
			if err := conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version=? AND trigger='phase_pass' AND from_state='building' AND to_state='publishing'`, transition.Ref.Channel, transition.Ref.Project, transition.Ref.Ticket, candidate.TicketVersion+1).Scan(&transitions); err != nil || transitions != 1 {
				return ErrPublicationEvidence
			}
			provider, _, providerErr := s.loadHistoricalProviderAttemptResult(ctx, conn, candidate.BuilderResult)
			if providerErr != nil || providerResultReachesFence(ctx, conn, candidate.BuilderResult, provider, newVersion, newFence) != nil {
				return ErrPublicationEvidence
			}
		}
		result.Version = newVersion
		return nil
	})
	return result, err
}

// authenticateBlockedResume binds the operator recovery to the exact typed
// blocker that put this ticket in blocked. A caller cannot manufacture a
// publication resume by merely naming a stored resume_state: both lifecycle
// events and the blocker code must agree at adjacent ticket versions.
func authenticateBlockedResume(ctx context.Context, q interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, ref domain.TicketRef, blockedVersion uint64, resumeState domain.State, blockedCode string, target domain.State) error {
	if blockedVersion == ^uint64(0) || !resumeTargetState(target) || resumeState != target || blockedCode == "" {
		return ErrPublicationEvidence
	}
	var count int
	if err := q.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version=?`, ref.Channel, ref.Project, ref.Ticket, blockedVersion).Scan(&count); err != nil || count != 1 {
		return ErrPublicationEvidence
	}
	var trigger string
	var from, to domain.State
	var payload string
	if err := q.QueryRowContext(ctx, `SELECT trigger,from_state,to_state,payload FROM events WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version=?`, ref.Channel, ref.Project, ref.Ticket, blockedVersion).Scan(&trigger, &from, &to, &payload); err != nil || trigger != "typed_blocker" || from != resumeState || to != domain.StateBlocked {
		return ErrPublicationEvidence
	}
	var blocker struct {
		Code string `json:"code"`
	}
	if len(payload) > maxEvidenceJSON || json.Unmarshal([]byte(payload), &blocker) != nil || blocker.Code != blockedCode {
		return ErrPublicationEvidence
	}
	if err := q.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version=?`, ref.Channel, ref.Project, ref.Ticket, blockedVersion+1).Scan(&count); err != nil || count != 1 {
		return ErrPublicationEvidence
	}
	var resumePayload string
	if err := q.QueryRowContext(ctx, `SELECT payload FROM events WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version=? AND trigger='operator_recover' AND from_state='blocked' AND to_state=?`, ref.Channel, ref.Project, ref.Ticket, blockedVersion+1, target).Scan(&resumePayload); err != nil {
		return ErrPublicationEvidence
	}
	expectedResumePayload, err := blockedResumeEventPayload(blocker.Code)
	if err != nil || resumePayload != expectedResumePayload {
		return ErrPublicationEvidence
	}
	if err := q.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version=? AND trigger='operator_recover' AND from_state='blocked' AND to_state=?`, ref.Channel, ref.Project, ref.Ticket, blockedVersion+1, target).Scan(&count); err != nil || count != 1 {
		return ErrPublicationEvidence
	}
	return nil
}

func blockedResumeEventPayload(code string) (string, error) {
	if !boundedText(code, 128) {
		return "", ErrPublicationEvidence
	}
	payload, err := json.Marshal(struct {
		BlockerCode string `json:"blocker_code"`
	}{code})
	return string(payload), err
}

// authenticateSemanticPublicationResume proves the non-runner-advancing
// retry-exhaustion pause and its exact operator continuation. This is kept
// separate from validateRunnerControlAdvance: semantic phase pauses invalidate
// the old phase baseline and may not be mistaken for a pause/take handoff.
func authenticateSemanticPublicationResume(ctx context.Context, q interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, ref domain.TicketRef, pausedVersion, resumedVersion uint64, target domain.State) error {
	if pausedVersion == 0 || pausedVersion == ^uint64(0) || resumedVersion != pausedVersion+1 || !resumeTargetState(target) {
		return ErrPublicationEvidence
	}
	var count int
	if err := q.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version=?`, ref.Channel, ref.Project, ref.Ticket, pausedVersion).Scan(&count); err != nil || count != 1 {
		return ErrPublicationEvidence
	}
	var trigger string
	var from, to domain.State
	var payload string
	if err := q.QueryRowContext(ctx, `SELECT trigger,from_state,to_state,payload FROM events WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version=?`, ref.Channel, ref.Project, ref.Ticket, pausedVersion).Scan(&trigger, &from, &to, &payload); err != nil || (trigger != "retry_or_correction_exhausted" && trigger != "ci_red_exhausted") || to != domain.StatePaused || from != target || len(payload) > maxEvidenceJSON || !json.Valid([]byte(payload)) {
		return ErrPublicationEvidence
	}
	if err := q.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version=?`, ref.Channel, ref.Project, ref.Ticket, resumedVersion).Scan(&count); err != nil || count != 1 {
		return ErrPublicationEvidence
	}
	if err := q.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version=? AND trigger IN ('operator_resume','operator_retry') AND from_state='paused' AND to_state=?`, ref.Channel, ref.Project, ref.Ticket, resumedVersion, target).Scan(&count); err != nil || count != 1 {
		return ErrPublicationEvidence
	}
	return nil
}

func authenticateBlockedPublicationResume(ctx context.Context, q interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, ref domain.TicketRef, blockedVersion, resumedVersion uint64, source, target domain.State) error {
	if blockedVersion == ^uint64(0) || resumedVersion != blockedVersion+1 || !resumeTargetState(target) || source != domain.StatePublishing && source != domain.StateWaitingCI {
		return ErrPublicationEvidence
	}
	var count int
	if err := q.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version=?`, ref.Channel, ref.Project, ref.Ticket, blockedVersion).Scan(&count); err != nil || count != 1 {
		return ErrPublicationEvidence
	}
	var trigger string
	var from, to domain.State
	var payload string
	if err := q.QueryRowContext(ctx, `SELECT trigger,from_state,to_state,payload FROM events WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version=?`, ref.Channel, ref.Project, ref.Ticket, blockedVersion).Scan(&trigger, &from, &to, &payload); err != nil || trigger != "typed_blocker" || from != source || to != domain.StateBlocked {
		return ErrPublicationEvidence
	}
	var blocker struct {
		Code string `json:"code"`
	}
	if len(payload) > maxEvidenceJSON || json.Unmarshal([]byte(payload), &blocker) != nil || blocker.Code == "" {
		return ErrPublicationEvidence
	}
	if !boundedText(blocker.Code, 128) {
		return ErrPublicationEvidence
	}
	var resumePayload string
	if err := q.QueryRowContext(ctx, `SELECT payload FROM events WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version=? AND trigger='operator_recover' AND from_state='blocked' AND to_state=?`, ref.Channel, ref.Project, ref.Ticket, resumedVersion, target).Scan(&resumePayload); err != nil {
		return ErrPublicationEvidence
	}
	expectedResumePayload, err := blockedResumeEventPayload(blocker.Code)
	if err != nil || resumePayload != expectedResumePayload {
		return ErrPublicationEvidence
	}
	if err := q.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version=? AND trigger='operator_recover' AND from_state='blocked' AND to_state=?`, ref.Channel, ref.Project, ref.Ticket, resumedVersion, target).Scan(&count); err != nil || count != 1 {
		return ErrPublicationEvidence
	}
	if err := q.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version=?`, ref.Channel, ref.Project, ref.Ticket, resumedVersion).Scan(&count); err != nil || count != 1 {
		return ErrPublicationEvidence
	}
	return nil
}

func authenticatePublishedWaitingEvent(ctx context.Context, q interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, ref domain.TicketRef, value PublishedCandidateEvidence, version uint64) error {
	payload, err := json.Marshal(struct {
		WitnessDigest    string `json:"witness_digest"`
		WitnessCreatedAt string `json:"witness_created_at"`
	}{value.WitnessDigest, value.CreatedAt.Format(time.RFC3339Nano)})
	if err != nil {
		return ErrPublicationEvidence
	}
	var count int
	if err := q.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version=? AND trigger='effects_confirmed' AND from_state='publishing' AND to_state='waiting_ci' AND payload=?`, ref.Channel, ref.Project, ref.Ticket, version, string(payload)).Scan(&count); err != nil || count != 1 {
		return ErrPublicationEvidence
	}
	if err := q.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version=?`, ref.Channel, ref.Project, ref.Ticket, version).Scan(&count); err != nil || count != 1 {
		return ErrPublicationEvidence
	}
	return nil
}

// TransitionPublishedCandidate is the only Store boundary that may consume a
// publication witness. It validates the exact immutable witness and both
// confirmed effects, then commits publishing -> waiting_ci and its event in
// one transaction. A lost response is authenticated as an exact replay.
func (s *Store) TransitionPublishedCandidate(ctx context.Context, transition Transition) (TransitionResult, error) {
	if transition.From != domain.StatePublishing || transition.To != domain.StateWaitingCI || transition.Trigger != "effects_confirmed" || transition.Ref.Validate() != nil {
		return TransitionResult{}, ErrPublicationEvidence
	}
	var result TransitionResult
	err := s.write(ctx, func(conn *sql.Conn) error {
		value, found, err := loadPublicationEvidenceRow(ctx, conn, transition.Ref)
		if err != nil || !found {
			return ErrPublicationEvidence
		}
		if err := loadLatestPublicationRebind(ctx, conn, &value); err != nil {
			return err
		}
		if err := checkPublicationEffect(ctx, conn, transition.Ref, value.TicketVersion, value.Fence, value.PushEffect); err != nil {
			return err
		}
		if err := checkPublicationEffect(ctx, conn, transition.Ref, value.TicketVersion, value.Fence, value.PRCreateOrUpdateEffect); err != nil {
			return err
		}
		var buildEvents int
		if err := conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version=? AND trigger='phase_pass' AND from_state='building' AND to_state='publishing' AND created_at=?`, transition.Ref.Channel, transition.Ref.Project, transition.Ref.Ticket, value.TicketVersion, value.BuildTransitionCreatedAt.Format(time.RFC3339Nano)).Scan(&buildEvents); err != nil || buildEvents != 1 {
			return ErrPublicationEvidence
		}
		payloadBytes, err := json.Marshal(struct {
			WitnessDigest    string `json:"witness_digest"`
			WitnessCreatedAt string `json:"witness_created_at"`
		}{value.WitnessDigest, value.CreatedAt.Format(time.RFC3339Nano)})
		if err != nil {
			return ErrPublicationEvidence
		}
		payload := string(payloadBytes)
		var state domain.State
		var version, runner uint64
		if err := conn.QueryRowContext(ctx, `SELECT state,version,runner_epoch FROM tickets WHERE channel=? AND project_id=? AND id=?`, transition.Ref.Channel, transition.Ref.Project, transition.Ref.Ticket).Scan(&state, &version, &runner); err != nil {
			return err
		}
		if state == domain.StateWaitingCI && version == transition.ExpectedVersion+1 {
			if runner != value.CurrentFence.RunnerEpoch || transition.Fence != value.CurrentFence {
				return ErrStaleFence
			}
			var leader uint64
			if err := conn.QueryRowContext(ctx, `SELECT leader_epoch FROM daemon_instances WHERE channel=?`, transition.Ref.Channel).Scan(&leader); err != nil || leader != transition.Fence.LeaderEpoch {
				return ErrStaleFence
			}
			var eventID int64
			var eventCreated, digest, witnessCreated string
			err := conn.QueryRowContext(ctx, `SELECT e.id,e.created_at,p.witness_digest,p.witness_created_at FROM events e JOIN publication_transition_evidence p ON p.channel=e.channel AND p.project_id=e.project_id AND p.ticket_id=e.ticket_id AND p.ticket_version=e.ticket_version AND p.event_created_at=e.created_at WHERE e.channel=? AND e.project_id=? AND e.ticket_id=? AND e.ticket_version=? AND e.trigger='effects_confirmed' AND e.from_state='publishing' AND e.to_state='waiting_ci' AND e.payload=?`, transition.Ref.Channel, transition.Ref.Project, transition.Ref.Ticket, version, payload).Scan(&eventID, &eventCreated, &digest, &witnessCreated)
			if err != nil || digest != value.WitnessDigest || witnessCreated != value.CreatedAt.Format(time.RFC3339Nano) || eventCreated == "" {
				return ErrPublicationEvidence
			}
			result.Version, result.EventID = version, eventID
			return nil
		}
		if state != domain.StatePublishing || version != transition.ExpectedVersion || version != value.CurrentTicketVersion || runner != value.CurrentFence.RunnerEpoch {
			return ErrStaleFence
		}
		if err := s.currentFence(ctx, conn, transition.Ref.Channel, version, runner, transition.Fence); err != nil {
			return err
		}
		if transition.Fence != value.CurrentFence {
			return ErrPublicationEvidence
		}
		now := time.Now().UTC()
		created := now.Format(time.RFC3339Nano)
		updated, err := conn.ExecContext(ctx, `UPDATE tickets SET state='waiting_ci',resume_state=NULL,version=version+1 WHERE channel=? AND project_id=? AND id=? AND state='publishing' AND version=? AND runner_epoch=?`, transition.Ref.Channel, transition.Ref.Project, transition.Ref.Ticket, version, runner)
		if err != nil {
			return err
		}
		if n, _ := updated.RowsAffected(); n != 1 {
			return ErrStaleFence
		}
		event, err := conn.ExecContext(ctx, `INSERT INTO events(channel,project_id,ticket_id,ticket_version,trigger,from_state,to_state,payload,created_at) VALUES(?,?,?,?,?,?,?,?,?)`, transition.Ref.Channel, transition.Ref.Project, transition.Ref.Ticket, version+1, "effects_confirmed", domain.StatePublishing, domain.StateWaitingCI, payload, created)
		if err != nil {
			return err
		}
		if _, err := conn.ExecContext(ctx, `INSERT INTO publication_transition_evidence(channel,project_id,ticket_id,witness_digest,witness_created_at,ticket_version,event_created_at) VALUES(?,?,?,?,?,?,?)`, transition.Ref.Channel, transition.Ref.Project, transition.Ref.Ticket, value.WitnessDigest, value.CreatedAt.Format(time.RFC3339Nano), version+1, created); err != nil {
			return err
		}
		result.Version = version + 1
		result.EventID, _ = event.LastInsertId()
		return nil
	})
	return result, err
}

// RebindRecoveredPublishedCandidates is the production startup fence for the
// publish boundary. It runs after FenceRecoveredRunners: every live
// publishing ticket must append (or exactly replay) its recovery rebind and
// then pass the full LoadPublishedCandidate authentication; waiting_ci rows
// must pass that same load before the daemon can continue either state.
func (s *Store) RebindRecoveredPublishedCandidates(ctx context.Context, channel domain.Channel, leaderEpoch uint64) error {
	if !channel.Valid() || leaderEpoch == 0 {
		return ErrPublicationEvidence
	}
	rows, err := s.db.QueryContext(ctx, `SELECT project_id,id,state,version,runner_epoch FROM tickets WHERE channel=? AND state IN ('publishing','waiting_ci') ORDER BY project_id,id`, channel)
	if err != nil {
		return err
	}
	type recovered struct {
		ref             domain.TicketRef
		state           domain.State
		version, runner uint64
	}
	var pending []recovered
	for rows.Next() {
		var project domain.ProjectID
		var ticket domain.TicketID
		var state domain.State
		var version, runner uint64
		if err := rows.Scan(&project, &ticket, &state, &version, &runner); err != nil {
			rows.Close()
			return err
		}
		pending = append(pending, recovered{domain.TicketRef{Channel: channel, Project: project, Ticket: ticket}, state, version, runner})
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	rows.Close()
	for _, item := range pending {
		if item.state == domain.StatePublishing {
			if err := s.RebindPublishedCandidate(ctx, item.ref, item.version, domain.Fence{LeaderEpoch: leaderEpoch, RunnerEpoch: item.runner}); err != nil {
				if !errors.Is(err, ErrNotFound) {
					return err
				}
				// A crash can leave the authenticated candidate and the
				// building->publishing event durable before the external publication
				// witness exists. Authenticate that candidate through the live
				// recovery/control chain and leave it available for publication; an
				// absent witness is not itself a startup failure.
				if err := s.validateCandidateOnlyPublishingRecovery(ctx, item.ref, item.version, domain.Fence{LeaderEpoch: leaderEpoch, RunnerEpoch: item.runner}); err != nil {
					return err
				}
				continue
			}
		}
		if _, err := s.LoadPublishedCandidate(ctx, item.ref); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) validateCandidateOnlyPublishingRecovery(ctx context.Context, ref domain.TicketRef, version uint64, fence domain.Fence) error {
	candidate, err := s.latestCandidateFrom(ctx, s.db, ref, false)
	if err != nil || candidate.TicketVersion == ^uint64(0) {
		return ErrPublicationEvidence
	}
	var transitions int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version=? AND trigger='phase_pass' AND from_state='building' AND to_state='publishing'`, ref.Channel, ref.Project, ref.Ticket, candidate.TicketVersion+1).Scan(&transitions); err != nil || transitions != 1 {
		return ErrPublicationEvidence
	}
	provider, _, err := s.loadHistoricalProviderAttemptResult(ctx, s.db, candidate.BuilderResult)
	if err != nil || providerResultReachesFence(ctx, s.db, candidate.BuilderResult, provider, version, fence) != nil {
		return ErrPublicationEvidence
	}
	return nil
}

// RebindPublishedCandidate records one controlled runner/leader recovery for
// a publication that is still in publishing. It does not change ticket state;
// the recovery coordinator must perform that lifecycle operation separately.
// Rebinding is a strict +1 runner/version handoff and is itself evented so a
// loader never infers recovery from a changed epoch alone.
func (s *Store) RebindPublishedCandidate(ctx context.Context, ref domain.TicketRef, currentVersion uint64, currentFence domain.Fence) error {
	if err := ref.Validate(); err != nil || currentVersion == 0 || currentFence.LeaderEpoch == 0 || currentFence.RunnerEpoch == 0 || currentFence.ClaimEpoch != 0 {
		return ErrPublicationEvidence
	}
	return s.write(ctx, func(conn *sql.Conn) error {
		row, found, err := loadPublicationEvidenceRow(ctx, conn, ref)
		if err != nil || !found {
			return ErrNotFound
		}
		if err := loadLatestPublicationRebind(ctx, conn, &row); err != nil {
			return err
		}
		// A typed blocker/recover pair does not advance runner_epoch and cannot
		// be represented as a publication_rebind row (the schema requires a
		// +1 runner). Authenticate that pair directly; LoadPublishedCandidate
		// will continue to use the original witness at the resumed publishing
		// fence.
		if currentVersion == row.CurrentTicketVersion+2 {
			var state domain.State
			var version, runner, leader uint64
			if err := conn.QueryRowContext(ctx, `SELECT t.state,t.version,t.runner_epoch,d.leader_epoch FROM tickets t JOIN daemon_instances d ON d.channel=t.channel WHERE t.channel=? AND t.project_id=? AND t.id=?`, ref.Channel, ref.Project, ref.Ticket).Scan(&state, &version, &runner, &leader); err != nil {
				return err
			}
			if state == domain.StatePublishing && version == currentVersion && runner == currentFence.RunnerEpoch && leader == currentFence.LeaderEpoch && authenticateBlockedPublicationResume(ctx, conn, ref, row.CurrentTicketVersion+1, currentVersion, domain.StatePublishing, domain.StatePublishing) == nil {
				return nil
			}
			if state == domain.StatePublishing && version == currentVersion && runner == currentFence.RunnerEpoch && leader == currentFence.LeaderEpoch && authenticateSemanticPublicationResume(ctx, conn, ref, row.CurrentTicketVersion+1, currentVersion, domain.StatePublishing) == nil {
				return nil
			}
		}
		// A lost response can replay an already persisted rebind, including the
		// 64th row. Authenticate that exact row before applying the insertion cap.
		if existing, found, err := loadPublicationRebindAt(ctx, conn, ref, row.Candidate.Snapshot.Generation, row.Candidate.Snapshot.HeadSHA, currentVersion); err != nil {
			return err
		} else if found {
			priorVersion, priorFence, priorDigest := row.TicketVersion, row.Fence, row.WitnessDigest
			if currentVersion != row.TicketVersion {
				prior, priorFound, err := loadPublicationRebindBefore(ctx, conn, ref, row.Candidate.Snapshot.Generation, row.Candidate.Snapshot.HeadSHA, currentVersion)
				if err != nil {
					return err
				}
				if priorFound {
					priorVersion, priorFence, priorDigest = prior.TicketVersion, prior.Fence, prior.RebindDigest
				}
				normalRecovery := existing.PriorTicketVersion == priorVersion && existing.PriorFence == priorFence && existing.TicketVersion == priorVersion+1
				pairRecovery := existing.PriorTicketVersion == priorVersion+2 && existing.PriorFence == priorFence && existing.TicketVersion == priorVersion+3
				if pairRecovery {
					if authenticateBlockedPublicationResume(ctx, conn, ref, priorVersion+1, priorVersion+2, domain.StatePublishing, domain.StatePublishing) != nil && authenticateSemanticPublicationResume(ctx, conn, ref, priorVersion+1, priorVersion+2, domain.StatePublishing) != nil {
						return ErrPublicationEvidence
					}
					priorVersion, priorFence = existing.PriorTicketVersion, existing.PriorFence
				} else if !normalRecovery {
					return ErrPublicationEvidence
				}
			}
			if err := validatePublicationAdvance(ctx, conn, ref, priorVersion, priorFence, currentVersion, existing.Fence); err != nil {
				return err
			}
			expected := PublicationRebind{Ref: ref, CandidateGeneration: row.Candidate.Snapshot.Generation, CandidateHeadOID: row.Candidate.Snapshot.HeadSHA, PriorWitnessDigest: priorDigest, PriorTicketVersion: priorVersion, PriorFence: priorFence, TicketVersion: existing.TicketVersion, Fence: existing.Fence, CreatedAt: existing.CreatedAt}
			payload, err := publicationRebindPayload(expected)
			if err != nil || existing.PriorWitnessDigest != expected.PriorWitnessDigest || existing.PriorTicketVersion != expected.PriorTicketVersion || existing.PriorFence != expected.PriorFence || existing.RebindDigest != publicationIdentityDigest(payload) || existing.Fence != currentFence {
				return ErrPublicationEvidence
			}
			var state string
			var version, runner, leader uint64
			if err := conn.QueryRowContext(ctx, `SELECT t.state,t.version,t.runner_epoch,d.leader_epoch FROM tickets t JOIN daemon_instances d ON d.channel=t.channel WHERE t.channel=? AND t.project_id=? AND t.id=?`, ref.Channel, ref.Project, ref.Ticket).Scan(&state, &version, &runner, &leader); err != nil || state != string(domain.StatePublishing) || version != currentVersion || runner != currentFence.RunnerEpoch || leader != currentFence.LeaderEpoch {
				return ErrStaleFence
			}
			var events int
			if err := conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version=? AND trigger='publication_rebind' AND from_state='publishing' AND to_state='publishing' AND payload=? AND created_at=?`, ref.Channel, ref.Project, ref.Ticket, currentVersion, string(payload), existing.CreatedAt.Format(time.RFC3339Nano)).Scan(&events); err != nil || events != 1 {
				return ErrPublicationEvidence
			}
			return nil
		}
		priorVersion, priorFence := row.TicketVersion, row.Fence
		priorDigest := row.WitnessDigest
		pairRecovery := false
		// A blocked/semantic publishing resume leaves the witness at N while the
		// resumed ticket is N+2. After a daemon takeover FenceRecoveredRunners
		// appends the signed N+3 recovery row; bind the publication to that exact
		// endpoint instead of treating the +2 pair as a rebind or counter gap.
		if currentVersion == row.CurrentTicketVersion+3 && currentFence.RunnerEpoch == row.CurrentFence.RunnerEpoch+1 {
			var state string
			var version, runner, leader uint64
			if err := conn.QueryRowContext(ctx, `SELECT t.state,t.version,t.runner_epoch,d.leader_epoch FROM tickets t JOIN daemon_instances d ON d.channel=t.channel WHERE t.channel=? AND t.project_id=? AND t.id=?`, ref.Channel, ref.Project, ref.Ticket).Scan(&state, &version, &runner, &leader); err != nil || state != string(domain.StatePublishing) || version != currentVersion || runner != currentFence.RunnerEpoch || leader != currentFence.LeaderEpoch {
				return ErrStaleFence
			}
			if authenticateBlockedPublicationResume(ctx, conn, ref, row.CurrentTicketVersion+1, row.CurrentTicketVersion+2, domain.StatePublishing, domain.StatePublishing) != nil && authenticateSemanticPublicationResume(ctx, conn, ref, row.CurrentTicketVersion+1, row.CurrentTicketVersion+2, domain.StatePublishing) != nil {
				return ErrPublicationEvidence
			}
			if err := authenticateRunnerRecoveryStep(ctx, conn, ref, row.CurrentTicketVersion+2, row.CurrentFence, currentVersion, currentFence); err != nil {
				return ErrPublicationEvidence
			}
			priorVersion, priorFence, pairRecovery = row.CurrentTicketVersion+2, row.CurrentFence, true
		}
		if row.CurrentTicketVersion != 0 && row.CurrentTicketVersion != row.TicketVersion {
			if pairRecovery {
				// The pair endpoint above is the predecessor; no publication rebind
				// digest exists at N+2, so the immutable witness remains the anchor.
				priorDigest = row.WitnessDigest
			} else {
				priorVersion, priorFence = row.CurrentTicketVersion, row.CurrentFence
				if err := conn.QueryRowContext(ctx, `SELECT rebind_digest FROM publication_evidence_rebinds WHERE channel=? AND project_id=? AND ticket_id=? AND candidate_generation=? AND candidate_head_sha=? AND ticket_version=?`, ref.Channel, ref.Project, ref.Ticket, row.Candidate.Snapshot.Generation, row.Candidate.Snapshot.HeadSHA, priorVersion).Scan(&priorDigest); err != nil {
					return ErrPublicationEvidence
				}
			}
		}
		var state string
		var version, runner, leader uint64
		if err := conn.QueryRowContext(ctx, `SELECT t.state,t.version,t.runner_epoch,d.leader_epoch FROM tickets t JOIN daemon_instances d ON d.channel=t.channel WHERE t.channel=? AND t.project_id=? AND t.id=?`, ref.Channel, ref.Project, ref.Ticket).Scan(&state, &version, &runner, &leader); err != nil {
			return err
		}
		if state != string(domain.StatePublishing) || version != currentVersion || runner != currentFence.RunnerEpoch || leader != currentFence.LeaderEpoch {
			return ErrStaleFence
		}
		if err := validatePublicationAdvance(ctx, conn, ref, priorVersion, priorFence, currentVersion, currentFence); err != nil {
			return err
		}
		candidateHead := row.Candidate.Snapshot.HeadSHA
		value := PublicationRebind{Ref: ref, CandidateGeneration: row.Candidate.Snapshot.Generation, CandidateHeadOID: candidateHead, PriorWitnessDigest: priorDigest, PriorTicketVersion: priorVersion, PriorFence: priorFence, TicketVersion: currentVersion, Fence: currentFence, CreatedAt: time.Now().UTC()}
		payload, err := publicationRebindPayload(value)
		if err != nil {
			return ErrPublicationEvidence
		}
		value.RebindDigest = publicationIdentityDigest(payload)
		var existing string
		err = conn.QueryRowContext(ctx, `SELECT rebind_digest FROM publication_evidence_rebinds WHERE channel=? AND project_id=? AND ticket_id=? AND candidate_generation=? AND candidate_head_sha=? AND ticket_version=?`, ref.Channel, ref.Project, ref.Ticket, value.CandidateGeneration, value.CandidateHeadOID, value.TicketVersion).Scan(&existing)
		if err == nil {
			if existing != value.RebindDigest {
				return ErrPublicationEvidence
			}
			return nil
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		var rebindCount int
		if err := conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM publication_evidence_rebinds WHERE channel=? AND project_id=? AND ticket_id=? AND candidate_generation=? AND candidate_head_sha=?`, ref.Channel, ref.Project, ref.Ticket, value.CandidateGeneration, value.CandidateHeadOID).Scan(&rebindCount); err != nil {
			return err
		}
		if rebindCount >= 64 {
			return ErrPublicationEvidence
		}
		if _, err := conn.ExecContext(ctx, `INSERT INTO publication_evidence_rebinds(channel,project_id,ticket_id,candidate_generation,candidate_head_sha,prior_witness_digest,prior_ticket_version,prior_leader_epoch,prior_runner_epoch,ticket_version,leader_epoch,runner_epoch,rebind_digest,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, ref.Channel, ref.Project, ref.Ticket, value.CandidateGeneration, value.CandidateHeadOID, value.PriorWitnessDigest, value.PriorTicketVersion, value.PriorFence.LeaderEpoch, value.PriorFence.RunnerEpoch, value.TicketVersion, value.Fence.LeaderEpoch, value.Fence.RunnerEpoch, value.RebindDigest, value.CreatedAt.Format(time.RFC3339Nano)); err != nil {
			return err
		}
		_, err = conn.ExecContext(ctx, `INSERT INTO events(channel,project_id,ticket_id,ticket_version,trigger,from_state,to_state,payload,created_at) VALUES(?,?,?,?,?,?,?,?,?)`, ref.Channel, ref.Project, ref.Ticket, currentVersion, "publication_rebind", domain.StatePublishing, domain.StatePublishing, string(payload), value.CreatedAt.Format(time.RFC3339Nano))
		return err
	})
}

// validatePublicationAdvance permits exactly one runner increment either as a
// normal recovery (+1 ticket version) or as a complete pause/take control
// handoff (the authenticated stopping/drained/resume event triplet). It never
// accepts a bare counter jump or a runner increment without control lineage.
func validatePublicationAdvance(ctx context.Context, conn *sql.Conn, ref domain.TicketRef, priorVersion uint64, priorFence domain.Fence, currentVersion uint64, currentFence domain.Fence) error {
	if priorVersion == 0 || priorFence.LeaderEpoch == 0 || priorFence.RunnerEpoch == 0 || currentVersion == 0 || currentFence.LeaderEpoch == 0 || currentFence.RunnerEpoch != priorFence.RunnerEpoch+1 {
		return ErrStaleFence
	}
	if currentVersion == priorVersion+1 {
		return authenticateRunnerRecoveryStep(ctx, conn, ref, priorVersion, priorFence, currentVersion, currentFence)
	}
	if currentVersion <= priorVersion || validateRunnerControlAdvance(ctx, conn, ref, priorVersion, priorFence.RunnerEpoch, priorFence.LeaderEpoch, currentVersion, currentFence.RunnerEpoch, currentFence.LeaderEpoch) != nil {
		return ErrStaleFence
	}
	return nil
}

func checkPublicationEffect(ctx context.Context, conn *sql.Conn, ref domain.TicketRef, version uint64, fence domain.Fence, value PublicationEffectEvidence) error {
	var effect Effect
	if err := conn.QueryRowContext(ctx, `SELECT channel,project_id,ticket_id,effect_kind,state,ticket_version,leader_epoch,runner_epoch,claim_epoch,request_digest,observed_identity FROM effects WHERE semantic_key=?`, value.SemanticKey).Scan(&effect.Ref.Channel, &effect.Ref.Project, &effect.Ref.Ticket, &effect.Kind, &effect.State, &effect.TicketVersion, &effect.LeaderEpoch, &effect.RunnerEpoch, &effect.ClaimEpoch, &effect.RequestDigest, &effect.ObservedIdentity); err != nil {
		return ErrPublicationEvidence
	}
	if effect.Ref != ref || effect.Kind != value.Kind || effect.State != EffectConfirmed || effect.TicketVersion != version || effect.LeaderEpoch != fence.LeaderEpoch || effect.RunnerEpoch != fence.RunnerEpoch || effect.ClaimEpoch != value.ClaimEpoch || effect.RequestDigest != value.RequestDigest || effect.ObservedIdentity != value.ObservedIdentity {
		return ErrPublicationEvidence
	}
	return nil
}

func loadPublicationEvidenceRow(ctx context.Context, q interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, ref domain.TicketRef) (PublishedCandidateEvidence, bool, error) {
	var value PublishedCandidateEvidence
	var project, ticket string
	var draft, owned int
	var observed, created, buildCreated, identityDigest, worktreeState string
	var identity []byte
	err := q.QueryRowContext(ctx, `SELECT channel,project_id,ticket_id,ticket_version,leader_epoch,runner_epoch,candidate_generation,candidate_ticket_version,candidate_leader_epoch,candidate_runner_epoch,candidate_base_sha,candidate_head_sha,candidate_tree_sha,candidate_source_digest,candidate_verification_intent_digest,candidate_proof_digest,candidate_command_policy_digest,candidate_builder_evidence_digest,candidate_builder_attempt_id,candidate_builder_attempt,candidate_commit_parent_oid,candidate_command_semantic_key,candidate_command_claim_epoch,candidate_command_ticket_version,candidate_command_leader_epoch,candidate_command_runner_epoch,candidate_command_digest,candidate_command_spec_digest,candidate_command_policy_claim_digest,candidate_command_executable_path,candidate_command_executable_digest,config_generation,config_digest,config_snapshot_digest,worktree_path,worktree_branch_ref,worktree_state,worktree_ticket_version,worktree_leader_epoch,worktree_runner_epoch,worktree_identity_json,worktree_identity_digest,worktree_base_sha,remote_branch_ref,remote_branch_oid,remote_base_oid,push_effect_semantic_key,push_effect_kind,push_effect_request_digest,push_effect_claim_epoch,push_effect_observed_identity,github_host,github_owner,github_name,github_pr_number,github_head_owner,github_head_repository,github_head_ref,github_head_oid,github_base_ref,github_base_oid,github_state,github_draft,github_factory_owned,github_observed_at,pr_effect_semantic_key,pr_effect_kind,pr_effect_request_digest,pr_effect_claim_epoch,pr_effect_observed_identity,build_transition_created_at,witness_digest,created_at FROM publication_evidence WHERE channel=? AND project_id=? AND ticket_id=? ORDER BY candidate_generation DESC,candidate_head_sha DESC`, ref.Channel, ref.Project, ref.Ticket).Scan(
		&value.Ref.Channel, &project, &ticket, &value.TicketVersion, &value.Fence.LeaderEpoch, &value.Fence.RunnerEpoch,
		&value.Candidate.Snapshot.Generation, &value.Candidate.TicketVersion, &value.Candidate.Fence.LeaderEpoch, &value.Candidate.Fence.RunnerEpoch, &value.Candidate.Snapshot.BaseSHA, &value.Candidate.Snapshot.HeadSHA, &value.Candidate.Snapshot.TreeSHA,
		&value.Candidate.Snapshot.SourceDigest, &value.Candidate.Snapshot.VerificationIntentDigest, &value.Candidate.Snapshot.ProofDigest, &value.Candidate.Snapshot.CommandPolicyDigest, &value.Candidate.Snapshot.BuilderEvidenceDigest,
		&value.Candidate.BuilderResult.AttemptID, &value.Candidate.BuilderResult.Attempt, &value.Candidate.Commit.ParentOID,
		&value.Candidate.CommandBinding.Key.SemanticKey, &value.Candidate.CommandBinding.Key.ClaimEpoch, &value.Candidate.CommandBinding.TicketVersion, &value.Candidate.CommandBinding.LeaderEpoch, &value.Candidate.CommandBinding.RunnerEpoch, &value.Candidate.CommandBinding.CommandDigest, &value.Candidate.CommandBinding.SpecDigest, &value.Candidate.CommandBinding.PolicyDigest, &value.Candidate.CommandBinding.ExecutablePath, &value.Candidate.CommandBinding.ExecutableDigest,
		&value.ConfigGeneration, &value.ConfigDigest, &value.ConfigSnapshotDigest, &value.Worktree.Path, &value.Worktree.Branch, &worktreeState, &value.Worktree.TicketVersion, &value.Worktree.Fence.LeaderEpoch, &value.Worktree.Fence.RunnerEpoch, &identity, &identityDigest, &value.Worktree.BaseSHA,
		&value.RemoteBranchRef, &value.RemoteBranchOID, &value.RemoteBaseOID, &value.PushEffect.SemanticKey, &value.PushEffect.Kind, &value.PushEffect.RequestDigest, &value.PushEffect.ClaimEpoch, &value.PushEffect.ObservedIdentity,
		&value.PullRequest.Repository.Host, &value.PullRequest.Repository.Owner, &value.PullRequest.Repository.Name, &value.PullRequest.Number, &value.PullRequest.HeadOwner, &value.PullRequest.HeadRepository, &value.PullRequest.HeadRef, &value.PullRequest.HeadOID, &value.PullRequest.BaseRef, &value.PullRequest.BaseOID, &value.PullRequestState, &draft, &owned, &observed,
		&value.PRCreateOrUpdateEffect.SemanticKey, &value.PRCreateOrUpdateEffect.Kind, &value.PRCreateOrUpdateEffect.RequestDigest, &value.PRCreateOrUpdateEffect.ClaimEpoch, &value.PRCreateOrUpdateEffect.ObservedIdentity, &buildCreated, &value.WitnessDigest, &created)
	if errors.Is(err, sql.ErrNoRows) {
		return PublishedCandidateEvidence{}, false, nil
	}
	if err != nil {
		return PublishedCandidateEvidence{}, false, normalizeBusy(ctx, err)
	}
	value.Ref.Project, value.Ref.Ticket, value.Worktree.IdentityJSON, value.Worktree.State = domain.ProjectID(project), domain.TicketID(ticket), append([]byte(nil), identity...), worktreeState
	value.Candidate.BuilderResult.Ref, value.Candidate.BuilderResult.Phase = value.Ref, domain.PhaseBuild
	value.Candidate.Commit.CommitOID, value.Candidate.Commit.TreeOID = value.Candidate.Snapshot.HeadSHA, value.Candidate.Snapshot.TreeSHA
	value.PullRequest.FactoryOwned = owned == 1
	value.PullRequestDraft = draft == 1
	var parseErr error
	if value.PullRequestObservedAt, parseErr = time.Parse(time.RFC3339Nano, observed); parseErr != nil {
		return PublishedCandidateEvidence{}, false, ErrPublicationEvidence
	}
	if value.CreatedAt, parseErr = parsePublicationTime(created); parseErr != nil {
		return PublishedCandidateEvidence{}, false, ErrPublicationEvidence
	}
	if value.BuildTransitionCreatedAt, parseErr = parsePublicationTime(buildCreated); parseErr != nil {
		return PublishedCandidateEvidence{}, false, ErrPublicationEvidence
	}
	if value.Ref != ref || value.Worktree.IdentityJSON == nil || sha256Digest(value.Worktree.IdentityJSON) != identityDigest {
		return PublishedCandidateEvidence{}, false, ErrPublicationEvidence
	}
	if err := validPublishedCandidateEvidence(value); err != nil {
		return PublishedCandidateEvidence{}, false, ErrPublicationEvidence
	}
	payload, err := publicationPayload(value)
	if err != nil || publicationIdentityDigest(payload) != value.WitnessDigest {
		return PublishedCandidateEvidence{}, false, ErrPublicationEvidence
	}
	return value, true, nil
}

func loadLatestPublicationRebind(ctx context.Context, q interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, value *PublishedCandidateEvidence) error {
	rows, err := q.QueryContext(ctx, `SELECT prior_witness_digest,prior_ticket_version,prior_leader_epoch,prior_runner_epoch,ticket_version,leader_epoch,runner_epoch,rebind_digest,created_at FROM publication_evidence_rebinds WHERE channel=? AND project_id=? AND ticket_id=? AND candidate_generation=? AND candidate_head_sha=? ORDER BY ticket_version`, value.Ref.Channel, value.Ref.Project, value.Ref.Ticket, value.Candidate.Snapshot.Generation, value.Candidate.Snapshot.HeadSHA)
	if err != nil {
		return err
	}
	defer rows.Close()
	priorVersion, priorFence := value.TicketVersion, value.Fence
	priorDigest := value.WitnessDigest
	var found bool
	var count int
	for rows.Next() {
		count++
		if count > 64 {
			return ErrPublicationEvidence
		}
		var rebind PublicationRebind
		var created string
		if err := rows.Scan(&rebind.PriorWitnessDigest, &rebind.PriorTicketVersion, &rebind.PriorFence.LeaderEpoch, &rebind.PriorFence.RunnerEpoch, &rebind.TicketVersion, &rebind.Fence.LeaderEpoch, &rebind.Fence.RunnerEpoch, &rebind.RebindDigest, &created); err != nil {
			return err
		}
		if rebind.CreatedAt, err = parsePublicationTime(created); err != nil {
			return ErrPublicationEvidence
		}
		rebind.Ref, rebind.CandidateGeneration, rebind.CandidateHeadOID = value.Ref, value.Candidate.Snapshot.Generation, value.Candidate.Snapshot.HeadSHA
		payload, err := publicationRebindPayload(rebind)
		if err != nil || publicationIdentityDigest(payload) != rebind.RebindDigest || rebind.PriorWitnessDigest != priorDigest || rebind.PriorFence.LeaderEpoch == 0 || rebind.PriorFence.RunnerEpoch == 0 || rebind.Fence.RunnerEpoch != rebind.PriorFence.RunnerEpoch+1 || rebind.Fence.LeaderEpoch == 0 || rebind.Fence.ClaimEpoch != 0 || rebind.PriorFence.ClaimEpoch != 0 {
			return ErrPublicationEvidence
		}
		normalRecovery := rebind.PriorTicketVersion == priorVersion && rebind.PriorFence == priorFence && rebind.TicketVersion == priorVersion+1
		pairRecovery := rebind.PriorTicketVersion == priorVersion+2 && rebind.PriorFence == priorFence && rebind.TicketVersion == priorVersion+3 &&
			(authenticateBlockedPublicationResume(ctx, q, value.Ref, priorVersion+1, priorVersion+2, domain.StatePublishing, domain.StatePublishing) == nil ||
				authenticateSemanticPublicationResume(ctx, q, value.Ref, priorVersion+1, priorVersion+2, domain.StatePublishing) == nil)
		if pairRecovery {
			if err := authenticateRunnerRecoveryStep(ctx, q, value.Ref, priorVersion+2, priorFence, rebind.TicketVersion, rebind.Fence); err != nil {
				return err
			}
		} else if !normalRecovery {
			return ErrPublicationEvidence
		} else if err := authenticateRunnerRecoveryStep(ctx, q, value.Ref, priorVersion, priorFence, rebind.TicketVersion, rebind.Fence); err != nil {
			if validateRunnerControlAdvance(ctx, q, value.Ref, priorVersion, priorFence.RunnerEpoch, priorFence.LeaderEpoch, rebind.TicketVersion, rebind.Fence.RunnerEpoch, rebind.Fence.LeaderEpoch) != nil {
				return err
			}
		}
		var eventCount int
		if err := q.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version=? AND trigger='publication_rebind' AND from_state='publishing' AND to_state='publishing' AND payload=? AND created_at=?`, value.Ref.Channel, value.Ref.Project, value.Ref.Ticket, rebind.TicketVersion, string(payload), rebind.CreatedAt.Format(time.RFC3339Nano)).Scan(&eventCount); err != nil || eventCount != 1 {
			return ErrPublicationEvidence
		}
		if err := q.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version=?`, value.Ref.Channel, value.Ref.Project, value.Ref.Ticket, rebind.TicketVersion).Scan(&eventCount); err != nil {
			return err
		}
		if eventCount != 1 {
			if eventCount != 2 {
				return ErrPublicationEvidence
			}
			if err := q.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version=? AND trigger IN ('operator_resume','operator_retry') AND from_state='paused' AND to_state='publishing'`, value.Ref.Channel, value.Ref.Project, value.Ref.Ticket, rebind.TicketVersion).Scan(&eventCount); err != nil || eventCount != 1 {
				return ErrPublicationEvidence
			}
		}
		priorVersion, priorFence, priorDigest, found = rebind.TicketVersion, rebind.Fence, rebind.RebindDigest, true
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if !found {
		value.CurrentTicketVersion, value.CurrentFence = value.TicketVersion, value.Fence
		return nil
	}
	value.CurrentTicketVersion, value.CurrentFence = priorVersion, priorFence
	return nil
}

func loadPublicationRebindAt(ctx context.Context, q interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, ref domain.TicketRef, generation uint64, head string, version uint64) (PublicationRebind, bool, error) {
	var row PublicationRebind
	var created string
	err := q.QueryRowContext(ctx, `SELECT prior_witness_digest,prior_ticket_version,prior_leader_epoch,prior_runner_epoch,ticket_version,leader_epoch,runner_epoch,rebind_digest,created_at FROM publication_evidence_rebinds WHERE channel=? AND project_id=? AND ticket_id=? AND candidate_generation=? AND candidate_head_sha=? AND ticket_version=?`, ref.Channel, ref.Project, ref.Ticket, generation, head, version).Scan(&row.PriorWitnessDigest, &row.PriorTicketVersion, &row.PriorFence.LeaderEpoch, &row.PriorFence.RunnerEpoch, &row.TicketVersion, &row.Fence.LeaderEpoch, &row.Fence.RunnerEpoch, &row.RebindDigest, &created)
	if errors.Is(err, sql.ErrNoRows) {
		return PublicationRebind{}, false, nil
	}
	if err != nil {
		return PublicationRebind{}, false, err
	}
	row.Ref, row.CandidateGeneration, row.CandidateHeadOID = ref, generation, head
	if row.CreatedAt, err = parsePublicationTime(created); err != nil {
		return PublicationRebind{}, false, ErrPublicationEvidence
	}
	return row, true, nil
}

func loadPublicationRebindBefore(ctx context.Context, q interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, ref domain.TicketRef, generation uint64, head string, version uint64) (PublicationRebind, bool, error) {
	var row PublicationRebind
	var created string
	err := q.QueryRowContext(ctx, `SELECT prior_witness_digest,prior_ticket_version,prior_leader_epoch,prior_runner_epoch,ticket_version,leader_epoch,runner_epoch,rebind_digest,created_at FROM publication_evidence_rebinds WHERE channel=? AND project_id=? AND ticket_id=? AND candidate_generation=? AND candidate_head_sha=? AND ticket_version<? ORDER BY ticket_version DESC LIMIT 1`, ref.Channel, ref.Project, ref.Ticket, generation, head, version).Scan(&row.PriorWitnessDigest, &row.PriorTicketVersion, &row.PriorFence.LeaderEpoch, &row.PriorFence.RunnerEpoch, &row.TicketVersion, &row.Fence.LeaderEpoch, &row.Fence.RunnerEpoch, &row.RebindDigest, &created)
	if errors.Is(err, sql.ErrNoRows) {
		return PublicationRebind{}, false, nil
	}
	if err != nil {
		return PublicationRebind{}, false, err
	}
	row.Ref, row.CandidateGeneration, row.CandidateHeadOID = ref, generation, head
	if row.CreatedAt, err = parsePublicationTime(created); err != nil {
		return PublicationRebind{}, false, ErrPublicationEvidence
	}
	return row, true, nil
}

// LoadPublishedCandidate authenticates the durable row and all of the
// immutable authorities it names. It accepts a row while the ticket remains
// in publishing; waiting_ci replay can use the narrow existence query without
// mistaking this evidence for a merge claim. It does not authenticate CI,
// review, approval, merge readiness, or the eventual merge result; those are
// separate later workflow authorities.
func (s *Store) LoadPublishedCandidate(ctx context.Context, ref domain.TicketRef) (PublishedCandidateEvidence, error) {
	if err := ref.Validate(); err != nil {
		return PublishedCandidateEvidence{}, err
	}
	value, found, err := loadPublicationEvidenceRow(ctx, s.db, ref)
	if err != nil {
		return PublishedCandidateEvidence{}, err
	}
	if !found {
		return PublishedCandidateEvidence{}, ErrNotFound
	}
	if err := loadLatestPublicationRebind(ctx, s.db, &value); err != nil {
		return PublishedCandidateEvidence{}, err
	}
	ticket, err := s.Ticket(ctx, ref)
	if err != nil {
		return PublishedCandidateEvidence{}, err
	}
	waitingReplay := ticket.State == domain.StateWaitingCI
	semanticPublishingReplay := false
	semanticWaitingReplay := false
	blockedPublishingReplay := false
	blockedWaitingReplay := false
	waitingPairRecovery := false
	if ticket.State == domain.StatePublishing && ticket.Version != value.CurrentTicketVersion {
		if ticket.Version != value.CurrentTicketVersion+2 || ticket.RunnerEpoch != value.CurrentFence.RunnerEpoch {
			return PublishedCandidateEvidence{}, ErrPublicationEvidence
		}
		if authenticateBlockedPublicationResume(ctx, s.db, ref, value.CurrentTicketVersion+1, ticket.Version, domain.StatePublishing, domain.StatePublishing) == nil {
			blockedPublishingReplay = true
		} else if authenticateSemanticPublicationResume(ctx, s.db, ref, value.CurrentTicketVersion+1, ticket.Version, domain.StatePublishing) == nil {
			semanticPublishingReplay = true
		} else {
			return PublishedCandidateEvidence{}, ErrPublicationEvidence
		}
	}
	if waitingReplay {
		waitingVersion := value.CurrentTicketVersion + 1
		if value.CurrentTicketVersion == ^uint64(0) || ticket.Version < waitingVersion || ticket.RunnerEpoch < value.CurrentFence.RunnerEpoch {
			return PublishedCandidateEvidence{}, ErrPublicationEvidence
		}
		if ticket.Version == waitingVersion {
			// Ordinary publishing -> waiting_ci replay.
		} else if ticket.Version == waitingVersion+2 && authenticateBlockedPublicationResume(ctx, s.db, ref, waitingVersion+1, ticket.Version, domain.StateWaitingCI, domain.StateWaitingCI) == nil {
			blockedWaitingReplay = true
		} else if ticket.Version == waitingVersion+2 && authenticateSemanticPublicationResume(ctx, s.db, ref, waitingVersion+1, ticket.Version, domain.StateWaitingCI) == nil {
			semanticWaitingReplay = true
		} else {
			// Runner recovery remains the only other admissible waiting-ci path.
			semanticWaitingReplay = false
		}
		if semanticWaitingReplay && ticket.RunnerEpoch != value.CurrentFence.RunnerEpoch {
			return PublishedCandidateEvidence{}, ErrPublicationEvidence
		}
		if blockedWaitingReplay && ticket.RunnerEpoch != value.CurrentFence.RunnerEpoch {
			return PublishedCandidateEvidence{}, ErrPublicationEvidence
		}
		if !blockedWaitingReplay && !semanticWaitingReplay && waitingVersion <= ^uint64(0)-3 && ticket.Version >= waitingVersion+3 &&
			(authenticateBlockedPublicationResume(ctx, s.db, ref, waitingVersion+1, waitingVersion+2, domain.StateWaitingCI, domain.StateWaitingCI) == nil ||
				authenticateSemanticPublicationResume(ctx, s.db, ref, waitingVersion+1, waitingVersion+2, domain.StateWaitingCI) == nil) {
			waitingPairRecovery = true
		}
	}
	waitingVersion := value.CurrentTicketVersion + 1
	if (ticket.State != domain.StatePublishing && !waitingReplay) || (!waitingReplay && !blockedPublishingReplay && !semanticPublishingReplay && ticket.Version != value.CurrentTicketVersion) || (!waitingReplay && !blockedPublishingReplay && !semanticPublishingReplay && ticket.RunnerEpoch != value.CurrentFence.RunnerEpoch) || ticket.SourceDigest != value.Candidate.Snapshot.SourceDigest || ticket.ConfigGeneration != value.ConfigGeneration || ticket.ConfigDigest != value.ConfigDigest || sha256Digest(ticket.ConfigSnapshot) != value.ConfigSnapshotDigest {
		return PublishedCandidateEvidence{}, ErrPublicationEvidence
	}
	var projectBaseRef string
	if err := s.db.QueryRowContext(ctx, `SELECT base_ref FROM projects WHERE channel=? AND id=?`, ref.Channel, ref.Project).Scan(&projectBaseRef); err != nil || projectBaseRef != value.PullRequest.BaseRef {
		return PublishedCandidateEvidence{}, ErrPublicationEvidence
	}
	if waitingReplay {
		payload, err := json.Marshal(struct {
			WitnessDigest    string `json:"witness_digest"`
			WitnessCreatedAt string `json:"witness_created_at"`
		}{value.WitnessDigest, value.CreatedAt.Format(time.RFC3339Nano)})
		if err != nil {
			return PublishedCandidateEvidence{}, ErrPublicationEvidence
		}
		var transitions int
		if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM events e JOIN publication_transition_evidence p ON p.channel=e.channel AND p.project_id=e.project_id AND p.ticket_id=e.ticket_id AND p.ticket_version=e.ticket_version AND p.event_created_at=e.created_at WHERE e.channel=? AND e.project_id=? AND e.ticket_id=? AND e.ticket_version=? AND e.trigger='effects_confirmed' AND e.from_state='publishing' AND e.to_state='waiting_ci' AND e.payload=? AND p.witness_digest=? AND p.witness_created_at=?`, ref.Channel, ref.Project, ref.Ticket, waitingVersion, string(payload), value.WitnessDigest, value.CreatedAt.Format(time.RFC3339Nano)).Scan(&transitions); err != nil || transitions != 1 {
			return PublishedCandidateEvidence{}, ErrPublicationEvidence
		}
		if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version=?`, ref.Channel, ref.Project, ref.Ticket, waitingVersion).Scan(&transitions); err != nil || transitions != 1 {
			return PublishedCandidateEvidence{}, ErrPublicationEvidence
		}
		if ticket.Version < waitingVersion || ticket.RunnerEpoch < value.CurrentFence.RunnerEpoch {
			return PublishedCandidateEvidence{}, ErrPublicationEvidence
		}
	}
	var leader uint64
	if err := s.db.QueryRowContext(ctx, `SELECT leader_epoch FROM daemon_instances WHERE channel=?`, ref.Channel).Scan(&leader); err != nil {
		return PublishedCandidateEvidence{}, ErrStaleFence
	}
	if waitingReplay {
		baselineVersion, baselineRunner, baselineLeader := waitingVersion, value.CurrentFence.RunnerEpoch, value.CurrentFence.LeaderEpoch
		if waitingPairRecovery {
			baselineVersion += 2
		}
		if ticket.RunnerEpoch == baselineRunner {
			if leader != value.CurrentFence.LeaderEpoch {
				return PublishedCandidateEvidence{}, ErrStaleFence
			}
		} else if validateRunnerRecoveryLedger(ctx, s.db, ref, baselineVersion, baselineRunner, baselineLeader, ticket.Version, ticket.RunnerEpoch, leader) != nil {
			return PublishedCandidateEvidence{}, ErrStaleFence
		}
	} else if (blockedPublishingReplay || semanticPublishingReplay) && leader != value.CurrentFence.LeaderEpoch {
		return PublishedCandidateEvidence{}, ErrStaleFence
	} else if !blockedPublishingReplay && !semanticPublishingReplay && leader != value.CurrentFence.LeaderEpoch {
		return PublishedCandidateEvidence{}, ErrStaleFence
	}
	if waitingReplay && !semanticWaitingReplay && !blockedWaitingReplay {
		baselineVersion, baselineRunner, baselineLeader := waitingVersion, value.CurrentFence.RunnerEpoch, value.CurrentFence.LeaderEpoch
		if waitingPairRecovery {
			baselineVersion += 2
		}
		if err := validateRunnerRecoveryLedger(ctx, s.db, ref, baselineVersion, baselineRunner, baselineLeader, ticket.Version, ticket.RunnerEpoch, leader); err != nil {
			return PublishedCandidateEvidence{}, err
		}
	}
	if semanticWaitingReplay {
		if err := authenticatePublishedWaitingEvent(ctx, s.db, ref, value, waitingVersion); err != nil {
			return PublishedCandidateEvidence{}, err
		}
	}
	candidate, err := s.RecoverableCandidate(ctx, ref)
	if err != nil || !publicationCandidateEqual(candidate, value.Candidate) {
		return PublishedCandidateEvidence{}, ErrPublicationEvidence
	}
	if value.TicketVersion == value.Candidate.TicketVersion+2 {
		// Candidate-only publishing recovery has no publication witness at the
		// original publishing endpoint. Require the exact phase endpoint and
		// signed +1/+1 recovery row before accepting the later witness.
		var transitions int
		if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version=? AND trigger='phase_pass' AND from_state='building' AND to_state='publishing'`, ref.Channel, ref.Project, ref.Ticket, value.Candidate.TicketVersion+1).Scan(&transitions); err != nil || transitions != 1 {
			return PublishedCandidateEvidence{}, ErrPublicationEvidence
		}
		if err := authenticateRunnerRecoveryStep(ctx, s.db, ref, value.Candidate.TicketVersion+1, value.Candidate.Fence, value.TicketVersion, value.Fence); err != nil {
			return PublishedCandidateEvidence{}, ErrPublicationEvidence
		}
	}
	worktree, err := s.Worktree(ctx, ref)
	if err != nil || worktree.Path != value.Worktree.Path || worktree.Branch != value.Worktree.Branch || !bytes.Equal(worktree.IdentityJSON, value.Worktree.IdentityJSON) || worktree.BaseSHA != value.Worktree.BaseSHA || worktree.TicketVersion != value.Worktree.TicketVersion || worktree.Fence != value.Worktree.Fence {
		return PublishedCandidateEvidence{}, ErrPublicationEvidence
	}
	var allocatedBranch string
	if err := s.db.QueryRowContext(ctx, `SELECT branch_ref FROM branch_allocations WHERE channel=? AND project_id=? AND ticket_id=?`, ref.Channel, ref.Project, ref.Ticket).Scan(&allocatedBranch); err != nil || allocatedBranch != value.Worktree.Branch {
		return PublishedCandidateEvidence{}, ErrPublicationEvidence
	}
	if err := s.validateStoredPublicationEffect(ctx, ref, value.TicketVersion, value.Fence, value.PushEffect); err != nil {
		return PublishedCandidateEvidence{}, err
	}
	if err := s.validateStoredPublicationEffect(ctx, ref, value.TicketVersion, value.Fence, value.PRCreateOrUpdateEffect); err != nil {
		return PublishedCandidateEvidence{}, err
	}
	return value, nil
}

// LoadHistoricalPublishedCandidate authenticates the most recent publication
// witness without requiring it to be bound to the ticket's latest candidate.
// It is used only to carry a single factory PR identity across a candidate
// correction; it never authorizes a ticket transition or external mutation.
func (s *Store) LoadHistoricalPublishedCandidate(ctx context.Context, ref domain.TicketRef) (PublishedCandidateEvidence, error) {
	if err := ref.Validate(); err != nil {
		return PublishedCandidateEvidence{}, err
	}
	value, found, err := loadPublicationEvidenceRow(ctx, s.db, ref)
	if err != nil {
		return PublishedCandidateEvidence{}, err
	}
	if !found {
		return PublishedCandidateEvidence{}, ErrNotFound
	}
	if err := loadLatestPublicationRebind(ctx, s.db, &value); err != nil {
		return PublishedCandidateEvidence{}, err
	}
	if err := s.validateStoredPublicationEffect(ctx, ref, value.TicketVersion, value.Fence, value.PushEffect); err != nil {
		return PublishedCandidateEvidence{}, err
	}
	if err := s.validateStoredPublicationEffect(ctx, ref, value.TicketVersion, value.Fence, value.PRCreateOrUpdateEffect); err != nil {
		return PublishedCandidateEvidence{}, err
	}
	worktree, err := s.Worktree(ctx, ref)
	// The allocation identity is durable across a candidate correction, but
	// its ticket/fence/base fields describe the historical registration and
	// may advance while the same branch/checkout is retained. Path, branch,
	// state, and full identity remain fixed ownership evidence.
	if err != nil || worktree.Path != value.Worktree.Path || worktree.Branch != value.Worktree.Branch || worktree.State != value.Worktree.State || !bytes.Equal(worktree.IdentityJSON, value.Worktree.IdentityJSON) {
		return PublishedCandidateEvidence{}, ErrPublicationEvidence
	}
	return value, nil
}

func publicationCandidateEqual(left, right StoredCandidate) bool {
	left.CreatedAt, right.CreatedAt = time.Time{}, time.Time{}
	return left.TicketVersion == right.TicketVersion && left.Fence == right.Fence && left.Snapshot == right.Snapshot && left.BuilderResult == right.BuilderResult && left.Commit == right.Commit && left.CommandBinding == right.CommandBinding
}

func (s *Store) validateStoredPublicationEffect(ctx context.Context, ref domain.TicketRef, version uint64, fence domain.Fence, value PublicationEffectEvidence) error {
	var effect Effect
	err := s.db.QueryRowContext(ctx, `SELECT channel,project_id,ticket_id,effect_kind,state,ticket_version,leader_epoch,runner_epoch,claim_epoch,request_digest,observed_identity FROM effects WHERE semantic_key=?`, value.SemanticKey).Scan(&effect.Ref.Channel, &effect.Ref.Project, &effect.Ref.Ticket, &effect.Kind, &effect.State, &effect.TicketVersion, &effect.LeaderEpoch, &effect.RunnerEpoch, &effect.ClaimEpoch, &effect.RequestDigest, &effect.ObservedIdentity)
	if err != nil || effect.Ref != ref || effect.Kind != value.Kind || effect.State != EffectConfirmed || effect.TicketVersion != version || effect.LeaderEpoch != fence.LeaderEpoch || effect.RunnerEpoch != fence.RunnerEpoch || effect.ClaimEpoch != value.ClaimEpoch || effect.RequestDigest != value.RequestDigest || effect.ObservedIdentity != value.ObservedIdentity {
		return ErrPublicationEvidence
	}
	return nil
}

// LoadPublicationEvidence is the descriptive alias used by RuntimeController
// integrations.
func (s *Store) LoadPublicationEvidence(ctx context.Context, ref domain.TicketRef) (PublishedCandidateEvidence, error) {
	return s.LoadPublishedCandidate(ctx, ref)
}

// PublishedCandidate is a convenience alias for callers that use the
// publication identity as a read-side capability.
func (s *Store) PublishedCandidate(ctx context.Context, ref domain.TicketRef) (PublishedCandidateEvidence, error) {
	return s.LoadPublishedCandidate(ctx, ref)
}

// PublishedCandidateExists is intentionally narrow: it reports only whether
// a valid publication identity is persisted. It does not assert checks,
// review, approval, or merge state.
func (s *Store) PublishedCandidateExists(ctx context.Context, ref domain.TicketRef) (bool, error) {
	if err := ref.Validate(); err != nil {
		return false, err
	}
	_, err := s.LoadPublishedCandidate(ctx, ref)
	if errors.Is(err, ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

func (s *Store) HasPublicationEvidence(ctx context.Context, ref domain.TicketRef) (bool, error) {
	return s.PublishedCandidateExists(ctx, ref)
}
