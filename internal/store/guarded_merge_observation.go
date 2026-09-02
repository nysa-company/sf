package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
)

// TransitionGuardedMergeObserved is the only Store boundary that records the
// guarded merging -> reconciling handoff. The state-machine guards select this
// transition, while this transaction durably proves that the exact current
// merge intent/effect, publication, and approval chain already exists.
func (s *Store) TransitionGuardedMergeObserved(ctx context.Context, transition Transition) (TransitionResult, error) {
	if !guardedMergeObservationTransition(transition) {
		return TransitionResult{}, ErrEvidenceConflict
	}
	if transition.EventPayload == "" {
		transition.EventPayload = "{}"
	}
	if transition.EventPayload != "{}" {
		return TransitionResult{}, ErrEvidenceConflict
	}
	return s.transitionWithEvidence(ctx, transition, func(ctx context.Context, conn *sql.Conn, version, runner uint64) error {
		var mode domain.MergeMode
		if err := conn.QueryRowContext(ctx, `SELECT merge_mode FROM tickets WHERE channel=? AND project_id=? AND id=?`, transition.Ref.Channel, transition.Ref.Project, transition.Ref.Ticket).Scan(&mode); err != nil || mode != domain.MergeGuarded {
			return ErrEvidenceConflict
		}
		intent, found, err := singleRecoveryMergeIntent(ctx, conn, transition.Ref)
		if err != nil || !found {
			return ErrEvidenceConflict
		}
		// The observation table is keyed by the immutable merge semantic key, so
		// this lookup proves there is exactly one sealed merged-PR observation
		// for the single current intent. Its reader verifies the canonical digest
		// and every stored merged-PR fact before it can authorize this handoff.
		observation, found, err := guardedMergeObservationFrom(ctx, conn, intent.SemanticKey)
		if err != nil || !found || !guardedMergeObservationMatchesIntent(observation, intent) {
			return ErrEvidenceConflict
		}
		confirmed, err := s.confirmedMergeRecoveryEndpoint(ctx, conn, transition.Ref)
		if err != nil || confirmed.version != version || confirmed.runner != runner || confirmed.leader != transition.Fence.LeaderEpoch {
			return ErrEvidenceConflict
		}
		effect, err := effectFrom(ctx, conn, intent.SemanticKey)
		if err != nil || effect.Ref != transition.Ref || effect.SemanticKey != intent.SemanticKey || effect.Kind != "merge" || effect.State != EffectConfirmed || effect.RequestDigest != intent.RequestDigest || !guardedMergeParentEffectIdentityMatches(observation, intent, effect.ObservedIdentity) || effect.TicketVersion != confirmed.version || effect.RunnerEpoch != confirmed.runner || effect.LeaderEpoch != confirmed.leader || effect.ClaimEpoch < observation.ClaimEpoch {
			return ErrEvidenceConflict
		}
		// GitHub's merged-PR observation only identifies the post-merge object.
		// The separate protected-ref fetch proves that object was actually read
		// from the protected ref under Store's Git mutation authority.  Derive
		// its key from the immutable intent plus the durable checkout identity;
		// accepting a caller-supplied child key would make this handoff forgeable.
		if err := guardedMergeProtectedRefFetchMatches(ctx, conn, intent, observation.MergeOID, confirmed); err != nil {
			return ErrEvidenceConflict
		}
		// transitionWithEvidence carries any open runtime admission over this
		// authenticated state change in the same transaction. Keeping that
		// responsibility in one shared boundary avoids double-advancing the
		// authority and turning the second update into a stale-fence failure.
		return nil
	})
}

// guardedMergeObservation is the immutable handoff between GitHub's exact
// merged-PR observation and the separate protected-branch proof.  HeadOID is
// deliberately the reviewed PR source commit; MergeOID is the separately
// observed post-merge protected-branch commit from GitHub. They may be equal
// for a valid fast-forward or rebase outcome, but they are never conflated.
type guardedMergeObservation struct {
	SemanticKey            string
	Ref                    domain.TicketRef
	RequestDigest          string
	TicketVersion          uint64
	LeaderEpoch            uint64
	RunnerEpoch            uint64
	ClaimEpoch             uint64
	Repository             contracts.RepositoryIdentity
	PullRequestNumber      int
	HeadOwner              string
	HeadRepository         string
	HeadRef                string
	HeadOID                string
	BaseRef                string
	OriginalBaseOID        string
	MergeOID               string
	ObservedState          string
	ObservedMerged         bool
	ObservedDraft          bool
	ObservedFactoryOwned   bool
	ObservedMergeCommit    string
	ObservedBaseOID        string
	ObservedBaseHeadOID    string
	ProtectionRuleID       string
	ProtectionKind         string
	ProtectionChecksDigest string
	StrictStatusChecks     bool
	AdminEnforced          bool
	ActiveRulesetCount     uint32
	Method                 string
	Digest                 string
}

// RecordGuardedMergeObservation persists the exact all-state GitHub response
// after a guarded merge has been observed and before any protected-ref proof
// may be issued.  It is intentionally idempotent for the same immutable
// observation: a lost response can repeat the observation but cannot retarget
// a source PR or descendant merge commit.
func (s *Store) RecordGuardedMergeObservation(ctx context.Context, intent domain.MergeIntent, observed contracts.PublishedPullRequestObservation) error {
	value, err := guardedMergeObservationFor(intent, observed)
	if err != nil {
		return err
	}
	value.Digest = canonicalGuardedMergeObservationDigest(value)
	return s.write(ctx, func(conn *sql.Conn) error {
		persisted, found, err := mergeIntentFrom(ctx, conn, intent.SemanticKey)
		if err != nil || !found || persisted != intent || validMergeIntentForProof(persisted) != nil {
			return ErrEvidenceConflict
		}
		// Once present, the observation is self-authenticating against the
		// immutable merge intent. A later recovery may promote the parent effect
		// to a newer ticket fence; requiring the original launch fence again
		// would strand an otherwise exact lost-response replay.
		existing, found, err := guardedMergeObservationFrom(ctx, conn, intent.SemanticKey)
		if err != nil {
			return ErrEvidenceConflict
		}
		if found {
			if existing != value {
				return ErrEvidenceConflict
			}
			return nil
		}
		if err := s.guardableMergeObservationFirstWrite(ctx, conn, intent); err != nil {
			return ErrEvidenceConflict
		}
		result, err := conn.ExecContext(ctx, `INSERT INTO guarded_merge_observations(
			semantic_key,channel,project_id,ticket_id,request_digest,ticket_version,leader_epoch,runner_epoch,claim_epoch,
			repository_host,repository_owner,repository_name,pull_request_number,head_owner,head_repository,head_ref,head_oid,base_ref,original_base_oid,merge_oid,
			observed_state,observed_merged,observed_draft,observed_factory_owned,observed_merge_commit,observed_base_oid,observed_base_head_oid,
			protection_rule_id,protection_kind,protection_checks_digest,strict_status_checks,admin_enforced,active_ruleset_count,method,observation_digest,created_at
		) VALUES(
			?,?,?,?,?,?,?,?,?,
			?,?,?,?,?,?,?,?,?,?,?,
			?,?,?,?,?,?,?,
			?,?,?,?,?,?,?,?,?
		) ON CONFLICT(semantic_key) DO NOTHING`,
			value.SemanticKey, value.Ref.Channel, value.Ref.Project, value.Ref.Ticket, value.RequestDigest, value.TicketVersion, value.LeaderEpoch, value.RunnerEpoch, value.ClaimEpoch,
			value.Repository.Host, value.Repository.Owner, value.Repository.Name, value.PullRequestNumber, value.HeadOwner, value.HeadRepository, value.HeadRef, value.HeadOID, value.BaseRef, value.OriginalBaseOID, value.MergeOID,
			value.ObservedState, boolInt(value.ObservedMerged), boolInt(value.ObservedDraft), boolInt(value.ObservedFactoryOwned), value.ObservedMergeCommit, value.ObservedBaseOID, value.ObservedBaseHeadOID,
			value.ProtectionRuleID, value.ProtectionKind, value.ProtectionChecksDigest, boolInt(value.StrictStatusChecks), boolInt(value.AdminEnforced), value.ActiveRulesetCount, value.Method, value.Digest, now())
		if err != nil {
			return err
		}
		if inserted, _ := result.RowsAffected(); inserted == 1 {
			return nil
		}
		existing, found, err = guardedMergeObservationFrom(ctx, conn, intent.SemanticKey)
		if err != nil || !found || existing != value {
			return ErrEvidenceConflict
		}
		return nil
	})
}

// guardableMergeObservationFirstWrite accepts only the original merge launch
// or the one authenticated recovery shape that can follow a daemon crash after
// GitHub applied the merge but before this observation committed. ReconcileEffects
// changes the stranded effect to uncertain under the new leader, then
// FenceRecoveredRunners advances ticket version/runner. Requiring the original
// live effect at that point would make the mandatory read-only recovery
// observation impossible; accepting any promoted/confirmed row would instead
// allow a fabricated proof handoff.
func (s *Store) guardableMergeObservationFirstWrite(ctx context.Context, conn *sql.Conn, intent domain.MergeIntent) error {
	effect, err := effectFrom(ctx, conn, intent.SemanticKey)
	if err != nil || effect.Ref != intent.Ref || effect.SemanticKey != intent.SemanticKey || effect.Kind != "merge" || effect.RequestDigest != intent.RequestDigest {
		return ErrEvidenceConflict
	}
	if effect.TicketVersion == intent.TicketVersion && effect.LeaderEpoch == intent.LeaderEpoch && effect.RunnerEpoch == intent.RunnerEpoch && effect.ClaimEpoch == intent.ClaimEpoch && (effect.State == EffectExecuting || effect.State == EffectUncertain) {
		return nil
	}
	if effect.State != EffectUncertain {
		return ErrEvidenceConflict
	}
	var state domain.State
	var version, runner, leader uint64
	if err := conn.QueryRowContext(ctx, `SELECT t.state,t.version,t.runner_epoch,d.leader_epoch FROM tickets t JOIN daemon_instances d ON d.channel=t.channel WHERE t.channel=? AND t.project_id=? AND t.id=?`, intent.Ref.Channel, intent.Ref.Project, intent.Ref.Ticket).Scan(&state, &version, &runner, &leader); err != nil || state != domain.StateMerging || leader == 0 {
		return ErrEvidenceConflict
	}
	approval, err := s.approvalRecoveryEndpoint(ctx, conn, intent.Ref)
	if err != nil {
		return ErrEvidenceConflict
	}
	if err := s.authenticateMergingRecoveryEffect(ctx, conn, intent.Ref, approval, normalRecoveryEndpoint{version: version, runner: runner, leader: leader}, leader, intent); err != nil {
		return ErrEvidenceConflict
	}
	return nil
}

func guardedMergeObservationFor(intent domain.MergeIntent, observed contracts.PublishedPullRequestObservation) (guardedMergeObservation, error) {
	identity := observed.Identity
	if validMergeIntentForProof(intent) != nil || (intent.ProtectionKind != "classic" && intent.ProtectionKind != "ruleset") || observed.State != "MERGED" || !observed.Merged || observed.Draft || !validOID(observed.MergeCommit) || len(observed.MergeCommit) != len(intent.OriginalBaseOID) || observed.MergeCommit == "" || identity.Repository.Host != intent.RepositoryHost || identity.Repository.Owner != intent.RepositoryOwner || identity.Repository.Name != intent.RepositoryName || identity.Number != intent.PullRequestNumber || identity.HeadOwner != intent.HeadOwner || identity.HeadRepository != intent.HeadRepository || identity.HeadRef != intent.HeadRef || identity.HeadOID != intent.HeadOID || identity.BaseRef != intent.BaseRef || !identity.FactoryOwned || !validOID(identity.BaseOID) || !validOID(observed.BaseHeadOID) || identity.BaseOID != observed.BaseHeadOID {
		return guardedMergeObservation{}, ErrEvidenceConflict
	}
	return guardedMergeObservation{
		SemanticKey: intent.SemanticKey, Ref: intent.Ref, RequestDigest: intent.RequestDigest,
		TicketVersion: intent.TicketVersion, LeaderEpoch: intent.LeaderEpoch, RunnerEpoch: intent.RunnerEpoch, ClaimEpoch: intent.ClaimEpoch,
		Repository:        contracts.RepositoryIdentity{Host: intent.RepositoryHost, Owner: intent.RepositoryOwner, Name: intent.RepositoryName},
		PullRequestNumber: intent.PullRequestNumber, HeadOwner: intent.HeadOwner, HeadRepository: intent.HeadRepository, HeadRef: intent.HeadRef, HeadOID: intent.HeadOID,
		BaseRef: intent.BaseRef, OriginalBaseOID: intent.OriginalBaseOID, MergeOID: observed.MergeCommit,
		ObservedState: observed.State, ObservedMerged: observed.Merged, ObservedDraft: observed.Draft, ObservedFactoryOwned: identity.FactoryOwned, ObservedMergeCommit: observed.MergeCommit, ObservedBaseOID: identity.BaseOID, ObservedBaseHeadOID: observed.BaseHeadOID,
		ProtectionRuleID: intent.ProtectionRuleID, ProtectionKind: intent.ProtectionKind, ProtectionChecksDigest: intent.ProtectionChecksDigest, StrictStatusChecks: intent.StrictStatusChecks, AdminEnforced: intent.AdminEnforced, ActiveRulesetCount: intent.ActiveRulesetCount, Method: intent.Method,
	}, nil
}

func canonicalGuardedMergeObservationDigest(value guardedMergeObservation) string {
	return sha256Digest([]byte(strings.Join([]string{
		value.SemanticKey, string(value.Ref.Channel), string(value.Ref.Project), string(value.Ref.Ticket), value.RequestDigest,
		fmt.Sprint(value.TicketVersion), fmt.Sprint(value.LeaderEpoch), fmt.Sprint(value.RunnerEpoch), fmt.Sprint(value.ClaimEpoch),
		value.Repository.Host, value.Repository.Owner, value.Repository.Name, fmt.Sprint(value.PullRequestNumber),
		value.HeadOwner, value.HeadRepository, value.HeadRef, value.HeadOID, value.BaseRef, value.OriginalBaseOID, value.MergeOID,
		value.ObservedState, fmt.Sprint(value.ObservedMerged), fmt.Sprint(value.ObservedDraft), fmt.Sprint(value.ObservedFactoryOwned), value.ObservedMergeCommit, value.ObservedBaseOID, value.ObservedBaseHeadOID,
		value.ProtectionRuleID, value.ProtectionKind, value.ProtectionChecksDigest, fmt.Sprint(value.StrictStatusChecks), fmt.Sprint(value.AdminEnforced), fmt.Sprint(value.ActiveRulesetCount), value.Method,
	}, "\x00")))
}

type guardedMergeObservationQuerier interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func guardedMergeObservationFrom(ctx context.Context, q guardedMergeObservationQuerier, semanticKey string) (guardedMergeObservation, bool, error) {
	var value guardedMergeObservation
	err := q.QueryRowContext(ctx, `SELECT channel,project_id,ticket_id,request_digest,ticket_version,leader_epoch,runner_epoch,claim_epoch,
		repository_host,repository_owner,repository_name,pull_request_number,head_owner,head_repository,head_ref,head_oid,base_ref,original_base_oid,merge_oid,
		observed_state,observed_merged,observed_draft,observed_factory_owned,observed_merge_commit,observed_base_oid,observed_base_head_oid,
		protection_rule_id,protection_kind,protection_checks_digest,strict_status_checks,admin_enforced,active_ruleset_count,method,observation_digest
		FROM guarded_merge_observations WHERE semantic_key=?`, semanticKey).Scan(
		&value.Ref.Channel, &value.Ref.Project, &value.Ref.Ticket, &value.RequestDigest, &value.TicketVersion, &value.LeaderEpoch, &value.RunnerEpoch, &value.ClaimEpoch,
		&value.Repository.Host, &value.Repository.Owner, &value.Repository.Name, &value.PullRequestNumber, &value.HeadOwner, &value.HeadRepository, &value.HeadRef, &value.HeadOID, &value.BaseRef, &value.OriginalBaseOID, &value.MergeOID,
		&value.ObservedState, &value.ObservedMerged, &value.ObservedDraft, &value.ObservedFactoryOwned, &value.ObservedMergeCommit, &value.ObservedBaseOID, &value.ObservedBaseHeadOID,
		&value.ProtectionRuleID, &value.ProtectionKind, &value.ProtectionChecksDigest, &value.StrictStatusChecks, &value.AdminEnforced, &value.ActiveRulesetCount, &value.Method, &value.Digest)
	if err == sql.ErrNoRows {
		return guardedMergeObservation{}, false, nil
	}
	if err != nil {
		return guardedMergeObservation{}, false, normalizeBusy(ctx, err)
	}
	value.SemanticKey = semanticKey
	if value.Ref.Validate() != nil || !validDigest(value.Digest) || value.Digest != canonicalGuardedMergeObservationDigest(value) || value.RequestDigest == "" || value.TicketVersion == 0 || value.LeaderEpoch == 0 || value.RunnerEpoch == 0 || value.ClaimEpoch == 0 || value.Repository.Host != "github.com" || !boundedText(value.Repository.Owner, 128) || !boundedText(value.Repository.Name, 128) || value.PullRequestNumber <= 0 || !boundedText(value.HeadOwner, 128) || !boundedText(value.HeadRepository, 128) || !boundedText(value.HeadRef, 300) || !validOID(value.HeadOID) || !boundedText(value.BaseRef, 255) || !validOID(value.OriginalBaseOID) || !validOID(value.MergeOID) || len(value.MergeOID) != len(value.OriginalBaseOID) || value.ObservedState != "MERGED" || !value.ObservedMerged || value.ObservedDraft || !value.ObservedFactoryOwned || value.ObservedMergeCommit != value.MergeOID || !validOID(value.ObservedMergeCommit) || !validOID(value.ObservedBaseOID) || !validOID(value.ObservedBaseHeadOID) || value.ObservedBaseOID != value.ObservedBaseHeadOID || value.ProtectionRuleID == "" || (value.ProtectionKind != "classic" && value.ProtectionKind != "ruleset") || value.Method != "merge" && value.Method != "squash" && value.Method != "rebase" || !value.StrictStatusChecks || !value.AdminEnforced || value.ProtectionChecksDigest != "" && !validSHA256(value.ProtectionChecksDigest) || value.ActiveRulesetCount > 0 && (value.ActiveRulesetCount != 1 || value.ProtectionKind != "ruleset") || value.ActiveRulesetCount == 0 && value.ProtectionKind != "" && value.ProtectionKind != "classic" {
		return guardedMergeObservation{}, false, ErrEvidenceConflict
	}
	return value, true, nil
}

func guardedMergeObservationMatchesIntent(value guardedMergeObservation, intent domain.MergeIntent) bool {
	return value.SemanticKey == intent.SemanticKey && value.Ref == intent.Ref && value.RequestDigest == intent.RequestDigest && value.TicketVersion == intent.TicketVersion && value.LeaderEpoch == intent.LeaderEpoch && value.RunnerEpoch == intent.RunnerEpoch && value.ClaimEpoch == intent.ClaimEpoch && value.Repository.Host == intent.RepositoryHost && value.Repository.Owner == intent.RepositoryOwner && value.Repository.Name == intent.RepositoryName && value.PullRequestNumber == intent.PullRequestNumber && value.HeadOwner == intent.HeadOwner && value.HeadRepository == intent.HeadRepository && value.HeadRef == intent.HeadRef && value.HeadOID == intent.HeadOID && value.BaseRef == intent.BaseRef && value.OriginalBaseOID == intent.OriginalBaseOID && value.ProtectionRuleID == intent.ProtectionRuleID && value.ProtectionKind == intent.ProtectionKind && value.ProtectionChecksDigest == intent.ProtectionChecksDigest && value.StrictStatusChecks == intent.StrictStatusChecks && value.AdminEnforced == intent.AdminEnforced && value.ActiveRulesetCount == intent.ActiveRulesetCount && value.Method == intent.Method
}

// guardedMergeParentEffectIdentityMatches recognizes the two exact durable
// outcomes produced by the same guarded merge boundary. A normal response is
// confirmed by the worker as merged/<reviewed-source-head>; a lost response is
// re-observed by GitHub as owner/repository@<post-merge-commit>. The immutable
// guarded observation and separate protected-ref child bind those values to
// one operation, so no free-form nonempty recovery identity is accepted.
func guardedMergeParentEffectIdentityMatches(value guardedMergeObservation, intent domain.MergeIntent, identity string) bool {
	if !guardedMergeObservationMatchesIntent(value, intent) {
		return false
	}
	return identity == "merged/"+intent.HeadOID || identity == intent.RepositoryOwner+"/"+intent.RepositoryName+"@"+value.MergeOID
}

// GuardedMergeProtectedRefFetch is the deterministic Store-owned child Git
// intent produced after GitHub records a guarded merge observation. Origin is
// returned so the coordinator can prove the live checkout still names the
// same durable remote before it invokes Git.
type GuardedMergeProtectedRefFetch struct {
	Intent GitMutationIntent
	Origin string
}

// GuardedMergeProtectedRefFetchIntent derives the protected-ref-fetch child
// from immutable merge evidence and the durable project/worktree identity. It
// deliberately has no lifecycle authority: callers still need a current
// ticket fence to plan and issue the returned Git mutation.
func (s *Store) GuardedMergeProtectedRefFetchIntent(ctx context.Context, intent domain.MergeIntent, mergeOID string) (GuardedMergeProtectedRefFetch, error) {
	if s == nil {
		return GuardedMergeProtectedRefFetch{}, ErrEvidenceConflict
	}
	return guardedMergeProtectedRefFetchIntentFrom(ctx, s.db, intent, mergeOID)
}

type guardedMergeWorktreeIdentity struct {
	Repository string
	Worktree   string
	Origin     string
	BaseRef    string
	HeadRef    string
}

// CanonicalGuardedMergeProtectedRefFetchRequestDigest is shared by the Store
// admission check and mergeproof.Coordinator.  Keep this null-delimited
// encoding stable: existing child effects use it as their durable request
// identity, while their semantic key additionally binds checkout identity.
func CanonicalGuardedMergeProtectedRefFetchRequestDigest(mergeSemanticKey, baseRef, originalBaseOID, mergeOID, origin string) string {
	h := sha256.New()
	for _, value := range []string{mergeSemanticKey, baseRef, originalBaseOID, mergeOID, origin} {
		_, _ = h.Write([]byte(value))
		_, _ = h.Write([]byte{0})
	}
	return fmt.Sprintf("sha256:%x", h.Sum(nil))
}

func guardedMergeProtectedRefFetchIntentFrom(ctx context.Context, q guardedMergeObservationQuerier, intent domain.MergeIntent, mergeOID string) (GuardedMergeProtectedRefFetch, error) {
	if validMergeIntentForProof(intent) != nil || !validOID(mergeOID) || len(mergeOID) != len(intent.OriginalBaseOID) {
		return GuardedMergeProtectedRefFetch{}, ErrEvidenceConflict
	}
	var projectPath, projectBaseRef, worktreePath, branch, worktreeBase string
	var identityJSON []byte
	if err := q.QueryRowContext(ctx, `SELECT p.canonical_path,p.base_ref,w.path,w.branch_ref,w.identity_json,w.base_sha
		FROM projects p JOIN worktrees w ON w.channel=p.channel AND w.project_id=p.id
		WHERE p.channel=? AND p.id=? AND w.ticket_id=?`, intent.Ref.Channel, intent.Ref.Project, intent.Ref.Ticket).
		Scan(&projectPath, &projectBaseRef, &worktreePath, &branch, &identityJSON, &worktreeBase); err != nil {
		return GuardedMergeProtectedRefFetch{}, ErrEvidenceConflict
	}
	var identity guardedMergeWorktreeIdentity
	if !validStorePath(projectPath) || projectBaseRef != intent.BaseRef || !validStorePath(worktreePath) || !boundedText(branch, 300) || worktreeBase != intent.OriginalBaseOID || !validRepositoryWorktreeIdentity(string(identityJSON), projectPath, worktreePath, branch, projectBaseRef, worktreeBase) || json.Unmarshal(identityJSON, &identity) != nil || identity.Repository != projectPath || identity.Worktree != worktreePath || identity.BaseRef != intent.BaseRef || identity.HeadRef != branch || !boundedText(identity.Origin, 2_000) {
		return GuardedMergeProtectedRefFetch{}, ErrEvidenceConflict
	}
	gitIntent := GitMutationIntent{
		EffectFence:     EffectFence{Ref: intent.Ref},
		RequestDigest:   CanonicalGuardedMergeProtectedRefFetchRequestDigest(intent.SemanticKey, intent.BaseRef, intent.OriginalBaseOID, mergeOID, identity.Origin),
		Repository:      projectPath,
		Worktree:        worktreePath,
		Branch:          branch,
		Operation:       "protected-ref-fetch",
		BaseRef:         intent.BaseRef,
		ExpectedBaseOID: intent.OriginalBaseOID,
		ExpectedHeadOID: mergeOID,
	}
	gitIntent.SemanticKey = CanonicalGitMutationSemanticKey(gitIntent)
	return GuardedMergeProtectedRefFetch{Intent: gitIntent, Origin: identity.Origin}, nil
}

func guardedMergeProtectedRefFetchMatches(ctx context.Context, conn *sql.Conn, intent domain.MergeIntent, mergeOID string, confirmed normalRecoveryEndpoint) error {
	proof, err := guardedMergeProtectedRefFetchIntentFrom(ctx, conn, intent, mergeOID)
	if err != nil {
		return ErrEvidenceConflict
	}
	facts, err := gitMutationIntentFactsFrom(ctx, conn, proof.Intent.SemanticKey)
	if err != nil {
		return ErrEvidenceConflict
	}
	claim := facts.Claim
	if claim.TicketRef != proof.Intent.Ref || claim.SemanticKey != proof.Intent.SemanticKey || claim.RequestDigest != proof.Intent.RequestDigest || claim.TicketVersion == 0 || claim.LeaderEpoch == 0 || claim.RunnerEpoch == 0 || claim.ClaimEpoch == 0 || claim.Repository != proof.Intent.Repository || claim.Worktree != proof.Intent.Worktree || claim.Branch != proof.Intent.Branch || claim.Operation != proof.Intent.Operation || claim.BaseRef != proof.Intent.BaseRef || claim.ExpectedBaseOID != proof.Intent.ExpectedBaseOID || claim.ExpectedHeadOID != proof.Intent.ExpectedHeadOID {
		return ErrEvidenceConflict
	}
	if facts.Effect.Ref != claim.TicketRef || facts.Effect.SemanticKey != claim.SemanticKey || facts.Effect.Kind != "git/protected-ref-fetch" || facts.Effect.State != EffectConfirmed || facts.Effect.RequestDigest != claim.RequestDigest || facts.Effect.ClaimEpoch < claim.ClaimEpoch || facts.ObservedIdentity != intent.BaseRef+"@"+mergeOID || facts.Effect.ObservedIdentity != facts.ObservedIdentity {
		return ErrEvidenceConflict
	}
	claimEndpoint := normalRecoveryEndpoint{version: claim.TicketVersion, runner: claim.RunnerEpoch, leader: claim.LeaderEpoch}
	proofEndpoint := normalRecoveryEndpoint{version: facts.Effect.TicketVersion, runner: facts.Effect.RunnerEpoch, leader: facts.Effect.LeaderEpoch}
	if claimEndpoint != proofEndpoint && authenticatePostPublicationEndpointBridge(ctx, conn, intent.Ref, domain.StateMerging, claimEndpoint, proofEndpoint) != nil {
		return ErrEvidenceConflict
	}
	if proofEndpoint != confirmed && authenticateCurrentPostPublicationEndpointBridge(ctx, conn, intent.Ref, domain.StateMerging, proofEndpoint, confirmed) != nil {
		return ErrEvidenceConflict
	}
	return nil
}
