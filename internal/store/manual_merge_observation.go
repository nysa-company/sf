package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
)

// ManualMergeObservation is the read-only authority for an externally merged
// manual PR. Publication is the immutable PR identity recorded by Store;
// Observed is the fresh all-state GitHub response. The ticket/fence pair is
// the authority at the waiting_manual_merge -> reconciling boundary.
type ManualMergeObservation struct {
	Ref                      domain.TicketRef
	CandidateGeneration      uint64
	CandidateHeadSHA         string
	CandidateBaseSHA         string
	CandidateTreeSHA         string
	PublicationWitnessDigest string
	Publication              contracts.PullRequestIdentity
	Observed                 contracts.PublishedPullRequestObservation
	MergeCommit              string
	ObservedProtectedBase    string
	CurrentTicketVersion     uint64
	CurrentFence             domain.Fence
	ObservationDigest        string
}

// NewManualMergeObservation translates the authenticated publication witness
// and fresh merged-PR response into the narrow manual authority. Current
// ticket/fence and the digest are filled by the Store transition boundary.
func NewManualMergeObservation(publication PublishedCandidateEvidence, observed contracts.PublishedPullRequestObservation) ManualMergeObservation {
	// Presentation fields such as Ready, Title, Body, MergeState, and
	// AutoMerge are deliberately not durable authority. Canonicalize at this
	// boundary so a Store round trip compares only the exact persisted merge
	// identity and proof facts.
	observed = contracts.PublishedPullRequestObservation{
		Identity: observed.Identity, Draft: observed.Draft, Merged: observed.Merged,
		MergeCommit: observed.MergeCommit, BaseHeadOID: observed.BaseHeadOID, State: observed.State,
	}
	return ManualMergeObservation{
		Ref: publication.Ref, CandidateGeneration: publication.Candidate.Snapshot.Generation,
		CandidateHeadSHA: publication.Candidate.Snapshot.HeadSHA, CandidateBaseSHA: publication.Candidate.Snapshot.BaseSHA,
		CandidateTreeSHA: publication.Candidate.Snapshot.TreeSHA, Publication: publication.PullRequest,
		PublicationWitnessDigest: publication.WitnessDigest,
		Observed:                 observed, MergeCommit: observed.MergeCommit, ObservedProtectedBase: observed.BaseHeadOID,
	}
}

// BindManualMergeObservation attaches the caller's current transition fence
// and computes the canonical digest. RecordManualMergeObservation still
// rechecks both values atomically, so this helper is only a composition aid.
func (s *Store) BindManualMergeObservation(ctx context.Context, ref domain.TicketRef, value ManualMergeObservation, fence domain.Fence) (ManualMergeObservation, error) {
	ticket, err := s.Ticket(ctx, ref)
	if err != nil {
		return ManualMergeObservation{}, err
	}
	value.Ref = ref
	value.CurrentTicketVersion = ticket.Version
	value.CurrentFence = fence
	value.ObservationDigest = CanonicalManualMergeObservationDigest(value)
	return value, nil
}

func manualMergeObservationPayload(value ManualMergeObservation) ([]byte, error) {
	return json.Marshal(struct {
		Ref                      domain.TicketRef
		CandidateGeneration      uint64
		CandidateHeadSHA         string
		CandidateBaseSHA         string
		CandidateTreeSHA         string
		PublicationWitnessDigest string
		Publication              contracts.PullRequestIdentity
		ObservedIdentity         contracts.PullRequestIdentity
		MergeCommit              string
		ObservedProtectedBase    string
		CurrentTicketVersion     uint64
		CurrentFence             domain.Fence
	}{value.Ref, value.CandidateGeneration, value.CandidateHeadSHA, value.CandidateBaseSHA, value.CandidateTreeSHA, value.PublicationWitnessDigest, value.Publication, value.Observed.Identity, value.MergeCommit, value.ObservedProtectedBase, value.CurrentTicketVersion, value.CurrentFence})
}

// CanonicalManualMergeObservationDigest exposes the stable digest contract to
// runtime adapters and tests. The digest covers every authority field except
// itself and volatile GitHub response presentation fields.
func CanonicalManualMergeObservationDigest(value ManualMergeObservation) string {
	payload, err := manualMergeObservationPayload(value)
	if err != nil {
		return ""
	}
	return publicationIdentityDigest(payload)
}

func validManualMergeObservation(value ManualMergeObservation) error {
	if value.Ref.Validate() != nil || value.CandidateGeneration == 0 || !validOID(value.CandidateHeadSHA) || !validOID(value.CandidateBaseSHA) || !validOID(value.CandidateTreeSHA) || !validManualDigest(value.PublicationWitnessDigest) || value.CurrentTicketVersion == 0 || value.CurrentFence.LeaderEpoch == 0 || value.CurrentFence.RunnerEpoch == 0 || value.CurrentFence.ClaimEpoch != 0 || value.Publication.Repository.Host != "github.com" || value.Publication.Repository.Owner == "" || value.Publication.Repository.Name == "" || value.Publication.Number <= 0 || !value.Publication.FactoryOwned || value.Publication.HeadOwner == "" || value.Publication.HeadRepository == "" || value.Publication.HeadRef == "" || !validOID(value.Publication.HeadOID) || value.Publication.BaseRef == "" || !validOID(value.Publication.BaseOID) || value.Publication.HeadOwner != value.Publication.Repository.Owner || value.Publication.HeadRepository != value.Publication.Repository.Name || value.Publication.HeadOID != value.CandidateHeadSHA || value.Observed.State != "MERGED" || !value.Observed.Merged || value.Observed.Draft || !validOID(value.MergeCommit) || value.MergeCommit != value.Observed.MergeCommit || !validOID(value.ObservedProtectedBase) || value.ObservedProtectedBase != value.Observed.BaseHeadOID || value.Observed.Identity.Repository != value.Publication.Repository || value.Observed.Identity.Number != value.Publication.Number || value.Observed.Identity.HeadOwner != value.Publication.HeadOwner || value.Observed.Identity.HeadRepository != value.Publication.HeadRepository || value.Observed.Identity.HeadRef != value.Publication.HeadRef || value.Observed.Identity.HeadOID != value.Publication.HeadOID || value.Observed.Identity.BaseRef != value.Publication.BaseRef || !value.Observed.Identity.FactoryOwned || value.Observed.Identity.BaseOID != value.ObservedProtectedBase {
		return ErrPublicationEvidence
	}
	if value.ObservationDigest == "" || value.ObservationDigest != CanonicalManualMergeObservationDigest(value) {
		return ErrPublicationEvidence
	}
	return nil
}

func validManualDigest(value string) bool {
	return len(value) == 71 && strings.HasPrefix(value, "sha256:") && validDigest(value[len("sha256:"):])
}

func manualMergeObservationEventPayload(digest string) string {
	payload, _ := json.Marshal(struct {
		ObservationDigest string `json:"observation_digest"`
	}{digest})
	return string(payload)
}

// authenticateManualMergePublication revalidates the publication authority
// referenced by a manual observation on the caller's SQLite connection. It is
// shared by the initial transition and post-pause rearm so neither path trusts
// the append-only observation row without its exact candidate/PR/effect chain.
func (s *Store) authenticateManualMergePublication(ctx context.Context, conn *sql.Conn, value ManualMergeObservation) error {
	publication, found, err := loadPublicationEvidenceRow(ctx, conn, value.Ref)
	if err != nil || !found {
		return fmt.Errorf("%w: publication row", ErrPublicationEvidence)
	}
	if err := loadLatestPublicationRebind(ctx, conn, &publication); err != nil {
		return fmt.Errorf("%w: publication rebind", ErrPublicationEvidence)
	}
	if err := validPublishedCandidateEvidence(publication); err != nil || publication.PullRequest != value.Publication || publication.Candidate.Snapshot.Generation != value.CandidateGeneration || publication.Candidate.Snapshot.HeadSHA != value.CandidateHeadSHA || publication.Candidate.Snapshot.BaseSHA != value.CandidateBaseSHA || publication.Candidate.Snapshot.TreeSHA != value.CandidateTreeSHA || publication.WitnessDigest != value.PublicationWitnessDigest {
		return fmt.Errorf("%w: publication identity", ErrPublicationEvidence)
	}
	if err := validateStoredPublicationEffectQuery(ctx, conn, value.Ref, publication.TicketVersion, publication.Fence, publication.PushEffect); err != nil {
		return fmt.Errorf("%w: push effect", ErrPublicationEvidence)
	}
	if err := validateStoredPublicationEffectQuery(ctx, conn, value.Ref, publication.TicketVersion, publication.Fence, publication.PRCreateOrUpdateEffect); err != nil {
		return fmt.Errorf("%w: pull request effect", ErrPublicationEvidence)
	}
	return nil
}

// RecordManualMergeObservation atomically appends the observation authority
// and advances waiting_manual_merge to reconciling. No GitHub operation is
// performed here; retrying after a crash only reads this authority.
func (s *Store) RecordManualMergeObservation(ctx context.Context, transition Transition, value ManualMergeObservation) (TransitionResult, error) {
	if transition.From != domain.StateWaitingManualMerge || transition.To != domain.StateReconciling || transition.Trigger != "external_merge_observed" || transition.Ref.Validate() != nil || transition.ResumeState != "" {
		return TransitionResult{}, ErrPublicationEvidence
	}
	if value.Ref != transition.Ref || value.CurrentTicketVersion != transition.ExpectedVersion || value.CurrentFence != transition.Fence {
		return TransitionResult{}, ErrStaleFence
	}
	if err := validManualMergeObservation(value); err != nil {
		return TransitionResult{}, err
	}
	var result TransitionResult
	err := s.write(ctx, func(conn *sql.Conn) error {
		var state domain.State
		var version, runner uint64
		var mergeMode domain.MergeMode
		if err := conn.QueryRowContext(ctx, `SELECT state,version,runner_epoch,merge_mode FROM tickets WHERE channel=? AND project_id=? AND id=?`, transition.Ref.Channel, transition.Ref.Project, transition.Ref.Ticket).Scan(&state, &version, &runner, &mergeMode); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return ErrNotFound
			}
			return err
		}
		if state != domain.StateWaitingManualMerge || mergeMode != domain.MergeManual || version != transition.ExpectedVersion || runner != transition.Fence.RunnerEpoch {
			return ErrStaleFence
		}
		if err := s.currentFence(ctx, conn, transition.Ref.Channel, version, runner, transition.Fence); err != nil {
			return err
		}
		// Authenticate the complete final-review -> waiting_manual_merge chain
		// on this same connection. A publication row alone cannot prove that
		// this PR was the candidate admitted to manual mode.
		endpoint, err := s.finalReviewRecoveryEndpoint(ctx, conn, transition.Ref, domain.StateWaitingManualMerge)
		if err != nil || endpoint.version > version || endpoint.runner > runner {
			return fmt.Errorf("%w: final review endpoint", ErrPublicationEvidence)
		}
		leader, err := normalRecoveryLeaderAt(ctx, conn, transition.Ref, endpoint, version, runner)
		if err != nil || leader != transition.Fence.LeaderEpoch {
			return fmt.Errorf("%w: manual merge endpoint", ErrStaleFence)
		}
		if err := s.authenticateManualMergePublication(ctx, conn, value); err != nil {
			return err
		}

		_, err = conn.ExecContext(ctx, `INSERT INTO manual_merge_observations(channel,project_id,ticket_id,current_ticket_version,current_leader_epoch,current_runner_epoch,candidate_generation,candidate_head_sha,candidate_base_sha,candidate_tree_sha,publication_witness_digest,publication_host,publication_owner,publication_name,publication_pr_number,publication_head_owner,publication_head_repository,publication_head_ref,publication_head_oid,publication_base_ref,publication_base_oid,publication_factory_owned,observed_host,observed_owner,observed_name,observed_pr_number,observed_head_owner,observed_head_repository,observed_head_ref,observed_head_oid,observed_base_ref,observed_base_oid,observed_factory_owned,merge_commit,observed_protected_base,observation_digest,created_at) VALUES(?,?,?,?,?,?,?,?,?,?, ?,?,?,?,?,?,?,?,?,?, ?,?,?,?,?,?,?,?,?,?, ?,?,?,?,?,?,?) ON CONFLICT(channel,project_id,ticket_id,candidate_generation,candidate_head_sha) DO NOTHING`, transition.Ref.Channel, transition.Ref.Project, transition.Ref.Ticket, value.CurrentTicketVersion, value.CurrentFence.LeaderEpoch, value.CurrentFence.RunnerEpoch, value.CandidateGeneration, value.CandidateHeadSHA, value.CandidateBaseSHA, value.CandidateTreeSHA, value.PublicationWitnessDigest, value.Publication.Repository.Host, value.Publication.Repository.Owner, value.Publication.Repository.Name, value.Publication.Number, value.Publication.HeadOwner, value.Publication.HeadRepository, value.Publication.HeadRef, value.Publication.HeadOID, value.Publication.BaseRef, value.Publication.BaseOID, boolInt(value.Publication.FactoryOwned), value.Observed.Identity.Repository.Host, value.Observed.Identity.Repository.Owner, value.Observed.Identity.Repository.Name, value.Observed.Identity.Number, value.Observed.Identity.HeadOwner, value.Observed.Identity.HeadRepository, value.Observed.Identity.HeadRef, value.Observed.Identity.HeadOID, value.Observed.Identity.BaseRef, value.Observed.Identity.BaseOID, boolInt(value.Observed.Identity.FactoryOwned), value.MergeCommit, value.ObservedProtectedBase, value.ObservationDigest, time.Now().UTC().Format(time.RFC3339Nano))
		if err != nil {
			return err
		}
		stored, found, err := loadManualMergeObservation(ctx, conn, transition.Ref)
		if err != nil || !found || stored != value {
			return ErrPublicationEvidence
		}
		payload := manualMergeObservationEventPayload(value.ObservationDigest)
		updated, err := conn.ExecContext(ctx, `UPDATE tickets SET state='reconciling',resume_state=NULL,version=version+1 WHERE channel=? AND project_id=? AND id=? AND state='waiting_manual_merge' AND version=? AND runner_epoch=?`, transition.Ref.Channel, transition.Ref.Project, transition.Ref.Ticket, version, runner)
		if err != nil {
			return err
		}
		if n, _ := updated.RowsAffected(); n != 1 {
			return ErrStaleFence
		}
		event, err := conn.ExecContext(ctx, `INSERT INTO events(channel,project_id,ticket_id,ticket_version,trigger,from_state,to_state,payload,created_at) VALUES(?,?,?,?,?,?,?,?,?)`, transition.Ref.Channel, transition.Ref.Project, transition.Ref.Ticket, version+1, transition.Trigger, transition.From, transition.To, payload, time.Now().UTC().Format(time.RFC3339Nano))
		if err != nil {
			return err
		}
		result.Version = version + 1
		result.EventID, _ = event.LastInsertId()
		return nil
	})
	return result, err
}

// LoadManualMergeObservation authenticates the immutable authority and its
// canonical digest. Missing or malformed rows are indistinguishable from
// unusable evidence to callers.
func (s *Store) LoadManualMergeObservation(ctx context.Context, ref domain.TicketRef) (ManualMergeObservation, error) {
	if ref.Validate() != nil {
		return ManualMergeObservation{}, ErrPublicationEvidence
	}
	value, found, err := loadManualMergeObservation(ctx, s.db, ref)
	if err != nil {
		return ManualMergeObservation{}, normalizeBusy(ctx, err)
	}
	if !found {
		return ManualMergeObservation{}, ErrNotFound
	}
	if err := validManualMergeObservation(value); err != nil {
		return ManualMergeObservation{}, err
	}
	return value, nil
}

func loadManualMergeObservation(ctx context.Context, q interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, ref domain.TicketRef) (ManualMergeObservation, bool, error) {
	var value ManualMergeObservation
	var project, ticket string
	var pub, observed contracts.PullRequestIdentity
	var pubOwned, observedOwned int
	var observedBase string
	err := q.QueryRowContext(ctx, `SELECT project_id,ticket_id,current_ticket_version,current_leader_epoch,current_runner_epoch,candidate_generation,candidate_head_sha,candidate_base_sha,candidate_tree_sha,publication_witness_digest,publication_host,publication_owner,publication_name,publication_pr_number,publication_head_owner,publication_head_repository,publication_head_ref,publication_head_oid,publication_base_ref,publication_base_oid,publication_factory_owned,observed_host,observed_owner,observed_name,observed_pr_number,observed_head_owner,observed_head_repository,observed_head_ref,observed_head_oid,observed_base_ref,observed_base_oid,observed_factory_owned,merge_commit,observed_protected_base,observation_digest FROM manual_merge_observations WHERE channel=? AND project_id=? AND ticket_id=? ORDER BY observation_id DESC LIMIT 1`, ref.Channel, ref.Project, ref.Ticket).Scan(&project, &ticket, &value.CurrentTicketVersion, &value.CurrentFence.LeaderEpoch, &value.CurrentFence.RunnerEpoch, &value.CandidateGeneration, &value.CandidateHeadSHA, &value.CandidateBaseSHA, &value.CandidateTreeSHA, &value.PublicationWitnessDigest, &pub.Repository.Host, &pub.Repository.Owner, &pub.Repository.Name, &pub.Number, &pub.HeadOwner, &pub.HeadRepository, &pub.HeadRef, &pub.HeadOID, &pub.BaseRef, &pub.BaseOID, &pubOwned, &observed.Repository.Host, &observed.Repository.Owner, &observed.Repository.Name, &observed.Number, &observed.HeadOwner, &observed.HeadRepository, &observed.HeadRef, &observed.HeadOID, &observed.BaseRef, &observed.BaseOID, &observedOwned, &value.MergeCommit, &observedBase, &value.ObservationDigest)
	if errors.Is(err, sql.ErrNoRows) {
		return ManualMergeObservation{}, false, nil
	}
	if err != nil {
		return ManualMergeObservation{}, false, err
	}
	value.Ref = ref
	value.Ref.Project, value.Ref.Ticket = domain.ProjectID(project), domain.TicketID(ticket)
	pub.FactoryOwned, observed.FactoryOwned = pubOwned == 1, observedOwned == 1
	value.Publication, value.ObservedProtectedBase = pub, observedBase
	value.Observed = contracts.PublishedPullRequestObservation{Identity: observed, State: "MERGED", Merged: true, MergeCommit: value.MergeCommit, BaseHeadOID: observedBase}
	return value, true, nil
}

// ValidateManualMergeObservation compares a fresh read-only observation with
// the stored authority before reconcile_pass. It deliberately has no GitHub
// or Git mutation capability.
func (s *Store) ValidateManualMergeObservation(ctx context.Context, ref domain.TicketRef, fresh ManualMergeObservation) error {
	stored, err := s.LoadManualMergeObservation(ctx, ref)
	if err != nil {
		return err
	}
	if stored.Ref != fresh.Ref || stored.CandidateGeneration != fresh.CandidateGeneration || stored.CandidateHeadSHA != fresh.CandidateHeadSHA || stored.CandidateBaseSHA != fresh.CandidateBaseSHA || stored.CandidateTreeSHA != fresh.CandidateTreeSHA || stored.PublicationWitnessDigest != fresh.PublicationWitnessDigest || stored.Publication != fresh.Publication || stored.Observed.Identity != fresh.Observed.Identity || stored.Observed.State != fresh.Observed.State || stored.Observed.Merged != fresh.Observed.Merged || stored.Observed.Draft != fresh.Observed.Draft || stored.Observed.BaseHeadOID != fresh.Observed.BaseHeadOID || stored.MergeCommit != fresh.MergeCommit || stored.ObservedProtectedBase != fresh.ObservedProtectedBase {
		return ErrPublicationEvidence
	}
	if fresh.ObservationDigest != "" && fresh.CurrentTicketVersion != 0 && fresh.CurrentFence.LeaderEpoch != 0 && fresh.ObservationDigest != CanonicalManualMergeObservationDigest(fresh) {
		return ErrPublicationEvidence
	}
	ticket, err := s.Ticket(ctx, ref)
	if err != nil {
		return err
	}
	if ticket.State != domain.StateReconciling || ticket.Version < stored.CurrentTicketVersion+1 || ticket.RunnerEpoch < stored.CurrentFence.RunnerEpoch {
		return ErrStaleFence
	}
	var baselineLeader uint64
	if err := s.db.QueryRowContext(ctx, `SELECT leader_epoch FROM daemon_instances WHERE channel=?`, ref.Channel).Scan(&baselineLeader); err != nil {
		return normalizeBusy(ctx, err)
	}
	if baselineLeader == 0 {
		return ErrStaleFence
	}
	endpoint := normalRecoveryEndpoint{version: stored.CurrentTicketVersion + 1, runner: stored.CurrentFence.RunnerEpoch, leader: stored.CurrentFence.LeaderEpoch}
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return normalizeBusy(ctx, err)
	}
	leader, err := normalRecoveryLeaderAt(ctx, conn, ref, endpoint, ticket.Version, ticket.RunnerEpoch)
	_ = conn.Close()
	if err != nil || leader != baselineLeader {
		return ErrPublicationEvidence
	}
	var count int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version=? AND trigger='external_merge_observed' AND from_state='waiting_manual_merge' AND to_state='reconciling' AND payload=?`, ref.Channel, ref.Project, ref.Ticket, stored.CurrentTicketVersion+1, manualMergeObservationEventPayload(stored.ObservationDigest)).Scan(&count); err != nil {
		return normalizeBusy(ctx, err)
	}
	if count != 1 {
		return ErrPublicationEvidence
	}
	return nil
}

// ReconcileManualMergeObservation is the descriptive recovery alias. It is a
// read-only check and intentionally does not advance the ticket.
func (s *Store) ReconcileManualMergeObservation(ctx context.Context, ref domain.TicketRef, fresh ManualMergeObservation) error {
	return s.ValidateManualMergeObservation(ctx, ref, fresh)
}

// ManualMergeObservation is also available under the concise reader name
// used by recovery composition.
func (s *Store) ManualMergeObservation(ctx context.Context, ref domain.TicketRef) (ManualMergeObservation, error) {
	return s.LoadManualMergeObservation(ctx, ref)
}
