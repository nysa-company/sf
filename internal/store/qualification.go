package store

import (
	"context"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
)

type QualificationProfile string

const (
	QualificationDisabled   QualificationProfile = "disabled"
	QualificationGuarded    QualificationProfile = "qualified_guarded"
	QualificationAutonomous QualificationProfile = "autonomous_eligible"
)

type ProviderQualification struct {
	ID            int64
	Channel       domain.Channel
	RunID         string
	Provider      domain.ProviderIdentity
	BinaryDigest  string
	PolicyDigest  string
	FixtureDigest string
	AuthDigest    string
	AuthMode      string
	Profile       QualificationProfile
	FailedProbes  []string
	ReasonCode    string
	CreatedAt     time.Time
	// Credential-bearing guarded records are admitted through the daemon's current
	// supervisor key. These values are non-secret audit evidence, never a
	// provider transcript or credential.
	ProbeDigest          string
	AttestedLeaderEpoch  uint64
	AttestationSignature []byte
}

type ProviderPair struct {
	Channel    domain.Channel
	Planner    ProviderQualification
	Builder    ProviderQualification
	Reviewer   ProviderQualification
	SelectedAt time.Time
}

var (
	qualificationName = regexp.MustCompile(`^[a-z][a-z0-9-]{0,47}$`)
	probeName         = regexp.MustCompile(`^[a-z][a-z0-9_]{0,63}$`)
	reasonCode        = regexp.MustCompile(`^[a-z][a-z0-9_]{0,99}$`)
)

func (s *Store) RecordProviderQualification(ctx context.Context, input ProviderQualification) (ProviderQualification, bool, error) {
	if (input.Provider.Provider == "codex" || input.AuthMode != "") && input.Profile == QualificationGuarded {
		return ProviderQualification{}, false, errors.New("guarded qualification requires a supervisor attestation")
	}
	return s.recordProviderQualification(ctx, input, nil)
}

// RecordAttestedProviderQualification is the sole production entrypoint for
// a passing credential-bearing qualification. It verifies an exact signature from the
// currently fenced daemon supervisor before SQLite admits the row.
func (s *Store) RecordAttestedProviderQualification(ctx context.Context, input ProviderQualification, attestation contracts.QualificationAttestation) (ProviderQualification, bool, error) {
	if !attestedProviderAuthMode(input.Provider.Provider, input.AuthMode) || input.Profile != QualificationGuarded || !sameQualificationAttestation(input, attestation) {
		return ProviderQualification{}, false, errors.New("invalid provider qualification attestation")
	}
	input.ProbeDigest = attestation.ProbeDigest
	input.AuthDigest = attestation.AuthDigest
	input.AuthMode = attestation.AuthMode
	// The leader is read and signature-verified inside the same SQLite write
	// transaction as the insert. A concurrent daemon takeover therefore makes
	// this observation fail rather than accepting a stale supervisor key.
	input.AttestedLeaderEpoch = 1
	input.AttestationSignature = append([]byte(nil), attestation.Signature...)
	return s.recordProviderQualification(ctx, input, &attestation)
}

func sameQualificationAttestation(input ProviderQualification, value contracts.QualificationAttestation) bool {
	return input.Channel == value.Channel && input.RunID == value.RunID && input.Provider == value.Identity && input.BinaryDigest == value.BinaryDigest && input.PolicyDigest == value.PolicyDigest && input.FixtureDigest == value.FixtureDigest && input.ProbeDigest == value.ProbeDigest && input.AuthMode == value.AuthMode && value.AuthDigest != "" && input.CreatedAt.UnixNano() == value.CreatedUnixNanos && value.Profile == contracts.ProfileGuarded
}

func (s *Store) recordProviderQualification(ctx context.Context, input ProviderQualification, attestation *contracts.QualificationAttestation) (ProviderQualification, bool, error) {
	normalized, failedJSON, err := normalizeQualification(input)
	if err != nil {
		return ProviderQualification{}, false, err
	}
	created := false
	var result ProviderQualification
	err = s.write(ctx, func(conn *sql.Conn) error {
		if attestation != nil {
			var leader uint64
			var key []byte
			if err := conn.QueryRowContext(ctx, `SELECT leader_epoch,recovery_public_key FROM daemon_instances WHERE channel=?`, normalized.Channel).Scan(&leader, &key); err != nil || leader == 0 || attestation.LeaderEpoch != leader || !contracts.VerifyQualificationAttestation(key, *attestation) {
				return errors.New("provider qualification attestation is not signed by the current supervisor")
			}
			normalized.AttestedLeaderEpoch = leader
		}
		existing, queryErr := qualificationByRun(ctx, conn, normalized.Channel, normalized.RunID)
		if queryErr == nil {
			if !sameQualification(existing, normalized) {
				return ErrQualificationConflict
			}
			result = existing
			return nil
		}
		if !errors.Is(queryErr, ErrNotFound) {
			return queryErr
		}
		createdAt := normalized.CreatedAt.UTC().Format(time.RFC3339Nano)
		row, insertErr := conn.ExecContext(ctx, `INSERT INTO provider_qualifications(
			channel, run_id, provider, model, family, provider_version, binary_digest, policy_digest, fixture_digest,
			profile, failed_probes_json, reason_code, created_at, auth_digest, auth_mode, probe_digest, attested_leader_epoch, attestation_signature) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			normalized.Channel, normalized.RunID, normalized.Provider.Provider, normalized.Provider.Model, normalized.Provider.Family,
			normalized.Provider.Version, normalized.BinaryDigest, normalized.PolicyDigest, normalized.FixtureDigest,
			normalized.Profile, string(failedJSON), normalized.ReasonCode, createdAt, normalized.AuthDigest, normalized.AuthMode, normalized.ProbeDigest, normalized.AttestedLeaderEpoch, normalized.AttestationSignature)
		if insertErr != nil {
			return insertErr
		}
		id, insertErr := row.LastInsertId()
		if insertErr != nil {
			return insertErr
		}
		// Any new verdict for this exact provider identity invalidates a selected
		// pair until the operator explicitly selects the now-current records.
		if _, deleteErr := conn.ExecContext(ctx, `DELETE FROM provider_pair_selections WHERE channel=? AND (
			builder_qualification_id IN (SELECT id FROM provider_qualifications WHERE channel=? AND provider=? AND model=? AND family=? AND provider_version=? AND id<>?) OR
			reviewer_qualification_id IN (SELECT id FROM provider_qualifications WHERE channel=? AND provider=? AND model=? AND family=? AND provider_version=? AND id<>?))`,
			normalized.Channel,
			normalized.Channel, normalized.Provider.Provider, normalized.Provider.Model, normalized.Provider.Family, normalized.Provider.Version, id,
			normalized.Channel, normalized.Provider.Provider, normalized.Provider.Model, normalized.Provider.Family, normalized.Provider.Version, id); deleteErr != nil {
			return deleteErr
		}
		if normalized.Profile == QualificationDisabled {
			if _, deleteErr := conn.ExecContext(ctx, `DELETE FROM provider_pair_selections WHERE channel=? AND (
				builder_qualification_id IN (SELECT id FROM provider_qualifications WHERE channel=? AND provider=? AND provider_version=?) OR
				reviewer_qualification_id IN (SELECT id FROM provider_qualifications WHERE channel=? AND provider=? AND provider_version=?))`,
				normalized.Channel,
				normalized.Channel, normalized.Provider.Provider, normalized.Provider.Version,
				normalized.Channel, normalized.Provider.Provider, normalized.Provider.Version); deleteErr != nil {
				return deleteErr
			}
		}
		normalized.ID = id
		result = normalized
		created = true
		return nil
	})
	if err != nil {
		return ProviderQualification{}, false, normalizeBusy(ctx, err)
	}
	return result, created, nil
}

func (s *Store) LatestProviderQualification(ctx context.Context, channel domain.Channel, identity domain.ProviderIdentity) (ProviderQualification, error) {
	if !channel.Valid() || validateProviderIdentity(identity) != nil {
		return ProviderQualification{}, errors.New("invalid provider qualification identity")
	}
	value, err := scanQualification(s.db.QueryRowContext(ctx, `SELECT id, channel, run_id, provider, model, family, provider_version,
		binary_digest, policy_digest, fixture_digest, profile, failed_probes_json, reason_code, created_at, auth_digest, auth_mode, probe_digest, attested_leader_epoch, attestation_signature
		FROM provider_qualifications WHERE channel=? AND provider=? AND model=? AND family=? AND provider_version=? ORDER BY id DESC LIMIT 1`,
		channel, identity.Provider, identity.Model, identity.Family, identity.Version))
	if err != nil {
		return ProviderQualification{}, normalizeBusy(ctx, err)
	}
	return value, nil
}

func (s *Store) SelectProviderPair(ctx context.Context, channel domain.Channel, builderID, reviewerID int64, selectedAt time.Time) (ProviderPair, bool, error) {
	return s.SelectProviderSet(ctx, channel, builderID, builderID, reviewerID, selectedAt)
}

// SelectProviderSet authenticates and records all three roles in one write
// transaction. A refused role cannot partially replace an existing selection.
// Planner may equal Builder; Reviewer must remain independent from Builder.
func (s *Store) SelectProviderSet(ctx context.Context, channel domain.Channel, plannerID, builderID, reviewerID int64, selectedAt time.Time) (ProviderPair, bool, error) {
	if !channel.Valid() || plannerID <= 0 || builderID <= 0 || reviewerID <= 0 || builderID == reviewerID || selectedAt.IsZero() {
		return ProviderPair{}, false, ErrProviderPairRefused
	}
	var pair ProviderPair
	created := false
	err := s.write(ctx, func(conn *sql.Conn) error {
		planner, err := currentQualificationByID(ctx, conn, channel, plannerID)
		if err != nil || planner.Profile == QualificationDisabled {
			return ErrProviderPairRefused
		}
		builder, err := currentQualificationByID(ctx, conn, channel, builderID)
		if err != nil {
			return ErrProviderPairRefused
		}
		reviewer, err := currentQualificationByID(ctx, conn, channel, reviewerID)
		if err != nil {
			return ErrProviderPairRefused
		}
		if builder.Profile == QualificationDisabled || reviewer.Profile == QualificationDisabled || builder.Provider.Family == reviewer.Provider.Family {
			return ErrProviderPairRefused
		}
		for _, q := range []ProviderQualification{planner, builder, reviewer} {
			if q.AuthMode == "" {
				continue
			} // credential-free historical fixtures
			var epoch uint64
			var key []byte
			if err := conn.QueryRowContext(ctx, `SELECT leader_epoch,recovery_public_key FROM daemon_instances WHERE channel=?`, channel).Scan(&epoch, &key); err != nil || q.AttestedLeaderEpoch != epoch || !attestedProviderAuthMode(q.Provider.Provider, q.AuthMode) || !contracts.VerifyQualificationAttestation(key, contracts.QualificationAttestation{Channel: q.Channel, RunID: q.RunID, Identity: q.Provider, BinaryDigest: q.BinaryDigest, PolicyDigest: q.PolicyDigest, FixtureDigest: q.FixtureDigest, AuthDigest: q.AuthDigest, AuthMode: q.AuthMode, ProbeDigest: q.ProbeDigest, Profile: contracts.ProfileGuarded, CreatedUnixNanos: q.CreatedAt.UnixNano(), LeaderEpoch: q.AttestedLeaderEpoch, Nonce: q.RunID, Signature: q.AttestationSignature}) {
				return ErrProviderPairRefused
			}
		}
		var oldPlanner, oldBuilder, oldReviewer int64
		err = conn.QueryRowContext(ctx, `SELECT planner_qualification_id, builder_qualification_id, reviewer_qualification_id FROM provider_pair_selections WHERE channel=?`, channel).Scan(&oldPlanner, &oldBuilder, &oldReviewer)
		if err == nil && oldPlanner == plannerID && oldBuilder == builderID && oldReviewer == reviewerID {
			pair = ProviderPair{Channel: channel, Planner: planner, Builder: builder, Reviewer: reviewer}
			var selected string
			if scanErr := conn.QueryRowContext(ctx, `SELECT selected_at FROM provider_pair_selections WHERE channel=?`, channel).Scan(&selected); scanErr != nil {
				return scanErr
			}
			pair.SelectedAt, err = time.Parse(time.RFC3339Nano, selected)
			if err != nil {
				return err
			}
			return nil
		}
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		stamp := selectedAt.UTC().Format(time.RFC3339Nano)
		if _, err := conn.ExecContext(ctx, `INSERT INTO provider_pair_selections(channel, planner_qualification_id, builder_qualification_id, reviewer_qualification_id, selected_at)
			VALUES (?, ?, ?, ?, ?) ON CONFLICT(channel) DO UPDATE SET planner_qualification_id=excluded.planner_qualification_id, builder_qualification_id=excluded.builder_qualification_id,
			reviewer_qualification_id=excluded.reviewer_qualification_id, selected_at=excluded.selected_at`, channel, plannerID, builderID, reviewerID, stamp); err != nil {
			return err
		}
		pair = ProviderPair{Channel: channel, Planner: planner, Builder: builder, Reviewer: reviewer, SelectedAt: selectedAt.UTC()}
		created = true
		return nil
	})
	if err != nil {
		return ProviderPair{}, false, normalizeBusy(ctx, err)
	}
	return pair, created, nil
}

func (s *Store) ProviderPair(ctx context.Context, channel domain.Channel) (ProviderPair, error) {
	if !channel.Valid() {
		return ProviderPair{}, ErrNotFound
	}
	var plannerID, builderID, reviewerID int64
	var selected string
	if err := s.db.QueryRowContext(ctx, `SELECT planner_qualification_id, builder_qualification_id, reviewer_qualification_id, selected_at FROM provider_pair_selections WHERE channel=?`, channel).Scan(&plannerID, &builderID, &reviewerID, &selected); err != nil {
		return ProviderPair{}, normalizeNotFound(ctx, err)
	}
	planner, err := currentQualificationByID(ctx, s.db, channel, plannerID)
	if err != nil {
		return ProviderPair{}, err
	}
	builder, err := currentQualificationByID(ctx, s.db, channel, builderID)
	if err != nil {
		return ProviderPair{}, err
	}
	reviewer, err := currentQualificationByID(ctx, s.db, channel, reviewerID)
	if err != nil {
		return ProviderPair{}, err
	}
	selectedAt, err := time.Parse(time.RFC3339Nano, selected)
	if err != nil {
		return ProviderPair{}, err
	}
	return ProviderPair{Channel: channel, Planner: planner, Builder: builder, Reviewer: reviewer, SelectedAt: selectedAt}, nil
}

func normalizeQualification(input ProviderQualification) (ProviderQualification, []byte, error) {
	if input.ID != 0 || !input.Channel.Valid() || !lowerHex(input.RunID, 16) || validateProviderIdentity(input.Provider) != nil ||
		!lowerHex(input.BinaryDigest, 32) || !lowerHex(input.PolicyDigest, 32) || !lowerHex(input.FixtureDigest, 32) || input.CreatedAt.IsZero() {
		return ProviderQualification{}, nil, errors.New("invalid provider qualification")
	}
	input.CreatedAt = input.CreatedAt.UTC()
	if input.AttestationSignature == nil {
		input.AttestationSignature = []byte{}
	}
	if (input.Provider.Provider == "codex" || input.AuthMode != "") && input.Profile == QualificationGuarded {
		if !attestedProviderAuthMode(input.Provider.Provider, input.AuthMode) || !lowerHex(input.AuthDigest, 32) || !lowerHex(input.ProbeDigest, 32) || input.AttestedLeaderEpoch == 0 || len(input.AttestationSignature) != 64 {
			return ProviderQualification{}, nil, errors.New("provider qualification attestation evidence is invalid")
		}
	} else if input.AuthDigest != "" || input.AuthMode != "" || input.ProbeDigest != "" || input.AttestedLeaderEpoch != 0 || len(input.AttestationSignature) != 0 {
		return ProviderQualification{}, nil, errors.New("unexpected qualification attestation evidence")
	}
	input.FailedProbes = append([]string(nil), input.FailedProbes...)
	if input.FailedProbes == nil {
		input.FailedProbes = []string{}
	}
	sort.Strings(input.FailedProbes)
	for index, probe := range input.FailedProbes {
		if !probeName.MatchString(probe) || index > 0 && input.FailedProbes[index-1] == probe {
			return ProviderQualification{}, nil, errors.New("invalid failed qualification probe")
		}
	}
	switch input.Profile {
	case QualificationDisabled:
		if len(input.FailedProbes) == 0 || !reasonCode.MatchString(input.ReasonCode) {
			return ProviderQualification{}, nil, errors.New("disabled qualification requires failed probes and a reason code")
		}
	case QualificationGuarded, QualificationAutonomous:
		if len(input.FailedProbes) != 0 || input.ReasonCode != "" {
			return ProviderQualification{}, nil, errors.New("passing qualification cannot contain failure data")
		}
	default:
		return ProviderQualification{}, nil, errors.New("invalid provider qualification profile")
	}
	failedJSON, err := json.Marshal(input.FailedProbes)
	if err != nil || len(failedJSON) > 16*1024 {
		return ProviderQualification{}, nil, errors.New("failed qualification probes exceed limit")
	}
	return input, failedJSON, nil
}

// LeaderEpoch is the currently fenced daemon epoch. Qualification binds this
// exact value before signing, so a later daemon restart cannot reuse it.
func (s *Store) LeaderEpoch(ctx context.Context, channel domain.Channel) (uint64, error) {
	if !channel.Valid() {
		return 0, ErrNotFound
	}
	var epoch uint64
	if err := s.db.QueryRowContext(ctx, `SELECT leader_epoch FROM daemon_instances WHERE channel=?`, channel).Scan(&epoch); err != nil {
		return 0, normalizeNotFound(ctx, err)
	}
	if epoch == 0 {
		return 0, ErrNotFound
	}
	return epoch, nil
}

// QualificationCurrent verifies that a credential-bearing qualification is signed by
// the daemon leader currently recorded for this channel. It is deliberately
// rechecked at composition and paid-attempt admission, not merely at insert.
func (s *Store) QualificationCurrent(ctx context.Context, channel domain.Channel, value ProviderQualification) bool {
	if value.Provider.Provider != "codex" && value.AuthMode == "" {
		return true
	}
	if !attestedProviderAuthMode(value.Provider.Provider, value.AuthMode) {
		return false
	}
	var epoch uint64
	var key []byte
	if err := s.db.QueryRowContext(ctx, `SELECT leader_epoch,recovery_public_key FROM daemon_instances WHERE channel=?`, channel).Scan(&epoch, &key); err != nil {
		return false
	}
	return value.AttestedLeaderEpoch == epoch && contracts.VerifyQualificationAttestation(key, contracts.QualificationAttestation{
		Channel: value.Channel, RunID: value.RunID, Identity: value.Provider,
		BinaryDigest: value.BinaryDigest, PolicyDigest: value.PolicyDigest, FixtureDigest: value.FixtureDigest,
		AuthDigest: value.AuthDigest, AuthMode: value.AuthMode, ProbeDigest: value.ProbeDigest,
		Profile: contracts.ProfileGuarded, CreatedUnixNanos: value.CreatedAt.UnixNano(), LeaderEpoch: value.AttestedLeaderEpoch,
		Nonce: value.RunID, Signature: value.AttestationSignature,
	})
}

// These are authentication classes, not assertions of free usage. Runtime
// policies still have to prove isolation and trustworthy monetary accounting.
// Credential-free legacy fixtures remain separate from executable admission.
func attestedProviderAuthMode(provider, mode string) bool {
	switch provider {
	case "codex":
		return mode == "chatgpt_subscription"
	case "claude":
		return mode == "claude_subscription"
	case "cursor":
		return mode == "cursor_api" || mode == "cursor_browser"
	default:
		return false
	}
}

func validateProviderIdentity(identity domain.ProviderIdentity) error {
	if !qualificationName.MatchString(identity.Provider) || !boundedSafe(identity.Model, 200) || !boundedSafe(identity.Family, 100) || !boundedSafe(identity.Version, 200) {
		return errors.New("invalid provider identity")
	}
	return nil
}

func boundedSafe(value string, maximum int) bool {
	if value == "" || len(value) > maximum || strings.TrimSpace(value) != value {
		return false
	}
	for _, character := range value {
		if character < 0x20 || character == 0x7f {
			return false
		}
	}
	return true
}

func lowerHex(value string, bytes int) bool {
	if len(value) != bytes*2 || strings.ToLower(value) != value {
		return false
	}
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == bytes
}

func qualificationByRun(ctx context.Context, query interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, channel domain.Channel, runID string) (ProviderQualification, error) {
	return scanQualification(query.QueryRowContext(ctx, `SELECT id, channel, run_id, provider, model, family, provider_version,
		binary_digest, policy_digest, fixture_digest, profile, failed_probes_json, reason_code, created_at, auth_digest, auth_mode, probe_digest, attested_leader_epoch, attestation_signature
		FROM provider_qualifications WHERE channel=? AND run_id=?`, channel, runID))
}

func currentQualificationByID(ctx context.Context, query interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, channel domain.Channel, id int64) (ProviderQualification, error) {
	return scanQualification(query.QueryRowContext(ctx, `SELECT q.id, q.channel, q.run_id, q.provider, q.model, q.family, q.provider_version,
		q.binary_digest, q.policy_digest, q.fixture_digest, q.profile, q.failed_probes_json, q.reason_code, q.created_at, q.auth_digest, q.auth_mode, q.probe_digest, q.attested_leader_epoch, q.attestation_signature
		FROM provider_qualifications q WHERE q.channel=? AND q.id=? AND NOT EXISTS (
			SELECT 1 FROM provider_qualifications newer WHERE newer.channel=q.channel AND newer.provider=q.provider AND newer.model=q.model
			AND newer.family=q.family AND newer.provider_version=q.provider_version AND newer.id>q.id) AND NOT EXISTS (
			SELECT 1 FROM provider_qualifications disabled WHERE disabled.channel=q.channel AND disabled.provider=q.provider
			AND disabled.provider_version=q.provider_version AND disabled.profile='disabled' AND disabled.id>q.id)`, channel, id))
}

func scanQualification(row *sql.Row) (ProviderQualification, error) {
	var value ProviderQualification
	var failedJSON, created string
	if err := row.Scan(&value.ID, &value.Channel, &value.RunID, &value.Provider.Provider, &value.Provider.Model, &value.Provider.Family,
		&value.Provider.Version, &value.BinaryDigest, &value.PolicyDigest, &value.FixtureDigest, &value.Profile, &failedJSON, &value.ReasonCode, &created, &value.AuthDigest, &value.AuthMode, &value.ProbeDigest, &value.AttestedLeaderEpoch, &value.AttestationSignature); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ProviderQualification{}, ErrNotFound
		}
		return ProviderQualification{}, err
	}
	if err := json.Unmarshal([]byte(failedJSON), &value.FailedProbes); err != nil {
		return ProviderQualification{}, fmt.Errorf("decode failed qualification probes: %w", err)
	}
	parsed, err := time.Parse(time.RFC3339Nano, created)
	if err != nil {
		return ProviderQualification{}, fmt.Errorf("decode qualification time: %w", err)
	}
	value.CreatedAt = parsed
	return value, nil
}

func sameQualification(left, right ProviderQualification) bool {
	return left.Channel == right.Channel && left.RunID == right.RunID && left.Provider == right.Provider && left.BinaryDigest == right.BinaryDigest &&
		left.PolicyDigest == right.PolicyDigest && left.FixtureDigest == right.FixtureDigest && left.AuthDigest == right.AuthDigest && left.AuthMode == right.AuthMode && left.Profile == right.Profile &&
		strings.Join(left.FailedProbes, "\x00") == strings.Join(right.FailedProbes, "\x00") && left.ReasonCode == right.ReasonCode && left.CreatedAt.Equal(right.CreatedAt) &&
		left.ProbeDigest == right.ProbeDigest && left.AttestedLeaderEpoch == right.AttestedLeaderEpoch && string(left.AttestationSignature) == string(right.AttestationSignature)
}

func normalizeNotFound(ctx context.Context, err error) error {
	if errors.Is(err, sql.ErrNoRows) {
		return ErrNotFound
	}
	return normalizeBusy(ctx, err)
}
