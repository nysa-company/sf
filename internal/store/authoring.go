package store

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"path/filepath"
	"strings"
	"time"

	"github.com/nysa-company/sf/internal/authoring"
	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/providerjson"
)

var ErrAuthoring = errors.New("authoring session is unavailable, exhausted, or has an undrained turn")

type AuthoringSession struct {
	Channel       domain.Channel
	ID            string
	Purpose       string
	Project       domain.ProjectID
	Capability    contracts.AuthoringCapability
	ContextDigest string
}
type AuthoringTurn struct {
	Project        domain.ProjectID
	Claim          contracts.AuthoringClaim
	State, Outcome string
	Launch         contracts.ProviderLaunch
	Result         *contracts.AuthoringResult
}

func (s *Store) CreateAuthoringSession(ctx context.Context, session AuthoringSession) error {
	if !validAuthoringSession(session) {
		return ErrAuthoring
	}
	capability, _ := json.Marshal(session.Capability)
	return s.write(ctx, func(conn *sql.Conn) error {
		_, err := conn.ExecContext(ctx, `INSERT INTO authoring_sessions(channel,id,purpose,project_id,capability,auth_digest,context_digest,created_at) VALUES(?,?,?,?,?,?,?,?)`, session.Channel, session.ID, session.Purpose, session.Project, capability, session.Capability.AuthDigest, session.ContextDigest, time.Now().UTC().Format(time.RFC3339Nano))
		return err
	})
}
func (s *Store) AuthoringSession(ctx context.Context, channel domain.Channel, id string) (AuthoringSession, error) {
	return authoringSessionRow(ctx, s.db, channel, id)
}
func authoringSessionRow(ctx context.Context, q authoringQuery, channel domain.Channel, id string) (AuthoringSession, error) {
	value := AuthoringSession{Channel: channel, ID: id}
	var capability []byte
	err := q.QueryRowContext(ctx, `SELECT project_id,purpose,capability,auth_digest,context_digest FROM authoring_sessions WHERE channel=? AND id=?`, channel, id).Scan(&value.Project, &value.Purpose, &capability, &value.Capability.AuthDigest, &value.ContextDigest)
	auth := value.Capability.AuthDigest
	if err != nil {
		return value, ErrAuthoring
	}
	if !decodeAuthoringObject(capability, &value.Capability) {
		return value, ErrAuthoring
	}
	value.Capability.AuthDigest = auth
	if !validAuthoringSession(value) {
		return value, ErrAuthoring
	}
	return value, nil
}
func validAuthoringSession(value AuthoringSession) bool {
	return value.Project != "" && contracts.ValidAuthoringClaim(contracts.AuthoringClaim{Purpose: value.Purpose, Channel: value.Channel, Session: value.ID, TurnKey: "validate", Turn: 1, LeaderEpoch: 1, Identity: value.Capability.Identity, BinaryDigest: value.Capability.BinaryDigest, AuthDigest: value.Capability.AuthDigest, PolicyDigest: value.Capability.PolicyDigest, RequestDigest: value.ContextDigest, ContextDigest: value.ContextDigest})
}
func (s *Store) ReserveAuthoringTurn(ctx context.Context, session AuthoringSession, key, digest string, epoch uint64) (AuthoringTurn, bool, error) {
	var turn AuthoringTurn
	created := false
	if key == "" || len(key) > 128 || len(digest) != 64 || epoch == 0 {
		return turn, false, ErrAuthoring
	}
	err := s.write(ctx, func(conn *sql.Conn) error {
		stored, err := authoringSessionRow(ctx, conn, session.Channel, session.ID)
		if err != nil || stored != session {
			return ErrAuthoring
		}
		var current uint64
		if conn.QueryRowContext(ctx, `SELECT leader_epoch FROM daemon_instances WHERE channel=?`, session.Channel).Scan(&current) != nil || current != epoch {
			return ErrAuthoring
		}
		prior, e := authoringTurnRow(ctx, conn, session.Channel, session.ID, key)
		if e == nil {
			if prior.Claim.RequestDigest != digest {
				return ErrAuthoring
			}
			turn = prior
			return nil
		}
		if !errors.Is(e, sql.ErrNoRows) {
			return e
		}
		var count int
		if conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM authoring_turns WHERE channel=? AND session_id=?`, session.Channel, session.ID).Scan(&count) != nil || count >= contracts.AuthoringTurnLimit {
			return ErrAuthoring
		}
		turn.Claim = contracts.AuthoringClaim{Purpose: session.Purpose, Channel: session.Channel, Session: session.ID, TurnKey: key, Turn: count + 1, LeaderEpoch: epoch, Identity: session.Capability.Identity, BinaryDigest: session.Capability.BinaryDigest, AuthDigest: session.Capability.AuthDigest, PolicyDigest: session.Capability.PolicyDigest, RequestDigest: digest, ContextDigest: session.ContextDigest}
		if !contracts.ValidAuthoringClaim(turn.Claim) {
			return ErrAuthoring
		}
		raw, _ := json.Marshal(turn.Claim)
		if _, err := conn.ExecContext(ctx, `INSERT INTO authoring_turns(channel,session_id,turn_key,turn,claim,state,created_at) VALUES(?,?,?,?,?,'reserved',?)`, session.Channel, session.ID, key, count+1, raw, time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
			return ErrAuthoring
		}
		turn.State = "reserved"
		turn.Project = session.Project
		created = true
		return nil
	})
	return turn, created, err
}

type authoringQuery interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func authoringTurnRow(ctx context.Context, q authoringQuery, channel domain.Channel, session, key string) (AuthoringTurn, error) {
	var turn AuthoringTurn
	var claim, launch, result []byte
	var ordinal int
	err := q.QueryRowContext(ctx, `SELECT claim,turn,state,launch,outcome,result FROM authoring_turns WHERE channel=? AND session_id=? AND turn_key=?`, channel, session, key).Scan(&claim, &ordinal, &turn.State, &launch, &turn.Outcome, &result)
	if err != nil {
		return turn, err
	}
	if !decodeAuthoringObject(claim, &turn.Claim) || !contracts.ValidAuthoringClaim(turn.Claim) || turn.Claim.Channel != channel || turn.Claim.Session != session || turn.Claim.TurnKey != key {
		return turn, ErrAuthoring
	}
	stored, err := authoringSessionRow(ctx, q, channel, session)
	turn.Project = stored.Project
	c := turn.Claim
	if err != nil || ordinal != c.Turn || c.Purpose != stored.Purpose || c.Identity != stored.Capability.Identity || c.BinaryDigest != stored.Capability.BinaryDigest || c.AuthDigest != stored.Capability.AuthDigest || c.PolicyDigest != stored.Capability.PolicyDigest || c.ContextDigest != stored.ContextDigest {
		return turn, ErrAuthoring
	}
	if len(launch) > 0 && !decodeAuthoringObject(launch, &turn.Launch) {
		return turn, ErrAuthoring
	}
	if len(launch) > 0 && !validAuthoringLaunch(turn.Launch) {
		return turn, ErrAuthoring
	}
	if len(result) > 0 {
		parsed, err := authoring.ParsePurpose(turn.Claim.Purpose, result)
		if err != nil {
			return turn, ErrAuthoring
		}
		turn.Result = &parsed
	}
	switch turn.State {
	case "reserved":
		if len(launch) != 0 || turn.Outcome != "" || turn.Result != nil {
			return turn, ErrAuthoring
		}
	case "launched":
		if len(launch) == 0 || turn.Outcome != "" || turn.Result != nil {
			return turn, ErrAuthoring
		}
	case "uncertain":
		if turn.Outcome != "" || turn.Result != nil {
			return turn, ErrAuthoring
		}
	case "completed":
		switch turn.Outcome {
		case "success":
			if len(launch) == 0 || turn.Result == nil {
				return turn, ErrAuthoring
			}
		case "failed", "cancelled", "interrupted":
			if turn.Result != nil {
				return turn, ErrAuthoring
			}
		default:
			return turn, ErrAuthoring
		}
	default:
		return turn, ErrAuthoring
	}
	return turn, nil
}

func decodeAuthoringObject(data []byte, target any) bool {
	if len(data) > 64<<10 {
		return false
	}
	if _, err := providerjson.Object(data); err != nil {
		return false
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if decoder.Decode(target) != nil {
		return false
	}
	var extra any
	return decoder.Decode(&extra) == io.EOF
}

func validAuthoringLaunch(launch contracts.ProviderLaunch) bool {
	return launch.PID > 0 && launch.PID == launch.PGID && len(launch.BootIdentity) > 0 && len(launch.BootIdentity) <= 1024 && len(launch.ProcessStartIdentity) > 0 && len(launch.ProcessStartIdentity) <= 1024 && filepath.IsAbs(launch.Worktree) && filepath.Clean(launch.Worktree) == launch.Worktree && launch.Worktree != "/" && len(launch.Worktree) <= 4096 && !strings.ContainsAny(launch.Worktree, "\x00\r\n")
}
func (s *Store) AuthoringTurn(ctx context.Context, channel domain.Channel, session, key string) (AuthoringTurn, error) {
	return authoringTurnRow(ctx, s.db, channel, session, key)
}
func (s *Store) RecordAuthoringLaunch(ctx context.Context, claim contracts.AuthoringClaim, launch contracts.ProviderLaunch) error {
	if !validAuthoringLaunch(launch) {
		return ErrAuthoring
	}
	return s.write(ctx, func(conn *sql.Conn) error {
		turn, err := authoringTurnRow(ctx, conn, claim.Channel, claim.Session, claim.TurnKey)
		if err != nil || turn.Claim != claim || turn.State != "reserved" {
			return ErrAuthoring
		}
		var epoch uint64
		if conn.QueryRowContext(ctx, `SELECT leader_epoch FROM daemon_instances WHERE channel=?`, claim.Channel).Scan(&epoch) != nil || epoch != claim.LeaderEpoch {
			return ErrAuthoring
		}
		raw, _ := json.Marshal(launch)
		_, err = conn.ExecContext(ctx, `UPDATE authoring_turns SET state='launched',launch=? WHERE channel=? AND session_id=? AND turn_key=?`, raw, claim.Channel, claim.Session, claim.TurnKey)
		return err
	})
}
func (s *Store) FinishAuthoringTurn(ctx context.Context, claim contracts.AuthoringClaim, proof contracts.AuthoringProof, outcome string, result *contracts.AuthoringResult) error {
	if outcome != "success" && outcome != "cancelled" && outcome != "failed" && outcome != "interrupted" {
		return ErrAuthoring
	}
	var raw []byte
	if result != nil {
		if outcome != "success" || authoring.ValidatePurpose(claim.Purpose, *result) != nil {
			return ErrAuthoring
		}
		raw, _ = authoring.MarshalResult(claim.Purpose, *result)
	} else if outcome == "success" {
		return ErrAuthoring
	}
	return s.write(ctx, func(conn *sql.Conn) error {
		turn, err := authoringTurnRow(ctx, conn, claim.Channel, claim.Session, claim.TurnKey)
		if err != nil || turn.Claim != claim {
			return ErrAuthoring
		}
		var epoch uint64
		var key []byte
		if conn.QueryRowContext(ctx, `SELECT leader_epoch,recovery_public_key FROM daemon_instances WHERE channel=?`, claim.Channel).Scan(&epoch, &key) != nil || !contracts.VerifyAuthoringProof(key, claim, epoch, proof) {
			return ErrAuthoring
		}
		if turn.State == "completed" {
			var prior []byte
			if turn.Result != nil {
				prior, _ = authoring.MarshalResult(claim.Purpose, *turn.Result)
			}
			if turn.Outcome == outcome && bytes.Equal(prior, raw) {
				return nil
			}
			return ErrAuthoring
		}
		if outcome == "success" && (turn.State != "launched" || turn.Launch.PID <= 0 || turn.Launch.PID != turn.Launch.PGID || turn.Launch.BootIdentity == "" || turn.Launch.ProcessStartIdentity == "") {
			return ErrAuthoring
		}
		_, err = conn.ExecContext(ctx, `UPDATE authoring_turns SET state='completed',outcome=?,result=?,finished_at=? WHERE channel=? AND session_id=? AND turn_key=?`, outcome, raw, time.Now().UTC().Format(time.RFC3339Nano), claim.Channel, claim.Session, claim.TurnKey)
		return err
	})
}
func (s *Store) MarkAuthoringUncertain(ctx context.Context, claim contracts.AuthoringClaim) error {
	return s.write(ctx, func(conn *sql.Conn) error {
		raw, _ := json.Marshal(claim)
		_, err := conn.ExecContext(ctx, `UPDATE authoring_turns SET state='uncertain' WHERE channel=? AND session_id=? AND turn_key=? AND claim=? AND state<>'completed'`, claim.Channel, claim.Session, claim.TurnKey, raw)
		return err
	})
}
func (s *Store) PendingAuthoringTurns(ctx context.Context, channel domain.Channel) ([]AuthoringTurn, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT session_id,turn_key FROM authoring_turns WHERE channel=? AND state<>'completed'`, channel)
	if err != nil {
		return nil, err
	}
	var ids [][2]string
	for rows.Next() {
		var pair [2]string
		if err := rows.Scan(&pair[0], &pair[1]); err != nil {
			rows.Close()
			return nil, err
		}
		ids = append(ids, pair)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, err
	}
	var turns []AuthoringTurn
	for _, id := range ids {
		turn, err := s.AuthoringTurn(ctx, channel, id[0], id[1])
		if err != nil {
			return nil, err
		}
		turns = append(turns, turn)
	}
	return turns, nil
}
