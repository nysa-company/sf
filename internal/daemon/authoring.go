package daemon

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"time"

	"github.com/nysa-company/sf/internal/api"
	"github.com/nysa-company/sf/internal/authoring"
	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/store"
)

const authoringSessionCapacity = 16
const authoringSessionTTL = 30 * time.Minute

// Only approved, frozen references live here. SQLite owns claims and outcomes;
// a restart cannot reconstruct or resend a prompt from this disposable cache.
type authoringSnapshot struct {
	session store.AuthoringSession
	context string
	files   []string
	expires time.Time
	timer   *time.Timer
}

func (daemon *Daemon) authoringFailure(request api.Request, message string) api.Response {
	response := daemon.failure(request, "authoring_unavailable", message, false)
	response.NextAction = &domain.NextAction{Code: "authoring_manual", Argv: []string{daemon.executable(), "ticket", "new", "--no-ai"}}
	return response
}

func (daemon *Daemon) createAuthoring(ctx context.Context, request api.Request) api.Response {
	var p struct {
		Channel      domain.Channel `json:"channel"`
		Project      string         `json:"project"`
		Model        string         `json:"model"`
		Purpose      string         `json:"purpose"`
		ContextFiles []string       `json:"context_files"`
	}
	if decodeParameters(request.Parameters, &p) != nil || p.Channel != daemon.channel || (p.Purpose != "ticket_draft" && p.Purpose != "home_intent") || (p.Purpose == "home_intent" && len(p.ContextFiles) != 0) || p.Project == "" || !contracts.AuthoringID(p.Model) {
		return daemon.authoringFailure(request, "authoring requires this channel, a registered project, an explicit model and a supported purpose; home intent accepts no reference files")
	}
	daemon.authoringMu.Lock()
	if daemon.authoringStopping || daemon.authoringQuarantined || daemon.authoringSupervisor == nil || len(daemon.authoringSessions)+daemon.authoringCreating >= authoringSessionCapacity {
		daemon.authoringMu.Unlock()
		return daemon.authoringFailure(request, "bounded authoring is unavailable; use manual ticket creation")
	}
	daemon.authoringCreating++
	daemon.authoringMu.Unlock()
	defer func() { daemon.authoringMu.Lock(); daemon.authoringCreating--; daemon.authoringMu.Unlock() }()
	ctx, cancel := context.WithTimeout(ctx, 25*time.Second)
	defer cancel()
	if daemon.lease.Validate() != nil {
		return daemon.authoringFailure(request, "daemon leadership is unavailable")
	}
	project, err := daemon.store.Project(ctx, daemon.channel, domain.ProjectID(p.Project))
	if err != nil {
		return daemon.authoringFailure(request, "authoring project is not available")
	}
	frozen, digest, err := authoring.Snapshot(project.Path, p.ContextFiles)
	if err != nil {
		return daemon.authoringFailure(request, "reference files are unsafe or exceed the authoring limits")
	}
	// This operation only checks installed runtime/auth status. It must never
	// infer or turn an unavailable provider into a different provider.
	capability, err := daemon.authoringSupervisor.PrepareAuthoring(ctx, p.Model)
	if err != nil || capability.Identity.Model != p.Model {
		return daemon.authoringFailure(request, "the selected model has no available bounded authoring capability")
	}
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		return daemon.authoringFailure(request, "authoring session could not be allocated")
	}
	session := store.AuthoringSession{Channel: daemon.channel, ID: hex.EncodeToString(id[:]), Project: project.ID, Purpose: p.Purpose, Capability: capability, ContextDigest: digest}
	daemon.authoringMu.Lock()
	if daemon.authoringStopping || daemon.authoringQuarantined || ctx.Err() != nil {
		daemon.authoringMu.Unlock()
		return daemon.authoringFailure(request, "authoring session preparation stopped")
	}
	daemon.authoringMu.Unlock()
	mutation := api.Mutation{Attempted: true, Kind: "authoring_create", Identity: session.ID}
	if err := daemon.store.CreateAuthoringSession(ctx, session); err != nil {
		response := daemon.authoringFailure(request, "authoring session could not be saved")
		response.Mutation = mutation
		return response
	}
	mutation.Observed = true
	daemon.authoringMu.Lock()
	defer daemon.authoringMu.Unlock()
	if daemon.authoringSessions == nil {
		daemon.authoringSessions = map[string]*authoringSnapshot{}
	}
	snapshot := &authoringSnapshot{session: session, context: frozen, files: append([]string{}, p.ContextFiles...), expires: time.Now().Add(authoringSessionTTL)}
	daemon.authoringSessions[session.ID] = snapshot
	snapshot.timer = time.AfterFunc(authoringSessionTTL, func() {
		daemon.authoringMu.Lock()
		defer daemon.authoringMu.Unlock()
		if !daemon.authoringSessionActive(session.ID) {
			delete(daemon.authoringSessions, session.ID)
		}
	})
	return daemon.success(request, mutation, map[string]any{"authoring_session": map[string]any{
		"channel": session.Channel, "id": session.ID, "project": session.Project, "purpose": session.Purpose, "capability": session.Capability, "context_digest": digest, "context_files": snapshot.files,
		"limits": map[string]any{"turns": contracts.AuthoringTurnLimit, "timeout_seconds": int(contracts.AuthoringTurnTimeout / time.Second), "context_files": 8, "context_file_bytes": 16 << 10, "context_total_bytes": 64 << 10, "session_seconds": int(authoringSessionTTL / time.Second), "cost": "unknown"},
	}})
}

func authoringWorkerKey(session, key string) string { return session + "/" + key }

// Caller holds authoringMu. Claims have exactly one active turn per channel.
func (daemon *Daemon) authoringSessionActive(session string) bool {
	for key := range daemon.authoringWorkers {
		if len(key) > len(session) && key[:len(session)+1] == session+"/" {
			return true
		}
	}
	return false
}

func (daemon *Daemon) turnAuthoring(ctx context.Context, request api.Request) api.Response {
	var p struct {
		Channel       domain.Channel `json:"channel"`
		Session       string         `json:"session"`
		Key           string         `json:"key"`
		Prompt        string         `json:"prompt"`
		ContextDigest string         `json:"context_digest"`
	}
	if decodeParameters(request.Parameters, &p) != nil || p.Channel != daemon.channel || !contracts.AuthoringID(p.Session) || !contracts.AuthoringID(p.Key) {
		return daemon.authoringFailure(request, "invalid authoring turn identity")
	}
	daemon.authoringMu.Lock()
	defer daemon.authoringMu.Unlock()
	snapshot := daemon.authoringSessions[p.Session]
	if daemon.authoringStopping || daemon.authoringQuarantined || daemon.authoringSupervisor == nil || snapshot == nil || !time.Now().Before(snapshot.expires) || p.ContextDigest != snapshot.session.ContextDigest {
		return daemon.authoringFailure(request, "authoring session expired, restarted or has different approved context; create a new explicit session")
	}
	if daemon.lease.Validate() != nil {
		return daemon.authoringFailure(request, "daemon leadership is unavailable")
	}
	input := contracts.AuthoringInput{Purpose: snapshot.session.Purpose, Prompt: p.Prompt, Context: snapshot.context}
	// Use the same encoder as the process boundary, including JSON expansion
	// and fixed instructions, before any durable or paid reservation.
	if _, err := authoring.EncodeRequest(input); err != nil {
		return daemon.authoringFailure(request, "combined authoring request exceeds its limit or contains invalid text")
	}
	workerKey := authoringWorkerKey(p.Session, p.Key)
	if len(daemon.authoringWorkers) > 0 {
		if daemon.authoringWorkers[workerKey] == nil {
			return daemon.authoringFailure(request, "an authoring worker still owns the channel")
		}
		// Active replay is read-only and cannot delay cancellation behind a
		// SQLite write reservation. The durable digest still authenticates it.
		daemon.authoringMu.Unlock()
		prior, err := daemon.store.AuthoringTurn(ctx, daemon.channel, p.Session, p.Key)
		daemon.authoringMu.Lock()
		if err != nil || prior.Claim.RequestDigest != contracts.AuthoringInputDigest(input) {
			return daemon.authoringFailure(request, "authoring key conflicts with its approved request")
		}
		response := daemon.authoringReceipt(request, prior)
		response.Mutation = api.Mutation{Kind: "authoring_turn", Identity: workerKey, Observed: true}
		return response
	}
	turn, created, err := daemon.store.ReserveAuthoringTurn(ctx, snapshot.session, p.Key, contracts.AuthoringInputDigest(input), daemon.epoch)
	if err != nil {
		response := daemon.authoringFailure(request, "authoring turn is exhausted, conflicts with its prior request or has an undrained predecessor")
		response.Mutation = api.Mutation{Attempted: true, Kind: "authoring_turn", Identity: workerKey}
		return response
	}
	if created {
		parent := daemon.runtimeContext
		if parent == nil {
			parent = context.Background()
		}
		turnCtx, cancel := context.WithTimeout(parent, contracts.AuthoringTurnTimeout)
		if daemon.authoringWorkers == nil {
			daemon.authoringWorkers = map[string]context.CancelFunc{}
		}
		daemon.authoringWorkers[authoringWorkerKey(p.Session, p.Key)] = cancel
		daemon.authoringWG.Add(1)
		// Cancellation ownership is published while still holding the reservation
		// lock, before either the receipt or the worker can be observed.
		go daemon.runAuthoring(turnCtx, cancel, turn.Claim, input)
	}
	response := daemon.authoringReceipt(request, turn)
	response.Mutation = api.Mutation{Attempted: created, Kind: "authoring_turn", Identity: workerKey, Observed: true}
	return response
}

func (daemon *Daemon) authoringReceipt(request api.Request, turn store.AuthoringTurn) api.Response {
	return daemon.success(request, api.Mutation{}, map[string]any{"authoring_turn": map[string]any{"channel": turn.Claim.Channel, "project": turn.Project, "purpose": turn.Claim.Purpose, "session": turn.Claim.Session, "key": turn.Claim.TurnKey, "state": turn.State, "outcome": turn.Outcome, "result": turn.Result, "cost": "unknown"}})
}

func (daemon *Daemon) statusAuthoring(ctx context.Context, request api.Request) api.Response {
	var p struct {
		Channel domain.Channel `json:"channel"`
		Session string         `json:"session"`
		Key     string         `json:"key"`
	}
	if decodeParameters(request.Parameters, &p) != nil || p.Channel != daemon.channel || !contracts.AuthoringID(p.Session) || !contracts.AuthoringID(p.Key) {
		return daemon.authoringFailure(request, "invalid authoring turn identity")
	}
	cancelRequested := false
	if request.Method == "authoring.cancel" {
		daemon.authoringMu.Lock()
		if cancel := daemon.authoringWorkers[authoringWorkerKey(p.Session, p.Key)]; cancel != nil {
			cancel()
			cancelRequested = true
		}
		daemon.authoringMu.Unlock()
	}
	turn, err := daemon.store.AuthoringTurn(ctx, daemon.channel, p.Session, p.Key)
	if err != nil {
		response := daemon.authoringFailure(request, "authoring turn is unavailable")
		if cancelRequested {
			response.Mutation = api.Mutation{Attempted: true, Kind: "authoring_cancel", Identity: authoringWorkerKey(p.Session, p.Key)}
		}
		return response
	}
	response := daemon.authoringReceipt(request, turn)
	if request.Method == "authoring.cancel" {
		response.Mutation = api.Mutation{Attempted: true, Kind: "authoring_cancel", Identity: authoringWorkerKey(p.Session, p.Key), Observed: turn.State == "completed"}
	}
	return response
}

func (daemon *Daemon) runAuthoring(ctx context.Context, cancel context.CancelFunc, claim contracts.AuthoringClaim, input contracts.AuthoringInput) {
	defer daemon.authoringWG.Done()
	defer cancel()
	defer func() {
		daemon.authoringMu.Lock()
		defer daemon.authoringMu.Unlock()
		delete(daemon.authoringWorkers, authoringWorkerKey(claim.Session, claim.TurnKey))
		if snapshot := daemon.authoringSessions[claim.Session]; snapshot != nil && !time.Now().Before(snapshot.expires) {
			delete(daemon.authoringSessions, claim.Session)
		}
	}()
	result, proof, err := daemon.authoringSupervisor.RunAuthoring(ctx, claim, input, func(recordCtx context.Context, launch contracts.ProviderLaunch) error {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if daemon.lease.Validate() != nil {
			return errors.New("authoring leadership lost")
		}
		return daemon.store.RecordAuthoringLaunch(recordCtx, claim, launch)
	})
	outcome := "failed"
	var clean *contracts.AuthoringResult
	if ctx.Err() != nil {
		outcome = "cancelled"
	} else if err == nil {
		if sanitized, e := authoring.SanitizePurpose(claim.Purpose, result, daemon.projector.Policy); e == nil {
			clean = &sanitized
			outcome = "success"
		}
	}
	finishCtx, finishCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer finishCancel()
	if daemon.store.FinishAuthoringTurn(finishCtx, claim, proof, outcome, clean) != nil {
		uncertainCtx, uncertainCancel := context.WithTimeout(context.Background(), 5*time.Second)
		_ = daemon.store.MarkAuthoringUncertain(uncertainCtx, claim)
		uncertainCancel()
		daemon.authoringMu.Lock()
		daemon.authoringQuarantined = true
		daemon.authoringMu.Unlock()
	}
}

func (daemon *Daemon) recoverAuthoring(ctx context.Context) error {
	turns, err := daemon.store.PendingAuthoringTurns(ctx, daemon.channel)
	if err != nil {
		return err
	}
	for _, turn := range turns {
		var proof contracts.AuthoringProof
		var drainErr error
		if daemon.authoringSupervisor == nil {
			drainErr = errors.New("authoring drain unavailable")
		} else {
			drainCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			proof, drainErr = daemon.authoringSupervisor.RecoverAuthoring(drainCtx, turn.Claim, turn.Launch, daemon.epoch)
			cancel()
		}
		if drainErr == nil {
			// A supplied proof that fails Store authentication is malformed
			// authority, not a normal quarantined process observation.
			if err := daemon.store.FinishAuthoringTurn(ctx, turn.Claim, proof, "interrupted", nil); err != nil {
				return err
			}
		} else {
			if err := daemon.store.MarkAuthoringUncertain(ctx, turn.Claim); err != nil {
				return err
			}
			daemon.authoringQuarantined = true
		}
	}
	return nil
}

func (daemon *Daemon) closeAuthoring() error {
	daemon.authoringMu.Lock()
	daemon.authoringStopping = true
	for _, cancel := range daemon.authoringWorkers {
		cancel()
	}
	for _, snapshot := range daemon.authoringSessions {
		if snapshot.timer != nil {
			snapshot.timer.Stop()
		}
	}
	daemon.authoringMu.Unlock()
	done := make(chan struct{})
	go func() { daemon.authoringWG.Wait(); close(done) }()
	grace := daemon.authoringShutdownTimeout
	if grace <= 0 {
		grace = 15 * time.Second
	}
	select {
	case <-done:
	case <-time.After(grace):
		return errors.New("authoring shutdown drain remains unknown; store and leader ownership retained")
	}
	daemon.authoringMu.Lock()
	daemon.authoringSessions = nil
	daemon.authoringMu.Unlock()
	return nil
}
