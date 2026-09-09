package daemon

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/api"
	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/daemon/runtimecontrol"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/store"
	"github.com/nysa-company/sf/internal/workflowruntime"
)

// All lifecycle facts come through the same Store APIs as the production
// path. Only provider execution is the existing credential-free fixture.
func daemonOperatorDecisionRetryPrefix(t *testing.T, d *Daemon, ticket store.Ticket, fence domain.Fence, binding contracts.RuntimeBinding, signer *contracts.DrainSigner) store.Ticket {
	t.Helper()
	ctx, db, ref := t.Context(), d.store, ticket.Ref
	stopping, err := db.TransitionAndInvalidateRunner(ctx, store.Transition{Ref: ref, ExpectedVersion: ticket.Version, From: domain.StatePlanning, To: domain.StateStopping, ResumeState: domain.StatePlanning, Trigger: "operator_pause_or_take", Fence: fence, EventPayload: `{"intent":"pause"}`})
	if err != nil {
		t.Fatal(err)
	}
	ticket, err = db.Ticket(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	fence.RunnerEpoch = ticket.RunnerEpoch
	paused, err := db.CompleteControlTransition(ctx, store.Transition{Ref: ref, ExpectedVersion: stopping.Version, From: domain.StateStopping, To: domain.StatePaused, ResumeState: domain.StatePlanning, Trigger: "process_and_effects_drained", Fence: fence, EventPayload: `{"drained":true}`})
	if err != nil {
		t.Fatal(err)
	}
	stopped, err := db.StoppedRuntimeTicket(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Transition(ctx, store.Transition{Ref: ref, ExpectedVersion: paused.Version, From: domain.StatePaused, To: domain.StatePlanning, Trigger: "operator_resume", Fence: fence, EventPayload: `{"operator":"test"}`}); err != nil {
		t.Fatal(err)
	}
	capability, err := db.RearmProof(ctx, ref, stopped)
	if err != nil {
		t.Fatal(err)
	}
	open := func(capability *store.RuntimeRearmCapability) {
		var token *store.RuntimeAdmissionCapability
		if err := db.ActivateRearm(ctx, capability, func(value *store.RuntimeAdmissionCapability) error {
			if _, _, _, ok := value.ConsumeRuntimeAdmission(); !ok {
				return store.ErrStaleFence
			}
			token = value
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		if token == nil {
			t.Fatal("missing fixture admission")
		}
		if err := token.OpenStoreAdmission(ctx); err != nil {
			t.Fatal(err)
		}
	}
	open(capability)
	ticket, err = db.Ticket(ctx, ref)
	if err != nil || ticket.RunnerEpoch <= 1 {
		t.Fatalf("control prefix: %+v %v", ticket, err)
	}
	worktree, err := db.Worktree(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	project, err := db.Project(ctx, ref.Channel, ref.Project)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		request := store.ProviderAttemptRequest{Ref: ref, ExpectedVersion: ticket.Version, Fence: fence, Phase: domain.PhasePlanning, Role: "planner", Binding: binding, ConfigDigest: ticket.ConfigDigest, Capacity: 1, At: time.Now().UTC(), Repository: project.Path, Worktree: worktree.Path, WorktreeIdentity: string(worktree.IdentityJSON), BaseSHA: worktree.BaseSHA, SupervisorKey: signer.PublicKey(), Input: contracts.PhaseInput{Ticket: ref, Phase: domain.PhasePlanning, LeaderEpoch: fence.LeaderEpoch, RunnerEpoch: fence.RunnerEpoch, ExpectedVersion: ticket.Version, Prompt: "bounded failed planner fixture", Repository: project.Path, Worktree: worktree.Path, WorktreeIdentity: string(worktree.IdentityJSON), BaseSHA: worktree.BaseSHA, AllowedPaths: []string{"."}, Provider: binding.Identity, AuthMode: binding.AuthMode, Timeout: time.Minute, Profile: contracts.ProfileGuarded, Schema: []byte(`{"type":"object"}`)}}
		claim, err := db.BeginProviderAttempt(ctx, request)
		if err != nil {
			t.Fatal(err)
		}
		if err := db.RecordProviderLaunch(ctx, claim, contracts.ProviderLaunch{PID: int(claim.ID), PGID: int(claim.ID), BootIdentity: "fixture", ProcessStartIdentity: fmt.Sprintf("fixture-%d", claim.ID), Worktree: claim.Worktree}); err != nil {
			t.Fatal(err)
		}
		if err := db.FinishProviderAttempt(ctx, claim, daemonFixtureDrainProof(t, signer, claim), ticket.Version, fence, "failed", "invalid_artifact", 1, time.Now().UTC()); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.TransitionProviderExhausted(ctx, store.Transition{Ref: ref, ExpectedVersion: ticket.Version, From: domain.StatePlanning, To: domain.StatePaused, ResumeState: domain.StatePlanning, Trigger: "retry_or_correction_exhausted", Fence: fence}); err != nil {
		t.Fatal(err)
	}
	stopped, err = db.Ticket(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.SealRuntimeControl(ctx, ref); err != nil {
		t.Fatal(err)
	}
	if _, err := db.TransitionProviderRetry(ctx, store.Transition{Ref: ref, ExpectedVersion: stopped.Version, From: domain.StatePaused, To: domain.StatePlanning, ResumeState: domain.StatePlanning, Trigger: "operator_retry", Fence: fence}); err != nil {
		t.Fatal(err)
	}
	capability, err = db.ProviderRetryRearmProof(ctx, ref, stopped)
	if err != nil {
		t.Fatal(err)
	}
	open(capability)
	ticket, err = db.Ticket(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	return ticket
}

func TestOperatorDecisionRealRetryRestartActivityAndMergeRecovery(t *testing.T) {
	d, paths, cancel := testDaemon(t)
	ticket := prepareDaemonGuardedLifecycle(t, d, "SF-retry-decision-composition", domain.StateWaitingApproval, daemonOperatorDecisionRetryPrefix)
	_, file, _, _ := runtime.Caller(0)
	specPath := filepath.Join(filepath.Dir(file), "..", "..", "docs", "plans", "2026-08-29-software-factory-v1-state-machine.json")
	auth := d.auth
	cancel()
	for restart := 0; restart < 3; restart++ {
		if err := d.Close(); err != nil {
			t.Fatal(err)
		}
		var err error
		d, err = Start(t.Context(), Config{Channel: ticket.Ref.Channel, Paths: paths, StateMachinePath: specPath, DaemonIdentity: fmt.Sprintf("decision-restart-%d", restart), Operator: auth})
		if err != nil {
			t.Fatalf("restart %d: %v", restart, err)
		}
		defer d.Close()
		if restart == 0 {
			continue
		}
		worker := newDaemonRuntimeWorker()
		r, err := workflowruntime.NewRuntime(workflowruntime.NewScheduler(d.channel, workflowruntime.StoreTicketSource{Store: d.store}, daemonRuntimeEnsure{}, worker), time.Hour)
		if err != nil {
			t.Fatal(err)
		}
		controller, err := runtimecontrol.New(d.store, r.ControlBundle(), nil)
		if err != nil {
			t.Fatal(err)
		}
		d.control = controller
		candidate, err := d.store.RecoverableCandidate(t.Context(), ticket.Ref)
		if err != nil {
			t.Fatal(err)
		}
		body, _ := json.Marshal(map[string]any{"channel": d.channel, "reviewed_head": candidate.Snapshot.HeadSHA})
		request := api.Request{Ticket: string(ticket.Ref.Ticket), Parameters: body}
		response := d.operatorDecision(t.Context(), request, domain.OperatorIdentity{UID: 501}, "approved")
		if !response.OK {
			t.Fatalf("restart %d approval: %+v", restart, response)
		}
		current, err := d.store.Ticket(t.Context(), ticket.Ref)
		if err != nil || current.State != domain.StateMerging {
			t.Fatalf("current=%+v err=%v", current, err)
		}
		ready, err := d.store.RuntimeAdmissionReady(t.Context(), ticket.Ref, current.Version, domain.Fence{LeaderEpoch: d.epoch, RunnerEpoch: current.RunnerEpoch})
		if err != nil || !ready {
			t.Fatalf("merge admission=%v err=%v", ready, err)
		}
		decisions, err := d.store.OperatorDecisions(t.Context(), ticket.Ref)
		if err != nil || len(decisions) != 1 {
			t.Fatalf("decisions=%+v err=%v", decisions, err)
		}
		if calls, active := worker.snapshot(ticket.Ref); calls != 0 || active {
			t.Fatal("decision recovery launched a worker")
		}
	}
}
