package store

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
)

type providerFailureDiagnosticFixture struct {
	db     *Store
	ctx    context.Context
	ticket Ticket
	fence  domain.Fence
	claim  ProviderAttemptClaim
}

func newProviderFailureDiagnosticFixture(t *testing.T) providerFailureDiagnosticFixture {
	t.Helper()
	db, ctx := openTestStore(t)
	digest := setupProviderProject(t, db, ctx)
	leader, err := db.AcquireLeader(ctx, domain.ChannelDev, "provider-diagnostic")
	if err != nil {
		t.Fatal(err)
	}
	ticket := setupProviderTicket(t, db, ctx, "SF-provider-diagnostic", leader)
	planner, _ := setupProviderPair(t, db, ctx)
	fence := domain.Fence{LeaderEpoch: leader, RunnerEpoch: ticket.RunnerEpoch}
	claim, err := db.BeginProviderAttempt(ctx, supervised(t, ProviderAttemptRequest{
		Ref:             ticket.Ref,
		ExpectedVersion: ticket.Version,
		Fence:           fence,
		Phase:           domain.PhasePlanning,
		Role:            "planner",
		Binding:         runtime(planner),
		ConfigDigest:    digest,
		Capacity:        1,
		At:              time.Now().UTC(),
	}))
	if err != nil {
		t.Fatal(err)
	}
	return providerFailureDiagnosticFixture{db: db, ctx: ctx, ticket: ticket, fence: fence, claim: claim}
}

func (f providerFailureDiagnosticFixture) assertAttemptActive(t *testing.T) {
	t.Helper()
	var attemptState, attemptOutcome, launchState, phaseState, phaseOutcome string
	if err := f.db.db.QueryRowContext(f.ctx, `SELECT state,outcome,launch_state FROM provider_attempts WHERE id=?`, f.claim.ID).Scan(&attemptState, &attemptOutcome, &launchState); err != nil {
		t.Fatal(err)
	}
	if err := f.db.db.QueryRowContext(f.ctx, `SELECT state,outcome FROM phase_runs WHERE channel=? AND project_id=? AND ticket_id=? AND phase=? AND attempt=?`, f.claim.Ref.Channel, f.claim.Ref.Project, f.claim.Ref.Ticket, f.claim.Phase, f.claim.Attempt).Scan(&phaseState, &phaseOutcome); err != nil {
		t.Fatal(err)
	}
	var leases int
	if err := f.db.db.QueryRowContext(f.ctx, `SELECT COUNT(*) FROM leases WHERE channel=? AND scope='provider' AND scope_key=? AND project_id=? AND ticket_id=? AND runner_epoch=?`, f.claim.Ref.Channel, f.claim.LeaseKey, f.claim.Ref.Project, f.claim.Ref.Ticket, f.claim.RunnerEpoch).Scan(&leases); err != nil {
		t.Fatal(err)
	}
	if attemptState != "active" || attemptOutcome != "running" || launchState != "launching" || phaseState != "active" || phaseOutcome != "running" || leases != 1 {
		t.Fatalf("attempt=%s/%s/%s phase=%s/%s leases=%d", attemptState, attemptOutcome, launchState, phaseState, phaseOutcome, leases)
	}
}

func (f providerFailureDiagnosticFixture) diagnosticCount(t *testing.T) int {
	t.Helper()
	var count int
	if err := f.db.db.QueryRowContext(f.ctx, `SELECT COUNT(*) FROM events WHERE channel=? AND project_id=? AND ticket_id=? AND trigger='provider_result_diagnostic'`, f.claim.Ref.Channel, f.claim.Ref.Project, f.claim.Ref.Ticket).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

func TestFinishProviderAttemptWithIndeterminateFailureRejectsUnknownReasonBeforeWrites(t *testing.T) {
	f := newProviderFailureDiagnosticFixture(t)
	unsafeReason := contracts.ProviderFailureReason("raw provider error: secret=must_not_persist")
	err := f.db.FinishProviderAttemptWithIndeterminateFailure(f.ctx, f.claim, proof(t, f.claim), f.ticket.Version, f.fence, unsafeReason, 1, time.Now().UTC())
	if !errors.Is(err, ErrProviderAttempt) {
		t.Fatalf("unknown reason=%v", err)
	}
	f.assertAttemptActive(t)
	if count := f.diagnosticCount(t); count != 0 {
		t.Fatalf("diagnostic events=%d", count)
	}
}

func TestFinishProviderAttemptWithIndeterminateFailurePersistsExactClosedDiagnostic(t *testing.T) {
	f := newProviderFailureDiagnosticFixture(t)
	if err := f.db.FinishProviderAttemptWithIndeterminateFailure(f.ctx, f.claim, proof(t, f.claim), f.ticket.Version, f.fence, contracts.ProviderFailureAdapter, 3, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}

	var attemptState, attemptOutcome, launchState, phaseState, phaseOutcome string
	if err := f.db.db.QueryRowContext(f.ctx, `SELECT state,outcome,launch_state FROM provider_attempts WHERE id=?`, f.claim.ID).Scan(&attemptState, &attemptOutcome, &launchState); err != nil {
		t.Fatal(err)
	}
	if err := f.db.db.QueryRowContext(f.ctx, `SELECT state,outcome FROM phase_runs WHERE channel=? AND project_id=? AND ticket_id=? AND phase=? AND attempt=?`, f.claim.Ref.Channel, f.claim.Ref.Project, f.claim.Ref.Ticket, f.claim.Phase, f.claim.Attempt).Scan(&phaseState, &phaseOutcome); err != nil {
		t.Fatal(err)
	}
	var leases int
	if err := f.db.db.QueryRowContext(f.ctx, `SELECT COUNT(*) FROM leases WHERE channel=? AND scope='provider' AND scope_key=? AND project_id=? AND ticket_id=?`, f.claim.Ref.Channel, f.claim.LeaseKey, f.claim.Ref.Project, f.claim.Ref.Ticket).Scan(&leases); err != nil {
		t.Fatal(err)
	}
	if attemptState != "failed" || attemptOutcome != "result_indeterminate" || launchState != "drained" || phaseState != "failed" || phaseOutcome != "result_indeterminate" || leases != 0 {
		t.Fatalf("attempt=%s/%s/%s phase=%s/%s leases=%d", attemptState, attemptOutcome, launchState, phaseState, phaseOutcome, leases)
	}

	var version uint64
	var trigger string
	var from, to domain.State
	var payload string
	if err := f.db.db.QueryRowContext(f.ctx, `SELECT ticket_version,trigger,from_state,to_state,payload FROM events WHERE channel=? AND project_id=? AND ticket_id=? AND trigger='provider_result_diagnostic'`, f.claim.Ref.Channel, f.claim.Ref.Project, f.claim.Ref.Ticket).Scan(&version, &trigger, &from, &to, &payload); err != nil {
		t.Fatal(err)
	}
	want := fmt.Sprintf(`{"schema":"sf.provider-diagnostic/v1","provider_attempt_id":%d,"phase":"%s","attempt":%d,"request_digest":"%s","fence":{"ticket_version":%d,"leader_epoch":%d,"runner_epoch":%d},"reason":"adapter_error"}`, f.claim.ID, f.claim.Phase, f.claim.Attempt, f.claim.RequestDigest, f.claim.ExpectedVersion, f.claim.LeaderEpoch, f.claim.RunnerEpoch)
	if version != f.ticket.Version || trigger != "provider_result_diagnostic" || from != f.ticket.State || to != f.ticket.State || payload != want || f.diagnosticCount(t) != 1 {
		t.Fatalf("event version=%d trigger=%q state=%s->%s payload=%q", version, trigger, from, to, payload)
	}
}

func TestFinishProviderAttemptWithIndeterminateFailureRollsBackOnDiagnosticInsertFailure(t *testing.T) {
	f := newProviderFailureDiagnosticFixture(t)
	if _, err := f.db.db.ExecContext(f.ctx, `CREATE TRIGGER provider_diagnostic_fault BEFORE INSERT ON events WHEN NEW.trigger='provider_result_diagnostic' BEGIN SELECT RAISE(ABORT,'injected diagnostic failure'); END`); err != nil {
		t.Fatal(err)
	}
	err := f.db.FinishProviderAttemptWithIndeterminateFailure(f.ctx, f.claim, proof(t, f.claim), f.ticket.Version, f.fence, contracts.ProviderFailureProtocol, 1, time.Now().UTC())
	if err == nil {
		t.Fatal("diagnostic insert failure was accepted")
	}
	f.assertAttemptActive(t)
	if count := f.diagnosticCount(t); count != 0 {
		t.Fatalf("diagnostic events=%d", count)
	}
}

func TestFinishProviderAttemptLegacyIndeterminateOmitsDiagnostic(t *testing.T) {
	f := newProviderFailureDiagnosticFixture(t)
	if err := f.db.FinishProviderAttempt(f.ctx, f.claim, proof(t, f.claim), f.ticket.Version, f.fence, "failed", "result_indeterminate", 1, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	if count := f.diagnosticCount(t); count != 0 {
		t.Fatalf("legacy finish unexpectedly wrote diagnostic events=%d", count)
	}
}
