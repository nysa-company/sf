package store

import (
	"bytes"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/processsupervisor"
)

func signedRejectionStoreFixture(t *testing.T) (*providerRetryWorktreeFixture, ProviderAttemptRequest, ProviderAttemptClaim, contracts.ServerRejectionAttestation) {
	t.Helper()
	f := newProviderRetryWorktreeFixture(t, "signed-rejection", 40, "claude")
	supervisor, err := processsupervisor.New(nil)
	if err != nil {
		t.Fatal(err)
	}
	supervisor.Signer = providerTestSigner // synthetic test authority, no CLI
	if err := f.db.SetRecoveryAuthority(f.ctx, domain.ChannelDev, f.fence.LeaderEpoch, supervisor.PublicKey()); err != nil {
		t.Fatal(err)
	}
	q := qualificationValue(strings.Repeat("c", 32), "claude", "anthropic-claude", QualificationGuarded)
	q.Provider.Model, q.Provider.Version = "claude-sonnet-5", "2.1.263"
	q.AuthMode, q.ProbeDigest = "claude_subscription", strings.Repeat("d", 64)
	attestation, err := supervisor.AttestQualification(contracts.QualificationAttestation{Channel: q.Channel, RunID: q.RunID, Identity: q.Provider, BinaryDigest: q.BinaryDigest, PolicyDigest: q.PolicyDigest, FixtureDigest: q.FixtureDigest, AuthDigest: strings.Repeat("e", 64), AuthMode: q.AuthMode, ProbeDigest: q.ProbeDigest, Profile: contracts.ProfileGuarded, CreatedUnixNanos: q.CreatedAt.UnixNano(), LeaderEpoch: f.fence.LeaderEpoch, Nonce: q.RunID})
	if err != nil {
		t.Fatal(err)
	}
	q, _, err = f.db.RecordAttestedProviderQualification(f.ctx, q, attestation)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := f.db.SelectProviderPair(f.ctx, domain.ChannelDev, q.ID, f.reviewer.ID, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	if err := f.db.ApproveProviderEstimatedAccounting(f.ctx, f.ticket.Ref, f.ticket.Version, f.fence); err != nil {
		t.Fatal(err)
	}
	binding := contracts.RuntimeBinding{Identity: q.Provider, BinaryDigest: q.BinaryDigest, PolicyDigest: q.PolicyDigest, FixtureDigest: q.FixtureDigest, AuthDigest: q.AuthDigest, AuthMode: q.AuthMode}
	request := f.request(t, domain.PhasePlanning, "planner", binding)
	claim, err := f.db.BeginProviderAttempt(f.ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	checkpoint, err := f.db.ProviderAttemptCheckpoint(f.ctx, claim)
	if err != nil {
		t.Fatal(err)
	}
	digest, err := ProviderAttemptCheckpointDigest(checkpoint, claim.RequestDigest)
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := providerTestSigner.SignServerRejection(drainRequestForClaim(claim), proof(t, claim), contracts.ServerRejectionEvidence{StreamDigest: strings.Repeat("a", 64), AllFailuresServerErrors: true, CheckpointHeadOID: checkpoint.ExpectedHead, CheckpointDigest: digest, ObservedUnixNanos: time.Now().UTC().UnixNano()})
	if err != nil {
		t.Fatal(err)
	}
	return f, request, claim, receipt
}

func TestServerRejectionFinishAndAdmissionAreAtomic(t *testing.T) {
	f, request, claim, receipt := signedRejectionStoreFixture(t)
	bad := receipt
	bad.Evidence.CheckpointHeadOID = strings.Repeat("f", 40)
	if err := f.db.FinishProviderAttemptWithServerRejection(f.ctx, claim, proof(t, claim), bad, time.Now().UTC()); err == nil {
		t.Fatal("forged receipt accepted")
	}
	var state string
	var count int
	if err := f.db.db.QueryRowContext(f.ctx, `SELECT state FROM provider_attempts WHERE id=?`, claim.ID).Scan(&state); err != nil || state != "active" {
		t.Fatal("failed receipt changed attempt")
	}
	if err := f.db.FinishProviderAttempt(f.ctx, claim, proof(t, claim), claim.ExpectedVersion, f.fence, "failed", providerServerRejected, 0, time.Now().UTC()); err == nil {
		t.Fatal("generic finish minted server rejection")
	}
	if err := f.db.FinishProviderAttemptWithServerRejection(f.ctx, claim, proof(t, claim), receipt, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	if err := f.db.FinishProviderAttemptWithServerRejection(f.ctx, claim, proof(t, claim), receipt, time.Now().UTC().Add(time.Second)); err != nil {
		t.Fatal("exact finish replay failed", err)
	}
	if err := f.db.FinishProviderAttemptWithServerRejection(f.ctx, claim, proof(t, claim), bad, time.Now().UTC()); err == nil {
		t.Fatal("changed receipt replay accepted")
	}
	if err := f.db.db.QueryRowContext(f.ctx, `SELECT COUNT(*) FROM leases WHERE scope='provider' AND scope_key=?`, claim.LeaseKey).Scan(&count); err != nil || count != 0 {
		t.Fatal("exact provider lease retained")
	}
	_, _, deadline, err := canonicalServerRejection(claim, receipt)
	if err != nil {
		t.Fatal(err)
	}
	request.At = time.Unix(0, deadline-1)
	if _, err := f.db.BeginProviderAttempt(f.ctx, request); !errors.Is(err, ErrProviderRetryBackoff) {
		t.Fatal("backoff not enforced")
	} else {
		var backoff *ProviderRetryBackoffError
		if !errors.As(err, &backoff) || backoff.NotBefore.UnixNano() != deadline {
			t.Fatal("authenticated deadline not exposed")
		}
	}
	request.At = time.Unix(0, deadline)
	changed := request
	changed.Input.Prompt += "changed"
	if _, err := f.db.BeginProviderAttempt(f.ctx, changed); !errors.Is(err, ErrProviderServerRejection) {
		t.Fatal("input changed during retry")
	}
	next, err := f.db.BeginProviderAttempt(f.ctx, request)
	if err != nil || next.Attempt != claim.Attempt+1 || next.Binding != claim.Binding || next.Input.Repair != nil {
		t.Fatal("exact retry refused", err)
	}
	checkpoint, required, err := f.db.ProviderRejectionRetryCheckpoint(f.ctx, next)
	if err != nil || !required || checkpoint.AttemptID != next.ID {
		t.Fatal("active retry physical checkpoint not required", err)
	}
	wrong := next
	wrong.RequestDigest = strings.Repeat("f", 64)
	if _, _, err := f.db.ProviderRejectionRetryCheckpoint(f.ctx, wrong); err == nil {
		t.Fatal("foreign retry checkpoint accepted")
	}
	if err := f.db.FinishProviderAttemptWithServerRejection(f.ctx, claim, proof(t, claim), receipt, time.Now().UTC()); err != nil {
		t.Fatal("finish replay while next attempt active refused", err)
	}
	if _, required, err := f.db.ProviderRejectionRetryCheckpoint(f.ctx, next); err != nil || !required {
		t.Fatal("old finish replay removed current lease")
	}
	if err := f.db.FinishProviderAttempt(f.ctx, next, proof(t, next), next.ExpectedVersion, f.fence, "failed", "invalid_artifact", 0, request.At.Add(time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	if _, _, err := f.db.ProviderRejectionRetryCheckpoint(f.ctx, next); err == nil {
		t.Fatal("terminal retry checkpoint accepted")
	}
	if _, err := f.db.BeginProviderAttempt(f.ctx, request); !errors.Is(err, ErrProviderAttemptLimit) {
		t.Fatal("API/artifact exceeded shared window")
	}
}

func TestServerRejectionTransactionRollsBackAndMissingReceiptRefuses(t *testing.T) {
	f, request, claim, receipt := signedRejectionStoreFixture(t)
	if _, err := f.db.db.ExecContext(f.ctx, `CREATE TRIGGER rejection_test_fail_phase BEFORE UPDATE ON phase_runs BEGIN SELECT RAISE(ABORT,'fixture phase failure'); END`); err != nil {
		t.Fatal(err)
	}
	if err := f.db.FinishProviderAttemptWithServerRejection(f.ctx, claim, proof(t, claim), receipt, time.Now().UTC()); err == nil {
		t.Fatal("phase failure ignored")
	}
	var receipts, leases int
	var state string
	if err := f.db.db.QueryRowContext(f.ctx, `SELECT COUNT(*) FROM provider_server_rejections`).Scan(&receipts); err != nil || receipts != 0 {
		t.Fatal("receipt survived rollback")
	}
	if err := f.db.db.QueryRowContext(f.ctx, `SELECT state FROM provider_attempts WHERE id=?`, claim.ID).Scan(&state); err != nil || state != "active" {
		t.Fatal("attempt survived partial commit")
	}
	if err := f.db.db.QueryRowContext(f.ctx, `SELECT COUNT(*) FROM leases WHERE scope='provider' AND scope_key=?`, claim.LeaseKey).Scan(&leases); err != nil || leases != 1 {
		t.Fatal("lease lost on rollback")
	}
	if _, err := f.db.db.ExecContext(f.ctx, `DROP TRIGGER rejection_test_fail_phase`); err != nil {
		t.Fatal(err)
	}
	if err := f.db.FinishProviderAttemptWithServerRejection(f.ctx, claim, proof(t, claim), receipt, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	// Simulate hostile durable loss only in this disposable test database.
	if _, err := f.db.db.ExecContext(f.ctx, `DROP TRIGGER provider_server_rejections_immutable_delete`); err != nil {
		t.Fatal(err)
	}
	if _, err := f.db.db.ExecContext(f.ctx, `DELETE FROM provider_server_rejections`); err != nil {
		t.Fatal(err)
	}
	request.At = time.Now().UTC().Add(4 * time.Second)
	if _, err := f.db.BeginProviderAttempt(f.ctx, request); !errors.Is(err, ErrProviderServerRejection) {
		t.Fatal("missing receipt fell through as generic failure")
	}
}

func TestServerRejectionAfterArtifactConsumesRepairWindow(t *testing.T) {
	f, request, first, _ := signedRejectionStoreFixture(t)
	if err := f.db.FinishProviderAttempt(f.ctx, first, proof(t, first), first.ExpectedVersion, f.fence, "failed", "invalid_artifact", 0, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	second, err := f.db.BeginProviderAttempt(f.ctx, request)
	if err != nil || second.Input.Repair == nil {
		t.Fatal("repair not bound", err)
	}
	checkpoint, err := f.db.ProviderAttemptCheckpoint(f.ctx, second)
	if err != nil {
		t.Fatal(err)
	}
	digest, err := ProviderAttemptCheckpointDigest(checkpoint, second.RequestDigest)
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := providerTestSigner.SignServerRejection(drainRequestForClaim(second), proof(t, second), contracts.ServerRejectionEvidence{StreamDigest: strings.Repeat("b", 64), AllFailuresServerErrors: true, CheckpointHeadOID: checkpoint.ExpectedHead, CheckpointDigest: digest, ObservedUnixNanos: time.Now().UTC().UnixNano()})
	if err != nil {
		t.Fatal(err)
	}
	if err := f.db.FinishProviderAttemptWithServerRejection(f.ctx, second, proof(t, second), receipt, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	request.At = time.Now().UTC().Add(4 * time.Second)
	if _, err := f.db.BeginProviderAttempt(f.ctx, request); !errors.Is(err, ErrProviderAttemptLimit) {
		t.Fatal("server retry added hidden repair budget")
	}
	if _, err := f.db.TransitionProviderExhausted(f.ctx, Transition{Ref: f.ticket.Ref, ExpectedVersion: f.ticket.Version, From: f.ticket.State, To: domain.StatePaused, ResumeState: f.ticket.State, Trigger: "retry_or_correction_exhausted", Fence: f.fence}); err != nil {
		t.Fatal("authenticated mixed pair could not pause", err)
	}
}

func TestServerRejectionRestartPreservesDeadlineAndAttemptBudget(t *testing.T) {
	f, _, claim, receipt := signedRejectionStoreFixture(t)
	if err := f.db.FinishProviderAttemptWithServerRejection(f.ctx, claim, proof(t, claim), receipt, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	q, err := f.db.LatestProviderQualification(f.ctx, domain.ChannelDev, claim.Binding.Identity)
	if err != nil {
		t.Fatal(err)
	}
	_, _, deadline, err := canonicalServerRejection(claim, receipt)
	if err != nil {
		t.Fatal(err)
	}
	var path, name string
	var sequence int
	if err := f.db.db.QueryRowContext(f.ctx, `PRAGMA database_list`).Scan(&sequence, &name, &path); err != nil {
		t.Fatal(err)
	}
	if err := f.db.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(f.ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	f.db = reopened
	leader, err := f.db.AcquireLeader(f.ctx, domain.ChannelDev, "rejection-restart")
	if err != nil {
		t.Fatal(err)
	}
	if err := f.db.SetRecoveryAuthority(f.ctx, domain.ChannelDev, leader, providerTestSigner.PublicKey()); err != nil {
		t.Fatal(err)
	}
	if changed, err := f.db.FenceRecoveredRunners(f.ctx, domain.ChannelDev, leader); err != nil || changed != 1 {
		t.Fatal("restart fencing failed", err)
	}
	f.fence.LeaderEpoch = leader
	f.reload(t)
	// Requalify the same immutable runtime, not a provider/model fallback.
	supervisor, err := processsupervisor.New(nil)
	if err != nil {
		t.Fatal(err)
	}
	supervisor.Signer = providerTestSigner
	q.ID = 0
	q.RunID = strings.Repeat("f", 32)
	q.CreatedAt = time.Now().UTC()
	attestation, err := supervisor.AttestQualification(contracts.QualificationAttestation{Channel: q.Channel, RunID: q.RunID, Identity: q.Provider, BinaryDigest: q.BinaryDigest, PolicyDigest: q.PolicyDigest, FixtureDigest: q.FixtureDigest, AuthDigest: q.AuthDigest, AuthMode: q.AuthMode, ProbeDigest: q.ProbeDigest, Profile: contracts.ProfileGuarded, CreatedUnixNanos: q.CreatedAt.UnixNano(), LeaderEpoch: leader, Nonce: q.RunID})
	if err != nil {
		t.Fatal(err)
	}
	q, _, err = f.db.RecordAttestedProviderQualification(f.ctx, q, attestation)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := f.db.SelectProviderPair(f.ctx, domain.ChannelDev, q.ID, f.reviewer.ID, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	request := f.request(t, domain.PhasePlanning, "planner", claim.Binding)
	request.At = time.Unix(0, deadline-1)
	if _, err := f.db.BeginProviderAttempt(f.ctx, request); !errors.Is(err, ErrProviderRetryBackoff) {
		t.Fatal("restart reset backoff", err)
	}
	request.At = time.Unix(0, deadline)
	next, err := f.db.BeginProviderAttempt(f.ctx, request)
	if err != nil || next.Attempt != claim.Attempt+1 || next.Binding != claim.Binding {
		t.Fatal("recovered exact retry refused", err)
	}
	var storedDeadline int64
	if err := f.db.db.QueryRowContext(f.ctx, `SELECT not_before_unix_nanos FROM provider_server_rejections WHERE provider_attempt_id=?`, claim.ID).Scan(&storedDeadline); err != nil || storedDeadline != deadline {
		t.Fatal("restart changed deadline")
	}
	if err := f.db.FinishProviderAttempt(f.ctx, next, proof(t, next), next.ExpectedVersion, f.fence, "failed", "invalid_artifact", 0, request.At.Add(time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	if _, err := f.db.BeginProviderAttempt(f.ctx, request); !errors.Is(err, ErrProviderAttemptLimit) {
		t.Fatal("restart reset attempt budget")
	}
}

func rejectionEncodingFixture(t *testing.T) (ProviderAttemptClaim, contracts.ServerRejectionAttestation) {
	t.Helper()
	d := strings.Repeat("a", 64)
	claim := ProviderAttemptClaim{ID: 1, Ref: domain.TicketRef{Channel: domain.ChannelDev, Project: "fixture", Ticket: "SF-rejection"}, Phase: domain.PhasePlanning, Role: "planner", Attempt: 1,
		Binding:  contracts.RuntimeBinding{Identity: domain.ProviderIdentity{Provider: "claude", Model: "claude-sonnet-5", Family: "anthropic-claude", Version: "2.1.263"}, BinaryDigest: d, PolicyDigest: d, AuthDigest: d, AuthMode: "claude_subscription"},
		LeaseKey: "fixture", BindingDigest: d, LeaderEpoch: 2, RunnerEpoch: 3, ExpectedVersion: 4, Repository: "/private/repo", Worktree: "/private/worktree", WorktreeIdentity: `{"fixture":true}`, BaseSHA: strings.Repeat("b", 40), RequestDigest: d, SupervisorKey: providerTestSigner.PublicKey()}
	r := drainRequestForClaim(claim)
	drain, err := providerTestSigner.ProveDrained(r)
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := providerTestSigner.SignServerRejection(r, drain, contracts.ServerRejectionEvidence{StreamDigest: d, AllFailuresServerErrors: true, CheckpointHeadOID: claim.BaseSHA, CheckpointDigest: d, ObservedUnixNanos: time.Now().UTC().UnixNano()})
	if err != nil {
		t.Fatal(err)
	}
	return claim, receipt
}

func TestServerRejectionEncodingAndDeadlineAreStable(t *testing.T) {
	claim, receipt := rejectionEncodingFixture(t)
	raw, digest, deadline, err := canonicalServerRejection(claim, receipt)
	if err != nil || deadline <= receipt.Evidence.ObservedUnixNanos || deadline-receipt.Evidence.ObservedUnixNanos >= int64(3*time.Second) {
		t.Fatal("invalid deadline")
	}
	for i := 0; i < 3; i++ {
		loaded, err := decodeServerRejection(claim, raw, digest, receipt.Evidence.ObservedUnixNanos, deadline)
		if err != nil {
			t.Fatal("valid persisted envelope refused")
		}
		again, againDigest, againDeadline, err := canonicalServerRejection(claim, loaded)
		if err != nil || !bytes.Equal(raw, again) || againDigest != digest || againDeadline != deadline {
			t.Fatal("replay recalculated evidence")
		}
	}
	if _, err := decodeServerRejection(claim, raw, digest, receipt.Evidence.ObservedUnixNanos, deadline+1); err == nil {
		t.Fatal("altered deadline accepted")
	}
	if _, err := decodeServerRejection(claim, raw, digest, receipt.Evidence.ObservedUnixNanos+1, deadline); err == nil {
		t.Fatal("altered observation time accepted")
	}
	for _, altered := range [][]byte{append([]byte(" "), raw...), append(append([]byte(nil), raw[:len(raw)-1]...), []byte(`,"Unknown":true}`)...), []byte(`{}`)} {
		if _, err := decodeServerRejection(claim, altered, rawDigest(altered), receipt.Evidence.ObservedUnixNanos, deadline); err == nil {
			t.Fatal("noncanonical envelope accepted")
		}
	}
	foreign := claim
	foreign.LeaseKey += "other"
	if _, err := decodeServerRejection(foreign, raw, digest, receipt.Evidence.ObservedUnixNanos, deadline); err == nil {
		t.Fatal("foreign immutable claim accepted")
	}
}
