package providercoord

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/store"
	"github.com/nysa-company/sf/internal/testkit"
	"github.com/nysa-company/sf/internal/worktreecoord"
)

type rejectionDrainFixture struct {
	*testkit.Supervisor
	rejected, ordinary int
	refuse, tamper     bool
}

func (s *rejectionDrainFixture) Drain(ctx context.Context, request contracts.DrainRequest) (contracts.DrainProof, error) {
	s.ordinary++
	return s.Supervisor.Drain(ctx, request)
}

func (s *rejectionDrainFixture) DrainServerRejection(ctx context.Context, request contracts.DrainRequest) (contracts.DrainProof, contracts.ServerRejectionAttestation, error) {
	s.rejected++
	if s.refuse {
		return contracts.DrainProof{}, contracts.ServerRejectionAttestation{}, errors.New("fixture rejection unavailable")
	}
	drain, err := s.Supervisor.Drain(ctx, request)
	if err != nil {
		return drain, contracts.ServerRejectionAttestation{}, err
	}
	receipt, err := s.Signer.SignServerRejection(request, drain, contracts.ServerRejectionEvidence{StreamDigest: strings.Repeat("b", 64), AllFailuresServerErrors: true, CheckpointHeadOID: request.BaseSHA, CheckpointDigest: strings.Repeat("c", 64), ObservedUnixNanos: time.Now().UnixNano()})
	if s.tamper && err == nil {
		receipt.Signature[0] ^= 1
	}
	return drain, receipt, err
}

func TestServerRejectionDrainNeverUpgradesUnprovedFailure(t *testing.T) {
	for _, scenario := range []string{"signed", "unavailable", "cancelled", "foreign-provider", "tampered"} {
		t.Run(scenario, func(t *testing.T) {
			s := &rejectionDrainFixture{Supervisor: testkit.NewSupervisor(), refuse: scenario == "unavailable", tamper: scenario == "tampered"}
			digest := strings.Repeat("a", 64)
			claim := store.ProviderAttemptClaim{ID: 1, Ref: domain.TicketRef{Channel: domain.ChannelDev, Project: "fixture", Ticket: "SF-rejection"}, Phase: domain.PhasePlanning, Role: "planner", Attempt: 1,
				Binding:  contracts.RuntimeBinding{Identity: domain.ProviderIdentity{Provider: "claude", Model: "claude-sonnet-5", Family: "anthropic-claude", Version: "2.1.263"}, BinaryDigest: digest, PolicyDigest: digest, AuthDigest: digest, AuthMode: "claude_subscription"},
				LeaseKey: "fixture", BindingDigest: digest, LeaderEpoch: 2, RunnerEpoch: 3, ExpectedVersion: 4, Repository: "/private/repo", Worktree: "/private/worktree", WorktreeIdentity: `{"fixture":true}`, BaseSHA: strings.Repeat("b", 40), RequestDigest: digest, SupervisorKey: s.PublicKey()}
			if scenario == "foreign-provider" {
				claim.Binding.Identity.Provider = "codex"
			}
			drain, receipt, err := drainWithServerRejection(context.Background(), s, claim, scenario != "cancelled")
			if scenario == "tampered" {
				if !errors.Is(err, store.ErrProviderServerRejection) || receipt != nil || s.ordinary != 0 {
					t.Fatal("invalid consumed receipt downgraded")
				}
				return
			}
			if err != nil || !contracts.VerifyDrainProof(s.PublicKey(), drainRequest(claim), drain) {
				t.Fatal("drain proof missing")
			}
			if scenario == "signed" {
				if receipt == nil || s.ordinary != 0 || s.rejected != 1 {
					t.Fatal("signed result not consumed once")
				}
			} else if receipt != nil || s.ordinary != 1 {
				t.Fatal("unproved failure upgraded")
			}
			if (scenario == "cancelled" || scenario == "foreign-provider") && s.rejected != 0 {
				t.Fatal("ineligible rejection requested")
			}
		})
	}
}

type rejectionClock struct{ at time.Time }

func (c rejectionClock) Now() time.Time { return c.at }

func TestServerRejectionBackoffIsBoundedAndCancelable(t *testing.T) {
	now := time.Now()
	clock := rejectionClock{now}
	for _, delay := range []time.Duration{4 * time.Second} {
		if err := waitServerRejectionBackoff(context.Background(), clock, now.Add(delay)); err == nil {
			t.Fatal("invalid backoff accepted")
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := waitServerRejectionBackoff(ctx, clock, now.Add(time.Second)); !errors.Is(err, context.Canceled) {
		t.Fatal("cancelled backoff continued")
	}
	if err := waitServerRejectionBackoff(context.Background(), clock, now); err != nil {
		t.Fatal("elapsed backoff refused")
	}
	// Store can observe a future deadline immediately before this clock read
	// observes it elapsed. Admission is rechecked by Store on the next loop.
	if err := waitServerRejectionBackoff(context.Background(), clock, now.Add(-time.Nanosecond)); err != nil {
		t.Fatal("deadline crossed between Store and coordinator was refused")
	}
}

// This joins real Store admission/finish with Coordinator orchestration.
// The process and filesystem observations are explicit fixtures, not native
// permission or clean-worktree acceptance evidence.
type rejectionLifecycleSupervisor struct {
	*testkit.Supervisor
	db                                   *store.Store
	runs, inspections                    int
	alwaysReject, refuseSecondInspection bool
	physical                             contracts.RejectionCheckpointInspector
	afterReceipt                         func() error
}

func (s *rejectionLifecycleSupervisor) ConfigureRejectionCheckpoint(contracts.RejectionCheckpointInspector) error {
	return nil
}
func (s *rejectionLifecycleSupervisor) Run(context.Context, contracts.DrainRequest, contracts.Invocation, contracts.PhaseInput) (contracts.CommandResult, error) {
	s.runs++
	if s.runs == 1 || s.alwaysReject {
		return contracts.CommandResult{}, errors.New("fixture server exit")
	}
	return contracts.CommandResult{}, nil
}
func (s *rejectionLifecycleSupervisor) InspectRejectionCheckpoint(ctx context.Context, request contracts.DrainRequest) (string, string, error) {
	s.inspections++
	if s.physical != nil {
		return s.physical.InspectRejectionCheckpoint(ctx, request)
	}
	if s.refuseSecondInspection && s.inspections == 2 {
		return "", "", errors.New("fixture changed checkout")
	}
	checkpoint, err := s.db.ProviderAttemptCheckpointForRequest(ctx, request)
	if err != nil {
		return "", "", err
	}
	digest, err := store.ProviderAttemptCheckpointDigest(checkpoint, request.RequestDigest)
	return checkpoint.ExpectedHead, digest, err
}
func (s *rejectionLifecycleSupervisor) DrainServerRejection(ctx context.Context, request contracts.DrainRequest) (contracts.DrainProof, contracts.ServerRejectionAttestation, error) {
	drain, err := s.Supervisor.Drain(ctx, request)
	if err != nil {
		return drain, contracts.ServerRejectionAttestation{}, err
	}
	head, digest, err := s.InspectRejectionCheckpoint(ctx, request)
	if err != nil {
		return drain, contracts.ServerRejectionAttestation{}, err
	}
	receipt, err := s.Signer.SignServerRejection(request, drain, contracts.ServerRejectionEvidence{StreamDigest: strings.Repeat("a", 64), AllFailuresServerErrors: true, CheckpointHeadOID: head, CheckpointDigest: digest, ObservedUnixNanos: time.Now().UnixNano()})
	if err == nil && s.afterReceipt != nil {
		err = s.afterReceipt()
	}
	return drain, receipt, err
}

func TestServerRejectionRetryReauthenticatesRealGitWorktree(t *testing.T) {
	for _, path := range []string{"", "untrusted.txt", "ignored.tmp", "baseline.txt"} {
		name := path
		if name == "" {
			name = "clean"
		}
		t.Run(name, func(t *testing.T) {
			dirty := path != ""
			db, request, original, provider, _ := estimatedRetryFixtureWithGit(t, true)
			request.Input.Timeout = 30 * time.Second
			physical := worktreecoord.Coordinator{Store: db, Git: rejectionPhysicalGit(t, db)}
			s := &rejectionLifecycleSupervisor{Supervisor: original.supervisor.(*testkit.Supervisor), db: db, physical: physical}
			retained := filepath.Join(request.Input.Worktree, path)
			if dirty {
				s.afterReceipt = func() error { return os.WriteFile(retained, []byte("retained change\n"), 0600) }
			}
			coordinator, err := New(original.registry, original.routes, db, nil, s)
			if err != nil {
				t.Fatal(err)
			}
			if err := coordinator.ConfigureRejectionCheckpoint(s); err != nil {
				t.Fatal(err)
			}
			provider.Steps[domain.PhasePlanning] = []testkit.ProviderStep{{Artifact: plannerArtifact()}}
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			result := coordinator.Run(ctx, request)
			want, runs := Completed, 2
			if dirty {
				want, runs = NeedsOperator, 1
			}
			if result.Code != want || s.runs != runs || s.inspections != 2 || len(result.Attempts) != 2 || result.PersistenceFailure {
				t.Fatalf("result=%s runs=%d inspections=%d attempts=%d persistence=%v", result.Code, s.runs, s.inspections, len(result.Attempts), result.PersistenceFailure)
			}
			attempts, err := db.ProviderAttempts(ctx, request.Input.Ticket)
			if err != nil || len(attempts) != 2 || attempts[0].Outcome != "server_rejected" {
				t.Fatal("durable rejection missing")
			}
			for _, attempt := range attempts {
				if attempt.State == "active" || attempt.Binding != provider.binding {
					t.Fatal("attempt authority drift")
				}
			}
			if dirty {
				contents, err := os.ReadFile(retained)
				if err != nil || string(contents) != "retained change\n" {
					t.Fatal("retry discarded provider changes")
				}
			} else {
				if replay := coordinator.Run(ctx, request); replay.Code != Completed || s.runs != 2 {
					t.Fatal("successful retry replay launched again")
				}
			}
		})
	}
}

func TestServerRejectionCoordinatorSharesBudgetAndRechecksCheckout(t *testing.T) {
	for _, scenario := range []string{"success", "second-server-error", "checkout-changed"} {
		t.Run(scenario, func(t *testing.T) {
			db, request, original, provider, _ := estimatedRetryFixture(t, true)
			s := &rejectionLifecycleSupervisor{Supervisor: original.supervisor.(*testkit.Supervisor), db: db, alwaysReject: scenario == "second-server-error", refuseSecondInspection: scenario == "checkout-changed"}
			coordinator, err := New(original.registry, original.routes, db, nil, s)
			if err != nil {
				t.Fatal(err)
			}
			if err := coordinator.ConfigureRejectionCheckpoint(s); err != nil {
				t.Fatal(err)
			}
			provider.Steps[domain.PhasePlanning] = []testkit.ProviderStep{{Artifact: plannerArtifact()}}
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			result := coordinator.Run(ctx, request)
			want, runs := Completed, 2
			if scenario == "second-server-error" {
				want = AttemptExhausted
			}
			if scenario == "checkout-changed" {
				want, runs = NeedsOperator, 1
			}
			if result.Code != want || s.runs != runs || len(result.Attempts) != 2 || result.PersistenceFailure {
				t.Fatalf("code=%s launches=%d receipts=%d persistence=%v", result.Code, s.runs, len(result.Attempts), result.PersistenceFailure)
			}
			attempts, err := db.ProviderAttempts(ctx, request.Input.Ticket)
			if err != nil || len(attempts) != 2 {
				t.Fatal("durable attempt count")
			}
			for _, attempt := range attempts {
				if attempt.Binding != provider.binding || attempt.Role != "planner" || attempt.State == "active" {
					t.Fatal("binding or terminal authority drift")
				}
			}
			if scenario != "checkout-changed" {
				replay := coordinator.Run(ctx, request)
				if replay.Code != want || s.runs != runs {
					t.Fatal("replay launched another attempt")
				}
			}
		})
	}
}
