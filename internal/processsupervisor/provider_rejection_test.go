package processsupervisor

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/nysa-company/sf/internal/claudeprovider"
	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
)

type rejectionInspectorFunc func(context.Context, contracts.DrainRequest) (string, string, error)

func (f rejectionInspectorFunc) InspectRejectionCheckpoint(ctx context.Context, r contracts.DrainRequest) (string, string, error) {
	return f(ctx, r)
}

// Seeds private completed-run metadata to isolate receipt ownership/races.
// Stream parsing and native process termination have separate suites.
func rejectionSupervisorFixture(t *testing.T) (*Supervisor, contracts.DrainRequest) {
	t.Helper()
	s, err := New(nil)
	if err != nil {
		t.Fatal(err)
	}
	d := strings.Repeat("a", 64)
	r := contracts.DrainRequest{ClaimID: 1, Identity: domain.ProviderIdentity{Provider: "claude", Model: "claude-sonnet-5", Family: "anthropic-claude", Version: "2.1.263"}, Ref: domain.TicketRef{Channel: domain.ChannelDev, Project: "fixture", Ticket: "SF-rejection"}, Phase: domain.PhasePlanning, Role: "planner", Attempt: 1, ExpectedVersion: 2, LeaderEpoch: 3, RunnerEpoch: 4, LeaseKey: "fixture", BindingDigest: d, BinaryDigest: d, PolicyDigest: d, AuthDigest: d, AuthMode: "claude_subscription", Repository: "/private/repo", Worktree: "/private/worktree", WorktreeIdentity: `{"fixture":true}`, BaseSHA: strings.Repeat("b", 40), RequestDigest: d}
	closed := make(chan struct{})
	close(closed)
	s.runs[key(r)] = &run{identity: Identity{PID: 999999, PGID: 999999}, done: closed, streams: closed, finished: closed, rejection: &claudeprovider.RejectionObservation{Category: "server_error", StreamDigest: d, AllFailuresServerErrors: true, InternalRetries: 1, LastRetryDelayMS: 1000}}
	s.rejectionCheckpoint = rejectionInspectorFunc(func(context.Context, contracts.DrainRequest) (string, string, error) { return r.BaseSHA, d, nil })
	return s, r
}

func TestDrainServerRejectionIsExactAndSingleUse(t *testing.T) {
	s, r := rejectionSupervisorFixture(t)
	wrong := r
	wrong.Attempt++
	if _, receipt, err := s.DrainServerRejection(context.Background(), wrong); err == nil || receipt.Signature != nil {
		t.Fatal("wrong attempt accepted")
	}
	drain, receipt, err := s.DrainServerRejection(context.Background(), r)
	if err != nil || !contracts.VerifyDrainProof(s.PublicKey(), r, drain) || !contracts.VerifyServerRejection(s.PublicKey(), r, receipt) {
		t.Fatal("exact drained receipt refused")
	}
	if _, _, err := s.DrainServerRejection(context.Background(), r); err == nil {
		t.Fatal("receipt replayed")
	}
}

func TestConfigureRejectionCheckpointRequiresIdleSupervisor(t *testing.T) {
	s, r := rejectionSupervisorFixture(t)
	inspector := s.rejectionCheckpoint
	if err := s.ConfigureRejectionCheckpoint(inspector); err == nil {
		t.Fatal("active run allowed inspector replacement")
	}
	delete(s.runs, key(r))
	if err := s.ConfigureRejectionCheckpoint(nil); err == nil {
		t.Fatal("nil inspector accepted")
	}
	if err := s.ConfigureRejectionCheckpoint(inspector); err != nil {
		t.Fatal("idle configuration refused")
	}
	s.closing = true
	if err := s.ConfigureRejectionCheckpoint(inspector); err == nil {
		t.Fatal("closing supervisor reconfigured")
	}
}

func TestDrainServerRejectionControlWinsDuringInspection(t *testing.T) {
	s, r := rejectionSupervisorFixture(t)
	entered, release := make(chan struct{}), make(chan struct{})
	s.rejectionCheckpoint = rejectionInspectorFunc(func(context.Context, contracts.DrainRequest) (string, string, error) {
		close(entered)
		<-release
		return r.BaseSHA, strings.Repeat("a", 64), nil
	})
	done := make(chan error, 1)
	go func() {
		_, receipt, err := s.DrainServerRejection(context.Background(), r)
		if receipt.Signature != nil {
			done <- errors.New("receipt escaped control")
			return
		}
		done <- err
	}()
	<-entered
	if _, err := s.Drain(context.Background(), r); err != nil {
		close(release)
		t.Fatal("control drain failed")
	}
	close(release)
	if err := <-done; !errors.Is(err, ErrUnclear) {
		t.Fatal("control did not suppress receipt")
	}
}

func TestDrainServerRejectionRefusesMissingOrUnclearEvidence(t *testing.T) {
	for _, mode := range []string{"missing capture", "missing inspector", "dirty", "cancelled", "closing"} {
		t.Run(mode, func(t *testing.T) {
			s, r := rejectionSupervisorFixture(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			switch mode {
			case "missing capture":
				s.runs[key(r)].rejection = nil
			case "missing inspector":
				s.rejectionCheckpoint = nil
			case "dirty":
				s.rejectionCheckpoint = rejectionInspectorFunc(func(context.Context, contracts.DrainRequest) (string, string, error) {
					return "", "", errors.New("fixture dirty")
				})
			case "cancelled":
				cancel()
			case "closing":
				s.closing = true
			}
			if _, receipt, err := s.DrainServerRejection(ctx, r); err == nil || receipt.Signature != nil {
				t.Fatal("unclear evidence signed")
			}
		})
	}
}
