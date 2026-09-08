package localruntime

import (
	"context"
	"errors"
	"testing"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/store"
)

type postbuildAmendmentDispatchFixture struct {
	ticket         store.Ticket
	key            store.ProviderAttemptResultKey
	mode           string
	reads, signals int
	cancel         context.CancelFunc
	request        contracts.SignalRequest
}

func (f *postbuildAmendmentDispatchFixture) Ticket(context.Context, domain.TicketRef) (store.Ticket, error) {
	return f.ticket, nil
}
func (f *postbuildAmendmentDispatchFixture) RuntimeAdmissionReady(context.Context, domain.TicketRef, uint64, domain.Fence) (bool, error) {
	return f.mode != "sealed runtime", nil
}
func (f *postbuildAmendmentDispatchFixture) PostbuildRepairPendingAmendment(context.Context, domain.TicketRef, uint64, domain.Fence) (store.ProviderAttemptResultKey, error) {
	f.reads++
	if f.mode == "absent" {
		return store.ProviderAttemptResultKey{}, store.ErrNotFound
	}
	if f.mode == "corrupt" {
		return store.ProviderAttemptResultKey{}, store.ErrEvidenceConflict
	}
	if f.mode == "cancel" {
		f.cancel()
	}
	key := f.key
	if f.mode == "replacement result" {
		key.AttemptID++
	}
	return key, nil
}
func (f *postbuildAmendmentDispatchFixture) AssertTicketFence(context.Context, domain.TicketRef, uint64, domain.Fence) error {
	if f.mode == "late fence" {
		return store.ErrStaleFence
	}
	return nil
}
func (f *postbuildAmendmentDispatchFixture) SignalVerificationAmendmentRequest(_ context.Context, request contracts.SignalRequest, key store.ProviderAttemptResultKey) (contracts.TransitionResult, error) {
	f.signals++
	f.request = request
	if key != f.key {
		return contracts.TransitionResult{}, store.ErrEvidenceConflict
	}
	if f.mode == "transition refused" {
		return contracts.TransitionResult{}, store.ErrEvidenceConflict
	}
	return contracts.TransitionResult{To: domain.StateVerifying, TicketVersion: request.TicketVersion + 1}, nil
}

func TestPostbuildAmendmentDispatchOnlySignalsExactPendingResult(t *testing.T) {
	for _, mode := range []string{"valid", "wrong state", "wrong version", "wrong runner", "sealed runtime", "absent", "corrupt", "replacement result", "late fence", "cancel", "transition refused"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			ref := domain.TicketRef{Channel: domain.ChannelDev, Project: "fixture", Ticket: "SF-amendment-dispatch"}
			fence := domain.Fence{LeaderEpoch: 2, RunnerEpoch: 3}
			key := store.ProviderAttemptResultKey{Ref: ref, Phase: domain.PhaseBuild, AttemptID: 12, Attempt: 2}
			fixture := &postbuildAmendmentDispatchFixture{ticket: store.Ticket{Ref: ref, State: domain.StateBuilding, Version: 7, RunnerEpoch: 3}, key: key, mode: mode, cancel: cancel}
			switch mode {
			case "wrong state":
				fixture.ticket.State = domain.StatePaused
			case "wrong version":
				fixture.ticket.Version++
			case "wrong runner":
				fixture.ticket.RunnerEpoch++
			}
			result, err := dispatchPostbuildRepairAmendment(ctx, ref, 7, fence, key, fixture, fixture)
			if mode == "valid" {
				if err != nil || !result.Transitioned || result.State != domain.StateVerifying || result.Version != 8 || fixture.signals != 1 || fixture.request.Ticket != ref || fixture.request.TicketVersion != 7 || fixture.request.Fence != fence || fixture.request.From != domain.StateBuilding || fixture.request.EventPayload != "{}" {
					t.Fatalf("exact dispatch failed: %+v %v signal=%+v", result, err, fixture.request)
				}
				return
			}
			if err == nil || result.Transitioned || mode != "transition refused" && fixture.signals != 0 {
				t.Fatalf("refused request signaled: %+v %v signals=%d", result, err, fixture.signals)
			}
			if mode == "sealed runtime" && fixture.reads != 0 {
				t.Fatal("sealed runtime read repair authority")
			}
			if mode == "cancel" && !errors.Is(err, context.Canceled) {
				t.Fatalf("cancellation lost: %v", err)
			}
		})
	}
}

func TestPostbuildAmendmentDispatchRequiresStoreAndEngine(t *testing.T) {
	if _, err := (Worker{}).DispatchPostbuildRepairAmendment(context.Background(), domain.TicketRef{}, 0, domain.Fence{}, store.ProviderAttemptResultKey{}); err == nil {
		t.Fatal("missing runtime dependencies accepted")
	}
}
