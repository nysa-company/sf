package daemon

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strconv"

	"github.com/nysa-company/sf/internal/api"
	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/store"
)

func (daemon *Daemon) ticketActivity(ctx context.Context, request api.Request, identity domain.OperatorIdentity) api.Response {
	var parameters struct {
		ticketParameters
		AfterEpoch    uint64 `json:"after_epoch"`
		AfterSequence uint64 `json:"after_sequence"`
	}
	if decodeParameters(request.Parameters, &parameters) != nil || parameters.Channel != daemon.channel || parameters.AfterEpoch == 0 && parameters.AfterSequence != 0 {
		return daemon.failure(request, "invalid_argument", "activity cursor or channel is invalid", false)
	}
	request.Parameters, _ = json.Marshal(parameters.ticketParameters)
	ref, failure := daemon.ticketRef(ctx, request)
	if failure != nil {
		return *failure
	}
	stored, err := daemon.store.Ticket(ctx, ref)
	if err != nil {
		return daemon.failure(request, "ticket_not_found", "ticket is not present in this channel", false)
	}
	if err := daemon.lease.Validate(); err != nil {
		return daemon.failure(request, "leader_lost", "daemon leadership is no longer valid", true)
	}
	statusRequest := request
	statusRequest.Ticket = string(ref.Ticket)
	statusRequest.Parameters, _ = json.Marshal(map[string]any{"channel": daemon.channel, "project": ref.Project, "watch": false})
	status := daemon.statusTickets(ctx, statusRequest, identity)
	if !status.OK {
		return status
	}
	var view map[string]any
	decoder := json.NewDecoder(bytes.NewReader(status.Data))
	decoder.UseNumber()
	if decoder.Decode(&view) != nil {
		return daemon.failure(request, "internal_error", "durable activity view could not be encoded", false)
	}
	activity := map[string]any{"available": false, "current": false, "running": false, "daemon_epoch": daemon.epoch, "sequence": uint64(0), "restart": parameters.AfterEpoch != 0 && parameters.AfterEpoch != daemon.epoch, "gap": parameters.AfterSequence != 0, "monitor_at": timeView(daemon.clock.Now()), "reason": "no observed attempt matches the current durable ticket fence"}
	reader, supported := daemon.providerSupervisor.(contracts.ProviderActivityReader)
	if !supported {
		activity["reason"] = "provider activity is unavailable in this runtime"
	} else {
		attempts, err := daemon.store.ProviderAttempts(ctx, ref)
		if err != nil {
			return daemon.failure(request, evidenceErrorCode(err), "provider attempt identity could not be read", errors.Is(err, store.ErrBusy))
		}
		if len(attempts) > 0 {
			attempt := attempts[len(attempts)-1]
			current := activityCurrentAttempt(stored, attempt, daemon.epoch)
			after := parameters.AfterSequence
			if parameters.AfterEpoch != daemon.epoch {
				after = 0
			}
			snapshot := reader.ActivitySnapshot(drainRequestForProviderClaim(attempt.ProviderAttemptClaim), after)
			latest, err := daemon.store.Ticket(ctx, ref)
			if err != nil {
				return daemon.failure(request, evidenceErrorCode(err), "ticket fence could not be rechecked", errors.Is(err, store.ErrBusy))
			}
			current = current && latest.Version == stored.Version && latest.RunnerEpoch == stored.RunnerEpoch && latest.State == stored.State
			if ticket, ok := view["ticket"].(map[string]any); !ok || ticket["version"] != json.Number(strconv.FormatUint(stored.Version, 10)) {
				current = false
			}
			activity["sequence"] = snapshot.Sequence
			activity["gap"] = snapshot.Gap
			if snapshot.Available {
				activity["available"], activity["current"] = true, current
				activity["historical"] = !current
				activity["running"] = current && snapshot.ExitedAt.IsZero() && snapshot.DrainedAt.IsZero() && snapshot.CancellationAt.IsZero()
				activity["phase"], activity["attempt"], activity["role"] = attempt.Phase, attempt.Attempt, attempt.Role
				activity["provider"], activity["model"] = attempt.Binding.Identity.Provider, attempt.Binding.Identity.Model
				activity["observation"] = snapshot
				activity["reason"] = "SF process observations and provider-reported categories are not proof results"
				if !current {
					activity["reason"] = "historical attempt; it is not running under the current durable ticket fence"
				}
			}
		}
	}
	view["activity"] = activity
	return daemon.success(request, api.Mutation{}, view)
}

func activityCurrentAttempt(ticket store.Ticket, attempt store.ProviderAttempt, epoch uint64) bool {
	if attempt.Ref != ticket.Ref || attempt.LeaderEpoch != epoch || attempt.RunnerEpoch != ticket.RunnerEpoch || attempt.ExpectedVersion != ticket.Version || attempt.State != "active" || attempt.Outcome != "running" {
		return false
	}
	switch ticket.State {
	case domain.StatePlanning:
		return attempt.Phase == domain.PhasePlanning
	case domain.StateVerifying:
		return attempt.Phase == domain.PhaseVerification
	case domain.StateBuilding:
		return attempt.Phase == domain.PhaseBuild
	case domain.StateReviewing:
		return attempt.Phase == domain.PhaseReview
	}
	return false
}
