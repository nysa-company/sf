package daemon

import (
	"context"
	"errors"

	"github.com/nysa-company/sf/internal/api"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/hostidentity"
	"github.com/nysa-company/sf/internal/store"
)

func (daemon *Daemon) externalCleanupRecovery(ctx context.Context, request api.Request) api.Response {
	var parameters struct {
		Channel domain.Channel `json:"channel"`
	}
	if request.Ticket != "" || decodeParameters(request.Parameters, &parameters) != nil || parameters.Channel != daemon.channel {
		return daemon.failure(request, "invalid_argument", "cleanup recovery requires the current channel and no ticket or host-evidence arguments", false)
	}
	if err := daemon.lease.Validate(); err != nil {
		return daemon.failure(request, "leader_lost", "daemon leadership is no longer valid", true)
	}
	daemon.runtimeMu.Lock()
	defer daemon.runtimeMu.Unlock()
	if daemon.isClosed() || daemon.runtimeStopped {
		return daemon.failure(request, "daemon_stopping", "cleanup recovery is unavailable while the daemon is stopping", true)
	}
	prepare := request.Method == "daemon.cleanup.prepare"
	var result store.ExternalCleanupRecoveryStatus
	var err error
	if prepare {
		result, err = daemon.store.PrepareExternalCleanupRecovery(ctx, daemon.channel, daemon.epoch)
	} else {
		result, err = daemon.store.RecoverExternalCleanup(ctx, daemon.channel, daemon.epoch)
	}
	if err != nil {
		code, message := "external_cleanup_recovery_refused", "cleanup recovery evidence was not accepted; keep quarantine sealed and prepare a checkpoint on the original host"
		verb := "prepare"
		switch {
		case errors.Is(err, store.ErrExternalRecoveryReboot):
			code, message, verb = "host_reboot_required", "save your work and reboot this host before recovery; a daemon restart is not sufficient", "recover"
		case errors.Is(err, hostidentity.ErrUnavailable):
			code, message = "host_identity_unavailable", "trusted macOS host identity could not be inspected; restore host inspection access before preparing or recovering"
			if !prepare {
				verb = "recover"
			}
		case errors.Is(err, store.ErrStaleFence):
			return daemon.failure(request, "leader_lost", "cleanup recovery requires the current daemon leader", true)
		}
		response := daemon.failure(request, code, message, false)
		response.NextAction = &domain.NextAction{Code: code, Argv: []string{daemon.executable(), "daemon", "cleanup", verb}}
		return response
	}
	message := "no persistent external cleanup quarantine is present"
	var next *domain.NextAction
	switch result.State {
	case "reboot_required":
		message = "checkpoint saved; quarantine remains sealed. Save your work and reboot this host, restart this channel daemon, then recover. SF will not reboot the host."
		next = &domain.NextAction{Code: "host_reboot_required", Argv: []string{daemon.executable(), "daemon", "cleanup", "recover"}}
	case "recovery_ready":
		message = "a different boot on the checkpointed host was observed; quarantine remains sealed until explicit recovery"
		next = &domain.NextAction{Code: "external_cleanup_recovery_ready", Argv: []string{daemon.executable(), "daemon", "cleanup", "recover"}}
	case "recovered":
		message = "checkpointed quarantine retired after verified same-host reboot; uncertain effects still require normal reconciliation"
		next = &domain.NextAction{Code: "external_cleanup_recovered", Argv: []string{daemon.executable(), "status"}}
	}
	response := daemon.success(request, api.Mutation{Attempted: result.Changed, Observed: !result.Changed, Kind: request.Method, Identity: string(daemon.channel)}, map[string]any{"state": result.State, "message": message, "channel": daemon.channel})
	response.NextAction = next
	return response
}
