package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/nysa-company/sf/internal/contracts"
)

type liveTicketActivity struct {
	Available   bool                       `json:"available"`
	Current     bool                       `json:"current"`
	Running     bool                       `json:"running"`
	Historical  bool                       `json:"historical"`
	DaemonEpoch uint64                     `json:"daemon_epoch"`
	Sequence    uint64                     `json:"sequence"`
	Restart     bool                       `json:"restart"`
	Gap         bool                       `json:"gap"`
	MonitorAt   time.Time                  `json:"monitor_at"`
	Reason      string                     `json:"reason"`
	Phase       string                     `json:"phase"`
	Attempt     int                        `json:"attempt"`
	Role        string                     `json:"role"`
	Provider    string                     `json:"provider"`
	Model       string                     `json:"model"`
	Observation contracts.ActivitySnapshot `json:"observation"`
}

func (a *app) watchTicketStatus(ctx context.Context, id string) error {
	var epoch, sequence uint64
	var previous string
	var heartbeat time.Time
	disconnected := false
	var previousDropped uint64
	previousGap := false
	for ctx.Err() == nil {
		response := a.request("ticket.activity", id, params(map[string]any{"after_epoch": epoch, "after_sequence": sequence}, a.channel))
		if ctx.Err() != nil {
			return nil
		}
		if !response.OK {
			if a.json || !disconnected {
				if err := a.emit(response); err != nil {
					return err
				}
			}
			if response.Error == nil || response.Error.Code != "daemon_unavailable" && response.Error.Code != "daemon_stopping" && response.Error.Code != "leader_lost" {
				return nil
			}
			if !a.json && !disconnected {
				if _, err := io.WriteString(a.errOut, "Monitor disconnected; retrying this read. Ticket state is unknown. Ctrl-C detaches.\n"); err != nil {
					return err
				}
			}
			disconnected = true
		} else {
			var data struct {
				Ticket struct {
					selectableTicket
					Version     uint64 `json:"version"`
					BlockedCode string `json:"blocked_code"`
				} `json:"ticket"`
				Activity   liveTicketActivity `json:"activity"`
				NextAction json.RawMessage    `json:"next_action"`
			}
			if json.Unmarshal(response.Data, &data) != nil || data.Ticket.ID != id || data.Ticket.Channel != a.channel || data.Ticket.Project == "" || !data.Ticket.State.Valid() || !validActivityCursor(data.Activity, epoch, sequence) {
				return a.emit(failure("invalid_response", "activity response identity or cursor is invalid", []string{binaryForChannel(a.channel), "ticket", "view", id}))
			}
			epoch, sequence = data.Activity.DaemonEpoch, data.Activity.Sequence
			state := string(data.Ticket.State) + "/" + data.Ticket.Title + data.Ticket.BlockedCode + string(data.NextAction) + fmt.Sprint(data.Ticket.Version, epoch, data.Activity.Attempt, data.Activity.Current, data.Activity.Running)
			full := previous != state || disconnected || terminalStatusResponse(response.Data)
			if a.json || full {
				if err := a.emit(response); err != nil {
					return err
				}
			} else {
				copy := response
				a.last = &copy
			}
			if !a.json {
				summary := full || heartbeat.IsZero() || time.Since(heartbeat) >= 5*time.Second || data.Activity.Restart || data.Activity.Gap && (!previousGap || data.Activity.Observation.Dropped != previousDropped)
				if err := renderLiveTicketActivity(a.out, data.Activity, summary); err != nil {
					return err
				}
				if summary {
					heartbeat = time.Now()
				}
			}
			previous, disconnected = state, false
			previousGap, previousDropped = data.Activity.Gap, data.Activity.Observation.Dropped
			if terminalStatusResponse(response.Data) {
				return nil
			}
		}
		timer := time.NewTimer(statusWatchInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil
		case <-timer.C:
		}
	}
	return nil
}

func renderLiveTicketActivity(writer io.Writer, activity liveTicketActivity, summary bool) error {
	if activity.Restart {
		if _, err := io.WriteString(writer, "Monitor reconnected to a new daemon epoch; earlier live activity is unavailable.\n"); err != nil {
			return err
		}
	}
	if activity.Gap && summary {
		if _, err := fmt.Fprintf(writer, "Activity gap: observations were dropped or the cursor is no longer retained (dropped %d).\n", activity.Observation.Dropped); err != nil {
			return err
		}
	}
	if summary {
		if !activity.Available {
			if _, err := fmt.Fprintf(writer, "Monitor connected · activity unavailable: %s\n", safeSelectionLabel(activity.Reason)); err != nil {
				return err
			}
		} else {
			status := "current attempt"
			if !activity.Current {
				status = "historical attempt; not current"
			}
			if _, err := fmt.Fprintf(writer, "%s / %s · %s attempt %d · %s · monitor connected\n", safeSelectionLabel(activity.Provider), safeSelectionLabel(activity.Model), safeSelectionLabel(activity.Phase), activity.Attempt, status); err != nil {
				return err
			}
			observation := activity.Observation
			elapsedEnd := activity.MonitorAt
			if !observation.ExitedAt.IsZero() {
				elapsedEnd = observation.ExitedAt
			}
			if _, err := fmt.Fprintf(writer, "Elapsed %s · last output %s · last validated frame %s · last activity event %s · last completed tool %s · bytes %d/%d stdout/stderr\n", activityAge(elapsedEnd, observation.StartedAt), activityAge(activity.MonitorAt, observation.LastOutputAt), activityAge(activity.MonitorAt, observation.LastFrameAt), activityAge(activity.MonitorAt, observation.LastEventAt), activityAge(activity.MonitorAt, observation.LastToolAt), observation.StdoutBytes, observation.StderrBytes); err != nil {
				return err
			}
			if !observation.DetailAvailable {
				if _, err := io.WriteString(writer, "Provider detail unavailable; byte arrival remains observable.\n"); err != nil {
					return err
				}
			}
		}
	}
	for _, event := range activity.Observation.Events {
		label := activityEventLabel(event.Source, event.Kind)
		if label == "" {
			continue
		}
		if _, err := fmt.Fprintf(writer, "%s  %s\n", event.At.UTC().Format("15:04:05"), label); err != nil {
			return err
		}
	}
	return nil
}

func validActivityCursor(value liveTicketActivity, epoch, sequence uint64) bool {
	if value.DaemonEpoch == 0 || value.MonitorAt.IsZero() || len(value.Observation.Events) > 64 {
		return false
	}
	if epoch != 0 && value.DaemonEpoch != epoch && !value.Restart {
		return false
	}
	if epoch == value.DaemonEpoch && value.Sequence < sequence && !value.Gap {
		return false
	}
	if value.Available && (!value.Observation.Available || value.Observation.Sequence != value.Sequence || value.Attempt <= 0 || value.Provider == "" || value.Model == "") {
		return false
	}
	if value.Running && (!value.Current || value.Historical || !value.Available) {
		return false
	}
	var previous uint64
	for _, event := range value.Observation.Events {
		if event.Sequence == 0 || event.Sequence <= previous || event.Sequence > value.Sequence || event.At.IsZero() || activityEventLabel(event.Source, event.Kind) == "" {
			return false
		}
		previous = event.Sequence
	}
	return true
}

func activityAge(now, at time.Time) string {
	if at.IsZero() {
		return "not observed"
	}
	duration := now.Sub(at)
	if duration < 0 {
		return "clock unavailable"
	}
	return duration.Round(time.Second).String()
}

func activityEventLabel(source, kind string) string {
	if source == "sf" {
		switch kind {
		case "process_started":
			return "SF: provider process started"
		case "process_exited":
			return "SF: provider process exited (drain is separate)"
		case "cancellation_requested":
			return "SF: process cancellation requested"
		case "drain_proven":
			return "SF: provider process drain proven"
		}
	}
	if source != "provider" {
		return ""
	}
	labels := map[string]string{"session_started": "session started", "session_finished": "session finished", "turn_started": "turn started", "turn_finished": "turn finished", "read_started": "reading operation started", "read_completed": "reading operation completed", "edit_started": "edit operation started", "edit_completed": "edit operation completed (not verified)", "command_started": "command operation started", "command_completed": "command operation completed (not an SF proof result)", "tool_started": "tool operation started", "tool_completed": "tool operation completed (not verified)", "retry_reported": "retry notice", "permission_reported": "permission notice", "error_reported": "error notice"}
	if label := labels[kind]; label != "" {
		return "Provider: " + label
	}
	return ""
}
