package daemon

import (
	"time"

	"github.com/nysa-company/sf/internal/store"
)

// ticketTiming is a projection of the immutable submission budget, never an
// admission or lifecycle decision. Queue and pause time consume this deadline.
func ticketTiming(value store.Ticket, now time.Time) map[string]any {
	if value.CreatedAt.IsZero() || value.MaxDuration <= 0 {
		return map[string]any{"available": false}
	}
	deadline := value.CreatedAt.Add(value.MaxDuration)
	age := now.Sub(value.CreatedAt)
	if age < 0 {
		age = 0
	}
	view := map[string]any{
		"available": true, "deadline_at": deadline.UTC().Format(time.RFC3339Nano),
		"age_seconds": int64(age / time.Second), "age": age.Truncate(time.Second).String(),
		"limit_seconds":          int64(value.MaxDuration / time.Second),
		"cost_ceiling_micro_usd": value.MaxCostMicroUSD,
		"note":                   "Deadline starts at submission and includes queue/pause time; this clock does not change ticket state.",
	}
	if !value.State.Terminal() {
		remaining := deadline.Sub(now)
		if remaining < 0 {
			remaining = 0
		}
		if remaining > value.MaxDuration {
			remaining = value.MaxDuration
		}
		view["remaining_seconds"] = int64(remaining / time.Second)
		view["remaining"] = remaining.Truncate(time.Second).String()
		view["deadline_elapsed"] = !now.Before(deadline)
	}
	return view
}
