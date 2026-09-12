package contracts

import "time"

// ProviderActivityReader is an optional observation capability, never lifecycle
// authority. The exact durable claim authenticates the read without exposing
// claim digests, provider session IDs, process identity, or transcript data.
type ProviderActivityReader interface {
	ActivitySnapshot(DrainRequest, uint64) ActivitySnapshot
}

type ActivityEvent struct {
	Sequence uint64    `json:"sequence"`
	At       time.Time `json:"at"`
	Source   string    `json:"source"` // sf or provider
	Kind     string    `json:"kind"`   // code-owned closed category
}

type ActivitySnapshot struct {
	Available       bool            `json:"available"`
	DetailAvailable bool            `json:"detail_available"`
	Sequence        uint64          `json:"sequence"`
	Gap             bool            `json:"gap"`
	Dropped         uint64          `json:"dropped"`
	StartedAt       time.Time       `json:"started_at"`
	ExitedAt        time.Time       `json:"exited_at"`
	DrainedAt       time.Time       `json:"drained_at"`
	CancellationAt  time.Time       `json:"cancellation_at"`
	LastOutputAt    time.Time       `json:"last_output_at"`
	LastEventAt     time.Time       `json:"last_event_at"`
	LastFrameAt     time.Time       `json:"last_frame_at"`
	LastToolAt      time.Time       `json:"last_tool_at"`
	StdoutBytes     uint64          `json:"stdout_bytes"`
	StderrBytes     uint64          `json:"stderr_bytes"`
	Events          []ActivityEvent `json:"events"`
}
