package processsupervisor

import (
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nysa-company/sf/internal/contracts"
)

const activityRecords = 128
const activityEvents = 64

type activityRegistry struct {
	mu       sync.Mutex
	sequence atomic.Uint64
	records  map[requestKey]*activityRecord
}

type activityRecord struct {
	registry                            *activityRegistry
	mu                                  sync.Mutex
	value                               contracts.ActivitySnapshot
	published                           bool
	finished                            atomic.Bool
	detailFailed                        atomic.Bool
	stdout, stderr, lastOutput, dropped atomic.Uint64
	lastFrame                           atomic.Uint64
}

func (s *Supervisor) beginActivity(request contracts.DrainRequest) *activityRecord {
	s.activity.mu.Lock()
	defer s.activity.mu.Unlock()
	if s.activity.records == nil {
		s.activity.records = map[requestKey]*activityRecord{}
	}
	k := key(request)
	if _, exists := s.activity.records[k]; exists {
		return nil
	}
	if len(s.activity.records) >= activityRecords {
		// Never evict a still-live record. If observation capacity is exhausted,
		// execution continues normally with detail explicitly unavailable.
		var oldest *activityRecord
		var remove requestKey
		for candidate, record := range s.activity.records {
			if record.finished.Load() && (oldest == nil || candidate.ClaimID < remove.ClaimID) {
				oldest, remove = record, candidate
			}
		}
		if oldest == nil {
			return nil
		}
		delete(s.activity.records, remove)
	}
	r := &activityRecord{registry: &s.activity, value: contracts.ActivitySnapshot{Events: []contracts.ActivityEvent{}}}
	s.activity.records[k] = r
	return r
}

func (r *activityRecord) publish() {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	// Pipe writers may have staged observations after gate release. Assign
	// sequences only now, with the start marker before every staged event.
	staged := r.value.Events
	r.value.Events = nil
	r.published, r.value.Available = true, true
	if r.value.StartedAt.IsZero() {
		r.value.StartedAt = time.Now().UTC()
	}
	r.appendLocked(contracts.ActivityEvent{At: r.value.StartedAt, Source: "sf", Kind: "process_started"})
	for _, event := range staged {
		r.appendLocked(event)
	}
}

func (r *activityRecord) releasingGate() {
	if r == nil {
		return
	}
	r.mu.Lock()
	r.value.StartedAt = time.Now().UTC()
	r.mu.Unlock()
}

func (r *activityRecord) abandon() {
	if r != nil {
		r.finished.Store(true)
	}
}

func activityAtomicMax(target *atomic.Uint64, value uint64) {
	for old := target.Load(); value > old; old = target.Load() {
		if target.CompareAndSwap(old, value) {
			return
		}
	}
}

func (r *activityRecord) appendLocked(event contracts.ActivityEvent) {
	if r.published {
		event.Sequence = r.registry.sequence.Add(1)
	}
	limit := activityEvents
	if !r.published {
		limit--
	} // reserve the first published slot for start
	if len(r.value.Events) == limit {
		copy(r.value.Events, r.value.Events[1:])
		r.value.Events = r.value.Events[:limit-1]
		r.dropped.Add(1)
	}
	r.value.Events = append(r.value.Events, event)
}

func (r *activityRecord) observe(kind, source string, at time.Time) {
	if r == nil {
		return
	}
	// A viewer or concurrent pipe can never hold up draining provider output.
	if !r.mu.TryLock() {
		r.dropped.Add(1)
		return
	}
	defer r.mu.Unlock()
	if source == "provider" {
		if r.detailFailed.Load() {
			return
		}
		r.value.DetailAvailable = true
		r.value.LastEventAt = at
		if kind == "read_completed" || kind == "edit_completed" || kind == "command_completed" || kind == "tool_completed" {
			r.value.LastToolAt = at
		}
	}
	r.appendLocked(contracts.ActivityEvent{At: at, Source: source, Kind: kind})
}

func (r *activityRecord) lifecycle(kind string) {
	if r == nil {
		return
	}
	at := time.Now().UTC()
	r.mu.Lock()
	defer r.mu.Unlock()
	switch kind {
	case "process_exited":
		r.value.ExitedAt = at
	case "cancellation_requested":
		if !r.value.CancellationAt.IsZero() {
			return
		}
		r.value.CancellationAt = at
	case "drain_proven":
		if !r.value.DrainedAt.IsZero() {
			return
		}
		r.value.DrainedAt = at
		r.finished.Store(true)
	}
	if r.published {
		r.appendLocked(contracts.ActivityEvent{At: at, Source: "sf", Kind: kind})
	}
}

func (s *Supervisor) ActivitySnapshot(request contracts.DrainRequest, after uint64) contracts.ActivitySnapshot {
	if s == nil {
		return contracts.ActivitySnapshot{}
	}
	s.activity.mu.Lock()
	r := s.activity.records[key(request)]
	s.activity.mu.Unlock()
	sequence := s.activity.sequence.Load()
	if r == nil {
		return contracts.ActivitySnapshot{Sequence: sequence, Gap: after != 0}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.published {
		return contracts.ActivitySnapshot{Sequence: sequence, Gap: after > sequence}
	}
	sequence = s.activity.sequence.Load()
	value := r.value
	value.DetailAvailable = value.DetailAvailable && !r.detailFailed.Load()
	value.Sequence = sequence
	value.StdoutBytes, value.StderrBytes = r.stdout.Load(), r.stderr.Load()
	if timestamp := r.lastOutput.Load(); timestamp != 0 {
		value.LastOutputAt = time.Unix(0, int64(timestamp)).UTC()
	}
	if timestamp := r.lastFrame.Load(); timestamp != 0 {
		value.LastFrameAt = time.Unix(0, int64(timestamp)).UTC()
	}
	value.Dropped = r.dropped.Load()
	value.Gap = after > sequence || value.Dropped > 0
	value.Events = make([]contracts.ActivityEvent, 0, len(r.value.Events))
	for _, event := range r.value.Events {
		if after == 0 || event.Sequence > after {
			value.Events = append(value.Events, event)
		}
	}
	return value
}

type activityWriter struct {
	capture io.Writer
	record  *activityRecord
	stderr  bool
	decoder activityDecoder // one private framing state per pipe
}

func (w *activityWriter) Write(data []byte) (int, error) {
	// Capture semantics and limits are independent of observations.
	n, err := w.capture.Write(data)
	if w.record != nil {
		at := time.Now().UTC()
		if w.stderr {
			w.record.stderr.Add(uint64(len(data)))
		} else {
			w.record.stdout.Add(uint64(len(data)))
		}
		activityAtomicMax(&w.record.lastOutput, uint64(at.UnixNano()))
		// stderr is a separate, byte-only observation stream. Provider protocol
		// frames are recognized only on the qualified stdout transport.
		if !w.stderr {
			w.decoder.feed(data, at, w.record)
		}
	}
	return n, err
}

var _ contracts.ProviderActivityReader = (*Supervisor)(nil)
