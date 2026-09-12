package processsupervisor

import (
	"bytes"
	"encoding/json"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/nysa-company/sf/internal/providerjson"
)

const activityFrameBytes = 16 << 10
const activityFramesPerWrite = 32
const activityPendingTools = 64
const activitySessionEvents = 4096
const activityDecodeBytes = 4 << 20

type activityDecoder struct {
	provider, model  string
	frame            []byte
	skipping, failed bool
	session          string
	seen             map[string]bool
	pending          map[string]string
	frames, bytes    int
}

func (d *activityDecoder) feed(data []byte, at time.Time, record *activityRecord) {
	if d.failed {
		return
	}
	if len(data) > activityDecodeBytes-d.bytes {
		d.fail(record)
		return
	}
	d.bytes += len(data)
	frames := 0
	for len(data) > 0 {
		if frames >= activityFramesPerWrite {
			// Discard the rest of this write and any incomplete trailing frame.
			// A later write must cross a newline before it can begin a frame.
			d.frame = nil
			d.skipping = data[len(data)-1] != '\n'
			d.fail(record)
			return
		}
		end := bytes.IndexByte(data, '\n')
		if end < 0 {
			end = len(data)
		}
		complete := end < len(data)
		part := data[:end]
		if !d.skipping {
			if len(d.frame)+len(part) > activityFrameBytes {
				d.frame = nil
				d.skipping = true
				d.fail(record)
				return
			}
			d.frame = append(d.frame, part...)
		}
		if !complete {
			return
		}
		data = data[end+1:]
		if d.skipping {
			d.skipping = false
			continue
		}
		frames++
		d.frames++
		if d.frames > activitySessionEvents {
			d.fail(record)
			return
		}
		if len(bytes.TrimSpace(d.frame)) != 0 {
			if !d.decode(d.frame, at, record) {
				d.fail(record)
				return
			}
			activityAtomicMax(&record.lastFrame, uint64(at.UnixNano()))
		}
		d.frame = d.frame[:0]
	}
}

func (d *activityDecoder) fail(record *activityRecord) {
	d.failed = true
	record.detailFailed.Store(true)
	d.frame, d.pending, d.seen = nil, nil, nil
	record.dropped.Add(1)
	if record.mu.TryLock() {
		record.value.DetailAvailable = false
		record.mu.Unlock()
	}
}

func activityString(object map[string]json.RawMessage, key string) string {
	var value string
	if json.Unmarshal(object[key], &value) != nil {
		return ""
	}
	return value
}

func activityID(id string) bool {
	if len(id) == 0 || len(id) > 256 {
		return false
	}
	for _, r := range id {
		if r < 33 || r > 126 {
			return false
		}
	}
	return true
}

func (d *activityDecoder) decode(frame []byte, at time.Time, record *activityRecord) bool {
	if !utf8.Valid(frame) {
		return false
	}
	object, err := providerjson.Object(frame)
	if err != nil {
		return false
	}
	kind := activityString(object, "type")
	if d.provider == "codex" {
		return d.codex(object, kind, at, record)
	}
	if d.provider != "claude" && d.provider != "cursor" {
		return false
	}
	session := activityString(object, "session_id")
	if !activityID(session) {
		return false
	}
	if d.session == "" {
		if d.provider == "claude" && !activityID(activityString(object, "uuid")) {
			return false
		}
		if kind != "system" || activityString(object, "subtype") != "init" || d.provider == "claude" && activityString(object, "model") != d.model {
			return false
		}
		d.session, d.pending, d.seen = session, map[string]string{}, map[string]bool{}
		record.observe("session_started", "provider", at)
		return true
	}
	if session != d.session {
		return false
	}
	if d.provider == "cursor" {
		// No stable tool payload contract is qualified here. Observe only a
		// same-session terminal notice; never interpret Cursor's free-form tool
		// payload, thinking, message, or advertised ask-mode as verification.
		if kind == "result" && activityResultFrame(object) {
			record.observe("session_finished", "provider", at)
			return true
		}
		// Other Cursor shapes lack a qualified activity contract.
		return false
	}
	uuid := activityString(object, "uuid")
	if !activityID(uuid) || d.seen[uuid] || len(d.seen) >= activitySessionEvents {
		return false
	}
	d.seen[uuid] = true
	switch kind {
	case "assistant", "user":
		message, err := providerjson.Object(object["message"])
		if err != nil || activityString(message, "role") != kind || kind == "assistant" && activityString(message, "model") != d.model {
			return false
		}
		var blocks []json.RawMessage
		if json.Unmarshal(message["content"], &blocks) != nil || len(blocks) > 128 {
			return false
		}
		// Validate the full frame before emitting anything from mixed content.
		next := make(map[string]string, len(d.pending))
		for id, category := range d.pending {
			next[id] = category
		}
		var events []string
		for _, raw := range blocks {
			block, err := providerjson.Object(raw)
			if err != nil {
				return false
			}
			switch activityString(block, "type") {
			case "tool_use":
				if kind != "assistant" {
					return false
				}
				id, category := activityString(block, "id"), activityToolCategory(activityString(block, "name"))
				if !activityID(id) || next[id] != "" || len(next) >= activityPendingTools {
					return false
				}
				if category == "" {
					category = "tool"
				}
				next[id] = category
				events = append(events, category+"_started")
			case "tool_result":
				id := activityString(block, "tool_use_id")
				if kind != "user" || next[id] == "" {
					return false
				}
				events = append(events, next[id]+"_completed")
				delete(next, id)
			case "text", "thinking", "redacted_thinking":
				// Intentionally never decode or expose text, reasoning/signatures.
			default:
				return false
			}
		}
		d.pending = next
		for _, event := range events {
			record.observe(event, "provider", at)
		}
	case "system":
		switch activityString(object, "subtype") {
		case "api_retry":
			var attempt, maximum int
			if json.Unmarshal(object["attempt"], &attempt) != nil || json.Unmarshal(object["max_retries"], &maximum) != nil || attempt < 1 || maximum < attempt || maximum > 15 {
				return false
			}
			record.observe("retry_reported", "provider", at)
		case "permission_denied":
			if d.pending[activityString(object, "tool_use_id")] == "" {
				return false
			}
			record.observe("permission_reported", "provider", at)
		}
	case "result":
		if !activityResultFrame(object) {
			return false
		}
		record.observe("session_finished", "provider", at)
	case "rate_limit_event", "tool_use_summary":
		// Normal native envelopes carry untrusted summaries/limit metadata.
		// Session and UUID are checked above; payload is never projected.
	default:
		return false
	}
	return true
}

func activityResultFrame(object map[string]json.RawMessage) bool {
	var isError *bool
	return activityString(object, "subtype") != "" && json.Unmarshal(object["is_error"], &isError) == nil && isError != nil
}

func activityToolCategory(name string) string {
	switch name {
	case "Read", "Glob", "Grep", "LS":
		return "read"
	case "Write", "Edit", "MultiEdit", "NotebookEdit":
		return "edit"
	case "Bash":
		return "command"
	}
	return ""
}

func (d *activityDecoder) codex(object map[string]json.RawMessage, kind string, at time.Time, record *activityRecord) bool {
	if d.session == "" {
		id := activityString(object, "thread_id")
		if kind != "thread.started" || !activityID(id) {
			return false
		}
		d.session, d.pending = id, map[string]string{}
		record.observe("session_started", "provider", at)
		return true
	}
	if id := activityString(object, "thread_id"); id != "" && id != d.session {
		return false
	}
	switch kind {
	case "item.started", "item.completed":
		item, err := providerjson.Object(object["item"])
		if err != nil {
			return false
		}
		category := ""
		switch activityString(item, "type") {
		case "command_execution":
			category = "command"
		case "file_change":
			category = "edit"
		case "web_search", "mcp_tool_call":
			category = "tool"
		case "agent_message", "reasoning":
			return true
		default:
			return true
		}
		id := activityString(item, "id")
		if !activityID(id) {
			return false
		}
		if strings.HasSuffix(kind, ".started") {
			if d.pending[id] != "" || len(d.pending) >= activityPendingTools {
				return false
			}
			d.pending[id] = category
			record.observe(category+"_started", "provider", at)
		} else {
			if d.pending[id] != category {
				return false
			}
			delete(d.pending, id)
			record.observe(category+"_completed", "provider", at)
		}
	case "turn.completed", "turn.failed":
		record.observe("turn_finished", "provider", at)
	case "turn.started":
		record.observe("turn_started", "provider", at)
	case "error":
		record.observe("error_reported", "provider", at)
	default:
		return false
	}
	return true
}
