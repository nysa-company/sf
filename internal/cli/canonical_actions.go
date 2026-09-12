package cli

import (
	"bytes"
	"encoding/json"

	"github.com/nysa-company/sf/internal/api"
)

// Only code-owned action fields on canonical routes are migrated. Compatibility
// routes preserve the wire response, including nested actions and JSON numbers.
func canonicalResponse(response api.Response) api.Response {
	if response.NextAction != nil {
		action := *response.NextAction
		action.Argv = canonicalActionArgv(action.Argv)
		response.NextAction = &action
	}
	var value any
	decoder := json.NewDecoder(bytes.NewReader(response.Data))
	decoder.UseNumber()
	if decoder.Decode(&value) != nil {
		return response
	}
	changed := false
	mapAction := func(value any) {
		object, ok := value.(map[string]any)
		if !ok {
			return
		}
		action, ok := object["next_action"].(map[string]any)
		if !ok {
			return
		}
		raw, ok := action["argv"].([]any)
		if !ok {
			return
		}
		argv := make([]string, len(raw))
		for index, item := range raw {
			argv[index], ok = item.(string)
			if !ok {
				return
			}
		}
		mapped := canonicalActionArgv(argv)
		if !sameArgv(argv, mapped) {
			action["argv"] = mapped
			changed = true
		}
	}
	// These are the only code-owned action locations in ticket responses.
	// Never descend through arbitrary evidence, source, or artifact objects.
	if object, ok := value.(map[string]any); ok {
		mapAction(object)
		mapAction(object["ticket"])
		if tickets, ok := object["tickets"].([]any); ok {
			for _, ticket := range tickets {
				mapAction(ticket)
			}
		}
	}
	if changed {
		if data, err := json.Marshal(value); err == nil {
			response.Data = data
		}
	}
	return response
}

func sameArgv(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

func canonicalActionArgv(argv []string) []string {
	copy := append([]string(nil), argv...)
	if len(argv) < 2 || argv[0] != "sf" && argv[0] != "sf-dev" && argv[0] != binaryName() {
		return copy
	}
	verb := argv[1]
	switch verb {
	case "daemon":
		if len(argv) >= 3 && (argv[2] == "run" || argv[2] == "status" || argv[2] == "cleanup" && len(argv) >= 4 && (argv[3] == "prepare" || argv[3] == "recover")) {
			copy[1] = "factory"
		}
		return copy
	case "tickets":
		verb = "list"
	case "show":
		verb = "view"
	case "status":
		// Unusual compatibility flags retain their original executable route.
		if len(argv) == 2 {
			verb = "list"
		} else if len(argv) == 3 && validSelectionID(argv[2]) {
			verb = "view"
		} else {
			return copy
		}
	case "run":
		if len(argv) < 3 || len(argv[2]) == 0 || argv[2][0] == '-' {
			return copy
		}
		return append([]string{argv[0], "ticket", "start", "--file", argv[2]}, argv[3:]...)
	case "submit", "start", "logs", "pause", "resume", "recover", "cancel", "retry", "take", "approve", "reject":
	default:
		return copy
	}
	return append([]string{argv[0], "ticket", verb}, argv[2:]...)
}
