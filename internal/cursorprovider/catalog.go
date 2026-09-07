package cursorprovider

import "strings"

// SessionDisplay accounts for the pinned CLI's parameterized selection, which
// is not always rendered like its catalog entry. Luna Low's exact explicit ID
// selects the 272K default context although its legacy catalog name says 1M.
// Sonnet Low similarly selects 300K with thinking disabled, not catalog 1M.
// This mapping was observed in native runs, not inferred from a result
// during parsing. A changed catalog entry refuses and needs requalification.
func SessionDisplay(model, catalog string) (string, error) {
	if FixtureDigest(model, catalog) == "" {
		return "", ErrRuntime
	}
	if model == "gpt-5.6-luna-low" {
		if catalog != "GPT-5.6 Luna 1M Low" {
			return "", ErrRuntime
		}
		return "GPT-5.6 Luna 272K Low", nil
	}
	if model == "claude-sonnet-5-low" {
		if catalog != "Claude Sonnet 5 1M Low" {
			return "", ErrRuntime
		}
		return "Claude Sonnet 5 300K Low No Thinking", nil
	}
	return catalog, nil
}

// CatalogDisplay reads the pinned CLI's no-color `models` format. It accepts
// one exact catalog ID, never substring aliases/default/auto, and never returns
// unrelated account or diagnostic lines. The resulting label is fixture-bound.
func CatalogDisplay(output []byte, model string) (string, error) {
	if _, ok := ModelFamily(model); !ok || len(output) == 0 || len(output) > 64<<10 || strings.ContainsAny(string(output), "\x00\x1b\r") {
		return "", ErrRuntime
	}
	lines := strings.Split(strings.TrimSuffix(string(output), "\n"), "\n")
	if len(lines) > 512 {
		return "", ErrRuntime
	}
	var display string
	for _, line := range lines {
		if !strings.HasPrefix(line, model+" ") && line != model {
			continue
		}
		if display != "" || !strings.HasPrefix(line, model+" - ") {
			return "", ErrRuntime
		}
		value := strings.TrimPrefix(line, model+" - ")
		for _, suffix := range []string{" (current, default)", " (default)", " (current)"} {
			value = strings.TrimSuffix(value, suffix)
		}
		if FixtureDigest(model, value) == "" {
			return "", ErrRuntime
		}
		display = value
	}
	if display == "" {
		return "", ErrRuntime
	}
	return display, nil
}
