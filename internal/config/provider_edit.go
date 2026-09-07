package config

import (
	"bytes"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"

	"github.com/pelletier/go-toml/v2/unstable"
)

// RewriteProviderPreset is a bounded, side-effect-free source transformation.
// Only provider values change; the caller must hold the configuration lock,
// validate effective machine policy, and install/apply the result explicitly.
func RewriteProviderPreset(source []byte, preset string) ([]byte, error) {
	providers, err := ProviderPreset(preset)
	if err != nil || preset == "" {
		return nil, errors.New("choose an explicit supported provider preset")
	}
	if len(source) > MaxFileBytes {
		return nil, errors.New("configuration exceeds bounded limit")
	}
	var before projectDocument
	if err := decodeStrict(source, &before); err != nil {
		return nil, err
	}
	values := map[string]string{"planner": providers.Planner[0], "builder": providers.Builder[0], "reviewer": providers.Reviewer[0]}
	type edit struct {
		start, end int
		text       string
	}
	var edits []edit
	var parser unstable.Parser
	parser.Reset(source)
	var table []string
	seen := map[string]bool{}
	headerEnd := -1
	for parser.NextExpression() {
		n := parser.Expression()
		if n.Kind != unstable.Table && n.Kind != unstable.KeyValue {
			continue
		}
		var keys []string
		lastEnd := 0
		it := n.Key()
		for it.Next() {
			key := it.Node()
			keys = append(keys, string(key.Data))
			lastEnd = int(key.Raw.Offset + key.Raw.Length)
		}
		if n.Kind == unstable.Table {
			table = keys
			if len(keys) == 1 && keys[0] == "providers" {
				// Table nodes have no Raw range in the pinned parser. Keys do;
				// insert after the complete header line, preserving its comment.
				closing := bytes.IndexByte(source[lastEnd:], ']')
				if closing < 0 {
					return nil, errors.New("provider table header is incomplete")
				}
				headerEnd = lastEnd + closing + 1
				if newline := bytes.IndexByte(source[headerEnd:], '\n'); newline >= 0 {
					headerEnd += newline
				} else {
					headerEnd = len(source)
				}
			}
			continue
		}
		path := append(append([]string(nil), table...), keys...)
		if len(path) == 0 || path[0] != "providers" {
			continue
		}
		end := int(n.Raw.Offset + n.Raw.Length)
		if lastEnd < 0 || lastEnd > end || end > len(source) {
			return nil, errors.New("invalid provider source range")
		}
		equal := bytes.IndexByte(source[lastEnd:end], '=')
		if equal < 0 {
			return nil, errors.New("provider assignment has no value")
		}
		start := lastEnd + equal + 1
		for start < end && (source[start] == ' ' || source[start] == '\t') {
			start++
		}
		var replacement string
		if len(path) == 1 {
			replacement = fmt.Sprintf("{planner = [%q], builder = [%q], reviewer = [%q]}", values["planner"], values["builder"], values["reviewer"])
			for role := range values {
				seen[role] = true
			}
		} else if len(path) == 2 && values[path[1]] != "" {
			replacement = fmt.Sprintf("[%q]", values[path[1]])
			seen[path[1]] = true
		} else {
			return nil, errors.New("unsupported provider configuration shape")
		}
		edits = append(edits, edit{start, end, replacement})
	}
	if err := parser.Error(); err != nil {
		return nil, err
	}
	var missing strings.Builder
	for _, role := range []string{"planner", "builder", "reviewer"} {
		if seen[role] {
			continue
		}
		key := role
		if headerEnd < 0 && before.Providers != nil {
			key = "providers." + role
		}
		fmt.Fprintf(&missing, "%s = [%q]\n", key, values[role])
	}
	if missing.Len() > 0 {
		at, text := 0, missing.String()
		if headerEnd >= 0 {
			at, text = headerEnd, "\n"+text
		} else if before.Providers == nil {
			at, text = len(source), "\n[providers]\n"+text
		}
		edits = append(edits, edit{at, at, text})
	}
	sort.Slice(edits, func(i, j int) bool { return edits[i].start < edits[j].start })
	var out bytes.Buffer
	position := 0
	for _, e := range edits {
		if e.start < position || e.end < e.start {
			return nil, errors.New("overlapping provider source edits")
		}
		out.Write(source[position:e.start])
		out.WriteString(e.text)
		position = e.end
	}
	out.Write(source[position:])
	result := out.Bytes()
	if len(result) > MaxFileBytes {
		return nil, errors.New("edited configuration exceeds bounded limit")
	}
	var after projectDocument
	if err := decodeStrict(result, &after); err != nil {
		return nil, err
	}
	if after.Providers == nil || !reflect.DeepEqual(after.Providers.Planner, providers.Planner) || !reflect.DeepEqual(after.Providers.Builder, providers.Builder) || !reflect.DeepEqual(after.Providers.Reviewer, providers.Reviewer) {
		return nil, errors.New("provider edit did not preserve selected pair")
	}
	before.Providers, after.Providers = nil, nil
	if !reflect.DeepEqual(before, after) {
		return nil, errors.New("provider edit changed unrelated configuration")
	}
	return result, nil
}
