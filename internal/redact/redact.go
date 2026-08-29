// Package redact applies conservative, deterministic redaction at output
// boundaries. It never attempts to infer arbitrary source text is secret.
package redact

import (
	"encoding/json"
	"regexp"
	"sort"
	"strings"
)

var (
	assignment  = regexp.MustCompile(`(?i)((?:^|[^a-z0-9_])(?:[a-z0-9_]*[_-])?(token|password|passwd|secret|api[_-]?key|private[_-]?key|authorization|credential|session|cookie)\b\s*[:=]\s*)("[^"\r\n]*"|'[^'\r\n]*'|[^\s,;}&]+)`)
	header      = regexp.MustCompile(`(?i)(\b(authorization|proxy-authorization)\s*:\s*)(?:\r?\n[ \t]+)?(?:"[^"\r\n]*"|'[^'\r\n]*'|[^\r\n\s]+(?:\s+[^\r\n\s]+)?)`)
	urlUserInfo = regexp.MustCompile(`(?i)(https?://)([^/@\s:]+):([^/@\s]+)@`)
	urlQuery    = regexp.MustCompile(`(?i)([?&](?:token|password|secret|api[_-]?key|access_token|refresh_token)=)([^&\s]+)`)
)

// Policy describes only paths that are safe to replace. Home is rendered as
// $HOME and explicitly named roots as stable labels; unrelated strings remain
// unchanged.
type Policy struct {
	Home  string
	Roots map[string]string
}

func NewPolicy(home string, roots map[string]string) Policy {
	copy := make(map[string]string, len(roots))
	for path, label := range roots {
		copy[path] = label
	}
	return Policy{Home: home, Roots: copy}
}

func (p Policy) String(value string) string {
	paths := make([]string, 0, len(p.Roots))
	for path := range p.Roots {
		if path != "" {
			paths = append(paths, path)
		}
	}
	sort.Slice(paths, func(i, j int) bool {
		if len(paths[i]) != len(paths[j]) {
			return len(paths[i]) > len(paths[j])
		}
		return paths[i] < paths[j]
	})
	for _, path := range paths {
		value = strings.ReplaceAll(value, path, p.Roots[path])
	}
	if p.Home != "" {
		value = strings.ReplaceAll(value, p.Home, "$HOME")
	}
	value = header.ReplaceAllString(value, `${1}: [REDACTED]`)
	value = assignment.ReplaceAllString(value, `${1}[REDACTED]`)
	value = urlUserInfo.ReplaceAllString(value, `${1}[REDACTED]@`)
	value = urlQuery.ReplaceAllString(value, `${1}[REDACTED]`)
	return value
}

func String(value string) string { return (Policy{}).String(value) }

// JSON redacts only string leaves while preserving the original JSON shape.
// Invalid JSON is treated as text and redacted without attempting to parse it.
func (p Policy) JSON(data []byte) []byte {
	var value any
	decoder := json.NewDecoder(strings.NewReader(string(data)))
	decoder.UseNumber()
	if err := decoder.Decode(&value); err != nil {
		return []byte(p.String(string(data)))
	}
	redacted := redactValue(p, value)
	encoded, err := json.Marshal(redacted)
	if err != nil {
		return []byte(p.String(string(data)))
	}
	return encoded
}

func JSON(data []byte) []byte { return (Policy{}).JSON(data) }

func redactValue(policy Policy, value any) any {
	switch typed := value.(type) {
	case string:
		return policy.String(typed)
	case []any:
		for index := range typed {
			typed[index] = redactValue(policy, typed[index])
		}
	case map[string]any:
		for key, child := range typed {
			if isSecretKey(key) {
				typed[key] = "[REDACTED]"
			} else {
				typed[key] = redactValue(policy, child)
			}
		}
	}
	return value
}

func isSecretKey(key string) bool {
	key = strings.ToLower(strings.ReplaceAll(strings.ReplaceAll(key, "-", "_"), " ", "_"))
	for _, word := range []string{"token", "password", "passwd", "secret", "api_key", "private_key", "authorization", "credential", "session", "cookie"} {
		if key == word || strings.Contains(key, word) {
			return true
		}
	}
	return false
}
