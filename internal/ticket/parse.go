// Package ticket parses the deliberately small local Markdown ticket format.
package ticket

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"strings"

	"github.com/nysa-company/sf/internal/domain"
)

const MaxSourceBytes = 1 << 20

type Parsed struct {
	Title      string
	Problem    string
	Acceptance []string
	Type       domain.TicketType
	MergeMode  domain.MergeMode
	Priority   string
	Source     []byte
	Digest     string
}

func Parse(reader io.Reader) (Parsed, error) {
	data, err := io.ReadAll(io.LimitReader(reader, MaxSourceBytes+1))
	if err != nil {
		return Parsed{}, fmt.Errorf("read ticket: %w", err)
	}
	if len(data) > MaxSourceBytes {
		return Parsed{}, fmt.Errorf("ticket exceeds %d bytes", MaxSourceBytes)
	}
	digest := sha256.Sum256(data)
	parsed := Parsed{
		Type:      domain.TicketFeature,
		MergeMode: domain.MergeGuarded,
		Priority:  "normal",
		Source:    bytes.Clone(data),
		Digest:    hex.EncodeToString(digest[:]),
	}

	lines, err := scanLines(data)
	if err != nil {
		return Parsed{}, err
	}
	index := 0
	if len(lines) > 0 && lines[0] == "---" {
		index, err = parseFrontMatter(lines, &parsed)
		if err != nil {
			return Parsed{}, err
		}
	}

	for index < len(lines) && strings.TrimSpace(lines[index]) == "" {
		index++
	}
	if index >= len(lines) || !strings.HasPrefix(lines[index], "# ") {
		return Parsed{}, fmt.Errorf("ticket requires one leading '# ' title")
	}
	parsed.Title = strings.TrimSpace(strings.TrimPrefix(lines[index], "# "))
	if parsed.Title == "" {
		return Parsed{}, fmt.Errorf("ticket title is empty")
	}
	index++

	var problem []string
	acceptanceIndex := -1
	for ; index < len(lines); index++ {
		line := lines[index]
		if line == "## Acceptance" {
			acceptanceIndex = index + 1
			break
		}
		if strings.HasPrefix(line, "# ") {
			return Parsed{}, fmt.Errorf("ticket contains more than one level-one title")
		}
		problem = append(problem, line)
	}
	parsed.Problem = strings.TrimSpace(strings.Join(problem, "\n"))
	if parsed.Problem == "" {
		return Parsed{}, fmt.Errorf("ticket problem text is empty")
	}
	if acceptanceIndex < 0 {
		return Parsed{}, fmt.Errorf("ticket requires an '## Acceptance' section")
	}
	for _, line := range lines[acceptanceIndex:] {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			continue
		}
		if strings.HasPrefix(trimmed, "## ") {
			break
		}
		if !strings.HasPrefix(trimmed, "- ") {
			return Parsed{}, fmt.Errorf("acceptance entries must be '- ' bullets")
		}
		entry := strings.TrimSpace(strings.TrimPrefix(trimmed, "- "))
		if entry == "" {
			return Parsed{}, fmt.Errorf("acceptance entry is empty")
		}
		parsed.Acceptance = append(parsed.Acceptance, entry)
	}
	if len(parsed.Acceptance) == 0 {
		return Parsed{}, fmt.Errorf("ticket requires at least one acceptance entry")
	}
	if parsed.Type == domain.TicketSpike && parsed.MergeMode == domain.MergeAutonomous {
		return Parsed{}, fmt.Errorf("spike tickets cannot request autonomous merge")
	}
	return parsed, nil
}

func parseFrontMatter(lines []string, parsed *Parsed) (int, error) {
	seen := make(map[string]struct{})
	for index := 1; index < len(lines); index++ {
		line := strings.TrimSpace(lines[index])
		if line == "---" {
			return index + 1, nil
		}
		if line == "" {
			continue
		}
		if strings.ContainsAny(line, "{}[]&*!") {
			return 0, fmt.Errorf("front matter supports only simple scalar values")
		}
		key, value, ok := strings.Cut(line, ":")
		if !ok || strings.TrimSpace(value) == "" {
			return 0, fmt.Errorf("invalid front matter line %q", line)
		}
		key = strings.TrimSpace(key)
		value = strings.TrimSpace(value)
		if _, duplicate := seen[key]; duplicate {
			return 0, fmt.Errorf("duplicate front matter key %q", key)
		}
		seen[key] = struct{}{}
		switch key {
		case "type":
			parsed.Type = domain.TicketType(value)
			if !parsed.Type.Valid() {
				return 0, fmt.Errorf("invalid ticket type %q", value)
			}
		case "merge":
			parsed.MergeMode = domain.MergeMode(value)
			if !parsed.MergeMode.Valid() {
				return 0, fmt.Errorf("invalid merge mode %q", value)
			}
		case "priority":
			switch value {
			case "low", "normal", "high":
				parsed.Priority = value
			default:
				return 0, fmt.Errorf("invalid priority %q", value)
			}
		default:
			return 0, fmt.Errorf("unknown front matter key %q", key)
		}
	}
	return 0, fmt.Errorf("unterminated front matter")
}

func scanLines(data []byte) ([]string, error) {
	scanner := bufio.NewScanner(bytes.NewReader(data))
	scanner.Buffer(make([]byte, 4096), MaxSourceBytes)
	var lines []string
	for scanner.Scan() {
		lines = append(lines, strings.TrimSuffix(scanner.Text(), "\r"))
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scan ticket: %w", err)
	}
	return lines, nil
}
