package main

// This file implements only the fixture-side Codex CLI surface used by the
// compiled E2E. It is deliberately selected before flag.Parse: Codex's argv
// is not the legacy fake-provider flag grammar.

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/phaseartifact"
)

const (
	maxCodexPrompt          = 64 << 10
	maxCodexPath            = 4096
	verificationFixtureFile = "sf_fixture_test.go"
	builderFixtureFile      = "sf_fixture.go"
	takeoverEnteredFile     = ".sf-e2e-takeover-entered"
)

func isCodexInvocation(argv []string) bool {
	if len(argv) == 0 {
		return false
	}
	if argv[0] == "codex" {
		argv = argv[1:]
	}
	if len(argv) == 0 {
		return true
	}
	switch argv[0] {
	case "--version", "login", "exec", "sandbox":
		return true
	default:
		return false
	}
}

func runCodexInvocation(raw []string) error {
	argv := append([]string(nil), raw...)
	if len(argv) > 0 && argv[0] == "codex" {
		argv = argv[1:]
	}
	if len(argv) == 1 && argv[0] == "--version" {
		_, err := io.WriteString(os.Stdout, "codex 1.2.3\n")
		return err
	}
	if len(argv) == 2 && argv[0] == "login" && argv[1] == "status" {
		_, err := io.WriteString(os.Stdout, "Logged in using ChatGPT\n")
		return err
	}
	if len(argv) >= 2 && argv[0] == "exec" && argv[1] == "--help" {
		return writeCodexHelp()
	}
	if len(argv) >= 1 && argv[0] == "sandbox" {
		return runCodexSandbox(argv)
	}
	if len(argv) >= 1 && argv[0] == "exec" {
		return runCodexExec(argv)
	}
	return errors.New("unsupported codex argv")
}

func writeCodexHelp() error {
	_, err := io.WriteString(os.Stdout, "Usage: codex exec [OPTIONS]\n  --json\n  --output-schema PATH\n  --output-last-message PATH\n  --ephemeral\n  --ignore-user-config\n  --ignore-rules\n  --config KEY=VALUE\n  --model MODEL\n  -C DIR\n")
	return err
}

func runCodexSandbox(argv []string) error {
	separator := -1
	for index, value := range argv {
		if value == "--" {
			separator = index
			break
		}
	}
	if separator < 0 || separator+1 >= len(argv) {
		return errors.New("sandbox command separator is required")
	}
	command := argv[separator+1:]
	joined := strings.Join(command, " ")
	// These outcomes mirror the qualification fixture's five bounded probes.
	// No command is executed by this helper.
	if strings.Contains(joined, "CODEX_HOME/auth.json") || strings.Contains(joined, "curl") || (strings.Contains(joined, "test -w /etc/hosts") && !strings.Contains(joined, "! test -w /etc/hosts")) {
		os.Exit(1)
	}
	if strings.Contains(joined, "test -w .") || strings.Contains(joined, "test -r /etc/hosts") {
		return nil
	}
	return errors.New("unsupported sandbox probe")
}

type codexExecArgs struct {
	model, worktree, schema, lastMessage string
}

func parseCodexExec(argv []string) (codexExecArgs, error) {
	if len(argv) < 2 || argv[0] != "exec" {
		return codexExecArgs{}, errors.New("exec argv is required")
	}
	parsed := codexExecArgs{}
	seen := map[string]bool{}
	for index := 1; index < len(argv); index++ {
		arg := argv[index]
		switch arg {
		case "--model", "-C", "--output-schema", "--output-last-message":
			if index+1 >= len(argv) || argv[index+1] == "" || seen[arg] {
				return codexExecArgs{}, fmt.Errorf("%s requires one value", arg)
			}
			seen[arg] = true
			value := argv[index+1]
			switch arg {
			case "--model":
				parsed.model = value
			case "-C":
				parsed.worktree = value
			case "--output-schema":
				parsed.schema = value
			case "--output-last-message":
				parsed.lastMessage = value
			}
			index++
		case "--ephemeral", "--json", "--ignore-user-config", "--ignore-rules":
		case "--config":
			if index+1 >= len(argv) || argv[index+1] == "" {
				return codexExecArgs{}, errors.New("--config requires one value")
			}
			index++
		case "-":
			if index != len(argv)-1 {
				return codexExecArgs{}, errors.New("prompt marker must be last")
			}
		default:
			return codexExecArgs{}, fmt.Errorf("unsupported exec flag %q", arg)
		}
	}
	if parsed.model == "" || parsed.worktree == "" || parsed.schema == "" || parsed.lastMessage == "" || !seen["--model"] || !seen["-C"] || !seen["--output-schema"] || !seen["--output-last-message"] {
		return codexExecArgs{}, errors.New("exec requires model, worktree, schema, and final artifact paths")
	}
	if !filepath.IsAbs(parsed.worktree) || filepath.Clean(parsed.worktree) != parsed.worktree || parsed.worktree == string(filepath.Separator) || len(parsed.worktree) > maxCodexPath {
		return codexExecArgs{}, errors.New("exec worktree must be a bounded absolute path")
	}
	info, err := os.Lstat(parsed.worktree)
	if err != nil || info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return codexExecArgs{}, errors.New("exec worktree is not a real directory")
	}
	for _, path := range []string{parsed.schema, parsed.lastMessage} {
		if !filepath.IsAbs(path) || filepath.Clean(path) != path || path == string(filepath.Separator) || len(path) > maxCodexPath {
			return codexExecArgs{}, errors.New("exec output path is unsafe")
		}
	}
	schema, err := os.ReadFile(parsed.schema)
	if err != nil || len(schema) == 0 || len(schema) > 1<<20 || !json.Valid(schema) {
		return codexExecArgs{}, errors.New("exec output schema is invalid")
	}
	return parsed, nil
}

func runCodexExec(argv []string) error {
	parsed, err := parseCodexExec(argv)
	if err != nil {
		return err
	}
	prompt, err := io.ReadAll(io.LimitReader(os.Stdin, maxCodexPrompt+1))
	if err != nil || len(prompt) == 0 || len(prompt) > maxCodexPrompt || bytes.IndexByte(prompt, 0) >= 0 {
		return errors.New("exec prompt is missing or oversized")
	}
	role := codexRole(string(prompt))
	if role == "" {
		return errors.New("unsupported workflow prompt")
	}
	if role == "builder" {
		if err := maybeBlockTakeoverBuilder(parsed.worktree, string(prompt)); err != nil {
			return err
		}
		if err := maybeBlockFirstBuilder(parsed.worktree); err != nil {
			return err
		}
	}
	var verificationFixture []byte
	if role == "verification" {
		verificationFixture, err = writeCodexVerificationFixture(parsed.worktree, string(prompt))
		if err != nil {
			return err
		}
	}
	artifact, err := codexArtifact(role, string(prompt), verificationFixture)
	if err != nil {
		return err
	}
	if role == "builder" {
		if strings.Contains(string(prompt), "SF_E2E_TAKEOVER") {
			if err := validateTakeoverBuilderFile(parsed.worktree); err != nil {
				return err
			}
			if err := completeTakeoverBuilderFile(parsed.worktree); err != nil {
				return err
			}
		} else if err := writeCodexWorktreeFile(parsed.worktree, builderFixtureFile, []byte(`package app

func SoftwareFactoryFixture() string { return "ready" }
`)); err != nil {
			return err
		}
	}
	file, err := os.OpenFile(parsed.lastMessage, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	if _, err := file.Write(artifact); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	usage := `{"type":"turn.completed","usage":{"input_tokens":1,"cached_input_tokens":0,"cache_write_input_tokens":0,"output_tokens":1,"reasoning_output_tokens":0}}` + "\n"
	_, err = io.WriteString(os.Stdout, usage)
	return err
}

func writeCodexVerificationFixture(worktree, prompt string) ([]byte, error) {
	content := []byte(`package app

import "testing"

func TestSoftwareFactoryFixture(t *testing.T) {
	if SoftwareFactoryFixture() != "ready" {
		t.Fatal("fixture implementation is missing")
	}
}
`)
	if strings.Contains(prompt, "SF_E2E_TAKEOVER") {
		path := filepath.Join(worktree, builderFixtureFile)
		if info, err := os.Lstat(path); err == nil {
			if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
				return nil, errors.New("operator takeover implementation is unsafe")
			}
			// The operator supplied the first implementation while the factory was
			// paused. The fresh independent Reviewer must still produce a genuinely
			// red proof before the resumed Builder cycle; re-emitting the retained
			// test would be green and Store correctly refuses that false evidence.
			content = []byte(`package app

import "testing"

func TestSoftwareFactoryFixture(t *testing.T) {
	if SoftwareFactoryFixture() != "ready" {
		t.Fatal("fixture implementation is missing")
	}
}

func TestSoftwareFactoryTakeoverReviewed(t *testing.T) {
	if SoftwareFactoryTakeoverReviewed() != "verified" {
		t.Fatal("fresh takeover review is not implemented")
	}
}
`)
		} else if !errors.Is(err, os.ErrNotExist) {
			return nil, err
		}
	}
	if err := writeCodexWorktreeFile(worktree, verificationFixtureFile, content); err != nil {
		return nil, err
	}
	return append([]byte(nil), content...), nil
}

// maybeBlockTakeoverBuilder gives the compiled operator-take acceptance one
// deterministic process to drain without widening the production supervisor's
// environment allowlist. The token is part of the disposable ticket prompt;
// the second Builder observes the operator-authored planned source file and
// proceeds normally.
func maybeBlockTakeoverBuilder(worktree, prompt string) error {
	if !strings.Contains(prompt, "SF_E2E_TAKEOVER") {
		return nil
	}
	path := filepath.Join(worktree, builderFixtureFile)
	info, err := os.Lstat(path)
	if err == nil {
		if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
			return errors.New("takeover fixture implementation is unsafe")
		}
		return nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	marker := filepath.Join(worktree, takeoverEnteredFile)
	file, err := os.OpenFile(marker, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return fmt.Errorf("create takeover entry marker: %w", err)
	}
	if _, err = io.WriteString(file, "provider-entered\n"); err == nil {
		err = file.Sync()
	}
	if closeErr := file.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return fmt.Errorf("persist takeover entry marker: %w", err)
	}
	// Normal completion is cancellation by the real supervisor. Keep a hard
	// bound so a broken acceptance cannot strand a fixture process forever.
	time.Sleep(2 * time.Minute)
	return errors.New("takeover fixture builder was not drained")
}

func validateTakeoverBuilderFile(worktree string) error {
	path := filepath.Join(worktree, builderFixtureFile)
	info, err := os.Lstat(path)
	if err != nil || info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() || info.Size() <= 0 || info.Size() > 16<<10 {
		return errors.New("operator takeover implementation is unavailable or unsafe")
	}
	data, err := os.ReadFile(path)
	if err != nil || !bytes.Contains(data, []byte(`func SoftwareFactoryFixture() string { return "ready" }`)) || !bytes.Contains(data, []byte("operator takeover preserved")) {
		return errors.New("operator takeover implementation is invalid")
	}
	return nil
}

func completeTakeoverBuilderFile(worktree string) error {
	path := filepath.Join(worktree, builderFixtureFile)
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	const addition = `
// fresh reviewer follow-up
func SoftwareFactoryTakeoverReviewed() string { return "verified" }
`
	if bytes.Contains(data, []byte(`func SoftwareFactoryTakeoverReviewed() string { return "verified" }`)) {
		return nil
	}
	data = append(data, []byte(addition)...)
	return os.WriteFile(path, data, 0o600)
}

func codexRole(prompt string) string {
	switch {
	case strings.HasPrefix(prompt, "You are the Planner for a software ticket."):
		return "planner"
	case strings.HasPrefix(prompt, "You are the independent pre-build Reviewer and verification author."):
		return "verification"
	case strings.HasPrefix(prompt, "You are the implementation Builder."):
		return "builder"
	case strings.HasPrefix(prompt, "You are the fresh, independent final Reviewer."):
		return "final_reviewer"
	default:
		return ""
	}
}

func codexArtifact(role, prompt string, verificationFixture []byte) ([]byte, error) {
	ticket := map[string]any{}
	_ = decodePromptObject(prompt, "TICKET=", &ticket)
	ticketType, _ := ticket["type"].(string)
	proof := proofKind(ticketType)
	switch role {
	case "planner":
		value := phaseartifact.Planner{Schema: "sf.planner/v1", Acceptance: []string{"fixture workflow completes"}, Proof: phaseartifact.ProofPlan{Kind: proof, Command: []string{"go", "test", "./..."}, Details: "fixture proof"}, Paths: []string{verificationFixtureFile, builderFixtureFile}, Commands: [][]string{{"go", "test", "./..."}}, Risks: []string{"fixture output"}, Questions: []phaseartifact.Question{}}
		return json.Marshal(value)
	case "verification":
		if len(verificationFixture) == 0 || len(verificationFixture) > 64<<10 {
			return nil, errors.New("verification fixture evidence is missing or oversized")
		}
		plan := map[string]any{}
		if err := decodePromptObject(prompt, "PLAN=", &plan); err != nil {
			return nil, err
		}
		acceptance, _ := plan["digest"].(string)
		// The evidence digest represents the actual verification source, not the
		// prompt. A source-only takeover deliberately asks the same ticket question
		// again, but the fresh Reviewer must produce a distinct test checkpoint.
		hash := sha256.Sum256(verificationFixture)
		value := phaseartifact.Verification{Schema: "sf.verification/v1", AcceptanceDigest: acceptance, ProofKind: proof, OwnedFiles: []string{verificationFixtureFile}, Command: []string{"go", "test", "./..."}, PrebuildOutcome: verificationOutcome(proof), EvidenceDigest: hex.EncodeToString(hash[:])}
		if proof == phaseartifact.ProofCharacterization {
			value.CharacterizationRef = "fixture-baseline"
		}
		if proof == phaseartifact.ProofValidation {
			value.RollbackCommand = []string{"true"}
		}
		return json.Marshal(value)
	case "builder":
		return json.Marshal(phaseartifact.Builder{Schema: "sf.builder/v1", Summary: "fixture implementation", ChangedFiles: []string{builderFixtureFile}, Commands: [][]string{{"go", "test", "./..."}}})
	case "final_reviewer":
		verification := map[string]any{}
		_ = decodePromptObject(prompt, "VERIFICATION=", &verification)
		candidate := map[string]any{}
		_ = decodePromptObject(prompt, "CANDIDATE=", &candidate)
		head, _ := candidate["head_sha"].(string)
		proofDigest, _ := verification["proof_digest"].(string)
		return json.Marshal(phaseartifact.Reviewer{Schema: "sf.reviewer/v1", Decision: phaseartifact.ReviewPass, Findings: []string{}, ReviewedHead: head, ProofDigest: proofDigest})
	default:
		return nil, errors.New("unsupported workflow role")
	}
}

func proofKind(ticketType string) phaseartifact.ProofKind {
	switch domain.TicketType(ticketType) {
	case domain.TicketBug:
		return phaseartifact.ProofRegression
	case domain.TicketFeature:
		return phaseartifact.ProofAcceptance
	case domain.TicketRefactor:
		return phaseartifact.ProofCharacterization
	case domain.TicketInfrastructure:
		return phaseartifact.ProofValidation
	case domain.TicketDocumentation:
		return phaseartifact.ProofDocumentation
	case domain.TicketSpike:
		return phaseartifact.ProofReport
	default:
		return phaseartifact.ProofRegression
	}
}

func verificationOutcome(proof phaseartifact.ProofKind) string {
	switch proof {
	case phaseartifact.ProofRegression:
		return "red"
	case phaseartifact.ProofAcceptance:
		return "missing"
	case phaseartifact.ProofCharacterization:
		return "baseline"
	case phaseartifact.ProofValidation:
		return "dry_run"
	case phaseartifact.ProofDocumentation:
		return "check_failed"
	default:
		return "report_ready"
	}
}

func decodePromptObject(prompt, marker string, target any) error {
	index := strings.Index(prompt, marker)
	if index < 0 {
		return fmt.Errorf("prompt marker %s is missing", marker)
	}
	line := prompt[index+len(marker):]
	if newline := strings.IndexByte(line, '\n'); newline >= 0 {
		line = line[:newline]
	}
	decoder := json.NewDecoder(strings.NewReader(line))
	if err := decoder.Decode(target); err != nil {
		return fmt.Errorf("decode %s: %w", marker, err)
	}
	return nil
}

func writeCodexWorktreeFile(worktree, name string, content []byte) error {
	path := filepath.Join(worktree, name)
	if filepath.Dir(path) != worktree {
		return errors.New("fixture worktree file escaped root")
	}
	return os.WriteFile(path, content, 0o600)
}

func maybeBlockFirstBuilder(worktree string) error {
	marker := os.Getenv("SF_FAKE_PROVIDER_FIRST_BUILDER_BLOCK")
	if marker == "" {
		marker = os.Getenv("SF_FAKE_PROVIDER_BLOCK_FIRST_BUILDER_MARKER")
	}
	if marker == "" {
		marker = os.Getenv("SF_FAKE_PROVIDER_FIRST_BUILDER_MARKER")
	}
	if marker == "" {
		marker = os.Getenv("SF_FAKE_PROVIDER_BLOCK_FIRST_BUILDER")
	}
	if marker == "" {
		if config := os.Getenv("SF_FAKE_PROVIDER_CONFIG"); config != "" {
			data, err := os.ReadFile(config)
			if err != nil || len(data) > 16<<10 {
				return errors.New("invalid fake-provider config")
			}
			var value struct {
				FirstBuilderBlockMarker string `json:"first_builder_block_marker"`
				FirstBuilderMarker      string `json:"first_builder_marker"`
				BlockFirstBuilder       bool   `json:"block_first_builder"`
			}
			if json.Unmarshal(data, &value) != nil {
				return errors.New("invalid fake-provider config")
			}
			marker = value.FirstBuilderBlockMarker
			if marker == "" {
				marker = value.FirstBuilderMarker
			}
			if marker == "" && value.BlockFirstBuilder {
				marker = filepath.Join(worktree, ".sf-fake-provider-first-builder.blocked")
			}
		}
	}
	if marker == "" {
		return nil
	}
	if marker == "1" {
		marker = filepath.Join(worktree, ".sf-fake-provider-first-builder.blocked")
	}
	if !filepath.IsAbs(marker) || filepath.Clean(marker) != marker || marker == string(filepath.Separator) || len(marker) > maxCodexPath {
		return errors.New("fake-provider block marker must be an absolute clean path")
	}
	file, err := os.OpenFile(marker, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err == nil {
		_ = file.Close()
		duration := 2 * time.Second
		if raw := os.Getenv("SF_FAKE_PROVIDER_FIRST_BUILDER_BLOCK_DURATION"); raw != "" {
			if parsed, parseErr := time.ParseDuration(raw); parseErr == nil && parsed > 0 && parsed <= maximumFixtureDuration {
				duration = parsed
			} else {
				return errors.New("invalid fake-provider block duration")
			}
		}
		time.Sleep(duration)
		return errors.New("first builder fixture was blocked")
	}
	if !errors.Is(err, os.ErrExist) {
		return err
	}
	_ = worktree
	return nil
}
