package daemon

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"unicode"

	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/redact"
	"github.com/nysa-company/sf/internal/store"
)

func validArtifactSection(section string) bool {
	switch section {
	case "all", "source", "plan", "acceptance", "proof", "changes", "pr":
		return true
	}
	return false
}

// These are historical read projections. Neither a stored provider plan nor
// an earlier passing proof grants authority to the current candidate.
func (daemon *Daemon) ticketArtifacts(ctx context.Context, ticket store.Ticket, section string) (map[string]any, error) {
	result := map[string]any{}
	policy := daemon.projector.Policy
	add := func(name, provenance string, value any, limit int) {
		text, ok := value.(string)
		if !ok {
			encoded, _ := json.MarshalIndent(value, "", "  ")
			text = string(encoded)
		}
		text, truncated := safeArtifactText(text, policy, limit)
		result[name] = map[string]any{"available": true, "provenance": provenance, "text": provenance + "\n" + text, "truncated": truncated}
	}
	wants := func(name string) bool { return section == "all" || section == name }
	unavailable := func(name, reason string) {
		result[name] = map[string]any{"available": false, "text": "Unavailable: " + reason}
	}
	if wants("source") {
		add("source", "Submitted ticket source (operator-authored; untrusted content).", string(ticket.Source), 32768)
	}
	if wants("acceptance") {
		add("acceptance", "Submitted acceptance criteria; not a claim that they passed.", ticket.Acceptance, 16384)
	}
	if wants("plan") {
		plan, err := daemon.store.Plan(ctx, ticket.Ref)
		if err == nil {
			var content any = map[string]any{"acceptance": plan.Document.Acceptance, "proof_kind": plan.Document.ProofKind, "paths": plan.Document.Paths, "commands": plan.Document.Commands, "risks": plan.Document.Risks}
			if plan.Document.Planner != nil {
				content = plan.Document.Planner
			}
			add("plan", fmt.Sprintf("Stored provider-reported plan; ticket version %d; digest %s. Not execution evidence.", plan.TicketVersion, plan.Digest), content, 32768)
		} else if errors.Is(err, store.ErrNotFound) {
			unavailable("plan", "no authenticated plan is recorded")
		} else {
			return nil, err
		}
	}
	if wants("proof") {
		proofs := []map[string]any{}
		appendProof := func(label string, binding store.RepositoryCommandResultBinding) error {
			observed, err := daemon.store.LoadRepositoryCommandResult(ctx, binding.Key)
			if err != nil {
				return err
			}
			proofs = append(proofs, map[string]any{"checkpoint": label, "ticket_version": binding.TicketVersion, "expected_outcome": binding.ExpectedOutcome, "exit_code": observed.Result.ExitCode, "command_digest": binding.CommandDigest, "observed_at": timeView(observed.CreatedAt)})
			return nil
		}
		verification, err := daemon.store.HistoricalVerification(ctx, ticket.Ref)
		if err == nil {
			if err := appendProof("prebuild verification", verification.CommandBinding); err != nil {
				return nil, err
			}
		} else if !errors.Is(err, store.ErrNotFound) {
			return nil, err
		}
		candidate, err := daemon.store.HistoricalCandidate(ctx, ticket.Ref)
		if err == nil {
			if err := appendProof("postbuild candidate", candidate.CommandBinding); err != nil {
				return nil, err
			}
		} else if !errors.Is(err, store.ErrNotFound) {
			return nil, err
		}
		if len(proofs) == 0 {
			unavailable("proof", "no authenticated SF repository-command proof is recorded")
		} else {
			add("proof", "SF-observed historical command results. An expected prebuild failure is not a passing postbuild proof; these records do not establish current merge authority.", proofs, 16384)
		}
	}
	if wants("changes") {
		candidate, err := daemon.store.HistoricalCandidate(ctx, ticket.Ref)
		if err == nil {
			add("changes", "Historical candidate identity. Changed-file inventory and diff are unavailable in this projection.", map[string]any{"base_sha": candidate.Snapshot.BaseSHA, "head_sha": candidate.Snapshot.HeadSHA, "tree_sha": candidate.Snapshot.TreeSHA, "ticket_version": candidate.TicketVersion}, 4096)
		} else if errors.Is(err, store.ErrNotFound) {
			unavailable("changes", "no authenticated candidate is recorded; no diff was read")
		} else {
			return nil, err
		}
	}
	if wants("pr") {
		publication, err := daemon.store.LoadHistoricalPublishedCandidate(ctx, ticket.Ref)
		if err == nil {
			pr := publication.PullRequest
			add("pr", "SF-observed historical publication; current GitHub state was not refreshed.", map[string]any{"url": fmt.Sprintf("https://%s/%s/%s/pull/%d", pr.Repository.Host, pr.Repository.Owner, pr.Repository.Name, pr.Number), "head_sha": pr.HeadOID, "state": publication.PullRequestState, "draft": publication.PullRequestDraft, "observed_at": timeView(publication.PullRequestObservedAt)}, 4096)
		} else if errors.Is(err, store.ErrNotFound) {
			unavailable("pr", "no authenticated published pull request is recorded")
		} else {
			return nil, err
		}
	}
	return result, nil
}

func safeArtifactText(value string, policy redact.Policy, limit int) (string, bool) {
	// Redact complete input before either control filtering or truncation.
	value = policy.String(value)
	value = strings.Map(func(r rune) rune {
		if r != '\n' && r != '\t' && (unicode.IsControl(r) || unicode.Is(unicode.Cf, r)) {
			return ' '
		}
		return r
	}, value)
	runes := []rune(policy.String(value))
	if len(runes) > limit {
		return string(runes[:limit]), true
	}
	return string(runes), false
}

func sanitizeArtifactObject(value map[string]any, policy redact.Policy) map[string]any {
	// Only this known, typed, top-level daemon action carries executable argv.
	// A nested object named next_action is still untrusted artifact data.
	action, hasAction := value["next_action"].(domain.NextAction)
	data, _ := json.Marshal(value)
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	var object map[string]any
	if decoder.Decode(&object) != nil {
		return map[string]any{}
	}
	var visit func(any) any
	visit = func(value any) any {
		switch item := value.(type) {
		case string:
			text, _ := safeArtifactText(item, policy, 65536)
			return text
		case []any:
			for index := range item {
				item[index] = visit(item[index])
			}
		case map[string]any:
			for key := range item {
				item[key] = visit(item[key])
			}
		}
		return value
	}
	result := visit(object).(map[string]any)
	if hasAction {
		result["next_action"] = action
	}
	return result
}
