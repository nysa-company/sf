package config

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/nysa-company/sf/internal/domain"
)

type Command struct {
	Argv []string `json:"argv"`
}

func (c Command) Validate(name string) error {
	if len(c.Argv) == 0 || strings.TrimSpace(c.Argv[0]) == "" {
		return fmt.Errorf("command %s requires a non-empty argv", name)
	}
	for _, arg := range c.Argv {
		if strings.ContainsRune(arg, '\x00') {
			return fmt.Errorf("command %s contains a NUL byte", name)
		}
	}
	return nil
}

type Commands struct {
	Verify Command `json:"verify"`
	Review Command `json:"review"`
}

type ProviderOrder struct {
	Planner  []string `json:"planner"`
	Builder  []string `json:"builder"`
	Reviewer []string `json:"reviewer"`
}

type MachineLimits struct {
	MaxConcurrentTickets  int           `json:"max_concurrent_tickets"`
	MaxPhaseTimeout       time.Duration `json:"max_phase_timeout"`
	MaxTicketTimeout      time.Duration `json:"max_ticket_timeout"`
	MaxTicketCostMicroUSD int64         `json:"max_ticket_cost_micro_usd"`
	AllowAutonomous       bool          `json:"allow_autonomous"`
}

func DefaultMachineLimits() MachineLimits {
	return MachineLimits{
		MaxConcurrentTickets:  2,
		MaxPhaseTimeout:       45 * time.Minute,
		MaxTicketTimeout:      4 * time.Hour,
		MaxTicketCostMicroUSD: 100_000_000,
		AllowAutonomous:       false,
	}
}

type Project struct {
	Name                  string           `json:"name"`
	Repository            string           `json:"repository"`
	BaseBranch            string           `json:"base_branch"`
	MergeMode             domain.MergeMode `json:"merge_mode"`
	MergeMethod           string           `json:"merge_method"`
	MaxConcurrentTickets  int              `json:"max_concurrent_tickets"`
	PhaseTimeout          time.Duration    `json:"phase_timeout"`
	TicketTimeout         time.Duration    `json:"ticket_timeout"`
	MaxTicketCostMicroUSD int64            `json:"max_ticket_cost_micro_usd"`
	Commands              Commands         `json:"commands"`
	Providers             ProviderOrder    `json:"providers"`
}

type TicketOverride struct {
	MergeMode       domain.MergeMode `json:"merge_mode,omitempty"`
	PhaseTimeout    time.Duration    `json:"phase_timeout,omitempty"`
	TicketTimeout   time.Duration    `json:"ticket_timeout,omitempty"`
	MaxCostMicroUSD int64            `json:"max_cost_micro_usd,omitempty"`
}

type Effective struct {
	Project
	Machine MachineLimits `json:"machine"`
}

func Resolve(machine MachineLimits, project Project, ticket TicketOverride) (Effective, error) {
	if err := validateMachine(machine); err != nil {
		return Effective{}, err
	}
	if project.MaxTicketCostMicroUSD == 0 {
		project.MaxTicketCostMicroUSD = machine.MaxTicketCostMicroUSD
	}
	if err := validateProject(machine, project); err != nil {
		return Effective{}, err
	}
	resolved := project
	if ticket.MergeMode != "" {
		if !ticket.MergeMode.Valid() {
			return Effective{}, fmt.Errorf("invalid ticket merge mode %q", ticket.MergeMode)
		}
		if mergeRank(ticket.MergeMode) > mergeRank(project.MergeMode) {
			return Effective{}, fmt.Errorf("ticket merge mode %q exceeds project maximum %q", ticket.MergeMode, project.MergeMode)
		}
		resolved.MergeMode = ticket.MergeMode
	}
	if ticket.PhaseTimeout > 0 {
		if ticket.PhaseTimeout > project.PhaseTimeout {
			return Effective{}, fmt.Errorf("ticket phase timeout exceeds project maximum")
		}
		resolved.PhaseTimeout = ticket.PhaseTimeout
	}
	if ticket.TicketTimeout > 0 {
		if ticket.TicketTimeout > project.TicketTimeout {
			return Effective{}, fmt.Errorf("ticket timeout exceeds project maximum")
		}
		resolved.TicketTimeout = ticket.TicketTimeout
	}
	if ticket.MaxCostMicroUSD > 0 {
		if ticket.MaxCostMicroUSD > project.MaxTicketCostMicroUSD {
			return Effective{}, fmt.Errorf("ticket cost ceiling exceeds project maximum")
		}
		resolved.MaxTicketCostMicroUSD = ticket.MaxCostMicroUSD
	}
	if ticket.MaxCostMicroUSD < 0 {
		return Effective{}, fmt.Errorf("ticket cost ceiling cannot be negative")
	}
	return Effective{Project: resolved, Machine: machine}, nil
}

func Snapshot(value Effective) ([]byte, string, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return nil, "", fmt.Errorf("marshal configuration snapshot: %w", err)
	}
	digest := sha256.Sum256(data)
	return data, hex.EncodeToString(digest[:]), nil
}

func validateMachine(machine MachineLimits) error {
	if machine.MaxConcurrentTickets < 1 {
		return fmt.Errorf("machine concurrency must be at least one")
	}
	if machine.MaxPhaseTimeout <= 0 || machine.MaxTicketTimeout <= 0 {
		return fmt.Errorf("machine timeouts must be positive")
	}
	if machine.MaxPhaseTimeout > machine.MaxTicketTimeout {
		return fmt.Errorf("machine phase timeout exceeds ticket timeout")
	}
	if machine.MaxTicketCostMicroUSD <= 0 {
		return fmt.Errorf("machine cost ceiling must be positive")
	}
	return nil
}

func validateProject(machine MachineLimits, project Project) error {
	if strings.TrimSpace(project.Name) == "" || strings.TrimSpace(project.Repository) == "" {
		return fmt.Errorf("project name and repository are required")
	}
	if strings.TrimSpace(project.BaseBranch) == "" {
		return fmt.Errorf("project base branch is required")
	}
	if !project.MergeMode.Valid() {
		return fmt.Errorf("invalid project merge mode %q", project.MergeMode)
	}
	if project.MergeMode == domain.MergeAutonomous && !machine.AllowAutonomous {
		return fmt.Errorf("machine policy disables autonomous mode")
	}
	switch project.MergeMethod {
	case "merge", "squash", "rebase":
	default:
		return fmt.Errorf("invalid merge method %q", project.MergeMethod)
	}
	if project.MaxConcurrentTickets < 1 || project.MaxConcurrentTickets > machine.MaxConcurrentTickets {
		return fmt.Errorf("project concurrency exceeds machine bounds")
	}
	if project.PhaseTimeout <= 0 || project.PhaseTimeout > machine.MaxPhaseTimeout {
		return fmt.Errorf("project phase timeout exceeds machine bounds")
	}
	if project.MaxTicketCostMicroUSD <= 0 || project.MaxTicketCostMicroUSD > machine.MaxTicketCostMicroUSD {
		return fmt.Errorf("project cost ceiling exceeds machine bounds")
	}
	if project.TicketTimeout <= 0 || project.TicketTimeout > machine.MaxTicketTimeout || project.PhaseTimeout > project.TicketTimeout {
		return fmt.Errorf("project ticket timeout exceeds machine bounds")
	}
	if err := project.Commands.Verify.Validate("verify"); err != nil {
		return err
	}
	if err := project.Commands.Review.Validate("review"); err != nil {
		return err
	}
	if len(project.Providers.Planner) == 0 || len(project.Providers.Builder) == 0 || len(project.Providers.Reviewer) == 0 {
		return fmt.Errorf("planner, builder, and reviewer provider orders are required")
	}
	return nil
}

func mergeRank(mode domain.MergeMode) int {
	switch mode {
	case domain.MergeManual:
		return 0
	case domain.MergeGuarded:
		return 1
	case domain.MergeAutonomous:
		return 2
	default:
		return 99
	}
}
