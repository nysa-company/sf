package config

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"time"

	"github.com/pelletier/go-toml/v2"

	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/goclosure"
)

const MaxFileBytes = 64 * 1024

// ErrCommandDetection identifies a repository whose command contract needs an
// explicit .sf/config.toml. The CLI turns it into a channel-correct next
// action; callers can still distinguish it from malformed explicit config.
var ErrCommandDetection = errors.New("project command detection failed")

type machineDocument struct {
	MaxConcurrentTickets *int    `toml:"max_concurrent_tickets"`
	MaxPhaseTimeout      *string `toml:"max_phase_timeout"`
	MaxTicketTimeout     *string `toml:"max_ticket_timeout"`
	MaxTicketCostUSD     *int64  `toml:"max_ticket_cost_usd"`
	AllowAutonomous      *bool   `toml:"allow_autonomous"`
}

type projectDocument struct {
	BaseBranch           *string `toml:"base_branch"`
	MergeMode            *string `toml:"merge_mode"`
	MergeMethod          *string `toml:"merge_method"`
	MaxConcurrentTickets *int    `toml:"max_concurrent_tickets"`
	PhaseTimeout         *string `toml:"phase_timeout"`
	TicketTimeout        *string `toml:"ticket_timeout"`
	MaxTicketCostUSD     *int64  `toml:"max_ticket_cost_usd"`
	Commands             *struct {
		Verify []string `toml:"verify"`
		Review []string `toml:"review"`
	} `toml:"commands"`
	Providers *struct {
		Planner  []string `toml:"planner"`
		Builder  []string `toml:"builder"`
		Reviewer []string `toml:"reviewer"`
	} `toml:"providers"`
}

// LoadMachine reads an optional owner-local channel policy. A missing file is
// the conservative built-in policy; malformed or unknown fields fail closed.
func LoadMachine(path string) (MachineLimits, error) {
	limits := DefaultMachineLimits()
	data, exists, err := readOptionalConfig(path)
	if err != nil || !exists {
		return limits, err
	}
	var document machineDocument
	if err := decodeStrict(data, &document); err != nil {
		return MachineLimits{}, fmt.Errorf("parse machine configuration: %w", err)
	}
	if document.MaxConcurrentTickets != nil {
		limits.MaxConcurrentTickets = *document.MaxConcurrentTickets
	}
	if document.MaxPhaseTimeout != nil {
		limits.MaxPhaseTimeout, err = parseDuration("max_phase_timeout", *document.MaxPhaseTimeout)
		if err != nil {
			return MachineLimits{}, err
		}
	}
	if document.MaxTicketTimeout != nil {
		limits.MaxTicketTimeout, err = parseDuration("max_ticket_timeout", *document.MaxTicketTimeout)
		if err != nil {
			return MachineLimits{}, err
		}
	}
	if document.MaxTicketCostUSD != nil {
		limits.MaxTicketCostMicroUSD, err = dollarsToMicroUSD("max_ticket_cost_usd", *document.MaxTicketCostUSD)
		if err != nil {
			return MachineLimits{}, err
		}
	}
	if document.AllowAutonomous != nil {
		limits.AllowAutonomous = *document.AllowAutonomous
	}
	if err := validateMachine(limits); err != nil {
		return MachineLimits{}, err
	}
	return limits, nil
}

// LoadProject reads the optional committed .sf/config.toml and resolves it
// under machine policy. It returns canonical JSON bytes suitable for durable
// registration; raw TOML never becomes runtime authority.
func LoadProject(repository, name string, machine MachineLimits) (Effective, []byte, string, error) {
	project := DefaultProject(name, repository)
	// Built-in defaults are conveniences, not a second policy layer. When the
	// owner-local machine policy is stricter, an otherwise empty repository
	// configuration inherits those caps rather than failing initialization.
	project.MaxConcurrentTickets = min(project.MaxConcurrentTickets, machine.MaxConcurrentTickets)
	project.PhaseTimeout = min(project.PhaseTimeout, machine.MaxPhaseTimeout)
	project.TicketTimeout = min(project.TicketTimeout, machine.MaxTicketTimeout)
	project.PhaseTimeout = min(project.PhaseTimeout, project.TicketTimeout)
	project.MaxTicketCostMicroUSD = min(project.MaxTicketCostMicroUSD, machine.MaxTicketCostMicroUSD)
	path := filepath.Join(repository, ".sf", "config.toml")
	data, exists, err := readOptionalConfig(path)
	if err != nil {
		return Effective{}, nil, "", err
	}
	var document projectDocument
	if exists {
		if err := decodeStrict(data, &document); err != nil {
			return Effective{}, nil, "", fmt.Errorf("parse project configuration: %w", err)
		}
		if document.Commands == nil || document.Commands.Verify == nil || document.Commands.Review == nil {
			detected, detectErr := detectRepositoryCommands(repository)
			if detectErr != nil {
				return Effective{}, nil, "", detectErr
			}
			if document.Commands == nil || document.Commands.Verify == nil {
				project.Commands.Verify = detected.Verify
			}
			if document.Commands == nil || document.Commands.Review == nil {
				project.Commands.Review = detected.Review
			}
		}
		if document.BaseBranch != nil {
			project.BaseBranch = *document.BaseBranch
		}
		if document.MergeMode != nil {
			project.MergeMode = domain.MergeMode(*document.MergeMode)
		}
		if document.MergeMethod != nil {
			project.MergeMethod = *document.MergeMethod
		}
		if document.MaxConcurrentTickets != nil {
			project.MaxConcurrentTickets = *document.MaxConcurrentTickets
		}
		if document.PhaseTimeout != nil {
			project.PhaseTimeout, err = parseDuration("phase_timeout", *document.PhaseTimeout)
			if err != nil {
				return Effective{}, nil, "", err
			}
		}
		if document.TicketTimeout != nil {
			project.TicketTimeout, err = parseDuration("ticket_timeout", *document.TicketTimeout)
			if err != nil {
				return Effective{}, nil, "", err
			}
		}
		if document.MaxTicketCostUSD != nil {
			project.MaxTicketCostMicroUSD, err = dollarsToMicroUSD("max_ticket_cost_usd", *document.MaxTicketCostUSD)
			if err != nil {
				return Effective{}, nil, "", err
			}
		}
		if document.Commands != nil {
			if document.Commands.Verify != nil {
				project.Commands.Verify = Command{Argv: append([]string(nil), document.Commands.Verify...)}
			}
			if document.Commands.Review != nil {
				project.Commands.Review = Command{Argv: append([]string(nil), document.Commands.Review...)}
			}
		}
		if document.Providers != nil {
			if document.Providers.Planner != nil {
				project.Providers.Planner = append([]string(nil), document.Providers.Planner...)
			}
			if document.Providers.Builder != nil {
				project.Providers.Builder = append([]string(nil), document.Providers.Builder...)
			}
			if document.Providers.Reviewer != nil {
				project.Providers.Reviewer = append([]string(nil), document.Providers.Reviewer...)
			}
		}
	} else {
		detected, detectErr := detectRepositoryCommands(repository)
		if detectErr != nil {
			return Effective{}, nil, "", detectErr
		}
		project.Commands = detected
	}
	effective, err := Resolve(machine, project, TicketOverride{})
	if err != nil {
		return Effective{}, nil, "", err
	}
	snapshot, digest, err := Snapshot(effective)
	return effective, snapshot, digest, err
}

// detectRepositoryCommands selects only a small, typed walking-skeleton
// default. It intentionally refuses to guess for repositories whose build
// contract is not obvious; an explicit .sf/config.toml can opt into another
// argv-only command without adding shell interpretation.
func detectRepositoryCommands(repository string) (Commands, error) {
	goMod, err := regularRepositoryFile(filepath.Join(repository, "go.mod"), "go.mod")
	if err != nil {
		return Commands{}, err
	}
	packageJSON, err := regularRepositoryFile(filepath.Join(repository, "package.json"), "package.json")
	if err != nil {
		return Commands{}, err
	}
	if goMod && packageJSON {
		return Commands{}, detectionError("repository contains both go.mod and package.json; add explicit commands to .sf/config.toml")
	}
	if goMod {
		if _, err := goclosure.Validate(repository); err != nil {
			if errors.Is(err, goclosure.ErrUnvendored) {
				return Commands{}, detectionError("Go dependencies require checked-in vendor/modules.txt for local v1 verification; use operator or credential-free CI takeover")
			}
			return Commands{}, detectionError("go.mod is not a bounded dependency-free or vendored local verification closure; use operator or credential-free CI takeover")
		}
		command := Command{Argv: []string{"go", "test", "./..."}}
		return Commands{Verify: command, Review: command}, nil
	}
	if packageJSON {
		return Commands{}, detectionError("package.json/npm is not a locally executable v1 repository-command recipe; use operator or credential-free CI takeover instead of local commands")
	}
	return Commands{}, detectionError("repository type is unsupported; add explicit commands to .sf/config.toml")
}

func regularRepositoryFile(path, name string) (bool, error) {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, detectionError("inspect " + name + " failed; add explicit commands to .sf/config.toml")
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return false, detectionError(name + " must be a regular non-symlink file; add explicit commands to .sf/config.toml")
	}
	if !info.Mode().IsRegular() {
		return false, detectionError(name + " must be a regular file; add explicit commands to .sf/config.toml")
	}
	return true, nil
}

func readBoundedRepositoryFile(path string, limit int64) ([]byte, error) {
	before, err := os.Lstat(path)
	if err != nil || before.Mode()&os.ModeSymlink != 0 || !before.Mode().IsRegular() {
		return nil, errors.New("repository marker is not a regular non-symlink file")
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	opened, err := file.Stat()
	if err != nil || !opened.Mode().IsRegular() || !os.SameFile(before, opened) {
		return nil, errors.New("repository marker changed while opening")
	}
	data, err := io.ReadAll(io.LimitReader(file, limit+1))
	if err != nil || int64(len(data)) > limit {
		return nil, errors.New("repository file exceeds bound")
	}
	return data, nil
}

func detectionError(message string) error {
	return fmt.Errorf("%w: %s", ErrCommandDetection, message)
}

func readOptionalConfig(path string) ([]byte, bool, error) {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("inspect configuration: %w", err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return nil, false, fmt.Errorf("configuration must be a regular non-symlink file")
	}
	parent, err := os.Lstat(filepath.Dir(path))
	if err != nil || parent.Mode()&os.ModeSymlink != 0 || !parent.IsDir() {
		return nil, false, fmt.Errorf("configuration parent must be a real directory")
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, false, fmt.Errorf("open configuration: %w", err)
	}
	defer file.Close()
	opened, err := file.Stat()
	if err != nil || !opened.Mode().IsRegular() || !os.SameFile(info, opened) {
		return nil, false, fmt.Errorf("configuration identity changed while opening")
	}
	reader := io.LimitReader(file, MaxFileBytes+1)
	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, false, fmt.Errorf("read configuration: %w", err)
	}
	if len(data) > MaxFileBytes {
		return nil, false, fmt.Errorf("configuration exceeds %d bytes", MaxFileBytes)
	}
	return data, true, nil
}

func decodeStrict(data []byte, target any) error {
	decoder := toml.NewDecoder(bytes.NewReader(data)).DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	return nil
}

func parseDuration(name, value string) (time.Duration, error) {
	duration, err := time.ParseDuration(value)
	if err != nil || duration <= 0 {
		return 0, fmt.Errorf("%s must be a positive Go duration", name)
	}
	return duration, nil
}

func dollarsToMicroUSD(name string, dollars int64) (int64, error) {
	if dollars <= 0 || dollars > math.MaxInt64/1_000_000 {
		return 0, fmt.Errorf("%s must be a positive whole-dollar value", name)
	}
	return dollars * 1_000_000, nil
}
