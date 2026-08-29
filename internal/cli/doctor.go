package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"

	"github.com/nysa-company/sf/internal/api"
	"github.com/nysa-company/sf/internal/config"
	"github.com/nysa-company/sf/internal/domain"
)

const doctorSchema = "sf.doctor/v1"

type CheckStatus string

const (
	CheckPass   CheckStatus = "pass"
	CheckFail   CheckStatus = "fail"
	CheckNotRun CheckStatus = "not_run"
	minimumFree             = uint64(100 * 1024 * 1024)
)

// DoctorCheck is deliberately a small, typed and sanitized diagnostic record.
// It contains no command output, credentials, or filesystem paths.
type DoctorCheck struct {
	ID         string             `json:"id"`
	Status     CheckStatus        `json:"status"`
	Summary    string             `json:"summary"`
	NextAction *domain.NextAction `json:"next_action,omitempty"`
}

type DoctorReport struct {
	Schema             string         `json:"schema"`
	Channel            domain.Channel `json:"channel"`
	Checks             []DoctorCheck  `json:"checks"`
	AutonomousEligible bool           `json:"autonomous_eligible"`
}

// DoctorDeps is the injected, read-only registry used by Doctor. Tests can
// replace every host probe without starting a daemon or invoking a provider.
type DoctorDeps struct {
	Channel    domain.Channel
	Paths      config.ChannelPaths
	Repo       string
	Binary     string
	Lookup     func(string) (string, error)
	Lstat      func(string) (os.FileInfo, error)
	StatFS     func(string) (*syscall.Statfs_t, error)
	CurrentUID func() uint32
	Worktree   func(context.Context, string) error
}

func (deps DoctorDeps) defaults() DoctorDeps {
	if deps.Binary == "" {
		deps.Binary = binaryName()
	}
	if deps.Lookup == nil {
		deps.Lookup = exec.LookPath
	}
	if deps.Lstat == nil {
		deps.Lstat = os.Lstat
	}
	if deps.StatFS == nil {
		deps.StatFS = func(path string) (*syscall.Statfs_t, error) {
			var stat syscall.Statfs_t
			if err := syscall.Statfs(path, &stat); err != nil {
				return nil, err
			}
			return &stat, nil
		}
	}
	if deps.CurrentUID == nil {
		deps.CurrentUID = func() uint32 { return uint32(os.Geteuid()) }
	}
	if deps.Worktree == nil {
		deps.Worktree = func(ctx context.Context, repo string) error {
			command := exec.CommandContext(ctx, "git", "-C", repo, "rev-parse", "--is-inside-work-tree")
			output, err := command.Output()
			if err != nil {
				return err
			}
			if string(output) != "true\n" {
				return errors.New("repository is not a worktree")
			}
			return nil
		}
	}
	if deps.Paths.Root == "" && deps.Channel.Valid() {
		if home, err := os.UserHomeDir(); err == nil {
			deps.Paths, _ = config.PathsFor(home, deps.Channel)
		}
	}
	return deps
}

// RunDoctor performs only local read-only probes. It never contacts a daemon,
// provider, gh authentication endpoint, or container runtime.
func RunDoctor(ctx context.Context, deps DoctorDeps) DoctorReport {
	deps = deps.defaults()
	report := DoctorReport{Schema: doctorSchema, Channel: deps.Channel, AutonomousEligible: false}
	if !deps.Channel.Valid() {
		report.Checks = append(report.Checks, failedCheck("channel", "channel is invalid", deps.Binary, "doctor"))
		return report
	}

	report.Checks = append(report.Checks, checkRoot(deps))
	report.Checks = append(report.Checks, checkSocket(deps))
	report.Checks = append(report.Checks, checkDisk(deps))
	report.Checks = append(report.Checks, checkExecutable(deps, "git", "Git executable is available"))
	if deps.Repo == "" {
		report.Checks = append(report.Checks, DoctorCheck{ID: "repository_worktree", Status: CheckNotRun, Summary: "optional repository was not selected"})
	} else if !filepath.IsAbs(deps.Repo) {
		report.Checks = append(report.Checks, failedCheck("repository_worktree", "repository path must be absolute", deps.Binary, "doctor"))
	} else if err := deps.Worktree(ctx, deps.Repo); err != nil {
		report.Checks = append(report.Checks, failedCheck("repository_worktree", "selected repository is not a Git worktree", deps.Binary, "doctor"))
	} else {
		report.Checks = append(report.Checks, DoctorCheck{ID: "repository_worktree", Status: CheckPass, Summary: "selected repository is a Git worktree"})
	}
	report.Checks = append(report.Checks, checkExecutable(deps, "gh", "gh executable is available"))
	report.Checks = append(report.Checks, DoctorCheck{ID: "container_runtime", Status: CheckNotRun, Summary: "Docker and Colima are not required"})
	report.Checks = append(report.Checks, DoctorCheck{ID: "autonomous_mode", Status: CheckPass, Summary: "autonomous mode is disabled by policy"})
	return report
}

func reportResponse(report DoctorReport) api.Response {
	data, err := json.Marshal(report)
	if err != nil {
		return failure("internal_error", "could not encode doctor report", []string{binaryName(), "--help"})
	}
	response := api.Response{Version: api.Version, RequestID: requestID(), OK: true, Mutation: api.Mutation{}, Data: data}
	var failures int
	for _, check := range report.Checks {
		if check.Status == CheckFail {
			failures++
			if response.NextAction == nil {
				response.NextAction = check.NextAction
			}
		}
	}
	if failures > 0 {
		response.OK = false
		response.Error = &api.Error{Code: "doctor_failed", Message: fmt.Sprintf("doctor found %d failing check(s); no mutations were run", failures)}
	}
	return response
}

func checkRoot(deps DoctorDeps) DoctorCheck {
	info, err := deps.Lstat(deps.Paths.Root)
	if err != nil {
		return failedCheck("channel_root", "channel root is missing", deps.Binary, "doctor")
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return failedCheck("channel_root", "channel root is not a directory", deps.Binary, "doctor")
	}
	if info.Mode().Perm() != 0o700 {
		return failedCheck("channel_root", "channel root mode is not owner-only", deps.Binary, "doctor")
	}
	if owner, ok := fileOwner(info); !ok || owner != deps.CurrentUID() {
		return failedCheck("channel_root", "channel root is not owned by the current user", deps.Binary, "doctor")
	}
	return DoctorCheck{ID: "channel_root", Status: CheckPass, Summary: "channel root has owner-only mode and current-user ownership"}
}

func checkSocket(deps DoctorDeps) DoctorCheck {
	info, err := deps.Lstat(deps.Paths.Socket)
	if os.IsNotExist(err) {
		return DoctorCheck{ID: "daemon_socket", Status: CheckNotRun, Summary: "daemon socket is not present"}
	}
	if err != nil || info.Mode()&os.ModeSymlink != 0 || info.Mode()&os.ModeSocket == 0 {
		return failedCheck("daemon_socket", "daemon socket is invalid", deps.Binary, "daemon", "run")
	}
	if info.Mode().Perm() != 0o600 {
		return failedCheck("daemon_socket", "daemon socket mode is not owner-only", deps.Binary, "daemon", "run")
	}
	if owner, ok := fileOwner(info); !ok || owner != deps.CurrentUID() {
		return failedCheck("daemon_socket", "daemon socket is not owned by the current user", deps.Binary, "daemon", "run")
	}
	return DoctorCheck{ID: "daemon_socket", Status: CheckPass, Summary: "daemon socket has owner-only mode and current-user ownership"}
}

func checkDisk(deps DoctorDeps) DoctorCheck {
	path := deps.Paths.Root
	if path == "" {
		path = "."
	}
	var stat *syscall.Statfs_t
	var err error
	for {
		stat, err = deps.StatFS(path)
		if err == nil {
			break
		}
		parent := filepath.Dir(path)
		if parent == path {
			break
		}
		path = parent
	}
	if err != nil || stat == nil {
		return failedCheck("disk_space", "disk space could not be checked", deps.Binary, "doctor")
	}
	free := uint64(stat.Bavail) * uint64(stat.Bsize)
	if free < minimumFree {
		return failedCheck("disk_space", "available disk space is below the safety threshold", deps.Binary, "doctor")
	}
	return DoctorCheck{ID: "disk_space", Status: CheckPass, Summary: "available disk space meets the safety threshold"}
}

func checkExecutable(deps DoctorDeps, name, summary string) DoctorCheck {
	if _, err := deps.Lookup(name); err != nil {
		return failedCheck(name+"_executable", name+" executable is unavailable", deps.Binary, "doctor")
	}
	return DoctorCheck{ID: name + "_executable", Status: CheckPass, Summary: summary}
}

func failedCheck(id, summary, binary string, argv ...string) DoctorCheck {
	return DoctorCheck{ID: id, Status: CheckFail, Summary: summary, NextAction: &domain.NextAction{Code: id, Argv: append([]string{binary}, argv...)}}
}

func fileOwner(info os.FileInfo) (uint32, bool) {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return 0, false
	}
	return uint32(stat.Uid), true
}
