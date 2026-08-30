package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/nysa-company/sf/internal/api"
	localauth "github.com/nysa-company/sf/internal/auth"
	"github.com/nysa-company/sf/internal/config"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/redact"
	"github.com/nysa-company/sf/internal/store"
	"github.com/nysa-company/sf/internal/transport"
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
	Schema             string              `json:"schema"`
	Channel            domain.Channel      `json:"channel"`
	Checks             []DoctorCheck       `json:"checks"`
	Authentication     []authStatusView    `json:"authentication"`
	ProviderPair       *DoctorProviderPair `json:"provider_pair,omitempty"`
	GuardedEligible    bool                `json:"guarded_eligible"`
	AutonomousEligible bool                `json:"autonomous_eligible"`
	CredentialsStored  bool                `json:"credentials_stored_by_sf"`
}

type DoctorProviderPair struct {
	Builder     DoctorProviderQualification `json:"builder"`
	Reviewer    DoctorProviderQualification `json:"reviewer"`
	Independent bool                        `json:"independent"`
}

type DoctorProviderQualification struct {
	Role          string                     `json:"role"`
	Provider      string                     `json:"provider"`
	Model         string                     `json:"model"`
	Family        string                     `json:"family"`
	Version       string                     `json:"version"`
	AuthMode      string                     `json:"auth_mode,omitempty"`
	Qualification store.QualificationProfile `json:"qualification"`
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
	AuthStatus func(context.Context) []localauth.Status
	Pair       func(context.Context, domain.Channel) (store.ProviderPair, error)
	Attempts   func(context.Context, domain.Channel) ([]store.ProviderAttempt, error)
	// DaemonStatus is an optional read-only protocol handshake. It is called
	// only when the socket passed the filesystem checks, so a fresh install
	// remains usable without a running daemon.
	DaemonStatus func(context.Context, config.ChannelPaths) error
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

func productionDoctorDeps(channel domain.Channel, repo string) DoctorDeps {
	deps := (DoctorDeps{Channel: channel, Repo: repo}).defaults()
	manager := localauth.NewManager()
	deps.AuthStatus = manager.StatusAll
	databasePath := deps.Paths.Database
	deps.Pair = func(ctx context.Context, selected domain.Channel) (store.ProviderPair, error) {
		database, err := store.OpenReadOnly(ctx, databasePath)
		if err != nil {
			return store.ProviderPair{}, err
		}
		defer database.Close()
		return database.ProviderPair(ctx, selected)
	}
	deps.Attempts = func(ctx context.Context, selected domain.Channel) ([]store.ProviderAttempt, error) {
		database, err := store.OpenReadOnly(ctx, databasePath)
		if err != nil {
			return nil, err
		}
		defer database.Close()
		return database.ActiveProviderAttempts(ctx, selected)
	}
	deps.DaemonStatus = func(ctx context.Context, paths config.ChannelPaths) error {
		probeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		response, err := transport.Call(probeCtx, paths.Socket, api.Request{
			Version: api.Version, RequestID: requestID(), Method: "daemon.status", Parameters: json.RawMessage(`{}`),
		})
		if err != nil {
			return err
		}
		if !response.OK || response.Version != api.Version {
			return errors.New("daemon did not complete a compatible status handshake")
		}
		var value struct {
			Channel domain.Channel `json:"channel"`
		}
		if err := json.Unmarshal(response.Data, &value); err != nil || value.Channel != deps.Channel {
			return errors.New("daemon channel identity does not match")
		}
		return nil
	}
	return deps
}

// RunDoctor performs only read-only probes. It never mutates channel state,
// invokes provider inference, or starts a container. When an owner-only
// socket is present it performs the injected read-only daemon status
// handshake; it does not start or repair the daemon.
func RunDoctor(ctx context.Context, deps DoctorDeps) DoctorReport {
	deps = deps.defaults()
	report := DoctorReport{
		Schema: doctorSchema, Channel: deps.Channel, Checks: []DoctorCheck{},
		Authentication: []authStatusView{}, GuardedEligible: false, AutonomousEligible: false, CredentialsStored: false,
	}
	if !deps.Channel.Valid() {
		report.Checks = append(report.Checks, failedCheck("channel", "channel is invalid", deps.Binary, "doctor"))
		return report
	}

	report.Checks = append(report.Checks, checkRoot(deps))
	socketCheck := checkSocket(deps)
	report.Checks = append(report.Checks, socketCheck)
	if socketCheck.Status == CheckPass && deps.DaemonStatus != nil {
		if err := deps.DaemonStatus(ctx, deps.Paths); err != nil {
			// The socket is present but unhealthy. `doctor` would repeat this
			// exact probe; daemon status is the bounded executable diagnostic
			// action, while daemon run is reserved for an absent socket.
			report.Checks = append(report.Checks, failedCheck("daemon_status", "daemon socket did not complete a compatible status handshake", deps.Binary, "daemon", "status"))
		} else {
			report.Checks = append(report.Checks, DoctorCheck{ID: "daemon_status", Status: CheckPass, Summary: "running daemon completed a compatible status handshake"})
		}
	}
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
	pair, pairAvailable := checkProviderPair(ctx, deps, &report)
	checkQuarantinedProviders(ctx, deps, &report)
	checkAuthentication(ctx, deps, pair, pairAvailable, &report)
	report.GuardedEligible = pairAvailable && guardedEligibilityChecksPass(report)
	report.Checks = append(report.Checks, DoctorCheck{ID: "container_runtime", Status: CheckNotRun, Summary: "Docker and Colima are not required"})
	report.Checks = append(report.Checks, DoctorCheck{ID: "autonomous_mode", Status: CheckPass, Summary: "autonomous mode is disabled by policy"})
	return report
}

func checkQuarantinedProviders(ctx context.Context, deps DoctorDeps, report *DoctorReport) {
	if deps.Attempts == nil {
		report.Checks = append(report.Checks, DoctorCheck{ID: "provider_recovery", Status: CheckNotRun, Summary: "provider recovery inspection was not configured"})
		return
	}
	attempts, err := deps.Attempts(ctx, deps.Channel)
	if err != nil {
		report.Checks = append(report.Checks, DoctorCheck{ID: "provider_recovery", Status: CheckNotRun, Summary: "provider recovery claims could not be inspected"})
		return
	}
	for _, attempt := range attempts {
		if attempt.State == "quarantined" {
			report.Checks = append(report.Checks, failedCheck("provider_recovery", "a provider process claim is quarantined; verify the host reboot state and recover only after the worktree is safe", deps.Binary, "doctor", "--json"))
			return
		}
	}
	report.Checks = append(report.Checks, DoctorCheck{ID: "provider_recovery", Status: CheckPass, Summary: "no quarantined provider process claims were found"})
}

func checkProviderPair(ctx context.Context, deps DoctorDeps, report *DoctorReport) (store.ProviderPair, bool) {
	if deps.Pair == nil {
		report.Checks = append(report.Checks,
			DoctorCheck{ID: "authority_database", Status: CheckNotRun, Summary: "authority inspection was not configured"},
			DoctorCheck{ID: "provider_pair", Status: CheckNotRun, Summary: "provider qualification inspection was not configured"},
		)
		return store.ProviderPair{}, false
	}
	pair, err := deps.Pair(ctx, deps.Channel)
	if errors.Is(err, store.ErrNotFound) {
		report.Checks = append(report.Checks,
			DoctorCheck{ID: "authority_database", Status: CheckPass, Summary: "authority database is readable and schema-compatible"},
			failedCheck("provider_pair", "a current independent provider pair is not selected", deps.Binary, "providers", "qualify", "--help"),
		)
		return store.ProviderPair{}, false
	}
	if err != nil {
		report.Checks = append(report.Checks,
			failedCheck("authority_database", "authority database is missing, unreadable, or schema-incompatible", deps.Binary, "init", "--help"),
			DoctorCheck{ID: "provider_pair", Status: CheckNotRun, Summary: "provider pair was not read because authority inspection failed"},
		)
		return store.ProviderPair{}, false
	}
	if !validDoctorPair(pair, deps.Channel) {
		report.Checks = append(report.Checks,
			DoctorCheck{ID: "authority_database", Status: CheckPass, Summary: "authority database is readable and schema-compatible"},
			failedCheck("provider_pair", "the selected provider pair is invalid or no longer independent", deps.Binary, "providers", "qualify", "--help"),
		)
		return store.ProviderPair{}, false
	}
	report.ProviderPair = &DoctorProviderPair{
		Builder:     doctorQualification("builder", pair.Builder),
		Reviewer:    doctorQualification("reviewer", pair.Reviewer),
		Independent: true,
	}
	report.Checks = append(report.Checks,
		DoctorCheck{ID: "authority_database", Status: CheckPass, Summary: "authority database is readable and schema-compatible"},
		DoctorCheck{ID: "provider_pair", Status: CheckPass, Summary: "selected provider pair is current, qualified, and independent"},
	)
	return pair, true
}

func validDoctorPair(pair store.ProviderPair, channel domain.Channel) bool {
	if pair.Channel != channel || pair.Builder.ID <= 0 || pair.Reviewer.ID <= 0 || pair.Builder.ID == pair.Reviewer.ID || pair.SelectedAt.IsZero() {
		return false
	}
	if pair.Builder.Channel != channel || pair.Reviewer.Channel != channel || pair.Builder.Provider.Family == pair.Reviewer.Provider.Family {
		return false
	}
	return safeDoctorProvider(pair.Builder.Provider) && safeDoctorProvider(pair.Reviewer.Provider) &&
		safeDoctorAuthMode(pair.Builder.AuthMode) && safeDoctorAuthMode(pair.Reviewer.AuthMode) &&
		passingQualification(pair.Builder.Profile) && passingQualification(pair.Reviewer.Profile)
}

func safeDoctorAuthMode(value string) bool {
	return value == "" || safeDoctorField(value, 100)
}

func safeDoctorProvider(identity domain.ProviderIdentity) bool {
	provider, err := localauth.ParseProvider(identity.Provider)
	if err != nil || provider == localauth.GitHub {
		return false
	}
	return safeDoctorField(identity.Model, 200) && safeDoctorField(identity.Family, 100) && safeDoctorField(identity.Version, 200)
}

func passingQualification(profile store.QualificationProfile) bool {
	return profile == store.QualificationGuarded || profile == store.QualificationAutonomous
}

func doctorQualification(role string, value store.ProviderQualification) DoctorProviderQualification {
	return DoctorProviderQualification{
		Role: role, Provider: value.Provider.Provider, Model: value.Provider.Model,
		Family: value.Provider.Family, Version: value.Provider.Version, AuthMode: value.AuthMode, Qualification: value.Profile,
	}
}

func guardedEligibilityChecksPass(report DoctorReport) bool {
	mandatory := []string{"channel_root", "disk_space", "git_executable", "gh_executable", "provider_pair", "github_auth", "builder_auth", "reviewer_auth"}
	for _, check := range report.Checks {
		if check.ID == "repository_worktree" && check.Status != CheckNotRun {
			mandatory = append(mandatory, check.ID)
		}
		if check.ID == "daemon_socket" && check.Status == CheckFail {
			mandatory = append(mandatory, check.ID)
		}
		if check.ID == "daemon_status" {
			mandatory = append(mandatory, check.ID)
		}
	}
	return doctorChecksPass(report, mandatory...)
}

func doctorChecksPass(report DoctorReport, ids ...string) bool {
	for _, id := range ids {
		found := false
		for _, check := range report.Checks {
			if check.ID == id {
				found = check.Status == CheckPass
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

func checkAuthentication(ctx context.Context, deps DoctorDeps, pair store.ProviderPair, pairAvailable bool, report *DoctorReport) {
	if deps.AuthStatus == nil {
		report.Checks = append(report.Checks, DoctorCheck{ID: "authentication", Status: CheckNotRun, Summary: "authentication inspection was not configured"})
		return
	}
	views, byProvider, valid := normalizeDoctorAuth(deps.Channel, deps.AuthStatus(ctx))
	report.Authentication = views
	if !valid {
		report.Checks = append(report.Checks, failedCheck("authentication", "authentication probe results were incomplete or invalid", deps.Binary, "auth", "status"))
	}
	report.Checks = append(report.Checks, requiredAuthCheck(deps.Binary, "github_auth", "GitHub", localauth.GitHub, byProvider))
	if !pairAvailable {
		return
	}
	roles := []struct {
		id       string
		label    string
		provider string
	}{
		{id: "builder_auth", label: "Builder", provider: pair.Builder.Provider.Provider},
		{id: "reviewer_auth", label: "Reviewer", provider: pair.Reviewer.Provider.Provider},
	}
	for _, role := range roles {
		provider, err := localauth.ParseProvider(role.provider)
		if err != nil || provider == localauth.GitHub {
			report.Checks = append(report.Checks, failedCheck(role.id, role.label+" provider is not supported", deps.Binary, "providers", "qualify", "--help"))
			continue
		}
		report.Checks = append(report.Checks, requiredAuthCheck(deps.Binary, role.id, role.label, provider, byProvider))
	}
}

func normalizeDoctorAuth(channel domain.Channel, statuses []localauth.Status) ([]authStatusView, map[localauth.Provider]authStatusView, bool) {
	valid := true
	raw := make(map[localauth.Provider]localauth.Status, len(statuses))
	for _, status := range statuses {
		if _, err := localauth.ParseProvider(string(status.Provider)); err != nil {
			valid = false
			continue
		}
		if _, exists := raw[status.Provider]; exists {
			valid = false
			continue
		}
		raw[status.Provider] = status
	}
	views := make([]authStatusView, 0, len(localauth.Providers()))
	byProvider := make(map[localauth.Provider]authStatusView, len(localauth.Providers()))
	for _, provider := range localauth.Providers() {
		status, exists := raw[provider]
		if !exists || !validDoctorAuthStatus(status) {
			valid = false
			status = localauth.Status{Provider: provider, Executable: doctorExecutable(provider), State: localauth.StateProbeFailed, Reason: "authentication result was unavailable or invalid"}
		} else {
			status.Reason = doctorAuthReason(status.State)
		}
		view := authView(channel, status)
		views = append(views, view)
		byProvider[provider] = view
	}
	return views, byProvider, valid
}

func validDoctorAuthStatus(status localauth.Status) bool {
	if status.Executable != doctorExecutable(status.Provider) || !safeDoctorVersion(status.Version) {
		return false
	}
	switch status.State {
	case localauth.StateAuthenticated:
		return status.Installed && status.Authenticated && status.Version != ""
	case localauth.StateUnauthenticated:
		return status.Installed && !status.Authenticated && status.Version != ""
	case localauth.StateUnavailable:
		return !status.Installed && !status.Authenticated && status.Version == ""
	case localauth.StateProbeFailed:
		return !status.Authenticated
	default:
		return false
	}
}

func safeDoctorVersion(value string) bool {
	if value == "" {
		return true
	}
	return safeDoctorField(value, 200)
}

func safeDoctorField(value string, maximum int) bool {
	return value != "" && len(value) <= maximum && strings.TrimSpace(value) == value &&
		!strings.ContainsAny(value, "\x00\r\n\t") && redact.String(value) == value
}

func doctorExecutable(provider localauth.Provider) string {
	switch provider {
	case localauth.GitHub:
		return "gh"
	case localauth.Cursor:
		return "cursor-agent"
	case localauth.Claude:
		return "claude"
	case localauth.Codex:
		return "codex"
	default:
		return ""
	}
}

func doctorAuthReason(state localauth.State) string {
	switch state {
	case localauth.StateAuthenticated:
		return ""
	case localauth.StateUnauthenticated:
		return "official CLI reports no active authentication"
	case localauth.StateUnavailable:
		return "official CLI executable is unavailable"
	default:
		return "authentication could not be verified safely"
	}
}

func requiredAuthCheck(binary, id, label string, provider localauth.Provider, statuses map[localauth.Provider]authStatusView) DoctorCheck {
	status, ok := statuses[provider]
	if ok && status.State == localauth.StateAuthenticated && status.Installed && status.Authenticated {
		return DoctorCheck{ID: id, Status: CheckPass, Summary: label + " authentication is active"}
	}
	action := []string{binary, "auth", "status"}
	if ok && status.State == localauth.StateUnauthenticated {
		action = []string{binary, "auth", "login", string(provider)}
	}
	return DoctorCheck{ID: id, Status: CheckFail, Summary: label + " authentication is unavailable or unverified", NextAction: &domain.NextAction{Code: "provider_auth_missing", Argv: action}}
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
