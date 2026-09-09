package cli

import (
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"time"

	localauth "github.com/nysa-company/sf/internal/auth"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/ghrunner"
	"github.com/nysa-company/sf/internal/gitcredential"
	"github.com/nysa-company/sf/internal/runtimeassets"
)

var errDoctorHTTPSCredentials = errors.New("HTTPS credential bridge unavailable")
var errDoctorHTTPSCleanup = errors.New("HTTPS credential probe cleanup unproven")

func productionDoctorHTTPSCredentials(channel domain.Channel) func(context.Context, string) error {
	return func(ctx context.Context, repository string) error {
		assets, err := runtimeassets.CurrentPublication(channel)
		if err != nil {
			return errDoctorHTTPSCredentials
		}
		home, err := os.UserHomeDir()
		if err != nil || !gitcredential.TrustedHome(home) {
			return errDoctorHTTPSCredentials
		}
		configDir, err := localauth.ExistingGitHubConfigDirectory(home, os.Getenv("GH_CONFIG_DIR"), os.Getenv("XDG_CONFIG_HOME"))
		if err != nil || configDir == "" {
			return errDoctorHTTPSCredentials
		}
		binary, err := exec.LookPath("gh")
		if err != nil {
			return errDoctorHTTPSCredentials
		}
		// Match the daemon's authenticated private gh snapshot, rather than
		// probing a different PATH executable or the operator's API-only login.
		runner, err := ghrunner.New(binary)
		if err != nil {
			return errDoctorHTTPSCredentials
		}
		capability, err := runner.CredentialCapability()
		if err != nil {
			_ = runner.Close()
			return errDoctorHTTPSCredentials
		}
		err = probeDoctorHTTPSCredentials(ctx, assets.CredentialHelper, capability, home, configDir, repository)
		if errors.Is(err, errDoctorHTTPSCleanup) {
			// Retain the snapshot if a live child cannot be ruled out. Do not
			// remove executable evidence after an ambiguous cleanup.
			return errDoctorHTTPSCredentials
		}
		if closeErr := runner.Close(); err != nil || closeErr != nil {
			return errDoctorHTTPSCredentials
		}
		return nil
	}
}

func probeDoctorHTTPSCredentials(ctx context.Context, helper string, capability ghrunner.CredentialCapability, home, configDir, repository string) error {
	probeCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	response := gitcredential.NewResponseCheck()
	defer response.Clear()
	command := doctorHTTPSCommand(probeCtx, helper, capability, home, configDir, repository)
	command.Stdout = response
	runErr := command.Run()
	// WaitDelay can close pipes held by a descendant after the helper exits;
	// it does not itself terminate that descendant. Clean the owned group on
	// every exit, not just when CommandContext invokes Cancel.
	if command.Process != nil {
		if err := syscall.Kill(-command.Process.Pid, syscall.SIGKILL); err != nil && !errors.Is(err, syscall.ESRCH) {
			return errDoctorHTTPSCleanup
		}
	}
	if runErr != nil || !response.Valid() {
		return errDoctorHTTPSCredentials
	}
	return nil
}

func doctorHTTPSCommand(ctx context.Context, helper string, capability ghrunner.CredentialCapability, home, configDir, repository string) *exec.Cmd {
	command := exec.CommandContext(ctx, helper, "get")
	command.Env = []string{"HOME=/var/empty", "LANG=C", "LC_ALL=C",
		"SF_GIT_GH_BINARY=" + capability.Path, "SF_GIT_GH_BINARY_DIGEST=" + capability.Digest,
		"SF_GIT_GH_CONFIG_DIR=" + configDir, "SF_GIT_GH_HOME=" + home,
		"SF_GIT_HTTPS_REPOSITORY=" + repository}
	command.Stdin = strings.NewReader("protocol=https\nhost=github.com\npath=" + repository + ".git\n\n")
	command.Stderr = io.Discard
	command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	command.Cancel = func() error {
		if command.Process == nil {
			return os.ErrProcessDone
		}
		err := syscall.Kill(-command.Process.Pid, syscall.SIGKILL)
		if errors.Is(err, syscall.ESRCH) {
			return os.ErrProcessDone
		}
		return err
	}
	command.WaitDelay = 500 * time.Millisecond
	return command
}
