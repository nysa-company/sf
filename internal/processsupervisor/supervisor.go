// Package processsupervisor owns guarded local provider processes. Adapters
// only provide argv; this package is the only os/exec boundary.
package processsupervisor

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/nysa-company/sf/internal/contracts"
)

var ErrUnclear = errors.New("provider process drain is unclear")

// LaunchRecorder persists the exact child identity before the gate is opened.
// Implementations must fail closed; a supervisor never releases a child after
// recorder failure.
type LaunchRecorder interface {
	RecordLaunch(context.Context, contracts.DrainRequest, Identity, string) error
}

type Identity struct {
	PID, PGID            int
	BootIdentity         string
	ProcessStartIdentity string
}

type Supervisor struct {
	Signer   *contracts.DrainSigner
	Recorder LaunchRecorder
	Env      []string
	// Executable is the sf binary that implements __provider_gate. It is only
	// overridden by compiled-boundary tests; production uses os.Executable.
	Executable           string
	SoftDrain, HardDrain time.Duration
	mu                   sync.Mutex
	runs                 map[string]*run
}
type run struct {
	identity Identity
	worktree string
	done     chan struct{}
	streams  chan struct{}
}

func New(recorder LaunchRecorder) (*Supervisor, error) {
	signer, err := contracts.NewDrainSigner()
	if err != nil {
		return nil, err
	}
	return &Supervisor{Signer: signer, Recorder: recorder, Env: []string{"PATH=/usr/bin:/bin"}, SoftDrain: 2 * time.Second, HardDrain: 2 * time.Second, runs: map[string]*run{}}, nil
}
func (s *Supervisor) PublicKey() []byte { return s.Signer.PublicKey() }
func (s *Supervisor) SetLaunchRecorder(recorder func(context.Context, contracts.DrainRequest, contracts.ProviderLaunch) error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if recorder == nil {
		s.Recorder = nil
		return
	}
	s.Recorder = launchRecorderFunc(recorder)
}

type launchRecorderFunc func(context.Context, contracts.DrainRequest, contracts.ProviderLaunch) error

func (f launchRecorderFunc) RecordLaunch(ctx context.Context, req contracts.DrainRequest, identity Identity, worktree string) error {
	return f(ctx, req, contracts.ProviderLaunch{PID: identity.PID, PGID: identity.PGID, BootIdentity: identity.BootIdentity, ProcessStartIdentity: identity.ProcessStartIdentity, Worktree: worktree})
}
func key(r contracts.DrainRequest) string {
	return fmt.Sprintf("%s/%s/%s/%s/%d", r.Ref.Channel, r.Ref.Project, r.Ref.Ticket, r.Phase, r.Attempt)
}

func (s *Supervisor) Run(ctx context.Context, request contracts.DrainRequest, invocation contracts.Invocation, input contracts.PhaseInput) (contracts.CommandResult, error) {
	if len(invocation.Argv) == 0 || !filepath.IsAbs(invocation.Argv[0]) || input.Worktree == "" || filepath.Clean(input.Worktree) != input.Worktree {
		return contracts.CommandResult{}, errors.New("guarded argv and worktree required")
	}
	self := s.Executable
	if self == "" {
		var err error
		self, err = os.Executable()
		if err != nil {
			return contracts.CommandResult{}, err
		}
	}
	gateRead, gateWrite, err := os.Pipe()
	if err != nil {
		return contracts.CommandResult{}, err
	}
	defer gateWrite.Close()
	argv := append([]string{"__provider_gate", invocation.Argv[0]}, invocation.Argv[1:]...)
	cmd := exec.Command(self, argv...)
	cmd.Dir = input.Worktree
	cmd.Env = append(append([]string(nil), s.Env...), invocation.Env...)
	cmd.ExtraFiles = []*os.File{gateRead} // FD 3: wrapper exits on EOF before release.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	var stdout, stderr limitedBuffer
	stdout.limit, stderr.limit = 64<<10, 64<<10
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	if err := cmd.Start(); err != nil {
		gateRead.Close()
		return contracts.CommandResult{}, err
	}
	gateRead.Close()
	startIdentity, identityErr := processStartIdentity(cmd.Process.Pid)
	if identityErr != nil {
		_ = signalGroup(cmd.Process.Pid, syscall.SIGKILL)
		_ = cmd.Wait()
		return contracts.CommandResult{}, ErrUnclear
	}
	bootIdentity, bootErr := hostBootIdentity()
	if bootErr != nil {
		_ = signalGroup(cmd.Process.Pid, syscall.SIGKILL)
		_ = cmd.Wait()
		return contracts.CommandResult{}, ErrUnclear
	}
	r := &run{identity: Identity{PID: cmd.Process.Pid, PGID: cmd.Process.Pid, BootIdentity: bootIdentity, ProcessStartIdentity: startIdentity}, worktree: input.Worktree, done: make(chan struct{}), streams: make(chan struct{})}
	s.mu.Lock()
	s.runs[key(request)] = r
	s.mu.Unlock()
	s.mu.Lock()
	recorder := s.Recorder
	s.mu.Unlock()
	if recorder == nil || recorder.RecordLaunch(ctx, request, r.identity, r.worktree) != nil {
		_ = signalGroup(r.identity.PGID, syscall.SIGKILL)
		_ = cmd.Wait()
		close(r.done)
		close(r.streams)
		return contracts.CommandResult{}, ErrUnclear
	}
	// Durable identity exists; the only release is closing the inherited gate.
	if _, err := gateWrite.Write([]byte{1}); err != nil {
		return contracts.CommandResult{}, ErrUnclear
	}
	if err := gateWrite.Close(); err != nil {
		return contracts.CommandResult{}, ErrUnclear
	}
	wait := make(chan error, 1)
	go func() { wait <- cmd.Wait(); close(r.done); close(r.streams) }()
	var runErr error
	select {
	case runErr = <-wait:
	case <-ctx.Done():
		runErr = ctx.Err()
		_ = s.terminate(r)
		<-wait
	}
	if err := s.proveGone(r); err != nil {
		return contracts.CommandResult{}, err
	}
	if runErr != nil {
		return contracts.CommandResult{Stdout: stdout.Bytes(), Stderr: stderr.Bytes(), ExitCode: -1}, runErr
	}
	return contracts.CommandResult{Stdout: stdout.Bytes(), Stderr: stderr.Bytes(), ExitCode: 0}, nil
}
func (s *Supervisor) Drain(ctx context.Context, request contracts.DrainRequest) (contracts.DrainProof, error) {
	s.mu.Lock()
	r := s.runs[key(request)]
	s.mu.Unlock()
	if r == nil {
		return contracts.DrainProof{}, ErrUnclear
	}
	if err := s.terminate(r); err != nil {
		return contracts.DrainProof{}, err
	}
	if err := s.proveGone(r); err != nil {
		return contracts.DrainProof{}, err
	}
	return s.Signer.ProveDrained(request)
}

// DrainPersisted is restart recovery: it accepts only the exact durable
// PID/PGID identity published through the launch gate.
func (s *Supervisor) DrainPersisted(ctx context.Context, request contracts.DrainRequest, launch contracts.ProviderLaunch) (contracts.DrainProof, error) {
	if launch.PID <= 0 || launch.PGID <= 0 || launch.PID != launch.PGID || launch.BootIdentity == "" || launch.ProcessStartIdentity == "" {
		return contracts.DrainProof{}, ErrUnclear
	}
	boot, err := hostBootIdentity()
	if err != nil {
		return contracts.DrainProof{}, ErrUnclear
	}
	if bootIdentityChanged(launch.BootIdentity, boot) {
		return s.Signer.ProveDrained(request) // reboot proves every old group gone.
	}
	if leaderErr := syscall.Kill(launch.PID, 0); leaderErr == syscall.ESRCH {
		groupErr := syscall.Kill(-launch.PGID, 0)
		if missingLeaderGroupGone(leaderErr, groupErr) {
			return s.Signer.ProveDrained(request) // leader and every group member are gone.
		}
		return contracts.DrainProof{}, ErrUnclear
	} else if leaderErr != nil {
		return contracts.DrainProof{}, ErrUnclear
	}
	pgid, err := syscall.Getpgid(launch.PID)
	if err != nil {
		return contracts.DrainProof{}, ErrUnclear
	}
	actualStart, err := processStartIdentity(launch.PID)
	if err != nil || !persistedIdentityMatches(launch, actualStart, pgid) {
		return contracts.DrainProof{}, ErrUnclear
	}
	r := &run{identity: Identity{PID: launch.PID, PGID: launch.PGID, BootIdentity: boot, ProcessStartIdentity: actualStart}, done: make(chan struct{}), streams: make(chan struct{})}
	close(r.streams)
	go func() {
		for {
			if err := syscall.Kill(launch.PID, 0); err == syscall.ESRCH {
				close(r.done)
				return
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(20 * time.Millisecond):
			}
		}
	}()
	if err := s.terminate(r); err != nil {
		return contracts.DrainProof{}, err
	}
	if err := s.proveGone(r); err != nil {
		return contracts.DrainProof{}, err
	}
	return s.Signer.ProveDrained(request)
}

func bootIdentityChanged(recorded, observed string) bool {
	return recorded != "" && observed != "" && recorded != observed
}
func missingLeaderGroupGone(leaderErr, groupErr error) bool {
	return leaderErr == syscall.ESRCH && groupErr == syscall.ESRCH
}

// persistedIdentityMatches is separated from signalling so a failed identity
// check has a mechanically obvious no-signal path. In particular, a rapid
// PID reuse with a matching human-readable lstart cannot reach terminate.
func persistedIdentityMatches(launch contracts.ProviderLaunch, observed string, observedPGID int) bool {
	return launch.PID > 0 && launch.PID == launch.PGID && launch.BootIdentity != "" && launch.ProcessStartIdentity != "" && observed == launch.ProcessStartIdentity && observedPGID == launch.PGID
}
func (s *Supervisor) terminate(r *run) error {
	_ = signalGroup(r.identity.PGID, syscall.SIGTERM)
	select {
	case <-r.done:
		return nil
	case <-time.After(s.SoftDrain):
	}
	_ = signalGroup(r.identity.PGID, syscall.SIGKILL)
	select {
	case <-r.done:
		return nil
	case <-time.After(s.HardDrain):
		return ErrUnclear
	}
}
func (s *Supervisor) proveGone(r *run) error {
	select {
	case <-r.done:
	case <-time.After(s.HardDrain):
		return ErrUnclear
	}
	select {
	case <-r.streams:
	case <-time.After(s.HardDrain):
		return ErrUnclear
	}
	if err := syscall.Kill(-r.identity.PGID, 0); err == nil || err != syscall.ESRCH {
		return ErrUnclear
	}
	return nil
}
func signalGroup(pgid int, sig syscall.Signal) error {
	if pgid <= 0 {
		return ErrUnclear
	}
	err := syscall.Kill(-pgid, sig)
	if err == syscall.ESRCH {
		return nil
	}
	return err
}

type limitedBuffer struct {
	bytes.Buffer
	limit int
}

func (b *limitedBuffer) Write(p []byte) (int, error) {
	if b.Len() < b.limit {
		n := b.limit - b.Len()
		if n > len(p) {
			n = len(p)
		}
		_, _ = b.Buffer.Write(p[:n])
	}
	return len(p), nil
}

var _ io.Writer = (*limitedBuffer)(nil)
