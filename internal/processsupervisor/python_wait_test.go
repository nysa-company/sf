package processsupervisor

import (
	"context"
	"errors"
	"os/exec"
	"runtime"
	"syscall"
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/pythonclosure"
)

func pythonWaitFixture(t *testing.T) (contracts.RepositoryCommandLaunch, <-chan error) {
	t.Helper()
	if runtime.GOOS != "darwin" {
		t.Skip("macOS process identity")
	}
	cmd := exec.Command("/bin/sleep", "30")
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	wait, done := make(chan error, 1), make(chan struct{})
	go func() { wait <- cmd.Wait(); close(done) }()
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Error("fixture did not reap")
		}
	})
	start, err := processStartIdentity(cmd.Process.Pid)
	if err != nil {
		t.Fatal(err)
	}
	boot, err := hostBootIdentity()
	if err != nil {
		t.Fatal(err)
	}
	return contracts.RepositoryCommandLaunch{PID: cmd.Process.Pid, PGID: cmd.Process.Pid, ProcessStartIdentity: start, BootIdentity: boot}, wait
}

func TestPythonWaitKeepsFactoryAbortSeparateFromExit(t *testing.T) {
	for _, test := range []struct {
		name  string
		fault error
		want  error
	}{
		{"quota", pythonclosure.ErrLimit, contracts.ErrRepositoryCommandResourceLimit},
		{"inspection", pythonclosure.ErrInvalid, ErrUnclear},
		{"closed monitor", nil, ErrUnclear},
	} {
		t.Run(test.name, func(t *testing.T) {
			launch, wait := pythonWaitFixture(t)
			resource := make(chan error, 1)
			if test.fault == nil {
				close(resource)
			} else {
				resource <- test.fault
			}
			s := RepositoryCommandSupervisor{SoftDrain: 100 * time.Millisecond, HardDrain: 100 * time.Millisecond}
			exit, abort, reaped := s.waitPythonCommand(t.Context(), wait, launch, resource)
			if !reaped || exit == nil || !errors.Is(abort, test.want) {
				t.Fatalf("exit=%v abort=%v", exit, abort)
			}
			if err := s.ensureGone(launch, false); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestPythonWaitCancellationAndIdentityMismatch(t *testing.T) {
	for _, mismatch := range []bool{false, true} {
		launch, wait := pythonWaitFixture(t)
		original := launch
		if mismatch {
			launch.ProcessStartIdentity = "wrong"
		}
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		s := RepositoryCommandSupervisor{SoftDrain: 100 * time.Millisecond, HardDrain: 100 * time.Millisecond}
		_, abort, reaped := s.waitPythonCommand(ctx, wait, launch, nil)
		if reaped == mismatch {
			t.Fatalf("wrong reaped state: %v", reaped)
		}
		if !errors.Is(abort, context.Canceled) {
			t.Fatal(abort)
		}
		if mismatch {
			if !errors.Is(abort, ErrUnclear) {
				t.Fatal("mismatch accepted", abort)
			}
			start, err := processStartIdentity(original.PID)
			if err != nil || start != original.ProcessStartIdentity {
				t.Fatal("signaled unauthenticated process", err)
			}
		} else if err := s.ensureGone(launch, false); err != nil {
			t.Fatal(err)
		}
	}
}

func TestPythonWaitRejectsOverlongDrainBeforeSignaling(t *testing.T) {
	launch, wait := pythonWaitFixture(t)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err, reaped := (RepositoryCommandSupervisor{HardDrain: time.Hour}).waitPythonCommand(ctx, wait, launch, nil)
	if reaped || !errors.Is(err, ErrUnclear) {
		t.Fatal(err)
	}
	start, err := processStartIdentity(launch.PID)
	if err != nil || start != launch.ProcessStartIdentity {
		t.Fatal("invalid settings signaled process", err)
	}
}

func TestPythonWaitHasHardBoundWhenWaitDoesNotReturn(t *testing.T) {
	launch, _ := pythonWaitFixture(t)
	// The fixture still reaps its real child. Withhold the wait notification
	// from the helper to model stuck I/O completion after process termination.
	withheld := make(chan error)
	resource := make(chan error, 1)
	resource <- pythonclosure.ErrLimit
	s := RepositoryCommandSupervisor{SoftDrain: 20 * time.Millisecond, HardDrain: 20 * time.Millisecond}
	started := time.Now()
	_, abort, reaped := s.waitPythonCommand(t.Context(), withheld, launch, resource)
	if reaped || !errors.Is(abort, ErrUnclear) || !errors.Is(abort, contracts.ErrRepositoryCommandResourceLimit) {
		t.Fatalf("missing uncertain resource disposition: %v", abort)
	}
	if elapsed := time.Since(started); elapsed > 3*time.Second {
		t.Fatalf("wait exceeded hard bound: %s", elapsed)
	}
}
