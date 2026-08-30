package auth

import (
	"bytes"
	"context"
	"errors"
	"os/exec"
	"sync"
	"syscall"
	"time"
)

type OSRunner struct{}

func (OSRunner) Probe(ctx context.Context, executable string, arguments, environment []string, limit int) (ProbeResult, error) {
	command := exec.CommandContext(ctx, executable, arguments...)
	command.Env = append([]string(nil), environment...)
	command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	command.WaitDelay = time.Second
	output := &boundedBuffer{limit: limit}
	command.Stdout = output
	command.Stderr = output
	command.Cancel = func() error {
		if command.Process == nil {
			return nil
		}
		err := syscall.Kill(-command.Process.Pid, syscall.SIGKILL)
		if errors.Is(err, syscall.ESRCH) {
			return nil
		}
		return err
	}
	err := command.Run()
	if output.exceededLimit() {
		return ProbeResult{}, errors.New("authentication probe output exceeds limit")
	}
	result := ProbeResult{ExitCode: 0, Output: output.bytes()}
	if err == nil {
		return result, nil
	}
	var exitError *exec.ExitError
	if errors.As(err, &exitError) && ctx.Err() == nil {
		result.ExitCode = exitError.ExitCode()
		return result, nil
	}
	return ProbeResult{}, err
}

func (OSRunner) Interactive(ctx context.Context, executable string, arguments, environment []string, terminal Terminal) (int, error) {
	command := exec.CommandContext(ctx, executable, arguments...)
	command.Env = append([]string(nil), environment...)
	command.Stdin = terminal.In
	command.Stdout = terminal.Out
	command.Stderr = terminal.Err
	command.WaitDelay = 2 * time.Second
	err := command.Run()
	if err == nil {
		return 0, nil
	}
	var exitError *exec.ExitError
	if errors.As(err, &exitError) && ctx.Err() == nil {
		return exitError.ExitCode(), nil
	}
	return -1, err
}

type boundedBuffer struct {
	mu       sync.Mutex
	buffer   bytes.Buffer
	limit    int
	exceeded bool
}

func (buffer *boundedBuffer) Write(data []byte) (int, error) {
	buffer.mu.Lock()
	defer buffer.mu.Unlock()
	if buffer.limit == 0 {
		return len(data), nil
	}
	remaining := buffer.limit - buffer.buffer.Len()
	if remaining > 0 {
		if remaining > len(data) {
			remaining = len(data)
		}
		_, _ = buffer.buffer.Write(data[:remaining])
	}
	if len(data) > remaining {
		buffer.exceeded = true
	}
	return len(data), nil
}

func (buffer *boundedBuffer) bytes() []byte {
	buffer.mu.Lock()
	defer buffer.mu.Unlock()
	return append([]byte(nil), buffer.buffer.Bytes()...)
}

func (buffer *boundedBuffer) exceededLimit() bool {
	buffer.mu.Lock()
	defer buffer.mu.Unlock()
	return buffer.exceeded
}
