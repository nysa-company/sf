package contracts

import (
	"context"
	"io"
	"time"
)

type CommandSpec struct {
	Argv       []string
	Directory  string
	Timeout    time.Duration
	Profile    ExecutionProfile
	PolicyHash string
	Stdin      io.Reader
}

type CommandResult struct {
	ExitCode int
	Stdout   []byte
	Stderr   []byte
	Duration time.Duration
}

type CommandExecutor interface {
	Run(context.Context, CommandSpec) (CommandResult, error)
	Drain(context.Context, string) error
	NoLiveWriter(context.Context, string) (bool, error)
}
