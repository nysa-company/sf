package cli

import (
	"context"
	"fmt"
	"time"

	"github.com/nysa-company/sf/internal/api"
	"github.com/nysa-company/sf/internal/transport"
)

// Client is the only boundary used by mutating/querying CLI commands. A
// command never opens SQLite or changes workflow state itself.
type Client interface {
	Call(context.Context, api.Request) (api.Response, error)
}

// SocketClient adapts the versioned local API to the owner-only Unix socket.
type SocketClient struct {
	Path    string
	Timeout time.Duration
}

func (c SocketClient) Call(ctx context.Context, request api.Request) (api.Response, error) {
	if c.Path == "" {
		return api.Response{}, fmt.Errorf("daemon socket path is not configured")
	}
	if c.Timeout <= 0 {
		c.Timeout = 30 * time.Second
	}
	callCtx, cancel := context.WithTimeout(ctx, c.Timeout)
	defer cancel()
	return transport.Call(callCtx, c.Path, request)
}

type fakeClient func(context.Context, api.Request) (api.Response, error)

func (f fakeClient) Call(ctx context.Context, request api.Request) (api.Response, error) {
	return f(ctx, request)
}
