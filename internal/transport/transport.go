// Package transport implements the owner-only local Unix-socket protocol.
package transport

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/nysa-company/sf/internal/api"
	"github.com/nysa-company/sf/internal/domain"
)

var ErrSocketExists = errors.New("socket path already exists")

type Peer struct {
	UID uint32
}

type Handler interface {
	Handle(context.Context, Peer, api.Request) api.Response
}

type HandlerFunc func(context.Context, Peer, api.Request) api.Response

func (function HandlerFunc) Handle(ctx context.Context, peer Peer, request api.Request) api.Response {
	return function(ctx, peer, request)
}

type Server struct {
	listener    *net.UnixListener
	path        string
	expectedUID uint32
	handler     Handler
	executable  string
	identity    fileIdentity
	closeOnce   sync.Once
	workers     sync.WaitGroup
	limit       chan struct{}
}

type fileIdentity struct {
	device uint64
	inode  uint64
}

func Listen(path string, expectedUID uint32, handler Handler) (*Server, error) {
	return listen(path, expectedUID, handler, "sf")
}

// ListenWithExecutable configures the recovery action for a channel-specific
// daemon. The generic listener keeps its stable default for non-daemon users.
func ListenWithExecutable(path string, expectedUID uint32, handler Handler, executable string) (*Server, error) {
	return listen(path, expectedUID, handler, executable)
}

func listen(path string, expectedUID uint32, handler Handler, executable string) (*Server, error) {
	if !filepath.IsAbs(path) {
		return nil, fmt.Errorf("socket path must be absolute")
	}
	if handler == nil {
		return nil, fmt.Errorf("socket handler is required")
	}
	if executable == "" {
		return nil, fmt.Errorf("socket recovery executable is required")
	}
	if err := ensureSocketParent(filepath.Dir(path)); err != nil {
		return nil, err
	}
	if _, err := os.Lstat(path); err == nil {
		return nil, fmt.Errorf("%w: %s", ErrSocketExists, path)
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("inspect socket path: %w", err)
	}

	listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
	if err != nil {
		return nil, fmt.Errorf("listen on local socket: %w", err)
	}
	// We remove the socket ourselves after verifying that it is still the inode
	// created above. The net package's automatic unlink could otherwise delete a
	// replacement placed at the same path while the daemon is shutting down.
	listener.SetUnlinkOnClose(false)
	createdIdentity, err := socketIdentity(path)
	if err != nil {
		_ = listener.Close()
		return nil, fmt.Errorf("identify created socket: %w", err)
	}
	cleanup := func() {
		_ = listener.Close()
		identity, identityErr := socketIdentity(path)
		if identityErr == nil && identity == createdIdentity {
			_ = os.Remove(path)
		}
	}
	if err := os.Chmod(path, 0o600); err != nil {
		cleanup()
		return nil, fmt.Errorf("set socket mode: %w", err)
	}
	identity, err := socketIdentity(path)
	if err != nil {
		cleanup()
		return nil, err
	}
	return &Server{
		listener:    listener,
		path:        path,
		expectedUID: expectedUID,
		handler:     handler,
		executable:  executable,
		identity:    identity,
		limit:       make(chan struct{}, 16),
	}, nil
}

func (server *Server) Serve(ctx context.Context) error {
	for {
		if err := server.listener.SetDeadline(time.Now().Add(250 * time.Millisecond)); err != nil {
			return fmt.Errorf("set socket deadline: %w", err)
		}
		connection, err := server.listener.AcceptUnix()
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			var networkError net.Error
			if errors.As(err, &networkError) && networkError.Timeout() {
				continue
			}
			if errors.Is(err, net.ErrClosed) {
				return nil
			}
			return fmt.Errorf("accept local connection: %w", err)
		}
		select {
		case server.limit <- struct{}{}:
			server.workers.Add(1)
			go func() {
				defer server.workers.Done()
				defer func() { <-server.limit }()
				server.serveConnection(ctx, connection)
			}()
		case <-ctx.Done():
			_ = connection.Close()
			return ctx.Err()
		}
	}
}

func (server *Server) Close() error {
	var closeErr error
	server.closeOnce.Do(func() {
		closeErr = server.listener.Close()
		server.workers.Wait()
		identity, err := socketIdentity(server.path)
		if err == nil && identity == server.identity {
			if removeErr := os.Remove(server.path); removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) && closeErr == nil {
				closeErr = removeErr
			}
		}
	})
	return closeErr
}

func (server *Server) serveConnection(ctx context.Context, connection *net.UnixConn) {
	defer connection.Close()
	_ = connection.SetDeadline(time.Now().Add(30 * time.Second))
	uid, err := peerUID(connection)
	if err != nil || uid != server.expectedUID {
		return
	}
	request, err := api.DecodeRequest(connection)
	if err != nil {
		return
	}
	handlerCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	response := server.handler.Handle(handlerCtx, Peer{UID: uid}, request)
	response.Version = api.Version
	response.RequestID = request.RequestID
	if err := response.Validate(); err != nil {
		response = api.Response{
			Version:    api.Version,
			RequestID:  request.RequestID,
			OK:         false,
			Mutation:   api.Mutation{},
			Error:      &api.Error{Code: "internal_response_invalid", Message: "daemon produced an invalid response"},
			NextAction: &domain.NextAction{Code: "internal_response_invalid", Argv: []string{server.executable, "doctor"}},
		}
	}
	_ = api.Encode(connection, response)
}

func Call(ctx context.Context, path string, request api.Request) (api.Response, error) {
	dialer := net.Dialer{}
	connection, err := dialer.DialContext(ctx, "unix", path)
	if err != nil {
		return api.Response{}, fmt.Errorf("connect to daemon: %w", err)
	}
	defer connection.Close()
	if deadline, ok := ctx.Deadline(); ok {
		_ = connection.SetDeadline(deadline)
	}
	if err := api.Encode(connection, request); err != nil {
		return api.Response{}, err
	}
	if unixConnection, ok := connection.(*net.UnixConn); ok {
		_ = unixConnection.CloseWrite()
	}
	response, err := api.DecodeResponse(connection)
	if err != nil {
		return api.Response{}, fmt.Errorf("read daemon response: %w", err)
	}
	if response.RequestID != request.RequestID {
		return api.Response{}, fmt.Errorf("daemon response request id mismatch")
	}
	return response, nil
}

func ensureSocketParent(path string) error {
	if err := os.MkdirAll(path, 0o700); err != nil {
		return fmt.Errorf("create socket directory: %w", err)
	}
	info, err := os.Lstat(path)
	if err != nil {
		return fmt.Errorf("inspect socket directory: %w", err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return fmt.Errorf("socket parent is not a real directory")
	}
	if info.Mode().Perm()&0o077 != 0 {
		if err := os.Chmod(path, 0o700); err != nil {
			return fmt.Errorf("secure socket directory: %w", err)
		}
	}
	return nil
}

func socketIdentity(path string) (fileIdentity, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return fileIdentity{}, err
	}
	if info.Mode()&os.ModeSymlink != 0 || info.Mode()&os.ModeSocket == 0 {
		return fileIdentity{}, fmt.Errorf("socket path is not an owned socket")
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return fileIdentity{}, fmt.Errorf("socket identity is unavailable")
	}
	return fileIdentity{device: uint64(stat.Dev), inode: uint64(stat.Ino)}, nil
}
