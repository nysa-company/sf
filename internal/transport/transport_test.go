package transport

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/api"
)

func TestOwnerOnlySocketRoundTrip(t *testing.T) {
	path := filepath.Join(shortTempDir(t), "run", "sf.sock")
	server, err := Listen(path, uint32(os.Getuid()), HandlerFunc(func(_ context.Context, peer Peer, request api.Request) api.Response {
		data, _ := json.Marshal(map[string]any{"method": request.Method, "uid": peer.UID})
		return api.Response{OK: true, Mutation: api.Mutation{}, Data: data}
	}))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- server.Serve(ctx) }()
	t.Cleanup(func() {
		cancel()
		_ = server.Close()
		<-done
	})

	callCtx, callCancel := context.WithTimeout(context.Background(), time.Second)
	defer callCancel()
	response, err := Call(callCtx, path, api.Request{Version: api.Version, RequestID: "1", Method: "status"})
	if err != nil {
		t.Fatal(err)
	}
	if !response.OK {
		t.Fatalf("response=%+v", response)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("socket mode=%#o", info.Mode().Perm())
	}
}

func TestListenRefusesExistingPath(t *testing.T) {
	path := filepath.Join(shortTempDir(t), "sf.sock")
	if err := os.WriteFile(path, []byte("not a socket"), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := Listen(path, uint32(os.Getuid()), HandlerFunc(func(context.Context, Peer, api.Request) api.Response {
		return api.Response{}
	}))
	if !errors.Is(err, ErrSocketExists) {
		t.Fatalf("expected existing-path refusal, got %v", err)
	}
}

func TestCloseDoesNotRemoveReplacement(t *testing.T) {
	path := filepath.Join(shortTempDir(t), "sf.sock")
	server, err := Listen(path, uint32(os.Getuid()), HandlerFunc(func(context.Context, Peer, api.Request) api.Response {
		return api.Response{}
	}))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("replacement"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := server.Close(); err != nil && !errors.Is(err, os.ErrClosed) {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("replacement removed: %v", err)
	}
	if string(data) != "replacement" {
		t.Fatalf("replacement data=%q", data)
	}
}

func shortTempDir(t *testing.T) string {
	t.Helper()
	directory, err := os.MkdirTemp("/tmp", "sf-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(directory) })
	return directory
}
