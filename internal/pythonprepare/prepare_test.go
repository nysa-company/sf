package pythonprepare

import (
	"context"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/nysa-company/sf/internal/pythonclosure"
)

func TestPrepareFailureLeavesNoPartialCache(t *testing.T) {
	for _, kind := range []string{"cancelled", "download failed", "unsafe parent"} {
		t.Run(kind, func(t *testing.T) {
			root, _ := extractionRoot(t)
			root, err := filepath.EvalSymlinks(root)
			if err != nil {
				t.Fatal(err)
			}
			catalog, err := DefaultCatalog("darwin", "arm64")
			if err != nil {
				t.Fatal(err)
			}
			ctx := t.Context()
			calls := 0
			if kind == "cancelled" {
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}
			if kind == "unsafe parent" {
				if err := os.Chmod(root, 0755); err != nil {
					t.Fatal(err)
				}
			}
			client := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) { calls++; return nil, errors.New("offline") })}
			if _, err := prepare(ctx, root, catalog, client); err == nil {
				t.Fatal("missing refusal")
			}
			entries, err := os.ReadDir(root)
			if err != nil || len(entries) != 0 {
				t.Fatal("partial cache retained", err)
			}
			if kind != "download failed" && calls != 0 {
				t.Fatal("network attempted before preconditions")
			}
		})
	}
}

func TestPinnedPreparationPublishesAndReplaysWithoutDownload(t *testing.T) {
	archives := os.Getenv("SF_TEST_PYTHON_ARCHIVES")
	if archives == "" {
		t.Skip("requires explicit pinned local archives")
	}
	if runtime.GOOS != "darwin" {
		t.Skip("macOS exclusive publication")
	}
	catalog, err := DefaultCatalog("darwin", "arm64")
	if err != nil {
		t.Fatal(err)
	}
	root, parent := extractionRoot(t)
	root, err = filepath.EvalSymlinks(root)
	if err != nil {
		t.Fatal(err)
	}
	locations := map[string]string{catalog.Runtime.URL: filepath.Join(archives, "python.tar.gz")}
	for _, a := range catalog.Wheels {
		locations[a.URL] = filepath.Join(archives, "wheels", a.Name)
	}
	calls := 0
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		calls++
		p, ok := locations[r.URL.String()]
		if !ok {
			return nil, errors.New("unlisted URL")
		}
		f, err := os.Open(p)
		if err != nil {
			return nil, err
		}
		info, err := f.Stat()
		if err != nil {
			f.Close()
			return nil, err
		}
		return &http.Response{StatusCode: 200, ContentLength: info.Size(), Body: f, Header: make(http.Header)}, nil
	})}
	result, err := prepare(t.Context(), root, catalog, client)
	if err != nil || result.AlreadyPrepared || result.EnvironmentDigest != catalog.EnvironmentDigest {
		t.Fatal("preparation failed", result, err)
	}
	if calls != 6 {
		t.Fatal("wrong artifact count", calls)
	}
	p, err := pythonclosure.OpenPreparedDirectoryFD(t.Context(), int(parent.Fd()), result.EnvironmentDigest, result.LockDigest, pythonclosure.BootstrapDigest())
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	result, err = prepare(t.Context(), root, catalog, client)
	if err != nil || !result.AlreadyPrepared || calls != 6 {
		t.Fatal("replay downloaded or failed", result, err, calls)
	}
	if err := p.Revalidate(t.Context()); err != nil {
		t.Fatal("replay changed retained runtime", err)
	}
	entries, err := os.ReadDir(root)
	if err != nil || len(entries) != 1 || entries[0].Name() != catalog.EnvironmentDigest[7:] {
		t.Fatal("staging not cleaned", err)
	}
	manifest := filepath.Join(root, catalog.EnvironmentDigest[7:], "environment.json")
	if err := os.Chmod(manifest, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(manifest, []byte("corrupt"), 0400); err != nil {
		t.Fatal(err)
	}
	if _, err := prepare(t.Context(), root, catalog, client); err == nil || calls != 6 {
		t.Fatal("corrupt cache silently repaired", err, calls)
	}
	if data, err := os.ReadFile(manifest); err != nil || string(data) != "corrupt" {
		t.Fatal("corrupt evidence overwritten", err)
	}
}
