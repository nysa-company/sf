package pythonprepare

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestDownloadArtifactVerifiesBeforeRetention(t *testing.T) {
	for _, kind := range []string{"valid", "wrong hash", "short", "long", "length", "status", "encoding", "cancelled", "network error", "existing", "symlink", "public staging", "traversal", "credential URL", "foreign URL"} {
		t.Run(kind, func(t *testing.T) {
			root := t.TempDir()
			if err := os.Chmod(root, 0700); err != nil {
				t.Fatal(err)
			}
			parent, err := os.Open(root)
			if err != nil {
				t.Fatal(err)
			}
			defer parent.Close()
			body := "verified bytes"
			a := Artifact{"input.whl", "https://files.pythonhosted.org/input.whl", fmt.Sprintf("%x", sha256.Sum256([]byte(body))), int64(len(body))}
			ctx := t.Context()
			response := &http.Response{StatusCode: 200, Header: make(http.Header), ContentLength: -1}
			target := filepath.Join(root, a.Name)
			switch kind {
			case "wrong hash":
				a.SHA256 = strings.Repeat("0", 64)
			case "short":
				body = "short"
			case "long":
				body += "extra"
			case "length":
				response.ContentLength = 999
			case "status":
				response.StatusCode = 500
			case "encoding":
				response.Header.Set("Content-Encoding", "gzip")
			case "cancelled":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			case "existing":
				if err := os.WriteFile(target, []byte("existing"), 0400); err != nil {
					t.Fatal(err)
				}
			case "symlink":
				if err := os.Symlink("missing", target); err != nil {
					t.Fatal(err)
				}
			case "public staging":
				if err := os.Chmod(root, 0755); err != nil {
					t.Fatal(err)
				}
			case "traversal":
				a.Name = "../input.whl"
			case "credential URL":
				a.URL = "https://user:secret@files.pythonhosted.org/input.whl"
			case "foreign URL":
				a.URL = "https://example.com/input.whl"
			}
			client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
				if r.Header.Get("Authorization") != "" || r.Header.Get("Cookie") != "" || r.Header.Get("Accept-Encoding") != "identity" {
					t.Error("unexpected request credentials/encoding")
				}
				if _, ok := r.Context().Deadline(); !ok {
					t.Error("missing deadline")
				}
				if kind == "network error" {
					return nil, errors.New("secret response must not escape")
				}
				response.Body = io.NopCloser(strings.NewReader(body))
				return response, nil
			})}
			err = downloadArtifact(ctx, int(parent.Fd()), a, client)
			if kind == "valid" {
				if err != nil {
					t.Fatal(err)
				}
				data, err := os.ReadFile(target)
				if err != nil || string(data) != body {
					t.Fatal("wrong download", err)
				}
				info, err := os.Stat(target)
				if err != nil || info.Mode().Perm() != 0400 {
					t.Fatal("unsafe artifact mode", err)
				}
				return
			}
			if err == nil || strings.Contains(err.Error(), "secret") {
				t.Fatal("missing refusal or leaked error", err)
			}
			if kind == "existing" {
				data, err := os.ReadFile(target)
				if err != nil || string(data) != "existing" {
					t.Fatal("existing file changed", err)
				}
				return
			}
			if kind == "symlink" {
				info, err := os.Lstat(target)
				if err != nil || info.Mode()&os.ModeSymlink == 0 {
					t.Fatal("existing link changed", err)
				}
				return
			}
			if _, err := os.Lstat(target); !os.IsNotExist(err) {
				t.Fatal("partial artifact retained", err)
			}
		})
	}
}

func TestPreparationClientRejectsRedirectEscapes(t *testing.T) {
	client := preparationHTTPClient()
	defer client.CloseIdleConnections()
	if client.Transport.(*http.Transport).Proxy != nil {
		t.Fatal("inherited proxy")
	}
	for _, raw := range []string{"http://github.com/x", "https://example.com/x", "https://github.com:443/x", "https://user:pass@github.com/x", "https://github.com/x#fragment"} {
		u, err := url.Parse(raw)
		if err != nil {
			t.Fatal(err)
		}
		if client.CheckRedirect(&http.Request{URL: u}, nil) == nil {
			t.Fatal("accepted redirect", raw)
		}
	}
	u, _ := url.Parse("https://release-assets.githubusercontent.com/x?signature=opaque")
	if client.CheckRedirect(&http.Request{URL: u}, nil) != nil {
		t.Fatal("refused release asset")
	}
	if client.CheckRedirect(&http.Request{URL: u}, make([]*http.Request, 5)) == nil {
		t.Fatal("unbounded redirects")
	}
}

func TestDefaultCatalogIsPinnedAndIndependent(t *testing.T) {
	for _, pair := range [][2]string{{"linux", "arm64"}, {"darwin", "amd64"}, {"windows", "amd64"}} {
		if _, err := DefaultCatalog(pair[0], pair[1]); !errors.Is(err, ErrUnsupported) {
			t.Fatal("unsupported platform admitted", pair, err)
		}
	}
	a, err := DefaultCatalog("darwin", "arm64")
	if err != nil {
		t.Fatal(err)
	}
	if len(a.Wheels) != 5 || a.Runtime.Size != 25147663 || LockDigest() != "sha256:a9b45fd1379d4d31cd7765c0a8b53b547b42762ba359d6aaaf0af949e726f428" {
		t.Fatal("catalog drift")
	}
	for _, wheel := range a.Wheels {
		if !strings.Contains(Lock, "--hash=sha256:"+wheel.SHA256+"\n") {
			t.Fatal("wheel not locked")
		}
	}
	a.Wheels[0].SHA256 = "changed"
	b, _ := DefaultCatalog("darwin", "arm64")
	if b.Wheels[0].SHA256 == "changed" {
		t.Fatal("shared mutable catalog")
	}
}
