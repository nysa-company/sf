package pythonprepare

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"strings"
	"time"

	"golang.org/x/sys/unix"
)

var ErrUnsupported = errors.New("Python preparation is unsupported for this platform")
var ErrArtifact = errors.New("Python preparation artifact verification failed")

func safeDownloadURL(u *url.URL) bool {
	if u == nil || u.Scheme != "https" || u.User != nil || u.Port() != "" || u.Fragment != "" {
		return false
	}
	switch u.Host {
	case "github.com", "release-assets.githubusercontent.com", "files.pythonhosted.org":
		return true
	default:
		return false
	}
}

func preparationHTTPClient() *http.Client {
	return &http.Client{
		Timeout:   2 * time.Minute,
		Transport: &http.Transport{Proxy: nil, DisableCompression: true, TLSHandshakeTimeout: 15 * time.Second, ResponseHeaderTimeout: 30 * time.Second},
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if len(via) >= 5 || !safeDownloadURL(req.URL) {
				return ErrArtifact
			}
			return nil
		},
	}
}

// downloadArtifact writes only an exclusive new file beneath an authenticated
// private staging descriptor. The owner excludes concurrent staging writers.
// It removes its own partial file on failure, never a pre-existing artifact.
// Responses and credential-bearing redirect query strings are not surfaced.
func downloadArtifact(ctx context.Context, stagingFD int, a Artifact, client *http.Client) error {
	if ctx == nil || stagingFD < 0 || client == nil || a.Name == "." || a.Name == ".." || path.Base(a.Name) != a.Name || strings.ContainsAny(a.Name, "\\\x00") || a.Name == "" || len(a.Name) > 255 || a.Size <= 0 || a.Size > 32<<20 || len(a.SHA256) != 64 || strings.ToLower(a.SHA256) != a.SHA256 {
		return ErrArtifact
	}
	if _, err := hex.DecodeString(a.SHA256); err != nil {
		return ErrArtifact
	}
	u, err := url.Parse(a.URL)
	if err != nil || !safeDownloadURL(u) {
		return ErrArtifact
	}
	var parent unix.Stat_t
	if unix.Fstat(stagingFD, &parent) != nil || parent.Mode&unix.S_IFMT != unix.S_IFDIR || parent.Uid != uint32(os.Geteuid()) || parent.Mode&0077 != 0 || parent.Mode&07000 != 0 {
		return ErrArtifact
	}
	bounded, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	if err := bounded.Err(); err != nil {
		return err
	}
	fd, err := unix.Openat(stagingFD, a.Name, unix.O_CREAT|unix.O_EXCL|unix.O_WRONLY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0600)
	if err != nil {
		return ErrArtifact
	}
	file := os.NewFile(uintptr(fd), "python-preparation-download")
	ok := false
	defer func() {
		file.Close()
		if !ok {
			_ = unix.Unlinkat(stagingFD, a.Name, 0)
		}
	}()
	req, err := http.NewRequestWithContext(bounded, http.MethodGet, a.URL, nil)
	if err != nil {
		return ErrArtifact
	}
	req.Header.Set("Accept-Encoding", "identity")
	response, err := client.Do(req)
	if err != nil {
		return errors.Join(ErrArtifact, bounded.Err())
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK || (response.ContentLength >= 0 && response.ContentLength != a.Size) || response.Header.Get("Content-Encoding") != "" {
		return ErrArtifact
	}
	hash := sha256.New()
	n, err := io.Copy(io.MultiWriter(file, hash), io.LimitReader(response.Body, a.Size+1))
	if err != nil || n != a.Size || hex.EncodeToString(hash.Sum(nil)) != a.SHA256 {
		return errors.Join(ErrArtifact, bounded.Err())
	}
	if err := bounded.Err(); err != nil {
		return err
	}
	if file.Chmod(0400) != nil || file.Sync() != nil || file.Close() != nil {
		return ErrArtifact
	}
	ok = true
	return nil
}
