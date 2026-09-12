package authoring

import (
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"unicode"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/redact"
	"golang.org/x/sys/unix"
)

// Snapshot opens each path from the registered root through no-follow dir FDs.
// The provider receives only these approved bytes on stdin, never file tools.
func Snapshot(root string, paths []string) (string, string, error) {
	if !filepath.IsAbs(root) || filepath.Clean(root) != root || root == "/" {
		return "", "", ErrContent
	}
	if len(paths) > 8 {
		return "", "", ErrContent
	}
	if len(paths) == 0 {
		return "", contracts.AuthoringDigest(nil), nil
	}
	fd, err := unix.Open(root, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return "", "", ErrContent
	}
	defer unix.Close(fd)
	files := map[string]string{}
	total := 0
	for _, path := range paths {
		if len(path) > 1024 || !filepath.IsLocal(path) || filepath.Clean(path) != path || strings.ContainsAny(path, "\\\x00\r\n") {
			return "", "", ErrContent
		}
		for _, r := range path {
			if unicode.IsControl(r) || unicode.Is(unicode.Cf, r) {
				return "", "", ErrContent
			}
		}
		for _, part := range strings.Split(path, string(filepath.Separator)) {
			lower := strings.ToLower(part)
			if strings.HasPrefix(part, ".") || strings.Contains(lower, "credential") || strings.Contains(lower, "secret") || strings.Contains(lower, "token") || strings.HasSuffix(lower, ".pem") || strings.HasSuffix(lower, ".key") || lower == "id_rsa" {
				return "", "", ErrContent
			}
		}
		if _, exists := files[path]; exists {
			return "", "", ErrContent
		}
		parent, err := unix.Dup(fd)
		if err != nil {
			return "", "", ErrContent
		}
		parts := strings.Split(path, string(filepath.Separator))
		for i, part := range parts {
			flags := unix.O_RDONLY | unix.O_NOFOLLOW | unix.O_NONBLOCK | unix.O_CLOEXEC
			if i < len(parts)-1 {
				flags |= unix.O_DIRECTORY
			}
			next, e := unix.Openat(parent, part, flags, 0)
			unix.Close(parent)
			if e != nil {
				return "", "", ErrContent
			}
			parent = next
		}
		file := os.NewFile(uintptr(parent), path)
		info, e := file.Stat()
		if e != nil || !info.Mode().IsRegular() || info.Size() > 16<<10 {
			file.Close()
			return "", "", ErrContent
		}
		data, e := io.ReadAll(io.LimitReader(file, 16<<10+1))
		closeErr := file.Close()
		if e != nil || closeErr != nil || len(data) > 16<<10 || !SafeText(string(data), 16<<10) {
			return "", "", ErrContent
		}
		if redact.NewPolicy("", nil).String(string(data)) != string(data) {
			return "", "", ErrContent
		}
		total += len(data)
		if total > 64<<10 {
			return "", "", ErrContent
		}
		files[path] = string(data)
	}
	data, _ := json.Marshal(files)
	return string(data), contracts.AuthoringDigest(data), nil
}
