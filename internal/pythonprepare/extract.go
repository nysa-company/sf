package pythonprepare

import (
	"archive/tar"
	"archive/zip"
	"compress/gzip"
	"context"
	"errors"
	"io"
	"os"
	"path"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"golang.org/x/sys/unix"
)

const maxEntries = 20000
const maxFileBytes int64 = 64 << 20
const maxTreeBytes int64 = 256 << 20

type contextReader struct {
	ctx context.Context
	r   io.Reader
}

func (r contextReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.r.Read(p)
}

type extraction struct {
	ctx     context.Context
	root    int
	entries int
	bytes   int64
	headers map[string]bool
	files   map[string]bool
}

func newExtraction(ctx context.Context, fd int) (*extraction, error) {
	if ctx == nil || fd < 0 {
		return nil, ErrArtifact
	}
	var stat unix.Stat_t
	if unix.Fstat(fd, &stat) != nil || stat.Mode&unix.S_IFMT != unix.S_IFDIR || stat.Uid != uint32(os.Geteuid()) || stat.Mode&0077 != 0 || stat.Mode&07000 != 0 {
		return nil, ErrArtifact
	}
	copyFD, err := unix.Openat(fd, ".", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, ErrArtifact
	}
	dir := os.NewFile(uintptr(copyFD), "python-extraction-root")
	names, readErr := dir.Readdirnames(1)
	dir.Close()
	if len(names) != 0 || (readErr != nil && !errors.Is(readErr, io.EOF)) {
		return nil, ErrArtifact
	}
	return &extraction{ctx: ctx, root: fd, headers: map[string]bool{}, files: map[string]bool{}}, nil
}

func validArchivePath(p string) bool {
	return p != "" && p != "." && p != ".." && !strings.HasPrefix(p, "../") && !strings.HasPrefix(p, "/") && path.Clean(p) == p && !strings.Contains(p, "\\") && len(p) <= 4096 && strings.Count(p, "/") < 32 && utf8.ValidString(p) && strings.IndexFunc(p, unicode.IsControl) < 0
}

func (e *extraction) header(p string) error {
	if !validArchivePath(p) || e.headers[p] || len(e.headers) >= maxEntries {
		return ErrArtifact
	}
	e.headers[p] = true
	return e.ctx.Err()
}

// parent creates only private real directories via retained descriptors. No
// archive pathname is reopened through an ambient absolute path.
func (e *extraction) parent(p string) (*os.File, error) {
	fd, err := unix.Openat(e.root, ".", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, ErrArtifact
	}
	current := os.NewFile(uintptr(fd), "python-extraction-parent")
	if p == "." {
		return current, nil
	}
	for _, part := range strings.Split(p, "/") {
		if err := e.ctx.Err(); err != nil {
			current.Close()
			return nil, err
		}
		if e.entries >= maxEntries {
			current.Close()
			return nil, ErrArtifact
		}
		err := unix.Mkdirat(int(current.Fd()), part, 0700)
		if err == nil {
			e.entries++
		} else if !errors.Is(err, unix.EEXIST) {
			current.Close()
			return nil, ErrArtifact
		}
		next, err := unix.Openat(int(current.Fd()), part, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
		current.Close()
		if err != nil {
			return nil, ErrArtifact
		}
		current = os.NewFile(uintptr(next), "python-extraction-parent")
	}
	return current, nil
}

func (e *extraction) write(p string, size int64, executable bool, r io.Reader) error {
	if size < 0 || size > maxFileBytes || e.bytes > maxTreeBytes-size || e.entries >= maxEntries {
		return ErrArtifact
	}
	parent, err := e.parent(path.Dir(p))
	if err != nil {
		return err
	}
	defer parent.Close()
	fd, err := unix.Openat(int(parent.Fd()), path.Base(p), unix.O_WRONLY|unix.O_CREAT|unix.O_EXCL|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0600)
	if err != nil {
		return ErrArtifact
	}
	file := os.NewFile(uintptr(fd), "python-extraction-file")
	defer file.Close()
	e.entries++
	e.bytes += size
	n, err := io.Copy(file, io.LimitReader(contextReader{e.ctx, r}, size+1))
	if err != nil || n != size {
		return ErrArtifact
	}
	mode := os.FileMode(0400)
	if executable {
		mode = 0500
	}
	if file.Chmod(mode) != nil || file.Sync() != nil {
		return ErrArtifact
	}
	e.files[p] = true
	return file.Close()
}

// These unused launcher/metadata aliases are omitted, never extracted as
// symlinks. Their exact internal regular targets must nevertheless exist.
var runtimeAliases = map[string]string{
	"bin/idle3": "idle3.13", "bin/pydoc3": "pydoc3.13", "bin/python": "python3.13",
	"bin/python3": "python3.13", "bin/python3-config": "python3.13-config",
	"lib/pkgconfig/python3-embed.pc": "python-3.13-embed.pc", "lib/pkgconfig/python3.pc": "python-3.13.pc",
	"share/man/man1/python3.1": "python3.13.1",
}

// extractRuntime accepts only an already hash-verified retained archive. The
// caller excludes source/staging writers and removes staging after any error.
func extractRuntime(ctx context.Context, source *os.File, root int) error {
	if ctx == nil || source == nil {
		return ErrArtifact
	}
	bounded, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	e, err := newExtraction(bounded, root)
	if err != nil {
		return err
	}
	if _, err := source.Seek(0, io.SeekStart); err != nil {
		return ErrArtifact
	}
	z, err := gzip.NewReader(contextReader{bounded, source})
	if err != nil {
		return ErrArtifact
	}
	defer z.Close()
	// Bound decompression including TAR padding and headers, not just files.
	limited := &io.LimitedReader{R: contextReader{bounded, z}, N: 512 << 20}
	reader := tar.NewReader(limited)
	aliases := map[string]string{}
	for {
		h, err := reader.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return ErrArtifact
		}
		name := strings.TrimSuffix(h.Name, "/")
		if name == "python" && h.Typeflag == tar.TypeDir {
			if err := e.header("python"); err != nil {
				return err
			}
			continue
		}
		if !strings.HasPrefix(name, "python/") {
			return ErrArtifact
		}
		name = strings.TrimPrefix(name, "python/")
		if err := e.header(name); err != nil {
			return err
		}
		switch h.Typeflag {
		case tar.TypeDir:
			if h.Size != 0 {
				return ErrArtifact
			}
			dir, err := e.parent(name)
			if err != nil {
				return err
			}
			dir.Close()
		case tar.TypeReg, tar.TypeRegA:
			if err := e.write(name, h.Size, h.Mode&0111 != 0, reader); err != nil {
				return err
			}
		case tar.TypeSymlink:
			if expected, ok := runtimeAliases[name]; !ok || expected != h.Linkname || h.Size != 0 {
				return ErrArtifact
			}
			aliases[name] = path.Join(path.Dir(name), h.Linkname)
		default:
			return ErrArtifact
		}
	}
	// Force gzip checksum validation even when TAR ended before its trailer.
	if _, err := io.Copy(io.Discard, limited); err != nil || limited.N == 0 {
		return ErrArtifact
	}
	for _, target := range aliases {
		if !e.files[target] {
			return ErrArtifact
		}
	}
	if !e.files["bin/python3.13"] {
		return ErrArtifact
	}
	return bounded.Err()
}

func supportedWheelMetadata(data []byte) bool {
	wanted := map[string]string{"Wheel-Version": "1.0", "Root-Is-Purelib": "true", "Tag": "py3-none-any"}
	counts := map[string]int{}
	for _, line := range strings.Split(string(data), "\n") {
		key, value, ok := strings.Cut(strings.TrimSuffix(line, "\r"), ":")
		if expected, found := wanted[key]; found {
			if !ok || strings.TrimSpace(value) != expected {
				return false
			}
			counts[key]++
		}
	}
	for key := range wanted {
		if counts[key] != 1 {
			return false
		}
	}
	return true
}

// extractWheels handles only the catalog's pure, directly importable wheels.
// Relocation, native extensions and path hooks are explicitly unsupported.
func extractWheels(ctx context.Context, sources []*os.File, root int) error {
	if ctx == nil || len(sources) == 0 || len(sources) > 16 {
		return ErrArtifact
	}
	bounded, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	e, err := newExtraction(bounded, root)
	if err != nil {
		return err
	}
	for _, source := range sources {
		if source == nil {
			return ErrArtifact
		}
		info, err := source.Stat()
		if err != nil || info.Size() > 32<<20 {
			return ErrArtifact
		}
		z, err := zip.NewReader(source, info.Size())
		if err != nil || len(z.File) > maxEntries {
			return ErrArtifact
		}
		metadata := 0
		for _, f := range z.File {
			name := strings.TrimSuffix(f.Name, "/")
			if !validArchivePath(name) || f.Flags&1 != 0 || (f.Method != zip.Store && f.Method != zip.Deflate) || f.UncompressedSize64 > uint64(maxFileBytes) || (!f.Mode().IsRegular() && !f.Mode().IsDir()) {
				return ErrArtifact
			}
			if strings.Contains(name, ".data/") || strings.HasSuffix(name, ".pth") || strings.HasSuffix(name, ".so") || strings.HasSuffix(name, ".dylib") {
				return ErrArtifact
			}
			if strings.HasSuffix(name, ".dist-info/WHEEL") {
				if f.UncompressedSize64 > 8192 {
					return ErrArtifact
				}
				r, err := f.Open()
				if err != nil {
					return ErrArtifact
				}
				data, err := io.ReadAll(io.LimitReader(contextReader{bounded, r}, 8193))
				r.Close()
				if err != nil || !supportedWheelMetadata(data) {
					return ErrArtifact
				}
				metadata++
			}
		}
		if metadata != 1 {
			return ErrArtifact
		}
		for _, f := range z.File {
			name := strings.TrimSuffix(f.Name, "/")
			if err := e.header(name); err != nil {
				return err
			}
			if f.Mode().IsDir() {
				if f.UncompressedSize64 != 0 {
					return ErrArtifact
				}
				dir, err := e.parent(name)
				if err != nil {
					return err
				}
				dir.Close()
				continue
			}
			r, err := f.Open()
			if err != nil {
				return ErrArtifact
			}
			err = e.write(name, int64(f.UncompressedSize64), false, r)
			closeErr := r.Close()
			if err != nil || closeErr != nil {
				return ErrArtifact
			}
		}
	}
	return bounded.Err()
}
