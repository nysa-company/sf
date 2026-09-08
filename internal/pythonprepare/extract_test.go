package pythonprepare

import (
	"archive/tar"
	"archive/zip"
	"compress/gzip"
	"context"
	"os"
	"path/filepath"
	"testing"
)

func extractionRoot(t *testing.T) (string, *os.File) {
	t.Helper()
	root := t.TempDir()
	if err := os.Chmod(root, 0700); err != nil {
		t.Fatal(err)
	}
	f, err := os.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { f.Close() })
	return root, f
}

func tarFixture(t *testing.T, headers []*tar.Header) *os.File {
	t.Helper()
	f, err := os.CreateTemp(t.TempDir(), "archive-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { f.Close() })
	z := gzip.NewWriter(f)
	w := tar.NewWriter(z)
	for _, h := range headers {
		if err := w.WriteHeader(h); err != nil {
			t.Fatal(err)
		}
		if h.Typeflag == tar.TypeReg && h.Size > 0 {
			if _, err := w.Write(make([]byte, h.Size)); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if err := z.Close(); err != nil {
		t.Fatal(err)
	}
	return f
}

func TestExtractRuntimeRefusesArchiveEscapes(t *testing.T) {
	for _, kind := range []string{"valid", "traversal", "absolute", "duplicate", "symlink", "known alias", "missing alias target", "hardlink", "device", "cancelled", "occupied"} {
		t.Run(kind, func(t *testing.T) {
			root, dir := extractionRoot(t)
			ctx := t.Context()
			headers := []*tar.Header{{Name: "python/bin/python3.13", Mode: 0755, Size: 3, Typeflag: tar.TypeReg}}
			switch kind {
			case "traversal":
				headers = append(headers, &tar.Header{Name: "python/../escape", Mode: 0644, Typeflag: tar.TypeReg})
			case "absolute":
				headers = append(headers, &tar.Header{Name: "/python/escape", Mode: 0644, Typeflag: tar.TypeReg})
			case "duplicate":
				headers = append(headers, headers[0])
			case "symlink":
				headers = append(headers, &tar.Header{Name: "python/lib", Typeflag: tar.TypeSymlink, Linkname: "../../escape"})
			case "known alias":
				headers = append(headers, &tar.Header{Name: "python/bin/python3", Typeflag: tar.TypeSymlink, Linkname: "python3.13"})
			case "missing alias target":
				headers = append(headers, &tar.Header{Name: "python/bin/idle3", Typeflag: tar.TypeSymlink, Linkname: "idle3.13"})
			case "hardlink":
				headers = append(headers, &tar.Header{Name: "python/linked", Typeflag: tar.TypeLink, Linkname: "python/bin/python3.13"})
			case "device":
				headers = append(headers, &tar.Header{Name: "python/device", Typeflag: tar.TypeChar})
			case "cancelled":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			case "occupied":
				if err := os.WriteFile(filepath.Join(root, "existing"), []byte("keep"), 0400); err != nil {
					t.Fatal(err)
				}
			}
			err := extractRuntime(ctx, tarFixture(t, headers), int(dir.Fd()))
			if kind != "valid" && kind != "known alias" {
				if err == nil {
					t.Fatal("accepted hostile archive")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			info, err := os.Stat(filepath.Join(root, "bin/python3.13"))
			if err != nil || info.Mode().Perm() != 0500 {
				t.Fatal("wrong interpreter mode", err)
			}
			if _, err := os.Lstat(filepath.Join(root, "bin/python3")); !os.IsNotExist(err) {
				t.Fatal("alias materialized as a link", err)
			}
		})
	}
}

type zipEntry struct {
	name, body string
	mode       os.FileMode
}

func wheelFixture(t *testing.T, entries []zipEntry) *os.File {
	t.Helper()
	f, err := os.CreateTemp(t.TempDir(), "wheel-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { f.Close() })
	w := zip.NewWriter(f)
	for _, entry := range entries {
		h := &zip.FileHeader{Name: entry.name, Method: zip.Deflate}
		h.SetMode(entry.mode)
		out, err := w.CreateHeader(h)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := out.Write([]byte(entry.body)); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return f
}

func TestExtractWheelsRequiresPureBoundedLayout(t *testing.T) {
	for _, kind := range []string{"valid", "traversal", "duplicate", "symlink", "native", "pth", "relocation", "metadata", "missing metadata", "cross wheel collision"} {
		t.Run(kind, func(t *testing.T) {
			root, dir := extractionRoot(t)
			entries := []zipEntry{{"sample.dist-info/WHEEL", "Wheel-Version: 1.0\nRoot-Is-Purelib: true\nTag: py3-none-any\n", 0644}, {"sample.py", "value = 1\n", 0644}}
			switch kind {
			case "traversal":
				entries = append(entries, zipEntry{"../escape", "bad", 0644})
			case "duplicate":
				entries = append(entries, entries[1])
			case "symlink":
				entries = append(entries, zipEntry{"link", "../../escape", os.ModeSymlink | 0777})
			case "native":
				entries = append(entries, zipEntry{"sample.so", "native", 0644})
			case "pth":
				entries = append(entries, zipEntry{"sample.pth", "import bad", 0644})
			case "relocation":
				entries = append(entries, zipEntry{"sample.data/scripts/run", "script", 0644})
			case "metadata":
				entries[0].body = "Wheel-Version: 2.0\nRoot-Is-Purelib: true\nTag: py3-none-any\n"
			case "missing metadata":
				entries = entries[1:]
			}
			sources := []*os.File{wheelFixture(t, entries)}
			if kind == "cross wheel collision" {
				sources = append(sources, wheelFixture(t, entries))
			}
			err := extractWheels(t.Context(), sources, int(dir.Fd()))
			if kind != "valid" {
				if err == nil {
					t.Fatal("accepted unsupported wheel")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			data, err := os.ReadFile(filepath.Join(root, "sample.py"))
			if err != nil || string(data) != "value = 1\n" {
				t.Fatal("wrong content", err)
			}
		})
	}
}

func TestExtractionBoundsBeforeCreatingFiles(t *testing.T) {
	for _, kind := range []string{"file", "total", "entries", "depth"} {
		t.Run(kind, func(t *testing.T) {
			root, dir := extractionRoot(t)
			e, err := newExtraction(t.Context(), int(dir.Fd()))
			if err != nil {
				t.Fatal(err)
			}
			size := int64(1)
			switch kind {
			case "file":
				size = maxFileBytes + 1
			case "total":
				e.bytes = maxTreeBytes
			case "entries":
				e.entries = maxEntries
			case "depth":
				if validArchivePath("a/a/a/a/a/a/a/a/a/a/a/a/a/a/a/a/a/a/a/a/a/a/a/a/a/a/a/a/a/a/a/a/a") {
					t.Fatal("unbounded depth")
				}
				return
			}
			if err := e.write("bad", size, false, nil); err == nil {
				t.Fatal("bound ignored")
			}
			if _, err := os.Lstat(filepath.Join(root, "bad")); !os.IsNotExist(err) {
				t.Fatal("created file before bound", err)
			}
		})
	}
}
