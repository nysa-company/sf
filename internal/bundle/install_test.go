package bundle

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestFailedPayloadCopyRetainsNonExecutablePartialFile(t *testing.T) {
	root := t.TempDir()
	source, destination := filepath.Join(root, "source"), filepath.Join(root, "destination")
	if err := os.WriteFile(source, []byte("wrong bytes"), 0600); err != nil {
		t.Fatal(err)
	}
	entry := File{Name: "sf-dev", Size: 11, Mode: 0755, SHA256: strings.Repeat("0", 64)}
	if err := copyPayload(context.Background(), source, destination, entry); err == nil {
		t.Fatal("accepted hash mismatch")
	}
	info, err := os.Stat(destination)
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatalf("partial mode=%v err=%v", info, err)
	}
	if err := copyPayload(context.Background(), source, destination, entry); err == nil {
		t.Fatal("overwrote partial file")
	}
}

func TestPayloadCopyRefusesSourceSymlink(t *testing.T) {
	root := t.TempDir()
	source, link, destination := filepath.Join(root, "source"), filepath.Join(root, "link"), filepath.Join(root, "destination")
	if err := os.WriteFile(source, []byte("bytes"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(source, link); err != nil {
		t.Fatal(err)
	}
	if err := copyPayload(context.Background(), link, destination, File{Mode: 0755}); err == nil {
		t.Fatal("accepted source symlink")
	}
	if _, err := os.Lstat(destination); !os.IsNotExist(err) {
		t.Fatal("created destination from unsafe source")
	}
}
