package config

import (
	"path/filepath"
	"testing"

	"github.com/nysa-company/sf/internal/domain"
)

func TestStableAndDevPathsAreDisjoint(t *testing.T) {
	home := t.TempDir()
	stable, err := PathsFor(home, domain.ChannelStable)
	if err != nil {
		t.Fatal(err)
	}
	dev, err := PathsFor(home, domain.ChannelDev)
	if err != nil {
		t.Fatal(err)
	}
	if stable.Root == dev.Root || stable.Database == dev.Database || stable.Socket == dev.Socket {
		t.Fatalf("channel paths overlap: stable=%+v dev=%+v", stable, dev)
	}
	if stable.Root != filepath.Join(home, "Library", "Application Support", "sf", "stable") {
		t.Fatalf("stable root=%s", stable.Root)
	}
}
