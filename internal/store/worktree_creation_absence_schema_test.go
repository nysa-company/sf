package store

import (
	"context"
	"database/sql"
	"path/filepath"
	"strings"
	"testing"
)

func TestV57GitMutationLeaseObservationOnlyColumn(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.sqlite")
	createDatabaseAtVersion(t, path, 56)
	db, err := Open(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var version int
	if err := db.db.QueryRow(`SELECT MAX(version) FROM schema_migrations`).Scan(&version); err != nil || version != schemaVersion {
		t.Fatalf("schema version=%d err=%v", version, err)
	}
	var ddl sql.NullString
	if err := db.db.QueryRow(`SELECT sql FROM sqlite_master WHERE type='table' AND name='git_mutation_leases'`).Scan(&ddl); err != nil {
		t.Fatal(err)
	}
	if !ddl.Valid || !strings.Contains(ddl.String, "observation_only INTEGER NOT NULL DEFAULT 0 CHECK(observation_only IN (0,1))") {
		t.Fatalf("git mutation lease DDL missing observation-only discriminator: %q", ddl.String)
	}
}
