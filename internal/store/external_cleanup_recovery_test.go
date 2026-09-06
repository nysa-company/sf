package store

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/hostidentity"
)

func cleanupRecoveryFixture(t *testing.T) (*Store, context.Context, uint64, hostidentity.Identity) {
	t.Helper()
	ctx := context.Background()
	s, err := Open(ctx, filepath.Join(t.TempDir(), "sf.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	leader, err := s.AcquireLeader(ctx, domain.ChannelDev, "recovery-test")
	if err != nil {
		t.Fatal(err)
	}
	if err := s.QuarantineExternalMutations(ctx); err != nil {
		t.Fatal(err)
	}
	return s, ctx, leader, hostidentity.Identity{MachineDigest: strings.Repeat("a", 64), BootID: "11111111-1111-1111-1111-111111111111"}
}

func assertCleanupQuarantined(t *testing.T, s *Store, want bool) {
	t.Helper()
	got, err := s.ExternalMutationsQuarantined(context.Background())
	if err != nil || got != want {
		t.Fatalf("quarantine=%v err=%v", got, err)
	}
}

func TestExternalCleanupRecoveryRequiresSameHostNewBootAndCurrentLeader(t *testing.T) {
	s, ctx, leader, host := cleanupRecoveryFixture(t)
	if _, err := s.recoverExternalCleanupAt(ctx, domain.ChannelStable, leader, host); !errors.Is(err, ErrStaleFence) {
		t.Fatalf("missing channel leader=%v", err)
	}
	if _, err := s.recoverExternalCleanupAt(ctx, domain.ChannelDev, leader, host); !errors.Is(err, ErrExternalRecovery) {
		t.Fatalf("missing checkpoint=%v", err)
	}
	for i := 0; i < 2; i++ {
		result, err := s.prepareExternalCleanupRecoveryAt(ctx, domain.ChannelDev, leader, host)
		if err != nil || result.State != "reboot_required" {
			t.Fatalf("prepare=%+v err=%v", result, err)
		}
	}
	assertCleanupQuarantined(t, s, true)
	if _, err := s.recoverExternalCleanupAt(ctx, domain.ChannelDev, leader, host); !errors.Is(err, ErrExternalRecoveryReboot) {
		t.Fatalf("same boot=%v", err)
	}
	other := host
	other.MachineDigest = strings.Repeat("b", 64)
	other.BootID = "22222222-2222-2222-2222-222222222222"
	if _, err := s.recoverExternalCleanupAt(ctx, domain.ChannelDev, leader, other); !errors.Is(err, ErrExternalRecovery) {
		t.Fatalf("different host=%v", err)
	}
	if _, err := s.prepareExternalCleanupRecoveryAt(ctx, domain.ChannelDev, leader, other); !errors.Is(err, ErrExternalRecovery) {
		t.Fatalf("checkpoint replacement=%v", err)
	}
	next, err := s.AcquireLeader(ctx, domain.ChannelDev, "reboot-test")
	if err != nil {
		t.Fatal(err)
	}
	host.BootID = other.BootID
	if _, err := s.recoverExternalCleanupAt(ctx, domain.ChannelDev, leader, host); !errors.Is(err, ErrStaleFence) {
		t.Fatalf("stale leader=%v", err)
	}
	assertCleanupQuarantined(t, s, true)
	result, err := s.recoverExternalCleanupAt(ctx, domain.ChannelDev, next, host)
	if err != nil || result.State != "recovered" {
		t.Fatalf("recover=%+v err=%v", result, err)
	}
	assertCleanupQuarantined(t, s, false)
	result, err = s.recoverExternalCleanupAt(ctx, domain.ChannelDev, next, host)
	if err != nil || result.State != "clear" {
		t.Fatalf("replay=%+v err=%v", result, err)
	}
	for _, table := range []string{"external_cleanup_recovery_checkpoints", "external_cleanup_recoveries"} {
		var count int
		if err := s.db.QueryRow("SELECT COUNT(*) FROM " + table).Scan(&count); err != nil || count != 1 {
			t.Fatalf("audit count=%d err=%v", count, err)
		}
		if _, err := s.db.Exec("DELETE FROM " + table); err == nil {
			t.Fatal("audit deletion accepted")
		}
		if _, err := s.db.Exec("UPDATE " + table + " SET quarantine_observed_at='changed'"); err == nil {
			t.Fatal("audit update accepted")
		}
	}
}

func TestExternalCleanupCheckpointDoesNotUnlockMutationGate(t *testing.T) {
	s, ctx, leader, host := cleanupRecoveryFixture(t)
	if err := s.mutations.lock(ctx); err != nil {
		t.Fatal(err)
	}
	defer s.mutations.unlock()
	if _, err := s.prepareExternalCleanupRecoveryAt(ctx, domain.ChannelDev, leader, host); err != nil {
		t.Fatal(err)
	}
	sameBoot, sameCancel := context.WithTimeout(ctx, time.Second)
	defer sameCancel()
	if _, err := s.recoverExternalCleanupAt(sameBoot, domain.ChannelDev, leader, host); !errors.Is(err, ErrExternalRecoveryReboot) {
		t.Fatalf("latched same-boot recovery hid reboot prerequisite: %v", err)
	}
	host.BootID = "22222222-2222-2222-2222-222222222222"
	short, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	if _, err := s.recoverExternalCleanupAt(short, domain.ChannelDev, leader, host); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("held gate=%v", err)
	}
	assertCleanupQuarantined(t, s, true)
}

func TestExternalCleanupRecoveryRejectsTamperedCheckpoint(t *testing.T) {
	s, ctx, leader, host := cleanupRecoveryFixture(t)
	if _, err := s.prepareExternalCleanupRecoveryAt(ctx, domain.ChannelDev, leader, host); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec(`DROP TRIGGER external_cleanup_checkpoints_immutable_update; UPDATE external_cleanup_recovery_checkpoints SET checkpoint='{}'`); err != nil {
		t.Fatal(err)
	}
	host.BootID = "22222222-2222-2222-2222-222222222222"
	if _, err := s.recoverExternalCleanupAt(ctx, domain.ChannelDev, leader, host); !errors.Is(err, ErrExternalRecovery) {
		t.Fatalf("tamper=%v", err)
	}
	assertCleanupQuarantined(t, s, true)
}

func TestExternalCleanupV58PreservesLegacyQuarantine(t *testing.T) {
	path := filepath.Join(t.TempDir(), "legacy.sqlite")
	createDatabaseAtVersion(t, path, 57)
	raw, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	_, err = raw.Exec(`INSERT INTO external_mutation_quarantine VALUES(1,'cleanup_uncertain','2026-09-01T00:00:00Z')`)
	raw.Close()
	if err != nil {
		t.Fatal(err)
	}
	s, err := Open(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	assertCleanupQuarantined(t, s, true)
	var count int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM external_cleanup_recovery_checkpoints`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("migration fabricated checkpoint=%d err=%v", count, err)
	}
}

func TestExternalCleanupRecoveryRejectsChangedQuarantine(t *testing.T) {
	s, ctx, leader, host := cleanupRecoveryFixture(t)
	if _, err := s.prepareExternalCleanupRecoveryAt(ctx, domain.ChannelDev, leader, host); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec(`UPDATE external_mutation_quarantine SET observed_at='2026-09-02T00:00:00Z'`); err != nil {
		t.Fatal(err)
	}
	host.BootID = "22222222-2222-2222-2222-222222222222"
	if _, err := s.recoverExternalCleanupAt(ctx, domain.ChannelDev, leader, host); !errors.Is(err, ErrExternalRecovery) {
		t.Fatalf("changed quarantine=%v", err)
	}
	assertCleanupQuarantined(t, s, true)
}

func TestExternalCleanupRecoveryAuditAndRetirementAreAtomic(t *testing.T) {
	s, ctx, leader, host := cleanupRecoveryFixture(t)
	if _, err := s.prepareExternalCleanupRecoveryAt(ctx, domain.ChannelDev, leader, host); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec(`CREATE TRIGGER injected_cleanup_failure BEFORE DELETE ON external_mutation_quarantine BEGIN SELECT RAISE(ABORT,'injected'); END`); err != nil {
		t.Fatal(err)
	}
	host.BootID = "22222222-2222-2222-2222-222222222222"
	if _, err := s.recoverExternalCleanupAt(ctx, domain.ChannelDev, leader, host); err == nil {
		t.Fatal("failed retirement returned success")
	}
	assertCleanupQuarantined(t, s, true)
	var count int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM external_cleanup_recoveries`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("rollback left recovery evidence=%d err=%v", count, err)
	}
}
