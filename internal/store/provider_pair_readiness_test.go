package store

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/processsupervisor"
)

func TestCurrentAttestedProviderPairChecksEveryRoleAndRestart(t *testing.T) {
	for _, change := range []string{"none", "planner", "builder", "reviewer", "unsigned_planner", "restart", "key", "database"} {
		t.Run(change, func(t *testing.T) {
			db, ctx := openTestStore(t)
			supervisor, err := processsupervisor.New(nil)
			if err != nil {
				t.Fatal(err)
			}
			leader, err := db.AcquireLeader(ctx, domain.ChannelDev, "readiness")
			if err != nil {
				t.Fatal(err)
			}
			if err := db.SetRecoveryAuthority(ctx, domain.ChannelDev, leader, supervisor.PublicKey()); err != nil {
				t.Fatal(err)
			}
			ids := make([]int64, 0, 3)
			for i, provider := range []string{"cursor", "codex", "claude"} {
				q := qualificationValue(strings.Repeat(string(rune('a'+i)), 32), provider, provider+"-family", QualificationGuarded)
				q.AuthMode = []string{"cursor_browser", "chatgpt_subscription", "claude_subscription"}[i]
				q.ProbeDigest = strings.Repeat("d", 64)
				proof, err := supervisor.AttestQualification(contracts.QualificationAttestation{Channel: q.Channel, RunID: q.RunID, Identity: q.Provider, BinaryDigest: q.BinaryDigest, PolicyDigest: q.PolicyDigest, FixtureDigest: q.FixtureDigest, AuthDigest: strings.Repeat("e", 64), AuthMode: q.AuthMode, ProbeDigest: strings.Repeat("d", 64), Profile: contracts.ProfileGuarded, CreatedUnixNanos: q.CreatedAt.UnixNano(), LeaderEpoch: leader, Nonce: q.RunID})
				if err != nil {
					t.Fatal(err)
				}
				stored, _, err := db.RecordAttestedProviderQualification(ctx, q, proof)
				if err != nil {
					t.Fatal(err)
				}
				ids = append(ids, stored.ID)
			}
			if _, _, err := db.SelectProviderSet(ctx, domain.ChannelDev, ids[0], ids[1], ids[2], time.Now()); err != nil {
				t.Fatal(err)
			}
			switch change {
			case "unsigned_planner":
				if _, err := db.db.ExecContext(ctx, `UPDATE provider_qualifications SET auth_mode='' WHERE id=?`, ids[0]); err != nil {
					t.Fatal(err)
				}
			case "planner", "builder", "reviewer":
				index := map[string]int{"planner": 0, "builder": 1, "reviewer": 2}[change]
				if _, err := db.db.ExecContext(ctx, `UPDATE provider_qualifications SET attestation_signature=zeroblob(64) WHERE id=?`, ids[index]); err != nil {
					t.Fatal(err)
				}
			case "restart":
				if _, err := db.AcquireLeader(ctx, domain.ChannelDev, "restart"); err != nil {
					t.Fatal(err)
				}
			case "key":
				other, err := processsupervisor.New(nil)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := db.db.ExecContext(ctx, `UPDATE daemon_instances SET recovery_public_key=? WHERE channel=?`, other.PublicKey(), domain.ChannelDev); err != nil {
					t.Fatal(err)
				}
			case "database":
				if err := db.Close(); err != nil {
					t.Fatal(err)
				}
			}
			_, err = db.CurrentAttestedProviderPair(ctx, domain.ChannelDev)
			if change == "none" {
				if err != nil {
					t.Fatal(err)
				}
			} else if change == "database" {
				if err == nil || errors.Is(err, ErrProviderQualificationNotCurrent) {
					t.Fatalf("database error=%v", err)
				}
			} else if !errors.Is(err, ErrProviderQualificationNotCurrent) {
				t.Fatalf("stale evidence error=%v", err)
			}
		})
	}
}
