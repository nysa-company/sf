package store

import (
	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
	"strings"
	"testing"
)

func TestAuthoringReservationAuthorityIdempotencyAndDrain(t *testing.T) {
	db, ctx := openTestStore(t)
	signer, err := contracts.NewDrainSigner()
	if err != nil {
		t.Fatal(err)
	}
	epoch, err := db.AcquireLeader(ctx, domain.ChannelDev, "authoring-test")
	if err != nil {
		t.Fatal(err)
	}
	if err := db.SetRecoveryAuthority(ctx, domain.ChannelDev, epoch, signer.PublicKey()); err != nil {
		t.Fatal(err)
	}
	digest := strings.Repeat("a", 64)
	session := AuthoringSession{Channel: domain.ChannelDev, ID: "draft-session", Purpose: "ticket_draft", Project: "nysa", ContextDigest: digest, Capability: contracts.AuthoringCapability{Identity: domain.ProviderIdentity{Provider: "claude", Model: "opus", Family: "claude", Version: "2.1.263"}, BinaryDigest: digest, AuthDigest: digest, PolicyDigest: digest}}
	if err := db.CreateAuthoringSession(ctx, session); err != nil {
		t.Fatal(err)
	}
	forged := session
	forged.ContextDigest = strings.Repeat("b", 64)
	if _, _, err := db.ReserveAuthoringTurn(ctx, forged, "turn", digest, epoch); err == nil {
		t.Fatal("forged session accepted")
	}
	turn, created, err := db.ReserveAuthoringTurn(ctx, session, "turn", digest, epoch)
	if err != nil || !created {
		t.Fatalf("created=%v err=%v", created, err)
	}
	replay, created, err := db.ReserveAuthoringTurn(ctx, session, "turn", digest, epoch)
	if err != nil || created || replay.Claim != turn.Claim {
		t.Fatal("reservation replay changed identity")
	}
	if _, _, err := db.ReserveAuthoringTurn(ctx, session, "other", digest, epoch); err == nil {
		t.Fatal("undrained channel admitted another turn")
	}
	proof, err := signer.ProveAuthoringDrained(turn.Claim, epoch)
	if err != nil {
		t.Fatal(err)
	}
	result := contracts.AuthoringResult{Kind: "draft", Title: "Count jobs", Problem: "Return count", Scope: []string{}, Acceptance: []string{"Empty returns zero"}, Assumptions: []string{}}
	if err := db.FinishAuthoringTurn(ctx, turn.Claim, proof, "success", &result); err == nil {
		t.Fatal("success before launch accepted")
	}
	if err := db.FinishAuthoringTurn(ctx, turn.Claim, proof, "cancelled", nil); err != nil {
		t.Fatal(err)
	}
	if err := db.FinishAuthoringTurn(ctx, turn.Claim, proof, "cancelled", nil); err != nil {
		t.Fatal("exact finish replay failed", err)
	}
	if err := db.FinishAuthoringTurn(ctx, turn.Claim, proof, "failed", nil); err == nil {
		t.Fatal("mismatched finish replay accepted")
	}
	for n := 2; n <= 4; n++ {
		key := strings.Repeat("x", n)
		next, created, err := db.ReserveAuthoringTurn(ctx, session, key, digest, epoch)
		if err != nil || !created {
			t.Fatal(err)
		}
		proof, _ := signer.ProveAuthoringDrained(next.Claim, epoch)
		if err := db.FinishAuthoringTurn(ctx, next.Claim, proof, "cancelled", nil); err != nil {
			t.Fatal(err)
		}
	}
	if _, _, err := db.ReserveAuthoringTurn(ctx, session, "fifth", digest, epoch); err == nil {
		t.Fatal("fifth turn accepted")
	}
}

func TestAuthoringNonSuccessCompletesWithEmptyBlobAndExactProof(t *testing.T) {
	for _, outcome := range []string{"cancelled", "failed", "interrupted"} {
		for _, launched := range []bool{false, true} {
			name := outcome + "-reserved"
			if launched {
				name = outcome + "-launched"
			}
			t.Run(name, func(t *testing.T) {
				db, ctx := openTestStore(t)
				signer, err := contracts.NewDrainSigner()
				if err != nil {
					t.Fatal(err)
				}
				epoch, err := db.AcquireLeader(ctx, domain.ChannelDev, "authoring-empty-result")
				if err != nil {
					t.Fatal(err)
				}
				if err := db.SetRecoveryAuthority(ctx, domain.ChannelDev, epoch, signer.PublicKey()); err != nil {
					t.Fatal(err)
				}
				digest := strings.Repeat("a", 64)
				session := AuthoringSession{Channel: domain.ChannelDev, ID: "empty-result", Purpose: "ticket_draft", Project: "nysa", ContextDigest: digest, Capability: contracts.AuthoringCapability{Identity: domain.ProviderIdentity{Provider: "claude", Model: "model", Family: "family", Version: "1"}, BinaryDigest: digest, AuthDigest: digest, PolicyDigest: digest}}
				if err := db.CreateAuthoringSession(ctx, session); err != nil {
					t.Fatal(err)
				}
				turn, created, err := db.ReserveAuthoringTurn(ctx, session, "turn", digest, epoch)
				if err != nil || !created {
					t.Fatal("reservation failed", err)
				}
				if launched {
					if err := db.RecordAuthoringLaunch(ctx, turn.Claim, contracts.ProviderLaunch{PID: 123, PGID: 123, BootIdentity: "fixture-boot", ProcessStartIdentity: "fixture-start", Worktree: "/private/tmp/authoring-empty-result"}); err != nil {
						t.Fatal(err)
					}
				}
				wrong := turn.Claim
				wrong.TurnKey = "other"
				wrongProof, err := signer.ProveAuthoringDrained(wrong, epoch)
				if err != nil {
					t.Fatal(err)
				}
				for _, badProof := range []contracts.AuthoringProof{{}, wrongProof} {
					if err := db.FinishAuthoringTurn(ctx, turn.Claim, badProof, outcome, nil); err == nil {
						t.Fatal("invalid proof released authoring slot")
					}
				}
				pending, err := db.PendingAuthoringTurns(ctx, domain.ChannelDev)
				if err != nil || len(pending) != 1 {
					t.Fatal("invalid proof changed pending turn", err)
				}
				proof, err := signer.ProveAuthoringDrained(turn.Claim, epoch)
				if err != nil {
					t.Fatal(err)
				}
				if err := db.FinishAuthoringTurn(ctx, turn.Claim, proof, outcome, nil); err != nil {
					t.Fatal("valid no-result completion failed", err)
				}
				if err := db.FinishAuthoringTurn(ctx, turn.Claim, proof, outcome, nil); err != nil {
					t.Fatal("exact no-result replay failed", err)
				}
				stored, err := db.AuthoringTurn(ctx, domain.ChannelDev, session.ID, "turn")
				if err != nil || stored.State != "completed" || stored.Outcome != outcome || stored.Result != nil {
					t.Fatal("no-result state did not roundtrip", err)
				}
				var kind string
				var length int
				if err := db.db.QueryRowContext(ctx, `SELECT typeof(result),length(result) FROM authoring_turns WHERE channel=? AND session_id=? AND turn_key=?`, domain.ChannelDev, session.ID, "turn").Scan(&kind, &length); err != nil || kind != "blob" || length != 0 {
					t.Fatalf("result storage kind=%s length=%d err=%v", kind, length, err)
				}
				pending, err = db.PendingAuthoringTurns(ctx, domain.ChannelDev)
				if err != nil || len(pending) != 0 {
					t.Fatal("valid proof left an undrained slot", err)
				}
			})
		}
	}
}
