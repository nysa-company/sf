package store

import (
	"strings"
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/processsupervisor"
)

func TestCredentialBearingProviderQualificationRequiresExactCurrentSignature(t *testing.T) {
	for _, tc := range []struct{ provider, mode string }{
		{"codex", "chatgpt_subscription"},
		{"claude", "claude_subscription"},
		{"cursor", "cursor_api"},
		{"cursor", "cursor_browser"},
	} {
		t.Run(tc.provider, func(t *testing.T) {
			database, ctx := openTestStore(t)
			value := qualificationValue(strings.Repeat("a", 32), tc.provider, "fixture-family", QualificationGuarded)
			value.AuthMode, value.ProbeDigest = tc.mode, strings.Repeat("d", 64)
			if _, _, err := database.RecordProviderQualification(ctx, value); err == nil {
				t.Fatal("credential-bearing unsigned qualification admitted")
			}
			supervisor, err := processsupervisor.New(nil)
			if err != nil {
				t.Fatal(err)
			}
			leader, err := database.AcquireLeader(ctx, domain.ChannelDev, "multi-provider-qualification")
			if err != nil || database.SetRecoveryAuthority(ctx, domain.ChannelDev, leader, supervisor.PublicKey()) != nil {
				t.Fatal("could not set recovery authority")
			}
			proof, err := supervisor.AttestQualification(contracts.QualificationAttestation{
				Channel: value.Channel, RunID: value.RunID, Identity: value.Provider,
				BinaryDigest: value.BinaryDigest, PolicyDigest: value.PolicyDigest,
				FixtureDigest: value.FixtureDigest, AuthDigest: strings.Repeat("e", 64),
				AuthMode: value.AuthMode, ProbeDigest: value.ProbeDigest,
				Profile: contracts.ProfileGuarded, CreatedUnixNanos: value.CreatedAt.UnixNano(),
				LeaderEpoch: leader, Nonce: value.RunID,
			})
			if err != nil {
				t.Fatal(err)
			}
			stored, created, err := database.RecordAttestedProviderQualification(ctx, value, proof)
			if err != nil || !created || !database.QualificationCurrent(ctx, domain.ChannelDev, stored) {
				t.Fatalf("signed qualification: created=%v err=%v", created, err)
			}
			binding := contracts.RuntimeBinding{Identity: stored.Provider, BinaryDigest: stored.BinaryDigest, PolicyDigest: stored.PolicyDigest, FixtureDigest: stored.FixtureDigest, AuthDigest: stored.AuthDigest, AuthMode: stored.AuthMode}
			admitted := func(binding contracts.RuntimeBinding) bool {
				t.Helper()
				conn, err := database.db.Conn(ctx)
				if err != nil {
					t.Fatal(err)
				}
				defer conn.Close()
				_, err = currentRuntimeQualification(ctx, conn, domain.ChannelDev, "", binding)
				return err == nil
			}
			if !admitted(binding) {
				t.Fatal("exact signed runtime binding refused")
			}
			wrongAuth := binding
			wrongAuth.AuthDigest = strings.Repeat("f", 64)
			if admitted(wrongAuth) {
				t.Fatal("runtime admitted with wrong auth digest")
			}
			wrongMode := binding
			wrongMode.AuthMode = ""
			if admitted(wrongMode) {
				t.Fatal("signed runtime downgraded to credential-free fixture")
			}
			if tc.provider == "cursor" {
				otherMode := binding
				otherMode.AuthMode = "cursor_browser"
				if tc.mode == "cursor_browser" {
					otherMode.AuthMode = "cursor_api"
				}
				if admitted(otherMode) {
					t.Fatal("runtime switched Cursor billing/auth route without qualification")
				}
			}
			changed := stored
			changed.AuthDigest = strings.Repeat("f", 64)
			if database.QualificationCurrent(ctx, domain.ChannelDev, changed) {
				t.Fatal("changed auth digest accepted")
			}
			if _, created, err := database.RecordAttestedProviderQualification(ctx, value, proof); err != nil || created {
				t.Fatalf("exact replay: created=%v err=%v", created, err)
			}
			if _, err := database.AcquireLeader(ctx, domain.ChannelDev, "restart"); err != nil {
				t.Fatal(err)
			}
			if database.QualificationCurrent(ctx, domain.ChannelDev, stored) {
				t.Fatal("previous leader qualification remained current")
			}
			if admitted(binding) {
				t.Fatal("runtime admitted using previous leader signature")
			}
			if _, _, err := database.RecordAttestedProviderQualification(ctx, value, proof); err == nil {
				t.Fatal("old supervisor replay admitted after takeover")
			}
		})
	}
}

func TestAttestedProviderAuthModesAreClosed(t *testing.T) {
	for _, tc := range []struct{ provider, mode string }{
		{"claude", "chatgpt_subscription"}, {"cursor", "claude_subscription"},
		{"codex", "cursor_api"}, {"unknown", "cursor_api"}, {"claude", ""},
	} {
		value := qualificationValue(strings.Repeat("a", 32), tc.provider, "fixture-family", QualificationGuarded)
		value.AuthMode = tc.mode
		value.AuthDigest, value.ProbeDigest = strings.Repeat("b", 64), strings.Repeat("c", 64)
		value.AttestedLeaderEpoch, value.AttestationSignature = 1, make([]byte, 64)
		value.CreatedAt = time.Now()
		if _, _, err := normalizeQualification(value); err == nil {
			t.Fatalf("accepted unsupported auth class %s/%s", tc.provider, tc.mode)
		}
	}
}
