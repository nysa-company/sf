package github

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/nysa-company/sf/internal/domain"
)

type observationQuarantine struct{ quarantined bool }

func (q observationQuarantine) ExternalMutationsQuarantined(context.Context) (bool, error) {
	return q.quarantined, nil
}
func (observationQuarantine) QuarantineExternalMutations(context.Context) error {
	return nil
}

func TestObserveMergeIntentPreservesReadFailure(t *testing.T) {
	intent := domain.MergeIntent{
		RepositoryHost: "github.com", RepositoryOwner: "owner", RepositoryName: "repo",
		PullRequestNumber: 1, HeadOwner: "owner", HeadRepository: "repo", HeadRef: "sf/dev/ticket",
		HeadOID: strings.Repeat("a", 40), BaseRef: "main", OriginalBaseOID: strings.Repeat("b", 40),
		StrictStatusChecks: true, AdminEnforced: true, ProtectionKind: "classic",
	}
	for _, tc := range []struct {
		name        string
		quarantined bool
		latched     bool
		want        error
	}{
		{"durable quarantine", true, false, ErrProcessCleanup},
		{"unpersisted quarantine", false, true, ErrCleanupQuarantineFatal},
		{"malformed observation", false, false, ErrMalformedResponse},
	} {
		t.Run(tc.name, func(t *testing.T) {
			latched := &atomic.Bool{}
			latched.Store(tc.latched)
			calls := 0
			client := Client{binaryPath: "/bin/echo", home: t.TempDir(), configDir: t.TempDir(),
				quarantiner: observationQuarantine{tc.quarantined}, cleanupLatched: latched,
				runner: commandRunnerFunc(func(context.Context, string, []string, []string) ([]byte, error) {
					calls++
					return []byte("not-json"), nil
				})}
			identity, err := client.ObserveMergeIntent(context.Background(), intent)
			if identity != "" || !errors.Is(err, tc.want) || errors.Is(err, ErrExternalMerged) {
				t.Fatalf("identity=%q err=%v; want original %v, not an external-merge verdict", identity, err, tc.want)
			}
			if (tc.quarantined || tc.latched) && calls != 0 {
				t.Fatalf("quarantine permitted %d command calls", calls)
			}
		})
	}
}
