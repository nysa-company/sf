package processsupervisor

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/config"
	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
	gitboundary "github.com/nysa-company/sf/internal/git"
	"github.com/nysa-company/sf/internal/store"
	"github.com/nysa-company/sf/internal/worktreecoord"
)

// The CLI, credentials, qualification verdict and gate helper are synthetic.
// Store issuance, launch recording, Git inspection and retry persistence are real.
type rejectionProcessStore struct {
	db       *store.Store
	path     string
	claim    store.ProviderAttemptClaim
	request  store.ProviderAttemptRequest
	physical worktreecoord.Coordinator
}

func setupRejectionProcessStore(t *testing.T, ctx context.Context, s *Supervisor, binding contracts.RuntimeBinding) *rejectionProcessStore {
	t.Helper()
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	repo := filepath.Join(root, "repo")
	if err := os.Mkdir(repo, 0700); err != nil {
		t.Fatal(err)
	}
	git := func(dir string, args ...string) {
		t.Helper()
		cmd := exec.CommandContext(ctx, "/usr/bin/git", args...)
		cmd.Dir = dir
		if err := cmd.Run(); err != nil {
			t.Fatal("fixture Git setup", err)
		}
	}
	git(root, "init", "--bare", filepath.Join(root, "remote.git"))
	git(repo, "init", "-b", "main")
	git(repo, "config", "user.name", "fixture")
	git(repo, "config", "user.email", "fixture@example.test")
	if err := os.WriteFile(filepath.Join(repo, "baseline.txt"), []byte("baseline\n"), 0600); err != nil {
		t.Fatal(err)
	}
	git(repo, "add", "baseline.txt")
	git(repo, "commit", "-m", "fixture")
	git(repo, "remote", "add", "origin", filepath.Join(root, "remote.git"))
	git(repo, "push", "origin", "main")
	path := filepath.Join(root, "state.sqlite")
	db, err := store.Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	f := &rejectionProcessStore{db: db, path: path}
	t.Cleanup(func() { _ = f.db.Close() })
	effective, err := config.Resolve(config.DefaultMachineLimits(), config.DefaultProject("p", repo), config.TicketOverride{})
	if err != nil {
		t.Fatal(err)
	}
	effective.Providers.Planner, effective.Providers.Builder = []string{"claude"}, []string{"claude"}
	raw, digest, err := config.Snapshot(effective)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.CreateProject(ctx, store.Project{Channel: domain.ChannelDev, ID: "p", Path: repo, BaseRef: "main", ConfigGeneration: 1, ConfigDigest: digest, ConfigSnapshot: raw}); err != nil {
		t.Fatal(err)
	}
	leader, err := db.AcquireLeader(ctx, domain.ChannelDev, "process-rejection")
	if err != nil {
		t.Fatal(err)
	}
	if err := db.SetRecoveryAuthority(ctx, domain.ChannelDev, leader, s.PublicKey()); err != nil {
		t.Fatal(err)
	}
	var ids []int64
	for index, b := range []contracts.RuntimeBinding{binding, {Identity: domain.ProviderIdentity{Provider: "codex", Model: "fixture", Family: "openai", Version: "fixture"}, BinaryDigest: strings.Repeat("a", 64), PolicyDigest: strings.Repeat("b", 64), FixtureDigest: strings.Repeat("c", 64), AuthDigest: strings.Repeat("d", 64), AuthMode: "chatgpt_subscription"}} {
		now, run := time.Now().UTC(), strings.Repeat(string(rune('a'+index)), 32)
		proof, err := s.Signer.SignQualification(contracts.QualificationAttestation{Channel: domain.ChannelDev, RunID: run, Identity: b.Identity, BinaryDigest: b.BinaryDigest, PolicyDigest: b.PolicyDigest, FixtureDigest: b.FixtureDigest, AuthDigest: b.AuthDigest, AuthMode: b.AuthMode, ProbeDigest: strings.Repeat("e", 64), Profile: contracts.ProfileGuarded, CreatedUnixNanos: now.UnixNano(), LeaderEpoch: leader, Nonce: run})
		if err != nil {
			t.Fatal(err)
		}
		q, _, err := db.RecordAttestedProviderQualification(ctx, store.ProviderQualification{Channel: domain.ChannelDev, RunID: run, Provider: b.Identity, BinaryDigest: b.BinaryDigest, PolicyDigest: b.PolicyDigest, FixtureDigest: b.FixtureDigest, AuthDigest: b.AuthDigest, AuthMode: b.AuthMode, ProbeDigest: proof.ProbeDigest, Profile: store.QualificationGuarded, CreatedAt: now}, proof)
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, q.ID)
	}
	if _, _, err := db.SelectProviderSet(ctx, domain.ChannelDev, ids[0], ids[0], ids[1], time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	ref := domain.TicketRef{Channel: domain.ChannelDev, Project: "p", Ticket: "SF-process-retry"}
	if err := db.CreateTicket(ctx, store.Ticket{Ref: ref, SourceDigest: "source", Type: domain.TicketFeature, MergeMode: domain.MergeGuarded, CreatedAt: time.Now().UTC(), MaxDuration: time.Hour, MaxCostMicroUSD: 100}); err != nil {
		t.Fatal(err)
	}
	ticket, err := db.StartOrAdopt(ctx, ref, 1, "dev/p/SF-process-retry", domain.Fence{LeaderEpoch: leader, RunnerEpoch: 1})
	if err != nil {
		t.Fatal(err)
	}
	fence := domain.Fence{LeaderEpoch: leader, RunnerEpoch: ticket.RunnerEpoch}
	f.physical = worktreecoord.Coordinator{Store: db, Git: gitboundary.Runner{Home: filepath.Join(root, "git-home"), TestLocalTransport: true, MutationAuthority: db, Run: func(ctx context.Context, _ string, args, env []string) ([]byte, error) {
		cmd := exec.CommandContext(ctx, "/usr/bin/git", args...)
		cmd.Env = env
		cmd.WaitDelay = time.Second
		return cmd.Output()
	}}}
	wt, err := f.physical.Ensure(ctx, worktreecoord.EnsureRequest{Ref: ref, Version: ticket.Version, Fence: fence})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.ApproveProviderEstimatedAccounting(ctx, ref, ticket.Version, fence); err != nil {
		t.Fatal(err)
	}
	f.request = store.ProviderAttemptRequest{Ref: ref, ExpectedVersion: ticket.Version, Fence: fence, Phase: domain.PhasePlanning, Role: "planner", Binding: binding, ConfigDigest: digest, Capacity: 1, At: time.Now().UTC(), Repository: repo, Worktree: wt.Path, WorktreeIdentity: string(wt.IdentityJSON), BaseSHA: wt.BaseSHA, SupervisorKey: s.PublicKey(), Input: contracts.PhaseInput{Ticket: ref, Phase: domain.PhasePlanning, Prompt: "fixture", Repository: repo, Worktree: wt.Path, WorktreeIdentity: string(wt.IdentityJSON), BaseSHA: wt.BaseSHA, AllowedPaths: []string{"x"}, Timeout: 30 * time.Second, Profile: contracts.ProfileGuarded, Schema: []byte(`{"type":"object"}`)}}
	f.request.Input.LeaderEpoch = fence.LeaderEpoch
	f.request.Input.RunnerEpoch = fence.RunnerEpoch
	f.request.Input.ExpectedVersion = ticket.Version
	f.request.Input.Provider = binding.Identity
	f.request.Input.AuthMode = binding.AuthMode
	f.claim, err = db.BeginProviderAttempt(ctx, f.request)
	if err != nil {
		t.Fatal(err)
	}
	prior := s.Recorder
	s.Recorder = recordingLaunches(func(ctx context.Context, r contracts.DrainRequest, i Identity, wt string) error {
		if err := prior.RecordLaunch(ctx, r, i, wt); err != nil {
			return err
		}
		return f.db.RecordProviderLaunch(ctx, f.claim, contracts.ProviderLaunch{PID: i.PID, PGID: i.PGID, BootIdentity: i.BootIdentity, ProcessStartIdentity: i.ProcessStartIdentity, Worktree: wt})
	})
	return f
}

func processClaimRequest(c store.ProviderAttemptClaim) contracts.DrainRequest {
	return contracts.DrainRequest{ClaimID: c.ID, Identity: c.Binding.Identity, Ref: c.Ref, Phase: c.Phase, Role: c.Role, Attempt: c.Attempt, ExpectedVersion: c.ExpectedVersion, LeaderEpoch: c.LeaderEpoch, RunnerEpoch: c.RunnerEpoch, LeaseKey: c.LeaseKey, BindingDigest: c.BindingDigest, BinaryDigest: c.Binding.BinaryDigest, PolicyDigest: c.Binding.PolicyDigest, AuthDigest: c.Binding.AuthDigest, AuthMode: c.Binding.AuthMode, Repository: c.Repository, Worktree: c.Worktree, WorktreeIdentity: c.WorktreeIdentity, BaseSHA: c.BaseSHA, RequestDigest: c.RequestDigest}
}

func (f *rejectionProcessStore) finishAndRetry(t *testing.T, ctx context.Context, drain contracts.DrainProof, receipt contracts.ServerRejectionAttestation) {
	t.Helper()
	if err := f.db.FinishProviderAttemptWithServerRejection(ctx, f.claim, drain, receipt, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	if err := f.db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err := store.Open(ctx, f.path)
	if err != nil {
		t.Fatal(err)
	}
	f.db = db
	f.physical.Store = db
	f.request.At = time.Unix(0, receipt.Evidence.ObservedUnixNanos)
	_, err = db.BeginProviderAttempt(ctx, f.request)
	var backoff *store.ProviderRetryBackoffError
	if !errors.As(err, &backoff) {
		t.Fatal("restart lost durable backoff", err)
	}
	f.request.At = backoff.NotBefore
	next, err := db.BeginProviderAttempt(ctx, f.request)
	if err != nil || next.Attempt != 2 || next.Binding != f.claim.Binding {
		t.Fatal("same-binding retry failed", err)
	}
	if _, required, err := db.ProviderRejectionRetryCheckpoint(ctx, next); err != nil || !required {
		t.Fatal("retry lost physical checkpoint requirement", err)
	}
	if _, err := f.physical.InspectProviderAttemptCheckpoint(ctx, next); err != nil {
		t.Fatal("retry physical inspection", err)
	}
	f.claim = next
}
