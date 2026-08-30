package processsupervisor

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
)

func TestMaterializeOutputSchemaUsesPrivateSupervisorFileAndBoundsStdin(t *testing.T) {
	temporary := t.TempDir()
	invocation := contracts.Invocation{
		Argv:         []string{"/fixture/codex", "exec", "--output-schema", contracts.OutputSchemaPlaceholder, "-"},
		Stdin:        []byte("untrusted ticket prompt"),
		OutputSchema: []byte(`{"type":"object"}`),
	}
	arguments, _, err := materializeInvocationFiles(invocation, temporary)
	if err != nil || arguments[3] == contracts.OutputSchemaPlaceholder || filepath.Dir(arguments[3]) != temporary {
		t.Fatalf("arguments=%q err=%v", arguments, err)
	}
	info, err := os.Stat(arguments[3])
	if err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("schema permissions=%v err=%v", info.Mode(), err)
	}
	contents, err := os.ReadFile(arguments[3])
	if err != nil || string(contents) != string(invocation.OutputSchema) {
		t.Fatalf("schema contents=%q err=%v", contents, err)
	}
	invocation.Stdin = make([]byte, 64<<10+1)
	if _, _, err := materializeInvocationFiles(invocation, temporary); err == nil {
		t.Fatal("oversized provider stdin was accepted")
	}
	invocation.Stdin = nil
	invocation.Argv = []string{"/fixture/codex", "exec", "-"}
	if _, _, err := materializeInvocationFiles(invocation, temporary); err == nil {
		t.Fatal("schema without a placeholder was accepted")
	}
}

func TestRunKeyRequiresExactClaimIdentity(t *testing.T) {
	base := contracts.DrainRequest{
		ClaimID: 7, Identity: domain.ProviderIdentity{Provider: "p", Model: "m", Family: "f", Version: "v"},
		Ref: domain.TicketRef{Channel: domain.ChannelDev, Project: "project", Ticket: "SF-key"}, Phase: domain.PhaseBuild, Role: "builder", Attempt: 2,
		LeaderEpoch: 3, RunnerEpoch: 4, ExpectedVersion: 5, LeaseKey: "lease", BindingDigest: "binding", BinaryDigest: "binary",
		Repository: "/repo", Worktree: "/worktree", WorktreeIdentity: "identity", BaseSHA: "base", RequestDigest: strings.Repeat("e", 64),
	}
	for name, mutate := range map[string]func(*contracts.DrainRequest){
		"role":              func(request *contracts.DrainRequest) { request.Role = "reviewer" },
		"leader":            func(request *contracts.DrainRequest) { request.LeaderEpoch++ },
		"runner":            func(request *contracts.DrainRequest) { request.RunnerEpoch++ },
		"binding":           func(request *contracts.DrainRequest) { request.BindingDigest = "other" },
		"policy":            func(request *contracts.DrainRequest) { request.PolicyDigest = "other" },
		"worktree":          func(request *contracts.DrainRequest) { request.Worktree = "/other" },
		"worktree identity": func(request *contracts.DrainRequest) { request.WorktreeIdentity = "other" },
		"request digest":    func(request *contracts.DrainRequest) { request.RequestDigest = strings.Repeat("f", 64) },
	} {
		t.Run(name, func(t *testing.T) {
			changed := base
			mutate(&changed)
			if key(base) == key(changed) {
				t.Fatal("mismatched claim identity reused the same supervisor run key")
			}
		})
	}
}

func TestDrainContextHonorsCallerDeadlineAndTotalBudget(t *testing.T) {
	supervisor, err := New(nil)
	if err != nil {
		t.Fatal(err)
	}
	supervisor.SoftDrain, supervisor.HardDrain = 80*time.Millisecond, 80*time.Millisecond
	caller, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	started := time.Now()
	bounded, boundedCancel, err := supervisor.drainContext(caller)
	if err != nil {
		t.Fatal(err)
	}
	defer boundedCancel()
	<-bounded.Done()
	if elapsed := time.Since(started); elapsed > 75*time.Millisecond {
		t.Fatalf("caller deadline was not honored: %s", elapsed)
	}
	started = time.Now()
	bounded, boundedCancel, err = supervisor.drainContext(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer boundedCancel()
	<-bounded.Done()
	if elapsed := time.Since(started); elapsed > 230*time.Millisecond {
		t.Fatalf("soft+hard drain budget was exceeded: %s", elapsed)
	}
}

func TestDrainPersistedLeaderGoneAfterSetsidRemainsUnclear(t *testing.T) {
	supervisor, err := New(nil)
	if err != nil {
		t.Fatal(err)
	}
	boot, err := hostBootIdentity()
	if err != nil {
		t.Fatal(err)
	}
	request := contracts.DrainRequest{ClaimID: 99, RequestDigest: strings.Repeat("a", 64)}
	// A setsid child can outlive a vanished leader. With no durable descendant
	// witness, v1 rejects recovery instead of claiming whole-tree containment.
	launch := contracts.ProviderLaunch{PID: 999999, PGID: 999999, BootIdentity: boot, ProcessStartIdentity: "old-leader", Worktree: "/worktree"}
	if _, err := supervisor.DrainPersisted(context.Background(), request, launch); !errors.Is(err, ErrUnclear) {
		t.Fatalf("leader-gone recovery was accepted: %v", err)
	}
}

func TestRunRequiresRegisteredMatchingExecutable(t *testing.T) {
	supervisor, err := New(nil)
	if err != nil {
		t.Fatal(err)
	}
	identity := domain.ProviderIdentity{Provider: "fixture", Model: "model", Family: "family", Version: "v1"}
	input := contracts.PhaseInput{Worktree: t.TempDir()}
	request := contracts.DrainRequest{ClaimID: 1, Identity: identity, Ref: domain.TicketRef{Channel: domain.ChannelDev, Project: "p", Ticket: "SF-executable"}, Phase: domain.PhaseBuild, Role: "builder", Attempt: 1, LeaderEpoch: 1, RunnerEpoch: 1, ExpectedVersion: 1, LeaseKey: "lease", BindingDigest: "binding", Worktree: input.Worktree}
	if _, err := supervisor.Run(context.Background(), request, contracts.Invocation{Argv: []string{"/bin/sh"}}, input); err == nil {
		t.Fatal("unregistered executable was accepted")
	}
	binaryDigest, err := supervisor.RegisterExecutable(identity, "/bin/sh")
	if err != nil {
		t.Fatal(err)
	}
	request.BinaryDigest = binaryDigest
	request.PolicyDigest = supervisor.PolicyDigest()
	for _, path := range []string{"/bin/echo"} {
		if _, err := supervisor.Run(context.Background(), request, contracts.Invocation{Argv: []string{path}}, input); err == nil {
			t.Fatalf("mismatched executable %q was accepted", path)
		}
	}
}

func TestRunRequiresBothDurableBindingDigests(t *testing.T) {
	supervisor, err := New(nil)
	if err != nil {
		t.Fatal(err)
	}
	identity := domain.ProviderIdentity{Provider: "fixture", Model: "model", Family: "family", Version: "v1"}
	binaryDigest, err := supervisor.RegisterExecutable(identity, "/bin/sh")
	if err != nil {
		t.Fatal(err)
	}
	request := contracts.DrainRequest{ClaimID: 1, Identity: identity, Ref: domain.TicketRef{Channel: domain.ChannelDev, Project: "p", Ticket: "SF-digest"}, Phase: domain.PhaseBuild, Role: "builder", Attempt: 1, LeaderEpoch: 1, RunnerEpoch: 1, ExpectedVersion: 1, LeaseKey: "lease", BindingDigest: "binding", BinaryDigest: binaryDigest, PolicyDigest: supervisor.PolicyDigest(), Worktree: t.TempDir()}
	for name, mutate := range map[string]func(*contracts.DrainRequest){
		"binary": func(r *contracts.DrainRequest) { r.BinaryDigest = "" },
		"policy": func(r *contracts.DrainRequest) { r.PolicyDigest = "" },
	} {
		t.Run(name, func(t *testing.T) {
			changed := request
			mutate(&changed)
			if _, err := supervisor.Run(context.Background(), changed, contracts.Invocation{Argv: []string{"/bin/sh"}}, contracts.PhaseInput{Worktree: changed.Worktree}); err == nil {
				t.Fatal("execution without complete binding digests was accepted")
			}
		})
	}
}

func TestRapidPIDReuseWithSameSecondDisplayIdentityNeverMatches(t *testing.T) {
	// Both processes could render as the same ps lstart second. The durable
	// kernel identity includes microseconds (Darwin) or start ticks (Linux), so
	// the replacement cannot pass the no-signal verification gate.
	launch := contracts.ProviderLaunch{PID: 4242, PGID: 4242, BootIdentity: "darwin:boot", ProcessStartIdentity: "darwin:1724934896:101", Worktree: "/tmp/worktree"}
	if persistedIdentityMatches(launch, "darwin:1724934896:102", 4242) {
		t.Fatal("rapidly reused PID was accepted for signalling")
	}
	if persistedIdentityMatches(launch, launch.ProcessStartIdentity, 4243) {
		t.Fatal("foreign process group was accepted for signalling")
	}
}

func TestRecoveryLivenessRules(t *testing.T) {
	if !bootIdentityChanged("linux:old-boot", "linux:new-boot") {
		t.Fatal("reboot did not prove the old process namespace dead")
	}
	if !missingLeaderGroupGone(syscall.ESRCH, syscall.ESRCH) {
		t.Fatal("missing leader with no group members did not release")
	}
	if missingLeaderGroupGone(syscall.ESRCH, nil) || missingLeaderGroupGone(syscall.ESRCH, errors.New("foreign group")) {
		t.Fatal("surviving or foreign group was released")
	}
}

func TestReadBoundedFileDoesNotFollowCredentialSymlink(t *testing.T) {
	directory := t.TempDir()
	credential := filepath.Join(directory, "auth.json")
	artifact := filepath.Join(directory, "output-last-message.json")
	if err := os.WriteFile(credential, []byte(`{"access_token":"must-not-leak"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(credential, artifact); err != nil {
		t.Fatal(err)
	}
	if _, _, err := readBoundedFile(artifact, 1<<20); err == nil {
		t.Fatal("final artifact reader followed a credential symlink")
	}
}

func TestReadBoundedFileAuthenticatesOpenedRegularFile(t *testing.T) {
	directory := t.TempDir()
	artifact := filepath.Join(directory, "output-last-message.json")
	if err := os.WriteFile(artifact, []byte(`{"ok":true}`), 0o600); err != nil {
		t.Fatal(err)
	}
	contents, truncated, err := readBoundedFile(artifact, 1<<20)
	if err != nil || truncated || string(contents) != `{"ok":true}` {
		t.Fatalf("contents=%q truncated=%v err=%v", contents, truncated, err)
	}
}

func TestRequestMatchesInputRequiresEveryDurableBinding(t *testing.T) {
	identity := domain.ProviderIdentity{Provider: "fixture", Model: "model", Family: "family", Version: "v1"}
	ref := domain.TicketRef{Channel: domain.ChannelDev, Project: "project", Ticket: "SF-binding"}
	request := contracts.DrainRequest{ClaimID: 1, Identity: identity, Ref: ref, Phase: domain.PhaseBuild, Role: "builder", Attempt: 2, LeaderEpoch: 3, RunnerEpoch: 4, ExpectedVersion: 5, LeaseKey: "lease", BindingDigest: "binding", BinaryDigest: "binary", PolicyDigest: "policy", Repository: "/repo", Worktree: "/worktree", WorktreeIdentity: "identity", BaseSHA: "base"}
	input := contracts.PhaseInput{Ticket: ref, Phase: domain.PhaseBuild, Attempt: 2, LeaderEpoch: 3, RunnerEpoch: 4, ExpectedVersion: 5, Provider: identity, Repository: "/repo", Worktree: "/worktree", WorktreeIdentity: "identity", BaseSHA: "base"}
	if !requestMatchesInput(request, input) {
		t.Fatal("matching durable request was rejected")
	}
	for name, mutate := range map[string]func(*contracts.PhaseInput){
		"ticket":            func(value *contracts.PhaseInput) { value.Ticket.Ticket = "SF-other" },
		"phase":             func(value *contracts.PhaseInput) { value.Phase = domain.PhasePlanning },
		"attempt":           func(value *contracts.PhaseInput) { value.Attempt++ },
		"leader":            func(value *contracts.PhaseInput) { value.LeaderEpoch++ },
		"runner":            func(value *contracts.PhaseInput) { value.RunnerEpoch++ },
		"ticket version":    func(value *contracts.PhaseInput) { value.ExpectedVersion++ },
		"provider":          func(value *contracts.PhaseInput) { value.Provider.Model = "other" },
		"repository":        func(value *contracts.PhaseInput) { value.Repository = "/other" },
		"worktree":          func(value *contracts.PhaseInput) { value.Worktree = "/other" },
		"worktree identity": func(value *contracts.PhaseInput) { value.WorktreeIdentity = "other" },
		"base":              func(value *contracts.PhaseInput) { value.BaseSHA = "other" },
	} {
		t.Run(name, func(t *testing.T) {
			changed := input
			mutate(&changed)
			if requestMatchesInput(request, changed) {
				t.Fatal("mismatched phase input was accepted")
			}
		})
	}
}
