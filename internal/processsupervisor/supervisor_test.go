package processsupervisor

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"syscall"
	"testing"

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
	arguments, err := materializeOutputSchema(invocation, temporary)
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
	if _, err := materializeOutputSchema(invocation, temporary); err == nil {
		t.Fatal("oversized provider stdin was accepted")
	}
	invocation.Stdin = nil
	invocation.Argv = []string{"/fixture/codex", "exec", "-"}
	if _, err := materializeOutputSchema(invocation, temporary); err == nil {
		t.Fatal("schema without a placeholder was accepted")
	}
}

func TestRunKeyRequiresExactClaimIdentity(t *testing.T) {
	base := contracts.DrainRequest{
		ClaimID: 7, Identity: domain.ProviderIdentity{Provider: "p", Model: "m", Family: "f", Version: "v"},
		Ref: domain.TicketRef{Channel: domain.ChannelDev, Project: "project", Ticket: "SF-key"}, Phase: domain.PhaseBuild, Role: "builder", Attempt: 2,
		LeaderEpoch: 3, RunnerEpoch: 4, ExpectedVersion: 5, LeaseKey: "lease", BindingDigest: "binding", BinaryDigest: "binary",
		Repository: "/repo", Worktree: "/worktree", WorktreeIdentity: "identity", BaseSHA: "base",
	}
	for name, mutate := range map[string]func(*contracts.DrainRequest){
		"role":              func(request *contracts.DrainRequest) { request.Role = "reviewer" },
		"leader":            func(request *contracts.DrainRequest) { request.LeaderEpoch++ },
		"runner":            func(request *contracts.DrainRequest) { request.RunnerEpoch++ },
		"binding":           func(request *contracts.DrainRequest) { request.BindingDigest = "other" },
		"policy":            func(request *contracts.DrainRequest) { request.PolicyDigest = "other" },
		"worktree":          func(request *contracts.DrainRequest) { request.Worktree = "/other" },
		"worktree identity": func(request *contracts.DrainRequest) { request.WorktreeIdentity = "other" },
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
