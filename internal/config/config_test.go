package config

import (
	"crypto/sha256"
	"encoding/hex"
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/domain"
)

func guardedProject() Project {
	return Project{
		Name:                 "nysa",
		Repository:           "/tmp/nysa",
		BaseBranch:           "main",
		MergeMode:            domain.MergeGuarded,
		MergeMethod:          "squash",
		MaxConcurrentTickets: 2,
		PhaseTimeout:         30 * time.Minute,
		TicketTimeout:        2 * time.Hour,
		Commands: Commands{
			Verify: Command{Argv: []string{"make", "test-focused"}},
			Review: Command{Argv: []string{"make", "test"}},
		},
		Providers: ProviderOrder{
			Planner:  []string{"cursor", "claude"},
			Builder:  []string{"cursor", "claude"},
			Reviewer: []string{"claude", "codex"},
		},
	}
}

func TestResolveAllowsOnlyNarrowing(t *testing.T) {
	machine := DefaultMachineLimits()
	project := guardedProject()

	resolved, err := Resolve(machine, project, TicketOverride{
		MergeMode:       domain.MergeManual,
		PhaseTimeout:    10 * time.Minute,
		MaxCostMicroUSD: 20_000_000,
	})
	if err != nil {
		t.Fatalf("resolve narrowing: %v", err)
	}
	if resolved.MergeMode != domain.MergeManual || resolved.PhaseTimeout != 10*time.Minute || resolved.MaxTicketCostMicroUSD != 20_000_000 {
		t.Fatalf("unexpected resolved config: %+v", resolved)
	}

	_, err = Resolve(machine, project, TicketOverride{MergeMode: domain.MergeAutonomous})
	if err == nil {
		t.Fatal("expected autonomy widening to fail")
	}
	_, err = Resolve(machine, project, TicketOverride{MaxCostMicroUSD: machine.MaxTicketCostMicroUSD + 1})
	if err == nil {
		t.Fatal("expected cost widening to fail")
	}
}

func TestResolveRejectsInvalidCostBounds(t *testing.T) {
	machine := DefaultMachineLimits()
	project := guardedProject()
	project.MaxTicketCostMicroUSD = machine.MaxTicketCostMicroUSD + 1
	if _, err := Resolve(machine, project, TicketOverride{}); err == nil {
		t.Fatal("project cost above machine bound accepted")
	}
	project.MaxTicketCostMicroUSD = 0
	if _, err := Resolve(machine, project, TicketOverride{MaxCostMicroUSD: -1}); err == nil {
		t.Fatal("negative ticket cost accepted")
	}
}

func TestAutonomousRequiresMachinePermission(t *testing.T) {
	machine := DefaultMachineLimits()
	project := guardedProject()
	project.MergeMode = domain.MergeAutonomous
	if _, err := Resolve(machine, project, TicketOverride{}); err == nil {
		t.Fatal("expected autonomous mode to fail closed")
	}
}

func TestSnapshotIsStable(t *testing.T) {
	effective, err := Resolve(DefaultMachineLimits(), guardedProject(), TicketOverride{})
	if err != nil {
		t.Fatal(err)
	}
	first, firstDigest, err := Snapshot(effective)
	if err != nil {
		t.Fatal(err)
	}
	second, secondDigest, err := Snapshot(effective)
	if err != nil {
		t.Fatal(err)
	}
	if string(first) != string(second) || firstDigest != secondDigest {
		t.Fatal("configuration snapshot is not deterministic")
	}
}

func TestDecodeSnapshotAuthenticatesCanonicalFrozenAuthority(t *testing.T) {
	value, err := Resolve(DefaultMachineLimits(), DefaultProject("nysa", "/tmp/nysa"), TicketOverride{})
	if err != nil {
		t.Fatal(err)
	}
	data, digest, err := Snapshot(value)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeSnapshot(data, digest)
	if err != nil || decoded.Name != "nysa" || decoded.MaxConcurrentTickets != 2 || decoded.Machine.MaxConcurrentTickets != 2 {
		t.Fatalf("decoded=%+v err=%v", decoded, err)
	}
	changed := append([]byte(nil), data...)
	changed[len(changed)-1] ^= 1
	if _, err := DecodeSnapshot(changed, digest); err == nil {
		t.Fatal("changed snapshot was accepted")
	}
	withUnknown := append(data[:len(data)-1], []byte(`,"unknown":true}`)...)
	unknownDigest := sha256.Sum256(withUnknown)
	if _, err := DecodeSnapshot(withUnknown, hex.EncodeToString(unknownDigest[:])); err == nil {
		t.Fatal("unknown snapshot field was accepted")
	}
}

func TestResolveRejectsUnsafeProjectAndProviderIdentities(t *testing.T) {
	machine := DefaultMachineLimits()
	for name, mutate := range map[string]func(*Project){
		"project path component": func(project *Project) { project.Name = "../nysa" },
		"unsafe base":            func(project *Project) { project.BaseBranch = "refs/heads/../main" },
		"provider flag":          func(project *Project) { project.Providers.Builder = []string{"--dangerously-skip"} },
	} {
		t.Run(name, func(t *testing.T) {
			project := guardedProject()
			mutate(&project)
			if _, err := Resolve(machine, project, TicketOverride{}); err == nil {
				t.Fatal("unsafe identity was accepted")
			}
		})
	}
}
