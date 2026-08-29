package config

import (
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
		MergeMode:    domain.MergeManual,
		PhaseTimeout: 10 * time.Minute,
	})
	if err != nil {
		t.Fatalf("resolve narrowing: %v", err)
	}
	if resolved.MergeMode != domain.MergeManual || resolved.PhaseTimeout != 10*time.Minute {
		t.Fatalf("unexpected resolved config: %+v", resolved)
	}

	_, err = Resolve(machine, project, TicketOverride{MergeMode: domain.MergeAutonomous})
	if err == nil {
		t.Fatal("expected autonomy widening to fail")
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
