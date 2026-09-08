package localruntime

import (
	"context"
	"errors"
	"testing"

	"github.com/nysa-company/sf/internal/config"
	"github.com/nysa-company/sf/internal/daemon"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/store"
)

func TestProjectStartChecksBothFrozenRecipes(t *testing.T) {
	for _, test := range []struct {
		name    string
		argv    []string
		allowed bool
	}{
		{"go", []string{"go", "test", "./..."}, true},
		{"node", []string{"node", "--test"}, true},
		{"python", []string{"python", "-m", "pytest"}, false},
		{"rails", []string{"bundle", "exec", "rails", "test"}, false},
		{"npm", []string{"npm", "test"}, false},
		{"shell", []string{"sh", "-c", "go test ./..."}, false},
	} {
		for _, role := range []string{"verify", "review"} {
			t.Run(test.name+"/"+role, func(t *testing.T) {
				project := config.DefaultProject("readiness", "/tmp/readiness")
				project.Commands.Verify.Argv = []string{"go", "test", "./..."}
				project.Commands.Review.Argv = []string{"go", "test", "./..."}
				if role == "verify" {
					project.Commands.Verify.Argv = test.argv
				} else {
					project.Commands.Review.Argv = test.argv
				}
				frozen, err := config.Resolve(config.DefaultMachineLimits(), project, config.TicketOverride{})
				if err != nil {
					t.Fatal(err)
				}
				payload, digest, err := config.Snapshot(frozen)
				if err != nil {
					t.Fatal(err)
				}
				value := store.Project{Channel: domain.ChannelDev, ID: "readiness", Path: project.Repository, ConfigGeneration: 1, ConfigSnapshot: payload, ConfigDigest: digest}
				err = checkProjectRecipes(value)
				if test.allowed && err != nil || !test.allowed && !errors.Is(err, daemon.ErrStartRecipeUnsupported) {
					t.Fatalf("allowed=%v err=%v", test.allowed, err)
				}
				value.ConfigSnapshot = append(value.ConfigSnapshot, ' ')
				if checkProjectRecipes(value) == nil {
					t.Fatal("changed snapshot accepted")
				}
			})
		}
	}
	if checkProjectRecipes(store.Project{}) == nil {
		t.Fatal("missing snapshot accepted")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if !errors.Is(CheckProjectStart(ctx, store.Project{}), context.Canceled) {
		t.Fatal("cancelled check continued")
	}
}
