package codexprovider

import (
	"context"
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/store"
)

func TestConfiguredProviderCapacityIsExactAndFailClosed(t *testing.T) {
	for _, test := range []struct {
		value string
		want  int
		ok    bool
	}{
		{"", 1, true}, {"1", 1, true}, {"2", 2, true},
		{" 2", 0, false}, {"2 ", 0, false}, {"0", 0, false}, {"3", 0, false}, {"two", 0, false},
	} {
		t.Run(test.value, func(t *testing.T) {
			t.Setenv("SF_CODEX_PROVIDER_CAPACITY", test.value)
			got, err := configuredProviderCapacity()
			if test.ok {
				if err != nil || got != test.want {
					t.Fatalf("capacity=%d err=%v", got, err)
				}
				return
			}
			if err == nil || got != 0 {
				t.Fatalf("invalid capacity=%d err=%v", got, err)
			}
		})
	}
}

func TestComposeProfilesWithCapacityPropagatesSharedRouteCapacity(t *testing.T) {
	ctx := context.Background()
	database, err := store.Open(ctx, t.TempDir()+"/sf.sqlite")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	builder, _ := adapterFixture(t, "codex-builder-capacity", "gpt-5.6-luna")
	reviewer, _ := adapterFixture(t, "codex-reviewer-capacity", "gpt-5.5")
	attestor := qualificationAttestor(t, database, domain.ChannelDev)
	for _, adapter := range []*Adapter{builder, reviewer} {
		if _, err := Qualify(ctx, database, domain.ChannelDev, adapter, fixture{}, attestor); err != nil {
			t.Fatal(err)
		}
	}
	binding, _ := builder.Binding(ctx)
	builderQ, _ := database.LatestProviderQualification(ctx, domain.ChannelDev, binding.Identity)
	binding, _ = reviewer.Binding(ctx)
	reviewerQ, _ := database.LatestProviderQualification(ctx, domain.ChannelDev, binding.Identity)
	if _, _, err := database.SelectProviderPair(ctx, domain.ChannelDev, builderQ.ID, reviewerQ.ID, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	supervisor := newCompositionSupervisor(t)
	coordinator, err := ComposeProfilesWithCapacity(ctx, domain.ChannelDev, database, supervisor, []Config{
		{Route: builder.route, Executable: builder.executable, AuthHome: builder.authHome, Model: builder.model, Runner: builder.runner},
		{Route: reviewer.route, Executable: reviewer.executable, AuthHome: reviewer.authHome, Model: reviewer.model, Runner: reviewer.runner},
	}, 2)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = coordinator.Close() })
	if err := coordinator.ReadyForPrePublishing(); err != nil {
		t.Fatalf("qualified composition not ready: %v", err)
	}
	for role, route := range composeRoutes(builder.Name(), reviewer.Name(), 2) {
		if route.Capacity != 2 || route.Primary == "" {
			t.Fatalf("route %s=%+v", role, route)
		}
	}
	for role, route := range composeRoutes(builder.Name(), reviewer.Name(), 1) {
		if route.Capacity != 1 {
			t.Fatalf("default route %s=%+v", role, route)
		}
	}
	for _, capacity := range []int{0, 3} {
		if _, err := ComposeProfilesWithCapacity(ctx, domain.ChannelDev, database, supervisor, nil, capacity); err == nil {
			t.Fatalf("capacity %d accepted", capacity)
		}
	}
}
