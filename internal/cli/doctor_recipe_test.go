package cli

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/store"
)

func TestDoctorRecipePreviewIsSeparateAndSanitized(t *testing.T) {
	for _, allowed := range []bool{true, false} {
		deps := healthyDoctorDeps(t)
		deps.Repo = "/tmp/selected-repository"
		deps.Worktree = func(context.Context, string) error { return nil }
		deps.Pair = func(context.Context, domain.Channel) (store.ProviderPair, error) { return qualifiedDoctorPair(), nil }
		deps.Attempts = func(context.Context, domain.Channel) ([]store.ProviderAttempt, error) { return nil, nil }
		deps.AuthStatus = completeDoctorAuth
		calls := 0
		deps.Recipe = func(_ context.Context, repository string) error {
			calls++
			if repository != deps.Repo {
				t.Fatalf("wrong repository: %q", repository)
			}
			if !allowed {
				return errors.New("untrusted-secret-value")
			}
			return nil
		}
		report := RunDoctor(context.Background(), deps)
		check := doctorCheckByID(t, report, "repository_recipe")
		if calls != 1 || (check.Status == CheckPass) != allowed || report.GuardedEligible != allowed {
			t.Fatalf("allowed=%v calls=%d report=%+v", allowed, calls, report)
		}
		var output bytes.Buffer
		if err := Render(&output, reportResponse(report), false); err != nil {
			t.Fatal(err)
		}
		if strings.Contains(output.String(), "untrusted-secret-value") || !strings.Contains(output.String(), "not ticket execution or merge approval") {
			t.Fatalf("output=%s", output.String())
		}
		deps.Worktree = func(context.Context, string) error { return errors.New("not a repository") }
		report = RunDoctor(context.Background(), deps)
		if calls != 1 || doctorCheckByID(t, report, "repository_recipe").Status != CheckNotRun {
			t.Fatal("preview ran despite failed repository check")
		}
	}
}

func TestProductionDoctorRecipePreviewDoesNotRegisterOrWrite(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	repository := initializedRepository(t)
	deps := productionDoctorDeps(domain.ChannelDev, repository)
	err := deps.Recipe(context.Background(), repository)
	if (err == nil) != (runtime.GOOS == "darwin") {
		t.Fatalf("preview=%v", err)
	}
	if _, err := os.Lstat(filepath.Join(repository, ".sf")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("preview wrote configuration: %v", err)
	}
	if _, err := os.Lstat(filepath.Join(home, "Library")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("preview created channel state: %v", err)
	}
}
