package github

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/nysa-company/sf/internal/contracts"
)

func TestCIRequiredPolicyAuthenticatesExactCheckApp(t *testing.T) {
	c, fake, identity := fixture(t)
	rules := exactRepositoryRuleset()
	rules.Rules[1].Parameters["required_status_checks"] = []any{map[string]any{"context": "test", "integration_id": 15368}}
	if err := fake.SetRulesetsForTest(rules); err != nil {
		t.Fatal(err)
	}
	pr := createDraft(t, c, identity, "app-bound CI", "body")
	const link = "https://github.com/example/app/actions/runs/9/job/11"
	if err := fake.SetChecks(pr.Identity.Number, contracts.RequiredCheck{Name: "test", ExternalID: link, State: "SUCCESS"}); err != nil {
		t.Fatal(err)
	}
	original := c.runner
	for _, name := range []string{"exact", "wrong-app", "wrong-head", "wrong-url", "wrong-state", "duplicate", "truncated", "full-page", "missing"} {
		t.Run(name, func(t *testing.T) {
			run := map[string]any{"id": 11, "name": "test", "head_sha": pr.Identity.HeadOID, "details_url": link, "status": "completed", "conclusion": "success", "app_id": 15368}
			runs := []any{run}
			total := 1
			switch name {
			case "wrong-app":
				run["app_id"] = 42
			case "wrong-head":
				run["head_sha"] = strings.Repeat("b", 40)
			case "wrong-url":
				run["details_url"] = link + "0"
			case "wrong-state":
				run["conclusion"] = "failure"
			case "duplicate":
				runs = append(runs, run)
				total = 2
			case "truncated":
				total = 2
			case "full-page":
				total = 100
			case "missing":
				runs = nil
				total = 0
			}
			called := false
			c.runner = commandRunnerFunc(func(ctx context.Context, binary string, args, env []string) ([]byte, error) {
				if len(args) > 1 && args[0] == "api" && strings.Contains(args[1], "/check-runs?") {
					called = true
					want := "repos/example/app/commits/" + pr.Identity.HeadOID + "/check-runs?per_page=100&filter=latest"
					if len(args) != 4 || args[1] != want || args[2] != "--jq" {
						t.Fatalf("unexpected check-run argv: %v", args)
					}
					return json.Marshal(map[string]any{"total_count": total, "check_runs": runs})
				}
				return original.Run(ctx, binary, args, env)
			})
			policy, err := c.ObserveCIRequiredCheckPolicy(context.Background(), pr.Identity)
			if name == "exact" {
				if err != nil || len(policy.RequiredChecks) != 1 || !called {
					t.Fatalf("exact app policy=%+v err=%v called=%v", policy, err, called)
				}
			} else if err == nil {
				t.Fatalf("accepted %s", name)
			}
		})
	}
}
