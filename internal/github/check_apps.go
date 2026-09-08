package github

import (
	"context"
	"strings"

	"github.com/nysa-company/sf/internal/contracts"
)

// Authenticate a ruleset's app-specific checks against the exact commit and
// exact run URLs returned by gh's required-check query. Names alone cannot
// distinguish a trusted integration from another app reporting the same name.
func (c Client) authenticateCheckApps(ctx context.Context, identity contracts.PullRequestIdentity, protection strictProtectionWitness, checks []checkWire) (map[string]int64, error) {
	appBound := make(map[string]bool)
	if protection.Kind == "ruleset" {
		for _, configured := range protection.Checks {
			parts := strings.SplitN(configured, "\x00", 2)
			if len(parts) == 2 && parts[1] != "-" && parts[1] != "0" {
				appBound[parts[0]] = true
			}
		}
	}
	if len(appBound) == 0 {
		return nil, nil
	}
	var response struct {
		Total int `json:"total_count"`
		Runs  []struct {
			ID         int64  `json:"id"`
			Name       string `json:"name"`
			Head       string `json:"head_sha"`
			URL        string `json:"details_url"`
			Status     string `json:"status"`
			Conclusion string `json:"conclusion"`
			App        int64  `json:"app_id"`
		} `json:"check_runs"`
	}
	// Project documented REST fields before strict decoding. A full page or
	// inconsistent total is ambiguous; never infer completeness from a subset.
	const projection = "{total_count,check_runs:[.check_runs[]|{id,name,head_sha,details_url,status,conclusion,app_id:.app.id}]}"
	endpoint := "repos/" + repoArg(identity.Repository) + "/commits/" + identity.HeadOID + "/check-runs?per_page=100&filter=latest"
	if err := c.json(ctx, &response, "api", endpoint, "--jq", projection); err != nil {
		return nil, err
	}
	if response.Total != len(response.Runs) || response.Total <= 0 || response.Total >= 100 {
		return nil, ErrChecksFailed
	}
	apps := make(map[string]int64)
	ids := make(map[int64]bool)
	for _, run := range response.Runs {
		if run.ID <= 0 || ids[run.ID] || run.Head != identity.HeadOID || run.App <= 0 {
			return nil, ErrChecksFailed
		}
		ids[run.ID] = true
	}
	for _, check := range checks {
		// Unbound contexts may be legacy commit statuses, which have no
		// check-run entry. The caller still validates the complete required
		// context set; only positive integration IDs need this app proof.
		if !appBound[check.Name] {
			continue
		}
		if _, duplicate := apps[check.Name]; duplicate {
			return nil, ErrChecksFailed
		}
		matches := 0
		for _, run := range response.Runs {
			if run.Name != check.Name || run.URL != check.Link {
				continue
			}
			state := strings.ToUpper(run.Status)
			if run.Status == "completed" {
				state = strings.ToUpper(run.Conclusion)
			} else if run.Conclusion != "" {
				return nil, ErrChecksFailed
			}
			if state != check.State {
				return nil, ErrChecksFailed
			}
			matches++
			apps[check.Name] = run.App
		}
		if matches != 1 {
			return nil, ErrChecksFailed
		}
	}
	return apps, nil
}
