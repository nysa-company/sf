//go:build !darwin

package git

import "github.com/nysa-company/sf/internal/contracts"

func persistedGitLaunchStatus(contracts.GitMutationLaunch) (bool, bool, error) {
	return false, false, ErrIdentityMismatch
}
