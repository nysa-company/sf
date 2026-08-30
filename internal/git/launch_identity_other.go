//go:build !darwin

package git

import "github.com/nysa-company/sf/internal/contracts"

func gitLaunchIdentity(int) (contracts.GitMutationLaunch, error) {
	// Non-macOS builds retain the conservative gate but cannot claim a durable
	// launch identity without a platform-specific observer.
	return contracts.GitMutationLaunch{}, ErrIdentityMismatch
}
