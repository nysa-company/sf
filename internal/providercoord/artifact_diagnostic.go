//go:build !sf_e2e

package providercoord

import "github.com/nysa-company/sf/internal/contracts"

// Native acceptance diagnostics are excluded from ordinary builds. Durable
// failure evidence remains the closed Store-owned ArtifactFailureReason.
func reportBuilderValidationFailure(error) {}

func reportPlannerValidationFailure(error) {}

func reportProviderRunFailure(contracts.CommandResult, bool, error) {}
