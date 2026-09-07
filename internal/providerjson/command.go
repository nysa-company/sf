package providerjson

import (
	"context"
	"errors"

	"github.com/nysa-company/sf/internal/contracts"
)

// Command classifies bounded supervisor output. Monetary authority is
// deliberately absent: neither a successful browser login, token counters nor
// a provider's list-price estimate proves an incremental charge. Until a
// Store-authenticated policy accepts a separately recorded estimate observation,
// the coordinator rejects this result as unaccounted. That explicit policy can
// permit completion or bounded artifact repair without setting UsageTrusted;
// command/terminal uncertainty never becomes repairable through accounting.
//
// stderr is never a fallback artifact and provider text never enters errors.
// The caller must authenticate the input/runtime and supervisor drain proof.
func Command(ctx context.Context, input contracts.PhaseInput, command contracts.CommandResult, schemaOutput bool) (contracts.PhaseResult, error) {
	result := contracts.PhaseResult{Provider: input.Provider, Outcome: contracts.PhaseResultIndeterminate}
	if err := ctx.Err(); err != nil {
		result.FailureReason = contracts.ProviderFailureCommand
		return result, err
	}
	if command.StdoutTruncated || command.StderrTruncated || len(command.Stdout) > MaxResultBytes || len(command.Stderr) > MaxResultBytes {
		result.FailureReason = contracts.ProviderFailureOutput
		return result, ErrProtocol
	}
	if command.ExitCode != 0 {
		result.FailureReason = contracts.ProviderFailureExit
		return result, ErrTerminal
	}
	if schemaOutput {
		var err error
		result.ReportedCostEstimateMicroUSD, err = ReportedCostEstimate(command.Stdout)
		if err != nil {
			result.FailureReason = contracts.ProviderFailureProtocol
			return result, err
		}
	}
	artifact, err := Artifact(command.Stdout, schemaOutput)
	if err != nil {
		switch {
		case errors.Is(err, ErrArtifact):
			result.Outcome = contracts.PhaseResultInvalidArtifact
			result.ArtifactFailureReason = contracts.ArtifactFailureFinalMessage
		case errors.Is(err, ErrTerminal):
			result.FailureReason = contracts.ProviderFailureTerminal
		default:
			result.FailureReason = contracts.ProviderFailureProtocol
		}
		return result, err
	}
	result.Outcome, result.Artifact = contracts.PhaseResultCompleted, artifact
	return result, nil
}
