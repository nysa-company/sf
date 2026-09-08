package workflowprompt

import (
	"errors"
	"strings"
)

const BuilderPostbuildRepairSchema = "sf.postbuild_repair/v1"

// BuilderPostbuildRepair contains no raw output, URLs, paths or caller-authored
// diagnosis. Store admission must establish the source; this validator only
// bounds the prompt projection and binds it to the retained verification.
type BuilderPostbuildRepair struct {
	Schema             string `json:"schema"`
	EntryTicketVersion uint64 `json:"entry_ticket_version"`
	FailedResultDigest string `json:"failed_result_digest"`
	BuilderTypedSHA256 string `json:"builder_typed_sha256"`
	ExitCode           int    `json:"exit_code"`
	ProofDigest        string `json:"proof_digest"`
	CheckpointID       string `json:"checkpoint_id"`
}

func validateBuilderPostbuildRepair(value BuilderPostbuildRepair, verification VerificationIdentity) error {
	if value.Schema != BuilderPostbuildRepairSchema || value.EntryTicketVersion == 0 || value.ExitCode < 1 || value.ExitCode > 255 || value.ProofDigest != verification.ProofDigest || value.CheckpointID != verification.CheckpointID {
		return errors.New("postbuild repair does not bind a normal failure and retained verification")
	}
	if !strings.HasPrefix(value.FailedResultDigest, "sha256:") {
		return errors.New("postbuild repair result digest must be typed SHA-256")
	}
	for _, digest := range []string{strings.TrimPrefix(value.FailedResultDigest, "sha256:"), value.BuilderTypedSHA256, value.ProofDigest} {
		if err := validateDigest("postbuild repair digest", digest); err != nil {
			return err
		}
	}
	return validateOID("postbuild repair checkpoint", value.CheckpointID)
}
