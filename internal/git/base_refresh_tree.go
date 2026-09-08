package git

import (
	"errors"
	"fmt"
)

var (
	errBaseRefreshTreeInput    = errors.New("base refresh tree input is invalid")
	errBaseRefreshTreeConflict = errors.New("base refresh tree has conflicts")
	errBaseRefreshTreeCommand  = errors.New("base refresh merge-tree command failed")
)

// baseRefreshTreeRequest is the immutable input to a future, mutation-authorized
// base-refresh operation. Parent order is deliberate: the retained candidate is
// first and the newly observed protected base is second.
type baseRefreshTreeRequest struct {
	OriginalBaseOID  string
	CandidateOID     string
	RefreshedBaseOID string
}

// baseRefreshTreeAncestry binds the exact operands of two successful,
// authenticated merge-base --is-ancestor observations. The pairs are
// [ancestor, descendant]. This file deliberately does not run those commands:
// merge-tree --write-tree writes objects, so execution belongs inside a future
// Git mutation lease rather than a read-only Runner method.
type baseRefreshTreeAncestry struct {
	CandidatePair     [2]string
	RefreshedBasePair [2]string
}

// baseRefreshMergeTreeResponse is the bounded, combined output and exit status
// of exactly:
//
//	git merge-tree --write-tree <candidate> <refreshed-base>
//
// The future authority wrapper must construct that argv from
// baseRefreshTreeRequest and execute it under the same repository mutation
// lease as the ancestry observations and eventual commit creation.
type baseRefreshMergeTreeResponse struct {
	Operands [2]string
	Output   []byte
	ExitCode int
}

// baseRefreshTreePreparation binds the deterministic tree to the exact
// intended parent order. It carries no authenticated authority of its own and
// is safe to consume only inside the future mutation-authorized operation that
// produced the observations.
type baseRefreshTreePreparation struct {
	TreeOID string
	Parents [2]string
}

// validateBaseRefreshTreePreparation is intentionally pure and unexported. It
// authenticates no repository and performs no command. In particular, it must
// not grow into a public way to invoke merge-tree --write-tree without the
// repository-writer authority required by Package git's mutation boundary.
func validateBaseRefreshTreePreparation(request baseRefreshTreeRequest, ancestry baseRefreshTreeAncestry, response baseRefreshMergeTreeResponse) (baseRefreshTreePreparation, error) {
	if !validOID(request.OriginalBaseOID) || !validOID(request.CandidateOID) || !validOID(request.RefreshedBaseOID) {
		return baseRefreshTreePreparation{}, fmt.Errorf("%w: object ids must be canonical", errBaseRefreshTreeInput)
	}
	oidWidth := len(request.OriginalBaseOID)
	if len(request.CandidateOID) != oidWidth || len(request.RefreshedBaseOID) != oidWidth {
		return baseRefreshTreePreparation{}, fmt.Errorf("%w: object id widths differ", errBaseRefreshTreeInput)
	}
	if request.OriginalBaseOID == request.CandidateOID || request.OriginalBaseOID == request.RefreshedBaseOID || request.CandidateOID == request.RefreshedBaseOID {
		return baseRefreshTreePreparation{}, fmt.Errorf("%w: base refresh commits must be distinct", errBaseRefreshTreeInput)
	}
	if ancestry.CandidatePair != [2]string{request.OriginalBaseOID, request.CandidateOID} ||
		ancestry.RefreshedBasePair != [2]string{request.OriginalBaseOID, request.RefreshedBaseOID} {
		return baseRefreshTreePreparation{}, fmt.Errorf("%w: original base is not an ancestor of both inputs", errBaseRefreshTreeInput)
	}
	if response.Operands != [2]string{request.CandidateOID, request.RefreshedBaseOID} {
		return baseRefreshTreePreparation{}, fmt.Errorf("%w: merge-tree operands do not match requested parent order", errBaseRefreshTreeInput)
	}
	if len(response.Output) > maxGitOutput {
		return baseRefreshTreePreparation{}, ErrOutputBound
	}

	switch response.ExitCode {
	case 0:
		// A successful --write-tree response is exactly one object id and one
		// LF. Diagnostics, additional object ids, CRLF, or missing termination
		// are ambiguous and must never be interpreted as the merged tree.
		if len(response.Output) != oidWidth+1 || response.Output[oidWidth] != '\n' {
			return baseRefreshTreePreparation{}, fmt.Errorf("%w: malformed successful merge-tree output", errBaseRefreshTreeCommand)
		}
		tree := string(response.Output[:oidWidth])
		if !validOID(tree) || len(tree) != oidWidth {
			return baseRefreshTreePreparation{}, fmt.Errorf("%w: malformed merged tree object id", errBaseRefreshTreeCommand)
		}
		return baseRefreshTreePreparation{
			TreeOID: tree,
			Parents: [2]string{request.CandidateOID, request.RefreshedBaseOID},
		}, nil
	case 1:
		// Git writes a provisional tree even when it reports conflicts. Never
		// return or retain that first-line object id as a clean preparation.
		return baseRefreshTreePreparation{}, errBaseRefreshTreeConflict
	default:
		return baseRefreshTreePreparation{}, fmt.Errorf("%w: exit status %d", errBaseRefreshTreeCommand, response.ExitCode)
	}
}
