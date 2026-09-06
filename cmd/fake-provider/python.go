// Python process-boundary fixture; never used by the production provider adapter.
package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"reflect"
	"strings"

	"github.com/nysa-company/sf/internal/phaseartifact"
	"github.com/nysa-company/sf/internal/pythonprepare"
)

const pythonFixtureMarker = "SF_E2E_PYTHON_PYTEST"
const pythonVerificationFile = "test_sf_fixture.py"
const pythonBuilderFile = "sf_fixture.py"

func pythonFixtureCommand(prompt string) ([]string, bool) {
	if !strings.Contains(prompt, pythonFixtureMarker) {
		return nil, false
	}
	command, err := pythonprepare.RecipeArgv(pythonVerificationFile)
	return command, err == nil
}

func pythonFixtureArtifact(role, prompt string, command []string) ([]byte, bool, error) {
	switch role {
	case "planner":
		value := phaseartifact.Planner{Schema: "sf.planner/v1", Acceptance: []string{"fixture workflow completes"}, Proof: phaseartifact.ProofPlan{Kind: phaseartifact.ProofAcceptance, Command: command, Details: "real pinned Python proof"}, Paths: []string{pythonVerificationFile, pythonBuilderFile}, Commands: [][]string{command}, Risks: []string{"fixture output"}, Questions: []phaseartifact.Question{}}
		data, err := json.Marshal(value)
		return data, true, err
	case "builder":
		data, err := json.Marshal(phaseartifact.Builder{Schema: "sf.builder/v1", Summary: "fixture implementation", ChangedFiles: []string{pythonBuilderFile}, Commands: [][]string{command}})
		return data, true, err
	case "verification":
		var binding struct {
			AcceptanceDigest        string                  `json:"acceptance_digest"`
			ProofKind               phaseartifact.ProofKind `json:"proof_kind"`
			AllowedPrebuildOutcomes []string                `json:"allowed_prebuild_outcomes"`
			Command                 []string                `json:"command"`
		}
		if err := decodePromptObject(prompt, "OUTPUT_BINDING=", &binding); err != nil {
			return nil, true, err
		}
		if !reflect.DeepEqual(binding.Command, command) || binding.ProofKind != phaseartifact.ProofAcceptance {
			return nil, true, errors.New("Python fixture requires exact configured acceptance recipe")
		}
		allowed := false
		for _, outcome := range binding.AllowedPrebuildOutcomes {
			allowed = allowed || outcome == "missing"
		}
		if !allowed {
			return nil, true, errors.New("Python fixture requires a missing-implementation proof")
		}
		digest := sha256.Sum256([]byte(pythonVerificationSource))
		data, err := json.Marshal(phaseartifact.Verification{Schema: "sf.verification/v1", AcceptanceDigest: binding.AcceptanceDigest, ProofKind: binding.ProofKind, OwnedFiles: []string{pythonVerificationFile}, Command: binding.Command, PrebuildOutcome: "missing", EvidenceDigest: hex.EncodeToString(digest[:])})
		return data, true, err
	default:
		return nil, false, nil
	}
}

const pythonVerificationSource = `def test_software_factory_fixture():
    from sf_fixture import software_factory_fixture
    assert software_factory_fixture() == "ready"
`

const pythonBuilderSource = `def software_factory_fixture():
    return "ready"
`
