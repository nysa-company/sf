package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/nysa-company/sf/internal/phaseartifact"
)

func TestPythonFixtureUsesExactControllerBinding(t *testing.T) {
	command, ok := pythonFixtureCommand(pythonFixtureMarker)
	if !ok || len(command) == 0 {
		t.Fatal("missing Python recipe")
	}
	if _, ok := pythonFixtureCommand("ordinary Go ticket"); ok {
		t.Fatal("changed ordinary fixture")
	}
	for _, tc := range []struct {
		name     string
		command  []string
		outcomes []string
		valid    bool
	}{
		{"exact", command, []string{"red", "missing"}, true},
		{"wrong command", []string{"go", "test", "./..."}, []string{"missing"}, false},
		{"wrong outcome", command, []string{"red"}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			binding, err := json.Marshal(map[string]any{"acceptance_digest": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "proof_kind": "acceptance", "allowed_prebuild_outcomes": tc.outcomes, "command": tc.command})
			if err != nil {
				t.Fatal(err)
			}
			data, handled, err := pythonFixtureArtifact("verification", "OUTPUT_BINDING="+string(binding), command)
			if !handled || (err == nil) != tc.valid {
				t.Fatalf("handled=%t err=%v", handled, err)
			}
			if !tc.valid {
				return
			}
			var proof phaseartifact.Verification
			if err := json.Unmarshal(data, &proof); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(proof.Command, command) || proof.PrebuildOutcome != "missing" || !reflect.DeepEqual(proof.OwnedFiles, []string{pythonVerificationFile}) {
				t.Fatal("wrong proof binding")
			}
		})
	}
}

func TestPythonReviewerFixtureWritesOnlyProof(t *testing.T) {
	root := t.TempDir()
	content, err := writeCodexVerificationFixture(root, pythonFixtureMarker)
	if err != nil || string(content) != pythonVerificationSource {
		t.Fatal("wrong test source", err)
	}
	entries, err := os.ReadDir(root)
	if err != nil || len(entries) != 1 || entries[0].Name() != pythonVerificationFile {
		t.Fatal("unexpected reviewer writes", err)
	}
	actual, err := os.ReadFile(filepath.Join(root, pythonVerificationFile))
	if err != nil || string(actual) != string(content) {
		t.Fatal("artifact bytes differ from written proof", err)
	}
}
