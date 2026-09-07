package processsupervisor

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

func TestClaudeObservationExplainsCredentialRenewalBeforeProbe(t *testing.T) {
	if runtime.GOOS != "darwin" {
		t.Skip("Darwin observer")
	}
	for _, remaining := range []time.Duration{-time.Minute, 30 * time.Minute} {
		t.Run(remaining.String(), func(t *testing.T) {
			path := filepath.Join(credentialHome(t), "claude")
			// Authentication must refuse before executing even a status probe.
			if err := os.WriteFile(path, []byte("#!/bin/sh\nexit 99\n"), 0700); err != nil {
				t.Fatal(err)
			}
			s, err := New(nil)
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			lookup := func(context.Context, string, string) ([]byte, error) {
				return json.Marshal(map[string]any{"claudeAiOauth": map[string]any{"accessToken": "fixture-renewal-secret", "expiresAt": time.Now().Add(remaining).UnixMilli()}})
			}
			_, err = s.observeClaudeRuntime(t.Context(), path, "claude-sonnet-5", lookup)
			if err == nil || !strings.Contains(err.Error(), "run claude auth login") || strings.Contains(err.Error(), "fixture-renewal-secret") {
				t.Fatal("credential refusal lost safe login-renewal guidance")
			}
		})
	}
}
