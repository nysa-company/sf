package git

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestObserveRepositoryBaseSafeStageDiagnostics(t *testing.T) {
	secretErr := errors.New("https://user:secret@example.test/private-path")
	for _, test := range []struct {
		name, code, output string
		failure            error
	}{
		{name: "identity", code: "git_local_identity", failure: secretErr},
		{name: "transport", code: "git_transport_setup", failure: ErrHTTPSCredentialBoundary},
		{name: "remote", code: "git_remote_read", failure: secretErr},
		{name: "cancel", code: "git_remote_read", failure: context.Canceled},
		{name: "deadline", code: "git_remote_read", failure: context.DeadlineExceeded},
		{name: "missing", code: "git_missing_base"},
		{name: "local_missing", code: "git_missing_base", failure: secretErr},
		{name: "malformed", code: "git_malformed_remote_response", output: "secret refs/heads/main"},
		{name: "ambiguous", code: "git_malformed_remote_response", output: strings.Repeat("a", 40) + " refs/heads/main extra-secret"},
	} {
		t.Run(test.name, func(t *testing.T) {
			repository, err := filepath.EvalSymlinks(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			if err := os.Mkdir(filepath.Join(repository, ".git"), 0700); err != nil {
				t.Fatal(err)
			}
			authority := &countingMutationAuthority{}
			runner := Runner{Home: t.TempDir(), TestLocalTransport: true, MutationAuthority: authority}
			origin := filepath.Join(repository, "remote.git")
			if test.name == "transport" {
				origin = "https://github.com/example/repository.git"
				runner.CredentialHelper = "partial"
			}
			remoteReads := 0
			runner.Run = func(_ context.Context, _ string, args, _ []string) ([]byte, error) {
				command := strings.Join(args, " ")
				switch {
				case strings.HasSuffix(command, "rev-parse --show-toplevel"):
					if test.name == "identity" {
						return []byte("secret"), test.failure
					}
					return []byte(repository), nil
				case strings.HasSuffix(command, "rev-parse --is-bare-repository"):
					return []byte("false"), nil
				case strings.HasSuffix(command, "remote get-url origin"):
					return []byte(origin), nil
				case strings.HasSuffix(command, "rev-parse --verify main^{commit}"):
					if test.name == "local_missing" {
						return []byte("secret"), test.failure
					}
					return []byte(strings.Repeat("a", 40)), nil
				case strings.HasSuffix(command, "ls-remote --heads "+origin+" refs/heads/main"):
					remoteReads++
					return []byte(test.output), test.failure
				default:
					t.Fatalf("unexpected command (possible mutation)")
					return nil, errors.New("unexpected command")
				}
			}
			path, base, err := runner.ObserveRepositoryBase(context.Background(), repository, "main")
			var diagnostic interface{ RuntimeDiagnosticCode() string }
			if err == nil || !errors.As(err, &diagnostic) || diagnostic.RuntimeDiagnosticCode() != test.code {
				t.Fatalf("diagnostic=%v, want %s", err, test.code)
			}
			if err.Error() != "git repository preflight failed: "+test.code || errors.Unwrap(err) != nil {
				t.Fatal("diagnostic exposed an unsafe error surface")
			}
			if test.failure != nil && !errors.Is(err, test.failure) {
				t.Fatal("lost underlying error identity")
			}
			if path != "" || base != "" || authority.acquisitions != 0 {
				t.Fatal("failed observation returned authority or a base")
			}
			if (test.name == "identity" || test.name == "transport" || test.name == "local_missing") && remoteReads != 0 {
				t.Fatal("remote read after failed local preflight")
			}
		})
	}
}
