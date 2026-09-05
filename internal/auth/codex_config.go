package auth

import (
	"errors"
	"path/filepath"
)

// codexConfigDirectory preserves Codex's documented CODEX_HOME selection for
// auth commands, matching the runtime composer. It never reads credentials.
func codexConfigDirectory(home, explicit string) (string, error) {
	path := explicit
	if path == "" {
		path = filepath.Join(home, ".codex")
	}
	if !safeGitHubConfigPath(path) {
		return "", errors.New("Codex configuration directory must be a clean absolute path")
	}
	existing, err := existingAuthenticationDirectory(path)
	if err != nil {
		return "", err
	}
	if existing != "" {
		return existing, nil
	}
	// Official login may create the missing selected location; SF must not
	// redirect it to another account or create/copy credentials itself.
	return path, nil
}
