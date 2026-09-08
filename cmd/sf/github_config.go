package main

import "github.com/nysa-company/sf/internal/auth"

func selectedGitHubConfigDirectory(home, explicit, xdg string) (string, error) {
	return auth.ExistingGitHubConfigDirectory(home, explicit, xdg)
}
