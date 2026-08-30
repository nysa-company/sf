// sf-ssh is the argv-validating SSH transport used only by sf-owned Git push.
package main

import (
	"fmt"
	"os"
	"os/exec"

	"github.com/nysa-company/sf/internal/gitssh"
)

func main() {
	req := gitssh.Request{SSHBinary: os.Getenv("SF_GIT_SSH_BINARY"), KnownHosts: os.Getenv("SF_GIT_SSH_KNOWN_HOSTS"), AgentSocket: os.Getenv("SSH_AUTH_SOCK"), Repository: os.Getenv("SF_GIT_SSH_REPOSITORY")}
	args, env, err := gitssh.Command(req, os.Args[1:])
	if err != nil {
		fmt.Fprintln(os.Stderr, "sf-ssh:", err)
		os.Exit(2)
	}
	cmd := exec.Command(req.SSHBinary, args...)
	cmd.Env, cmd.Stdin, cmd.Stdout, cmd.Stderr = env, os.Stdin, os.Stdout, os.Stderr
	if err := cmd.Run(); err != nil {
		fmt.Fprintln(os.Stderr, "sf-ssh: ssh failed")
		os.Exit(1)
	}
}
