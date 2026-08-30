// sf-git-exec starts one fixed argv from an inherited, already-open directory.
// It is intentionally not a generic command runner: fd 3 is the only accepted
// directory capability and the caller supplies Git's already validated argv.
package main

import (
	"fmt"
	"os"

	"golang.org/x/sys/unix"
)

func main() {
	if len(os.Args) < 4 || os.Args[1] != "--fd=3" || os.Args[2] != "--" || os.Args[3] == "" {
		fmt.Fprintln(os.Stderr, "sf-git-exec: invalid invocation")
		os.Exit(2)
	}
	if err := unix.Fchdir(3); err != nil {
		fmt.Fprintln(os.Stderr, "sf-git-exec: directory capability refused")
		os.Exit(2)
	}
	if err := unix.Exec(os.Args[3], os.Args[3:], os.Environ()); err != nil {
		fmt.Fprintln(os.Stderr, "sf-git-exec: exec failed")
		os.Exit(127)
	}
}
