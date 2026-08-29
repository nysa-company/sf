package main

import (
	"fmt"
	"os"

	"github.com/nysa-company/sf/internal/version"
)

func main() {
	os.Exit(run(os.Args[1:]))
}

func run(args []string) int {
	if len(args) == 1 && args[0] == "version" {
		fmt.Printf("sf %s (%s, %s)\n", version.Version, version.Commit, version.Channel)
		return 0
	}
	fmt.Fprintln(os.Stderr, "sf: implementation bootstrap; try 'sf version'")
	return 2
}
