// fake-gh is a stateful, credential-free replacement for the gh executable in
// contract tests. Set SF_FAKE_GH_STATE to a JSON state file created with
// --init, then invoke it with the exact gh argv the adapter would use.
package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/testkit"
)

func main() {
	statePath := os.Getenv("SF_FAKE_GH_STATE")
	configDir := os.Getenv("GH_CONFIG_DIR")
	initState := flag.Bool("init", false, "initialize the state file")
	repository := flag.String("repository", "", "owner/name for --init")
	flag.Parse()
	if statePath == "" && configDir != "" {
		statePath = filepath.Join(configDir, "sf-fake-gh.json")
	}
	if statePath == "" {
		fmt.Fprintln(os.Stderr, "fake-gh: SF_FAKE_GH_STATE or GH_CONFIG_DIR is required")
		os.Exit(2)
	}
	if *initState {
		parts := strings.Split(*repository, "/")
		if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
			fmt.Fprintln(os.Stderr, "fake-gh: --repository must be owner/name")
			os.Exit(2)
		}
		remote, err := testkit.NewFakeGH(statePath, contracts.RepositoryIdentity{Host: "github.com", Owner: parts[0], Name: parts[1]})
		if err == nil && os.Getenv("SF_FAKE_GH_AUTHENTICATED") == "1" {
			err = remote.SetAuthenticated(true)
		}
		if err != nil {
			fmt.Fprintln(os.Stderr, "fake-gh:", err)
			os.Exit(2)
		}
		return
	}
	remote, err := testkit.OpenFakeGH(statePath)
	if err != nil {
		fmt.Fprintln(os.Stderr, "fake-gh:", err)
		os.Exit(2)
	}
	if configDir != "" {
		if err := remote.UseBareRepositoryFromMarker(configDir); err != nil {
			fmt.Fprintln(os.Stderr, "fake-gh:", err)
			os.Exit(2)
		}
	}
	output, err := remote.Run(flag.Args())
	if len(output) > 1<<20 {
		fmt.Fprintln(os.Stderr, "fake-gh: bounded output exceeded")
		os.Exit(3)
	}
	// Reconcile the local fixture ref before reporting a lost hosted response.
	// FakeGH's durable state distinguishes an error-before-mutation from a
	// mutation whose response was dropped, so this never guesses from stderr.
	if len(flag.Args()) >= 2 && flag.Args()[0] == "pr" && flag.Args()[1] == "merge" && configDir != "" {
		if advanceErr := remote.AdvanceBareRefFromMarker(configDir); advanceErr != nil {
			fmt.Fprintln(os.Stderr, "fake-gh:", advanceErr)
			os.Exit(1)
		}
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "fake-gh:", err)
		os.Exit(1)
	}
	_, _ = os.Stdout.Write(output)
}
