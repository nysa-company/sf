package version

import (
	"encoding/json"
	"io"
)

// PrintHelperBuildInfo retains the same embedded identity in every helper.
// This read-only invocation does not inspect credentials or execute Git/SSH.
func PrintHelperBuildInfo(args []string, output io.Writer) bool {
	if len(args) != 2 || args[1] != "--sf-build-info" {
		return false
	}
	_ = json.NewEncoder(output).Encode(map[string]string{"version": Version, "commit": Commit, "channel": Channel})
	return true
}
