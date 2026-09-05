package version

import (
	"bytes"
	"encoding/json"
	"testing"
)

func TestHelperBuildInfoOnlyHandlesExactReadOnlyInvocation(t *testing.T) {
	for _, args := range [][]string{{"helper"}, {"helper", "--sf-build-info", "extra"}, {"helper", "--worktree-fd=3"}} {
		var output bytes.Buffer
		if PrintHelperBuildInfo(args, &output) || output.Len() != 0 {
			t.Fatalf("intercepted normal helper invocation %v", args)
		}
	}
	var output bytes.Buffer
	if !PrintHelperBuildInfo([]string{"helper", "--sf-build-info"}, &output) {
		t.Fatal("identity invocation not handled")
	}
	var value map[string]string
	if err := json.Unmarshal(output.Bytes(), &value); err != nil || len(value) != 3 || value["version"] != Version || value["commit"] != Commit || value["channel"] != Channel {
		t.Fatalf("identity=%v err=%v", value, err)
	}
}
