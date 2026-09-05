package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/nysa-company/sf/internal/api"
)

func TestDecisionHeadFlagBindsExactHeadWithoutPrompting(t *testing.T) {
	for _, command := range []string{"approve", "reject"} {
		for _, head := range []string{"", "abcdef", strings.Repeat("A", 40), strings.Repeat("a", 40), strings.Repeat("b", 64)} {
			t.Run(command+"/"+head, func(t *testing.T) {
				calls := 0
				client := fakeClient(func(_ context.Context, r api.Request) (api.Response, error) {
					calls++
					var values map[string]any
					if err := json.Unmarshal(r.Parameters, &values); err != nil {
						t.Fatal(err)
					}
					if values["reviewed_head"] != head {
						t.Fatalf("unbound head: %s", r.Parameters)
					}
					return responseOK(), nil
				})
				args := []string{command, "SF-1", "--head", head, "--json"}
				if command == "reject" {
					args = append(args, "--reason", "needs tests")
				}
				code := Execute(context.Background(), args, &bytes.Buffer{}, &bytes.Buffer{}, client)
				valid := head == strings.Repeat("a", 40) || head == strings.Repeat("b", 64)
				if valid && (code != 0 || calls != 1) || !valid && (code == 0 || calls != 0) {
					t.Fatalf("code=%d calls=%d", code, calls)
				}
			})
		}
	}
}
