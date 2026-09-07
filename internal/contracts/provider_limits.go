package contracts

import "time"

// MultiCLIRequestLimit is the maximum total SF provider launches on a ticket
// admitting Claude/Cursor. All providers/phases/outcomes count; a retry or
// restart cannot refund a request. It does not count provider-internal API
// calls and is not a dollar spending guarantee.
const MultiCLIRequestLimit = 16

const MultiCLIRequestTimeout = 45 * time.Minute

// MultiCLIRequestPolicy is part of signed runtime policy. Changing either
// limit requires changing this identity and requalifying the runtime.
const MultiCLIRequestPolicy = "sf-cli-requests-v1:ticket-total=16:timeout-seconds=2700"

func UsesMultiCLIRequestLimit(provider string) bool {
	return provider == "claude" || provider == "cursor"
}
