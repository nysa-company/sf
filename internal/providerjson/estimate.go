package providerjson

import (
	"bytes"
	"math/big"
	"regexp"
)

// Bound number size and exponent before arbitrary-precision parsing. JSON
// strings, null, negative values and nonfinite values are not cost estimates.
var estimateNumber = regexp.MustCompile(`^(0|[1-9][0-9]*)(\.[0-9]+)?([eE][+-]?[0-9]{1,2})?$`)

// ReportedCostEstimate reads optional list-price metadata, NOT actual charges.
// Missing is unknown; a malformed supplied estimate is an error, never zero.
// Decimal arithmetic and upward rounding avoid understating sub-micro costs.
// This function supplies neither pricing authority nor a spending guarantee.
func ReportedCostEstimate(output []byte) (*int64, error) {
	fields, err := Object(output)
	if err != nil {
		return nil, err
	}
	raw, present := fields["total_cost_usd"]
	if !present {
		return nil, nil
	}
	raw = bytes.TrimSpace(raw)
	if len(raw) > 64 || !estimateNumber.Match(raw) {
		return nil, ErrProtocol
	}
	value, ok := new(big.Rat).SetString(string(raw))
	if !ok || value.Sign() < 0 {
		return nil, ErrProtocol
	}
	value.Mul(value, big.NewRat(1_000_000, 1))
	units, remainder := new(big.Int), new(big.Int)
	units.QuoRem(value.Num(), value.Denom(), remainder)
	if remainder.Sign() != 0 {
		units.Add(units, big.NewInt(1))
	}
	if !units.IsInt64() {
		return nil, ErrProtocol
	}
	result := units.Int64()
	return &result, nil
}
