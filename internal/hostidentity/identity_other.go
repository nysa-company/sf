//go:build !darwin

package hostidentity

import "context"

func Observe(context.Context) (Identity, error) { return Identity{}, ErrUnavailable }
