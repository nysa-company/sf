//go:build !darwin && !linux

package ghrunner

import "errors"

func processStartIdentity(int) (string, error) { return "", errors.New("process identity unavailable") }
func hostBootIdentity() (string, error)        { return "", errors.New("boot identity unavailable") }
