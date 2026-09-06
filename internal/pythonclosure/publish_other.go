//go:build !darwin

package pythonclosure

func preparedPublicationAvailable() bool { return false }

func renamePreparedExclusive(int, string, int, string) error { return ErrInvalid }
