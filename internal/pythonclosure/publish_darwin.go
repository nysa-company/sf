package pythonclosure

import "golang.org/x/sys/unix"

func preparedPublicationAvailable() bool { return true }

func renamePreparedExclusive(from int, source string, to int, destination string) error {
	return unix.RenameatxNp(from, source, to, destination, unix.RENAME_EXCL)
}
