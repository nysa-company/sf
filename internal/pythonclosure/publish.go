package pythonclosure

import (
	"context"
	"errors"
	"time"

	"golang.org/x/sys/unix"
)

// PublishPreparedDirectoryFD authenticates a prepared digest directory beneath
// stagingFD and atomically moves it beneath channelFD without replacement.
// Both parent descriptors must be private, caller-authenticated directories on
// the same filesystem. The caller must exclude source writers throughout this
// operation. No project code, downloader or package installer is executed here.
//
// Existing exact content is idempotent; malformed existing content is never
// repaired or removed. Staging cleanup belongs to the caller. published reports
// whether the rename occurred, even if subsequent synchronization fails. Such
// an error is safely retryable by authenticating the destination, not replacing
// it. Atomic visibility is not a substitute for verification after a crash.
func PublishPreparedDirectoryFD(ctx context.Context, channelFD, stagingFD int, digest, lock, bootstrap string) (published bool, err error) {
	if ctx == nil || !preparedPublicationAvailable() || channelFD < 0 || stagingFD < 0 {
		return false, ErrInvalid
	}
	bounded, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	channel, err := openPrivateDirectoryAt(channelFD, ".")
	if err != nil {
		return false, err
	}
	defer channel.Close()
	staging, err := openPrivateDirectoryAt(stagingFD, ".")
	if err != nil {
		return false, err
	}
	defer staging.Close()
	var a, b unix.Stat_t
	if unix.Fstat(int(channel.Fd()), &a) != nil || unix.Fstat(int(staging.Fd()), &b) != nil || a.Dev != b.Dev || a.Ino == b.Ino {
		return false, ErrInvalid
	}
	// Validate identities before slicing the digest or inspecting its pathname.
	if !validDigest(digest) || !validDigest(lock) || !validDigest(bootstrap) {
		return false, ErrInvalid
	}
	name := digest[7:]
	verifyDestination := func() error {
		value, err := OpenPreparedDirectoryFD(bounded, int(channel.Fd()), digest, lock, bootstrap)
		if err != nil {
			return err
		}
		return value.Close()
	}
	var existing unix.Stat_t
	err = unix.Fstatat(int(channel.Fd()), name, &existing, unix.AT_SYMLINK_NOFOLLOW)
	if err == nil {
		return false, verifyDestination()
	}
	if !errors.Is(err, unix.ENOENT) {
		return false, ErrInvalid
	}
	source, err := OpenPreparedDirectoryFD(bounded, int(staging.Fd()), digest, lock, bootstrap)
	if err != nil {
		return false, err
	}
	defer source.Close()
	// Ensure the retained snapshot is still the named directory being moved.
	var retained, named unix.Stat_t
	if unix.Fstat(int(source.root.Fd()), &retained) != nil || unix.Fstatat(int(staging.Fd()), name, &named, unix.AT_SYMLINK_NOFOLLOW) != nil || named.Mode&unix.S_IFMT != unix.S_IFDIR || retained.Dev != named.Dev || retained.Ino != named.Ino {
		return false, ErrInvalid
	}
	if err := bounded.Err(); err != nil {
		return false, err
	}
	err = renamePreparedExclusive(int(staging.Fd()), name, int(channel.Fd()), name)
	if errors.Is(err, unix.EEXIST) {
		// Another preparation won. Only its independently verified content can
		// satisfy the request; never replace it with our staging directory.
		return false, verifyDestination()
	}
	if err != nil {
		return false, err
	}
	if err := errors.Join(channel.Sync(), staging.Sync()); err != nil {
		return true, err
	}
	return true, verifyDestination()
}
