package pythonclosure

import (
	"context"
	"errors"
	"io"
	"os"
	"time"

	"golang.org/x/sys/unix"
)

const ScratchFileBytes = 16 << 20
const ScratchTotalBytes = 128 << 20
const ScratchEntries = 4096

type ScratchUsage struct {
	Entries                      int
	LogicalBytes, AllocatedBytes int64
}

// InspectScratchDirectoryFD accounts for one private retained scratch root
// without following symlinks or reading file content. A live writer can change
// usage between observations: this is a bounded monitor input, not an atomic
// filesystem quota. The supervisor must stop on limit/error, inspect at normal
// completion too, and combine monitoring with hard child resource limits.
func InspectScratchDirectoryFD(ctx context.Context, rootFD int) (ScratchUsage, error) {
	if ctx == nil || rootFD < 0 {
		return ScratchUsage{}, ErrInvalid
	}
	bounded, cancel := context.WithTimeout(ctx, 250*time.Millisecond)
	defer cancel()
	root, err := openPrivateDirectoryAt(rootFD, ".")
	if err != nil {
		return ScratchUsage{}, err
	}
	defer root.Close()
	var usage ScratchUsage
	var walk func(int, int) error
	walk = func(parent, depth int) error {
		if err := bounded.Err(); err != nil {
			return err
		}
		if depth > 32 {
			return ErrLimit
		}
		fd, err := unix.Openat(parent, ".", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
		if err != nil {
			return ErrInvalid
		}
		dir := os.NewFile(uintptr(fd), "python-scratch-directory")
		defer dir.Close()
		for {
			children, readErr := dir.ReadDir(128)
			for _, child := range children {
				if err := bounded.Err(); err != nil {
					return err
				}
				usage.Entries++
				if usage.Entries > ScratchEntries {
					return ErrLimit
				}
				var stat unix.Stat_t
				err := unix.Fstatat(fd, child.Name(), &stat, unix.AT_SYMLINK_NOFOLLOW)
				// Ordinary temporary-file removal is expected during observation.
				if errors.Is(err, unix.ENOENT) {
					continue
				}
				if err != nil {
					return ErrInvalid
				}
				switch stat.Mode & unix.S_IFMT {
				case unix.S_IFREG, unix.S_IFLNK:
					if stat.Size < 0 || stat.Size > ScratchFileBytes || stat.Size > ScratchTotalBytes-usage.LogicalBytes || stat.Blocks < 0 || stat.Blocks > (ScratchTotalBytes-usage.AllocatedBytes)/512 {
						return ErrLimit
					}
					usage.LogicalBytes += stat.Size
					usage.AllocatedBytes += stat.Blocks * 512
				case unix.S_IFDIR:
					childFD, err := unix.Openat(fd, child.Name(), unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
					if errors.Is(err, unix.ENOENT) {
						continue
					}
					if err != nil {
						return ErrInvalid
					}
					var opened unix.Stat_t
					if unix.Fstat(childFD, &opened) != nil || opened.Dev != stat.Dev || opened.Ino != stat.Ino {
						unix.Close(childFD)
						return ErrInvalid
					}
					err = walk(childFD, depth+1)
					unix.Close(childFD)
					if err != nil {
						return err
					}
				default:
					return ErrInvalid
				}
			}
			if readErr == io.EOF {
				return nil
			}
			if readErr != nil {
				return ErrInvalid
			}
		}
	}
	err = walk(int(root.Fd()), 0)
	return usage, err
}
