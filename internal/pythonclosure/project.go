package pythonclosure

import (
	"os"
	"strings"

	"golang.org/x/sys/unix"
)

// ValidateTestPath checks the selected file/directory without executing Python
// or following a symlink. It does not infer support for project dependencies.
// The caller separately authenticates the repository root and runtime recipe.
func ValidateTestPath(root, testPath string) error {
	if !ValidTestPath(testPath) {
		return ErrInvalid
	}
	fd, err := unix.Open(root, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return ErrInvalid
	}
	current := os.NewFile(uintptr(fd), "python-project-root")
	parts := strings.Split(testPath, "/")
	for i, part := range parts {
		flags := unix.O_RDONLY | unix.O_NOFOLLOW | unix.O_NONBLOCK | unix.O_CLOEXEC
		if i < len(parts)-1 || part == "tests" {
			flags |= unix.O_DIRECTORY
		}
		next, err := unix.Openat(int(current.Fd()), part, flags, 0)
		current.Close()
		if err != nil {
			return ErrInvalid
		}
		current = os.NewFile(uintptr(next), "python-test-path")
	}
	defer current.Close()
	info, err := current.Stat()
	if err != nil {
		return ErrInvalid
	}
	if parts[len(parts)-1] == "tests" {
		if !info.IsDir() {
			return ErrInvalid
		}
	} else if !info.Mode().IsRegular() || info.Size() > maxFileBytesForProject {
		return ErrInvalid
	}
	return nil
}

const maxFileBytesForProject = 64 << 20
