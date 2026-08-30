// sf-git-exec starts one fixed Git argv from already-open control-plane
// directories. It authenticates the worktree's live .git pointer against the
// supplied gitdir and common-dir descriptors immediately before exec.
package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"golang.org/x/sys/unix"
)

func main() {
	if len(os.Args) < 6 || strings.Join(os.Args[1:6], "\x00") != "--worktree-fd=3\x00--git-dir-fd=4\x00--common-dir-fd=5\x00--\x00/usr/bin/git" {
		refuse("invalid invocation")
	}
	if err := validateCapabilities(); err != nil {
		refuse("directory capability refused")
	}
	if err := unix.Fchdir(3); err != nil {
		refuse("directory capability refused")
	}
	if err := unix.Exec(os.Args[5], os.Args[5:], os.Environ()); err != nil {
		fmt.Fprintln(os.Stderr, "sf-git-exec: exec failed")
		os.Exit(127)
	}
}

func refuse(reason string) { fmt.Fprintln(os.Stderr, "sf-git-exec:", reason); os.Exit(2) }

func validateCapabilities() error {
	var worktree, gitdir, common unix.Stat_t
	if err := unix.Fstat(3, &worktree); err != nil || worktree.Mode&unix.S_IFMT != unix.S_IFDIR {
		return fmt.Errorf("worktree")
	}
	if err := unix.Fstat(4, &gitdir); err != nil || gitdir.Mode&unix.S_IFMT != unix.S_IFDIR {
		return fmt.Errorf("gitdir")
	}
	if err := unix.Fstat(5, &common); err != nil || common.Mode&unix.S_IFMT != unix.S_IFDIR {
		return fmt.Errorf("common")
	}
	if err := unix.Fchdir(3); err != nil {
		return err
	}
	info, err := os.Lstat(".git")
	if err != nil || info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("git pointer")
	}
	if info.IsDir() {
		if matchesPath(".git", gitdir) && matchesPath(".git", common) {
			return nil
		}
		return fmt.Errorf("primary gitdir")
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("git pointer")
	}
	data, err := os.ReadFile(".git")
	if err != nil {
		return err
	}
	pointer := strings.TrimSpace(string(data))
	if !strings.HasPrefix(pointer, "gitdir: ") {
		return fmt.Errorf("git pointer")
	}
	gitPath := strings.TrimSpace(strings.TrimPrefix(pointer, "gitdir: "))
	if !filepath.IsAbs(gitPath) || filepath.Clean(gitPath) != gitPath || !matchesPath(gitPath, gitdir) {
		return fmt.Errorf("gitdir")
	}
	commonText, err := os.ReadFile(filepath.Join(gitPath, "commondir"))
	if err != nil {
		return err
	}
	commonPath := strings.TrimSpace(string(commonText))
	if commonPath == "" || filepath.IsAbs(commonPath) || !matchesPath(filepath.Clean(filepath.Join(gitPath, commonPath)), common) {
		return fmt.Errorf("common")
	}
	return nil
}

func matchesPath(path string, expected unix.Stat_t) bool {
	var actual unix.Stat_t
	if err := unix.Stat(path, &actual); err != nil {
		return false
	}
	return actual.Dev == expected.Dev && actual.Ino == expected.Ino && actual.Mode&unix.S_IFMT == unix.S_IFDIR
}
