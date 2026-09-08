package pythonclosure

import (
	"context"
	"io"
	"os"
	"time"

	"golang.org/x/sys/unix"
)

// Prepared retains verified snapshot descriptors. It never deletes or repairs
// cache content. Its owner must exclude writers and keep it open until process
// drain is proven; Close must not race inspection or execution. Channel-root
// authentication and Store command authority remain caller responsibilities.
type Prepared struct {
	root, runtime, dependencies *os.File
	environment                 Environment
}

// OpenPreparedDirectoryFD reads <digest-hex>/environment.json, runtime/ and
// dependencies/ beneath the caller-authenticated channel snapshot directory.
// It does not discover an ambient HOME, follow links or fall back to another
// channel. Expected digests come from frozen configuration and factory code.
func OpenPreparedDirectoryFD(ctx context.Context, channelFD int, digest, lock, bootstrap string) (*Prepared, error) {
	if ctx == nil || channelFD < 0 || !validDigest(digest) || !validDigest(lock) || !validDigest(bootstrap) {
		return nil, ErrInvalid
	}
	bounded, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := bounded.Err(); err != nil {
		return nil, err
	}
	channel, err := openPrivateDirectoryAt(channelFD, ".")
	if err != nil {
		return nil, err
	}
	defer channel.Close()
	value := &Prepared{}
	ok := false
	defer func() {
		if !ok {
			value.Close()
		}
	}()
	value.root, err = openPrivateDirectoryAt(int(channel.Fd()), digest[7:])
	if err != nil {
		return nil, err
	}
	data, err := readPreparedManifest(bounded, int(value.root.Fd()))
	if err != nil {
		return nil, err
	}
	value.runtime, err = openPrivateDirectoryAt(int(value.root.Fd()), "runtime")
	if err != nil {
		return nil, err
	}
	value.dependencies, err = openPrivateDirectoryAt(int(value.root.Fd()), "dependencies")
	if err != nil {
		return nil, err
	}
	value.environment, err = AuthenticateEnvironmentFD(bounded, value.RuntimeFD(), value.DependenciesFD(), data, digest, lock, bootstrap)
	if err != nil {
		return nil, err
	}
	ok = true
	return value, nil
}

func (p *Prepared) RuntimeFD() int {
	if p == nil || p.runtime == nil {
		return -1
	}
	return int(p.runtime.Fd())
}

func (p *Prepared) DependenciesFD() int {
	if p == nil || p.dependencies == nil {
		return -1
	}
	return int(p.dependencies.Fd())
}

func (p *Prepared) Interpreter() string {
	if p == nil || p.root == nil {
		return ""
	}
	return p.environment.Interpreter
}

func (p *Prepared) Revalidate(ctx context.Context) error {
	if p == nil {
		return ErrInvalid
	}
	return VerifyEnvironmentFD(ctx, p.RuntimeFD(), p.DependenciesFD(), p.environment)
}

func (p *Prepared) Close() error {
	if p == nil {
		return nil
	}
	var first error
	for _, file := range []*os.File{p.dependencies, p.runtime, p.root} {
		if file != nil {
			if err := file.Close(); err != nil && first == nil {
				first = err
			}
		}
	}
	p.dependencies, p.runtime, p.root = nil, nil, nil
	return first
}

func openPrivateDirectoryAt(parent int, name string) (*os.File, error) {
	fd, err := unix.Openat(parent, name, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC|unix.O_NONBLOCK, 0)
	if err != nil {
		return nil, ErrInvalid
	}
	file := os.NewFile(uintptr(fd), "prepared-python-directory")
	var stat unix.Stat_t
	if unix.Fstat(fd, &stat) != nil || stat.Uid != uint32(os.Geteuid()) || stat.Mode&0077 != 0 || stat.Mode&07000 != 0 {
		file.Close()
		return nil, ErrInvalid
	}
	return file, nil
}

func readPreparedManifest(ctx context.Context, root int) ([]byte, error) {
	fd, err := unix.Openat(root, "environment.json", unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_CLOEXEC|unix.O_NONBLOCK, 0)
	if err != nil {
		return nil, ErrInvalid
	}
	file := os.NewFile(uintptr(fd), "prepared-python-manifest")
	defer file.Close()
	var stat unix.Stat_t
	if unix.Fstat(fd, &stat) != nil || stat.Mode&unix.S_IFMT != unix.S_IFREG || stat.Uid != uint32(os.Geteuid()) || stat.Mode&0077 != 0 || stat.Mode&07000 != 0 || stat.Size <= 0 || stat.Size > maxEnvironmentBytes {
		return nil, ErrInvalid
	}
	before, err := file.Stat()
	if err != nil {
		return nil, ErrInvalid
	}
	data, err := io.ReadAll(io.LimitReader(contextReader{ctx, file}, stat.Size+1))
	if err != nil {
		return nil, err
	}
	after, err := file.Stat()
	if err != nil || int64(len(data)) != stat.Size || after.Size() != before.Size() || after.Mode() != before.Mode() || !after.ModTime().Equal(before.ModTime()) {
		return nil, ErrInvalid
	}
	return data, nil
}
