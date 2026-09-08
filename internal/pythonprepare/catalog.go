// Package pythonprepare prepares code-owned Python inputs separately from
// project registration and execution. It never invokes ambient Python or pip.
package pythonprepare

import (
	"crypto/sha256"
	"fmt"
)

type Artifact struct {
	Name, URL, SHA256 string
	Size              int64
}

type Catalog struct {
	Runtime           Artifact
	Wheels            []Artifact
	EnvironmentDigest string
}

// DefaultCatalog is deliberately platform-specific. It is publisher-metadata
// integrity, not a claim of independently reproduced upstream binaries.
func DefaultCatalog(goos, arch string) (Catalog, error) {
	if goos != "darwin" || arch != "arm64" {
		return Catalog{}, ErrUnsupported
	}
	return Catalog{
		// Produced by the bounded extractor and verified by the pinned-archive
		// fixture. Bootstrap or extraction changes require a new explicit pin.
		EnvironmentDigest: "sha256:e65fcb836e0f19815114cf5a06349ef7260e03f60d4a62cce63b7406f4cf8b1a",
		Runtime:           Artifact{"cpython-3.13.15+20260901-aarch64-apple-darwin-install_only_stripped.tar.gz", "https://github.com/astral-sh/python-build-standalone/releases/download/20260901/cpython-3.13.15%2B20260901-aarch64-apple-darwin-install_only_stripped.tar.gz", "d3904bd6a072246e07aa0bdadee9a14e80521e42a943c0848059feb16a2816dc", 25147663},
		Wheels: []Artifact{
			{"iniconfig-2.3.0-py3-none-any.whl", "https://files.pythonhosted.org/packages/cb/b1/3846dd7f199d53cb17f49cba7e651e9ce294d8497c8c150530ed11865bb8/iniconfig-2.3.0-py3-none-any.whl", "f631c04d2c48c52b84d0d0549c99ff3859c98df65b3101406327ecc7d53fbf12", 7484},
			{"packaging-26.3-py3-none-any.whl", "https://files.pythonhosted.org/packages/63/34/ba1c580383c9eada3711951fef0795c80b829a078d72188184bcab9dd527/packaging-26.3-py3-none-any.whl", "d7193f7c8e4e93f444fde0262bf90af30e16fa0ad0ad44cb553c87339b23cd1c", 129956},
			{"pluggy-1.6.0-py3-none-any.whl", "https://files.pythonhosted.org/packages/54/20/4d324d65cc6d9205fabedc306948156824eb9f0ee1633355a8f7ec5c66bf/pluggy-1.6.0-py3-none-any.whl", "e920276dd6813095e9377c0bc5566d94c932c33b27a3e3945d8389c374dd4746", 20538},
			{"pygments-2.21.0-py3-none-any.whl", "https://files.pythonhosted.org/packages/71/46/17f022dd3e953bf20a04a028a21ec746d942f8d2af30fa0f124fa0e6a684/pygments-2.21.0-py3-none-any.whl", "2363c69b61c4a97c838da3b130dcd6468f4848992b21a82f2a63ec34377137d9", 1250147},
			{"pytest-8.4.2-py3-none-any.whl", "https://files.pythonhosted.org/packages/a8/a4/20da314d277121d6534b3a980b29035dcd51e6744bd79075a6ce8fa4eb8d/pytest-8.4.2-py3-none-any.whl", "872f880de3fc3a5bdc88a11b39c9710c3497a547cfa9320bc3c5e62fbf272e79", 365750},
		},
	}, nil
}

const Lock = `iniconfig==2.3.0 --hash=sha256:f631c04d2c48c52b84d0d0549c99ff3859c98df65b3101406327ecc7d53fbf12
packaging==26.3 --hash=sha256:d7193f7c8e4e93f444fde0262bf90af30e16fa0ad0ad44cb553c87339b23cd1c
pluggy==1.6.0 --hash=sha256:e920276dd6813095e9377c0bc5566d94c932c33b27a3e3945d8389c374dd4746
pygments==2.21.0 --hash=sha256:2363c69b61c4a97c838da3b130dcd6468f4848992b21a82f2a63ec34377137d9
pytest==8.4.2 --hash=sha256:872f880de3fc3a5bdc88a11b39c9710c3497a547cfa9320bc3c5e62fbf272e79
`

func LockDigest() string { return fmt.Sprintf("sha256:%x", sha256.Sum256([]byte(Lock))) }
