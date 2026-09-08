# Python preparation inputs

Status: verified preparation inputs, **not a released Python setup feature**.
Public metadata was re-read on 2026-09-05. No download, global installation,
project registration or live-runtime change was performed for this check.

## Runtime

Publisher: [Astral Python Build Standalone, release 20260901](https://github.com/astral-sh/python-build-standalone/releases/tag/20260901).
The release API reports the following artifact, matching the previously tested
disposable fixture:

- Filename: `cpython-3.13.15+20260901-aarch64-apple-darwin-install_only_stripped.tar.gz`
- Size: 25,147,663 bytes.
- SHA-256: `d3904bd6a072246e07aa0bdadee9a14e80521e42a943c0848059feb16a2816dc`.
- Platform: macOS ARM64 only. This is not evidence for Intel macOS support.

These checks authenticate against the publisher's metadata, not an independent
reproducible build. A code-owned catalog must pin these inputs; a downloaded
manifest's self-reported checksum must not become its own trust anchor.

The runtime archive contains eight relative symlink aliases (Python, pydoc,
idle, python-config, two pkg-config aliases, and the manpage alias). Preparation
must not extract these as filesystem symlinks. Resolve only bounded, internal
regular-file targets and materialize their verified bytes in private staging,
or omit explicitly unused aliases. Reject traversal, cycles, absolute targets,
special files, duplicate destinations and expansion beyond inspection limits.
The execution manifest continues to admit only directories and regular files.

## Pinned pytest environment

Public PyPI release JSON metadata matches the fixture's hash-locked wheels:

| Wheel | Bytes | SHA-256 |
|---|---:|---|
| `iniconfig-2.3.0-py3-none-any.whl` | 7,484 | `f631c04d2c48c52b84d0d0549c99ff3859c98df65b3101406327ecc7d53fbf12` |
| `packaging-26.3-py3-none-any.whl` | 129,956 | `d7193f7c8e4e93f444fde0262bf90af30e16fa0ad0ad44cb553c87339b23cd1c` |
| `pluggy-1.6.0-py3-none-any.whl` | 20,538 | `e920276dd6813095e9377c0bc5566d94c932c33b27a3e3945d8389c374dd4746` |
| `pygments-2.21.0-py3-none-any.whl` | 1,250,147 | `2363c69b61c4a97c838da3b130dcd6468f4848992b21a82f2a63ec34377137d9` |
| `pytest-8.4.2-py3-none-any.whl` | 365,750 | `872f880de3fc3a5bdc88a11b39c9710c3497a547cfa9320bc3c5e62fbf272e79` |

Metadata endpoints are `https://pypi.org/pypi/<package>/<version>/json`;
artifact URLs use `files.pythonhosted.org`. Exact artifact URLs, sizes and
digests belong in the preparation catalog, not runtime discovery.

The [wheel format](https://packaging.python.org/en/latest/specifications/binary-distribution-format/)
includes metadata and potentially relocated `.data` trees. A constrained
extractor must validate its supported wheel shape rather than assume every ZIP
is directly installable. Preserve distribution/license metadata; do not execute
setup scripts or install hooks. Refuse unsupported relocation/native-extension
shapes explicitly. No ambient pip, user packages or credential-bearing
environment is needed for this pinned pure-Python environment.

## Remaining acceptance

The new publication primitive alone is insufficient. Still required: bounded
download and archive verification/extraction, deterministic environment capture,
cancelled/partial preparation cleanup, clean-HOME CLI preparation, channel-root
composition, doctor/init readiness, and a Python workflow delivery fixture.
The catalog does not authorize arbitrary project dependencies or pytest flags.
Normal project setup remains network-free; preparation is a separate explicit
step and never replaces an existing snapshot or runs project code.
