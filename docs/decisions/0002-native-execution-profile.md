# ADR 0002: native execution-profile capability verdict

Status: accepted for the guarded beta; autonomous capability rejected on the
tested platform.

## Context

Manual and guarded `sf` execution are limited to explicitly trusted providers
and repositories. Autonomous merge has a stronger requirement: the exact
provider and repository-command executor must prove OS-enforced filesystem,
credential, Git-control, child-process, and network restrictions on the
target macOS release. Provider allow/deny prompts are not an OS boundary.

## Probe

`spikes/native-profile/run.sh` is the bounded, automated capability probe. It
runs against synthetic non-secret parent/home, SSH, Keychain, GitHub, and
provider-home sentinels. It also creates a synthetic worktree `.git` control
file and a changed command helper. It checks real sensitive paths only through
metadata-only `test -r` calls. The probe does not read host credential bytes or
persist raw command output.

Tested host:

- macOS 26.6.2 (build 25G83), Darwin 25.6.0, arm64.
- Candidate primitive: `/usr/bin/sandbox-exec` (Seatbelt), invoked with an
  explicit generated profile.
- The profile denies network, synthetic and metadata-only real home-sensitive
  reads, `.git` writes, `launchctl`, and `installer`; its child processes
  inherit these restrictions.
- The profile needs broad system-library reads to start ordinary macOS tools.
  It therefore demonstrates the listed denials only, not complete arbitrary
  filesystem confinement.

Observed bounded results:

| Capability | Result | Evidence |
| --- | --- | --- |
| Synthetic home/SSH/Keychain/GitHub/provider-home reads | Denied | Known-existing sentinel reads fail under the profile. |
| Metadata-only real home/SSH/Keychain/GitHub reads | Denied | `test -r` fails without reading credential bytes. |
| Worktree source write | Allowed | A normal file in synthetic worktree is written. |
| `.git` control write | Denied | The synthetic `.git` control path cannot be created/changed. |
| Arbitrary network | Denied | Profile has `deny network*`; loopback curl fails. |
| Changed command exfiltration | Denied | Helper cannot read sentinel or complete loopback curl. |
| `launchctl` / installer execution | Denied | Exact executable paths are denied; no service/package mutation is attempted. |
| `setsid` + double fork writer | **Not contained** | Detached grandchild writes an allowed worktree marker after the initial child exits. |

## Decision

`sandbox-exec` is available and can demonstrate several useful Seatbelt
restrictions on this host. It does **not** prove a complete arbitrary-filesystem
boundary or the required process-lifecycle guarantee: a detached descendant can
retain worktree write access after the supervised process returns. Therefore:

- `autonomous_eligible=false` for this primitive/profile on macOS 26.6.2.
- Guarded/manual work may continue only under the documented trusted
  provider/repository baseline; this ADR makes no hostile same-UID containment
  claim for that baseline.
- No Docker or Colima fallback is introduced.
- A future autonomous candidate must prove both inherited restrictions and a
  bounded mechanism that prevents or reliably contains detached worktree
  writers. It must then repeat this probe for the exact provider versions and
  repository-command executor.

## Consequences

The factory must treat a detected possible writer as `blocked_process` and
quarantine the worktree. It must not commit, clean, resume, or hand over that
worktree until host repair/reboot plus `sf recover` proves the writer is gone.
The spike is intentionally not a production execution implementation.

## Guarded dependency-free Node 22 recipe

The guarded local baseline additionally supports one fixed Node recipe without
npm: source `node --test` for a bounded dependency-free JavaScript/CJS/MJS
worktree. `package.json` is a regular non-symlink JSON object no larger than
1 MiB and may not declare dependencies, workspaces, or bundled packages. The
worktree has no symlinks, `node_modules`, native `.node` add-ons, or TypeScript
source, and must contain an official Node test-discovery file. Zero tests
refuse rather than turning local verification into a no-op.

The implementation resolves only code-owned Node paths, authenticates major
22, copies the complete non-system Mach-O closure to an owner-only stage, and
binds a canonical closure digest to the durable command claim. The Node gate
uses the staged binary with `DYLD_LIBRARY_PATH`; its Seatbelt profile permits
only the worktree, private closure, and necessary system reads while denying
writes, network, fork, and further exec. Node's fixed permission/no-addon/test
flags are defense in depth. npm, package scripts, dependencies, and TypeScript
remain durable GitHub CI or operator takeover. Docker and Colima are not a
prerequisite for this recipe, and this does not change the ADR's negative
autonomous eligibility verdict.
