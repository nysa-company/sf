---
status: in-progress
branch: feat/local-factory-v1
timestamp: 2026-09-06T14:15:45Z
files_modified: []
---

## Working on: Acceptance reboot recovery

### Summary

User is saving work before manually restarting this Mac. SF's self-serve CLI beta
has delivered and merged the real acceptance PR, but the local ticket has not
reached done. Preserve the quarantine until normal same-host/new-boot recovery.
Source is committed at 3f36eb799cf6a4f3a40941ab26ddac5b3cfa82e8; docs HEAD before
this checkpoint is a7197d0. Branch was clean, ahead 46. No push is requested.
This checkpoint is kept in the repository's tracked context directory so it
survives workspace handoff without relying on an external skill-state directory.

### Exact saved state

- Repository: `/Users/sofiagonzalez-2/Projects/nysa-company/sf`.
- Installed executable: `/Users/sofiagonzalez-2/Projects/nysa-company/sf/.context/onboarding5.Ga8mYI/installed/sf-dev`.
- Bundle: same parent, `bundle/`; version 0.1.0-dev.onboarding5, source 3f36eb7, darwin/arm64. Build, manifest verification and exclusive install passed.
- Acceptance HOME: `/private/tmp/sf-onboarding-acceptance.R2QvPm/home`.
- Database: HOME + `/Library/Application Support/sf/dev/sf.sqlite`.
- Acceptance project: `/private/tmp/sf-onboarding-acceptance.R2QvPm/project`.
- Ticket: `SF-543bc4cd3b9a9a6291c2bbc7ca20b3b1`, project `onboarding-counter`.
- Fresh read-only save check: merging v15/runner6; quarantine1, checkpoints1, recoveries0.
- No sf daemon process or acceptance socket owner found by native ps/lsof at save time. Previous session75245 is unavailable. Do not reuse old PIDs.
- Private SQLite backup: `.context/reboot-backup.Lt2dWD/sf.sqlite`, quick_check=ok. This is a fallback preservation copy only. Never substitute it automatically for the original database.
- Private runtime/project/worktrees are under `/private/tmp`; verify they still exist after reboot. If missing, stop and investigate preserved data rather than creating a replacement identity.

### Decisions made

- No automatic reboot, forced kill, database edits, quarantine deletion, forged boot identity, second merge, or repeated approval.
- V58 prepare records an immutable checkpoint, not permission to unlock. Recover requires OS-observed same machine and different boot plus exact current authority/CAS checks.
- Retiring quarantine does not confirm effects. Existing uncertain merge/protected proof must reconcile normally.
- GitHub PR1 in `nysa-company/sf-cli-beta-acceptance-20260905` merged at 2026-09-06T05:03:08Z. Approved head `ee35025e60092cfd25480121537acec4e4f33a1d`; merge `00860455167278a63b17ad40d5599b74aae5f636`. Do not request another hash authorization.
- No stable-channel, Nysa, public release, or remote updates are part of this save.

### Remaining work, in order

1. After user reboot, inspect this checkpoint and repository state; verify the exact installed version and original acceptance paths remain present.
2. Start only the isolated daemon with the original HOME and real credential locations:

```sh
env -i \
 HOME=/private/tmp/sf-onboarding-acceptance.R2QvPm/home \
 TMPDIR=/private/var/folders/01/fnjrykjs5k721nqj3wf04t3r0000gn/T \
 GH_CONFIG_DIR=/Users/sofiagonzalez-2/.config/gh \
 CODEX_HOME=/Users/sofiagonzalez-2/.codex \
 SF_CODEX_PROVIDER_CAPACITY=2 \
 PATH=/Users/sofiagonzalez-2/.local/bin:/opt/homebrew/bin:/usr/bin:/bin:/usr/sbin:/sbin \
 LANG=C \
 /Users/sofiagonzalez-2/Projects/nysa-company/sf/.context/onboarding5.Ga8mYI/installed/sf-dev daemon run
```

3. With the same HOME, run installed `sf-dev daemon cleanup recover --json`. Let the CLI prove same-host/new-boot; expected audit1/quarantine0 only on valid evidence. Same-boot exit3 is an intentional refusal, not a bypass opportunity.
4. Run installed `sf-dev status SF-543bc4cd3b9a9a6291c2bbc7ca20b3b1 --json`; allow normal exact merge/protected-ref reconciliation. If qualification is required, use ordinary `providers qualify --builder codex --reviewer codex` with the real environment. No model rerun is implicitly authorized or needed to fabricate delivery.
5. Verify durable done and settled leases/effects before claiming acceptance complete. Update `docs/reports/2026-09-05-cli-onboarding-acceptance.md` and goal status with actual results.

### Validation and notes

Final frozen-source session50924 exit0: focused race hostidentity/Store/daemon/CLI,
full `go test -p 1 ./...`, vet, repo-check, secret-scan, docs-smoke, diff-check.
No Go edits followed. Bundle session88508 exit0. Live prepare and idempotent
prepare passed; same-boot recover exit3 made no mutation. No new tests run while
saving this checkpoint.

Durable lessons: keep source frozen during validation; use the trusted per-user
TMPDIR for gh/helper snapshots; bounded independent cleanup contexts fix future
cancellation but cannot retroactively prove old processes died. Daemon restart
alone does not supply the required new-boot evidence. The larger beta still has
external adoption/reliability targets and explicit stack-support limitations;
do not equate this merged PR with all of stable v1 being complete.
