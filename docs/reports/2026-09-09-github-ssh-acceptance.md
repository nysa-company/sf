# GitHub SSH acceptance — 2026-09-09

Verdict: implementation goal complete on `feat/github-ssh`.
Tested source: `fcb44195afbca4a3e81cd4d286ff58437d8142c4`.
This receipt and the associated plan/memory updates are documentation-only.
Main and installed binaries remain unchanged.

## Requirement evidence

| Requirement | Evidence |
| --- | --- |
| Independent corroboration | Three agent lanes reviewed the plan, transport and CLI diagnostics. Findings about Git SSH-option precedence, agent-only identity use, and HTTPS diagnostic compatibility were repaired before final validation. |
| Safe origins and helper dispatch | `internal/gitssh/ssh_test.go` covers all four supported origins, fixed endpoint/host keys and hostile argv/URL refusal. `internal/git/git_test.go` exercises real Runner/helper dispatch and a complete local Git protocol exchange. |
| Production integration | `internal/localruntime/factory_test.go` binds channel-specific sibling assets and the explicit agent, and excludes SSH capability from prepublication-only composition. Runtime-assets and Store/publication tests cover asset and repository-identity agreement. |
| CLI and readiness | Auth tests cover explicit SSH/HTTPS selection, existing-login behavior and no automatic key upload. Doctor tests distinguish API login, local agent/assets and unverified repository access, including conflicting fetch/push identities. |
| HTTPS compatibility | Existing HTTPS suites remain passing; new Doctor regressions preserve accepted `.github`, `_repo` and `-repo` names. No transport fallback or registered-origin rewrite was added. |
| Hosted validation | Focused SSH workflow and all 21 full acceptance jobs passed on the exact source commit above. |
| Private-repository acceptance | The exact CI-built helper authenticated the user's SSH agent, read private Relay main and completed a push dry-run. Scope is detailed below. |
| Setup and recovery | [GitHub SSH how-to](../how-to/github-ssh.md), CLI reference and architecture documentation describe setup, API requirements, fixed port 443, agent restart recovery and origin-drift refusal. |

## Hosted results

- [Focused SSH run 34379839983](https://github.com/nysa-company/sf/actions/runs/34379839983):
  normal auth/CLI/Git/SSH/assets/localruntime/publication tests; Store SSH parser
  regression; selected race tests; `go vet ./...`; `scripts/repo-check`,
  `scripts/docs-smoke`, `scripts/secret-scan`; complete development bundle build.
- [Full acceptance 34379860551](https://github.com/nysa-company/sf/actions/runs/34379860551):
  all 21 jobs passed, including Store race shards, runtime integration shards,
  broad race lanes, crash/recovery, security, upgrade, compiled end-to-end,
  static validation and the required aggregate.

The superseded source run exposed a broken-pipe fixture error: its fake SSH
helper exited before Git completed the protocol exchange. The final fixture
uses real local `git-upload-pack`; the failing run was not counted as passing.

## Host-only acceptance and limits

Hosted CI cannot access the user's personal SSH agent. On the user's Mac, the
downloaded CI bundle reported the exact tested commit and development channel.
Using its compiled `sf-ssh-dev`, pinned known-hosts file, explicit agent socket,
scrubbed environment and the common `git@github.com:OWNER/REPO.git` spelling:

- GitHub authenticated the key as `javieraldape`.
- Private Relay `refs/heads/main` read succeeded at
  `72d28a59f41caec297545a97ed57d12763fcdf3f`.
- `push --dry-run` to a validation branch succeeded through the same helper.

The dry-run did not create or update a remote ref. No live SSH ticket, PR,
merge, branch-protection change or private-repository CI execution is claimed.
Higher-level workflow acceptance here is provided by hosted hermetic tests,
not by a new real Relay delivery. No local automated test suite or build ran.

No private-key contents were read, copied or uploaded. The default login agent
was empty; an explicit user-owned agent worked. A foreground daemon must inherit
the working `SSH_AUTH_SOCK`, and must be restarted when that socket changes.
GitHub API authentication remains independently required for PRs/checks/merges.

## Handoff

Use the feature branch with a complete matching-channel bundle and the setup
guide. The task did not install a daemon, change an active ticket/database,
rewrite registered origins, create a PR or merge into main. Those are separate
release actions, not unverified claims attached to this acceptance verdict.
