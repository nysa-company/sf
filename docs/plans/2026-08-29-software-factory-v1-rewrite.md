# Software Factory v1 replacement

Status: Approved; release-candidate hardening and validation in progress

Plan owner: Sofia, with Codex as plan author

Working product name: Software Factory; CLI name: sf

Target: a new Go repository, not an in-place rewrite of this legacy repository
Related artifacts:

- [Verification plan](2026-08-29-software-factory-v1-test-plan.md)
- [Normative state machine](2026-08-29-software-factory-v1-state-machine.json)
- [Implementation tasks](2026-08-29-software-factory-v1-tasks.jsonl)

The implementation-task JSONL is the original execution backlog. Historical
Cursor/Claude, LaunchAgent, and autonomous items in that backlog are not v1
product claims; this plan and the current verification plan are authoritative.

## Outcome

Software Factory v1 is a local, operator-friendly service that turns one
Markdown ticket into a reviewed GitHub pull request and, according to the
selected merge mode, either waits for Sofia to merge externally or requests an
exact reviewed-head merge after her approval.

The shortest successful path is:

~~~text
sf submit ticket.md --project nysa
sf start SF-123

ticket
  -> plan
  -> independent verification written before implementation
  -> build
  -> verified implementation commit
  -> draft pull request
  -> fresh independent review + GitHub checks
  -> human external merge or approval followed by guarded exact-head merge
  -> merged-state reconciliation
  -> done
~~~

At any active or waiting workflow point, Sofia can see what is happening,
pause safely, stop the current agent, enter the worktree when one exists, make
changes, and return control to the factory. A queued ticket is controlled with
`start`; a typed blocked ticket remains fail-closed and is controlled with
`status` and `recover`, never by bypassing its recovery prerequisite.
Every stopped state must name the cause and print one exact next action or one
typed host prerequisite followed by the recovery command. The CLI never invents
a retry when disk, authentication, filesystem ownership, or a live escaped
process must be repaired first.

## Product boundary

### v1 is for

- Sofia building Nysa on a trusted local macOS machine.
- Trusted repositories explicitly registered by the operator.
- Feature, bug, refactor, infrastructure/configuration, documentation, and
  non-merging spike tickets.

Broader open-source contributor support follows the Nysa pilot; it is not a v1
release claim.

### v1 includes

- A friendly sf CLI and foreground local daemon.
- Local Markdown tickets; GitHub Issues are not required.
- Per-ticket Git worktrees.
- Planner, Builder, and Reviewer logical roles.
- A pre-build verification invocation of Reviewer and a fresh post-build
  review invocation.
- Codex execution for both logical Builder and Reviewer roles. Cursor, Claude,
  and provider fallback selection are future work.
- SQLite as the single application-state authority.
- The selected custom Go state engine over SQLite; DBOS remains only as a
  rejected, reproducible proof spike.
- GitHub pull requests and merge through the official gh CLI.
- Manual external merge observation and guarded automatic merge after human
  approval.
- Bounded concurrency, defaulting to two active tickets.
- Separate stable and development channel builds and state roots.
- Crash recovery from the last durable phase.
- Time and optional cost ceilings.

### Explicitly not in v1

- Legacy ticket, passport, receipt, journal, ledger, or partial-run migration.
- Linear, Jira, GitHub Issues, initiatives, or a backlog-management system.
- Ticket dependency graphs or automatic initiative scheduling.
- Deployment, preview-environment orchestration, or production canarying.
- Laptop-off execution, remote workers, a hosted control plane, or multi-host
  coordination.
- A web dashboard, Slack bot, or mobile operator UI.
- GitLab, Bitbucket, Gitea, or a generic forge abstraction.
- Linux and Windows packaging or daemon support. The Go core should remain
  portable, but v1 support and safety qualification are macOS-only.
- GitHub merge queues. A repository that requires one may use manual mode; sf
  does not enqueue or interpret merge-group commits in v1.
- Docker, Docker Desktop, Colima, or container execution as a prerequisite.
- Untrusted repository execution.
- Qualification generations, sealed per-product releases, or product-specific
  factory pins.
- Passports, transition receipts, route journals, tracked audit ledgers, or
  multiple projections of authority.
- Mandatory Spec-linter, Test-author, Narrator, or six-agent separation.
- Dynamic model portfolios, automatic provider benchmarking, or Kimi.
- Deployment after merge.

The v1 factory stops at a reconciled GitHub merge. Another tool or a later
version may own deployment.

## First install and first ticket contract

The release tutorial is a tested artifact, not an illustrative README. v1
supports macOS. A stable binary does not require Go; a source/development build
does.

Prerequisites:

- Git and the official gh CLI.
- gh authenticated to the target GitHub account.
- One qualified Builder provider and one qualified independent Reviewer
  provider. The same executable may expose multiple model families only when
  the recorded inference families are actually distinct.
- A trusted local repository with an origin on GitHub.
- No Docker, Colima, root privilege, deployment credential, or GitHub token
  copied into sf.

The development walking path must remain executable throughout implementation.
Until a remote is approved, its documentation test starts from the existing
local source checkout rather than inventing a clone URL:

~~~text
cd /absolute/path/to/sf-source
make build-dev

./bin/sf-dev auth status
./bin/sf-dev auth login github       # delegates interactively to gh
./bin/sf-dev auth login codex        # delegates to the installed provider

./bin/sf-dev init --project nysa --repo /absolute/path/to/nysa-app

# In another terminal during the foreground milestone:
./bin/sf-dev daemon run

# Back in the first terminal:
./bin/sf-dev providers qualify --builder codex --reviewer codex
./bin/sf-dev doctor --repo /absolute/path/to/nysa-app

./bin/sf-dev submit ./tickets/fix-duplicate-reminders.md \
  --project nysa
# Submitted SF-<id> to dev/nysa. No work has started.

./bin/sf-dev start SF-<id>
./bin/sf-dev status SF-<id> --watch
~~~

version, auth, init, providers qualify, and doctor have a direct local path and
do not require a running daemon. Doctor also reports daemon/socket status when
one exists. Ticket lifecycle commands require the daemon. auth login never
asks the daemon to read or store a displayed token. It starts
the official interactive provider/GitHub login flow in the operator's terminal.
auth status and doctor report only provider, account label when safely
available, credential state, and remediation; they never print credential
bytes.

The v1 provider is Codex for both the Builder and Reviewer logical roles.
Separate invocations and Store-bound evidence preserve role boundaries; v1
does not claim independent provider families. Doctor prints the configured
provider, authentication state, and qualification level. Cursor, Claude, and
cross-provider fallback remain post-v1 work.

The stable tutorial must replace `make build-dev` with the exact checksummed
release-installation command selected and tested before the first public
release; this plan does not publish a placeholder command. Until then, local
v1 acceptance is credential-free and proves only the documented local gates;
it is not real-provider qualification, disposable-GitHub qualification, or a
stable-promotion decision. Those are manual release gates: provider
qualification uses an isolated Codex fixture with no GitHub mutation, and the
separately authorized disposable non-production GitHub guarded flow precedes
the ten-ticket guarded Nysa pilot. `update` and `rollback` currently fail
closed, so no CLI promotion command is implied. The complete path from an
already installed/authenticated machine to a submitted, visible ticket has a
five-minute documentation-test budget, excluding model runtime.

## Why replacement is the correct boundary

The legacy system has accumulated more than 200,000 executable, source, and CI
lines. Its main controller is approximately 14,700 lines, its state machine is
approximately 4,300 lines, and its launcher and role wrapper add thousands
more. The architecture document requires several mutually authenticating
representations of the same lifecycle: passports, transition receipts, route
journals, runtime manifests, tracked and untracked ledgers, activation records,
qualification generations, and protected migration records.

Those mechanisms are individually defensible, but together they make recovery
proof harder than ticket delivery. The current provider lock also serializes
provider intervals even when two ticket leases exist. Thirty of the forty-eight
newer commits inspected during the audit describe fixes, recovery,
stabilization, retry, refusal, or related repair work.

The new repository avoids two traps:

1. It does not translate old workflow state into a different model.
2. It does not reuse the old controller or state-machine implementation and
   then attempt to delete complexity around it.

The legacy repository is frozen and tagged. It remains a reference for tested
behaviors and failure fixtures, not a runtime dependency.

## What is retained and what replaces it

| Legacy capability | v1 treatment | Reason |
| --- | --- | --- |
| Exact-head Git and GitHub checks | Retain | Prevents stale review or merge |
| Process-group cancellation | Retain | Required for pause, cancel, and takeover |
| Provider identity/readiness checks | Simplify and retain | Fail before paid or mutating work |
| Independent tests before implementation | Retain as verification-first policy | Protects acceptance intent |
| Bounded repair loops | Retain, maximum two automatic corrections | Prevents invisible infinite work |
| Cost/time accounting | Replace with phase attempts and one ticket ceiling | No separate financial ledgers |
| Qualification generations | Replace with ordinary CI, a disposable walking skeleton, SemVer, and stable/dev channels | Familiar open-source release model |
| Per-product sealed releases | Replace with explicit stable binary version and isolated dev binary | Sofia can improve sf without moving Nysa |
| Passports and transition receipts | Replace with transactional ticket, phase-run, and effect rows | SQLite is the authority |
| Route journals | Replace with the provider/model/version recorded on each phase attempt | No mid-ticket routing language |
| Multiple ledgers | Replace with attempt usage columns and one derived event stream | One authority, one readable projection |
| Six mandatory roles | Replace with Planner, Builder, Reviewer | Independence is applied where it changes risk |
| Narrator evidence bundle | Replace with a deterministic PR summary from plan, proof, tests, review, and checks | Evidence stays useful without another agent |
| Spec-linter | Replace with typed plan validation plus Reviewer verification | No separate paid role for schema checking |
| Test immutability across Git history | Replace with protected verification intent and explicit amendment review | Tests can be corrected without chronology repair |

## Design principles

1. One SQLite database per channel is the only mutable authority.
2. A human-readable event file is a projection, never a second source of truth.
3. Model output is untrusted input; typed code validates it before state or
   external effects change.
4. The factory, not a provider, owns commits, pushes, PR creation, approval,
   and merge.
5. Every external mutation has an idempotency key and a read-before-retry
   reconciliation path.
6. A worktree isolates Git changes; provider restrictions and sf policy form
   the trusted-repository safety boundary.
7. Every loop has a visible counter and a small limit.
8. Pausing is a normal state, not an exceptional failure.
9. Operator commands use product language and always show the next action.
10. Stable Nysa work never reads development factory state.

## System architecture

~~~text
                         local macOS user
                                |
                   +------------+-------------+
                   |                          |
                 sf CLI                    sf-dev CLI
                   | Unix socket              | separate socket
                   v                          v
          +-----------------+        +-----------------+
          | stable daemon   |        | dev daemon      |
          | one Go binary   |        | one Go binary   |
          +--------+--------+        +--------+--------+
                   |                          |
            stable SQLite DB             dev SQLite DB
            stable worktrees             dev worktrees
            stable logs/events           dev logs/events
                   |
        +----------+-----------+--------------------+
        |                      |                    |
  durable workflow       scheduler/limits     operator control
  custom Go engine       global/repo/provider pause/take/resume
        |
  +-----+---------+----------------+------------------+
  |               |                |                  |
Planner       Reviewer verify    Builder          Reviewer final
  |               |                |                  |
  +---------------+-------+--------+------------------+
                          |
                    provider runner
                    Codex (v1)
                          |
                disposable ticket worktree
                          |
             +------------+-------------+
             |                          |
          local Git                official gh CLI
          commit/push              PR/checks/merge
~~~

### Components

#### CLI

The public CLI uses Cobra for commands, help, completion, suggestions, and
consistent exit handling. Cobra is mature and supplies the conventional
subcommand behavior expected from a Go CLI:
[Cobra repository](https://github.com/spf13/cobra).

The CLI is a thin client. It validates obvious arguments, sends a versioned
request over the channel's Unix socket, renders the response, and exits with a
stable code. It never mutates workflow state directly.

#### Daemon

The daemon owns:

- SQLite and schema migration.
- Workflow start/resume and durable signals.
- Admission and concurrency limits.
- Process supervision.
- Worktree lifecycle.
- Provider invocation.
- Git and GitHub effects.
- Event projection and log redaction.
- Recovery after restart.

On macOS, the operator launches the selected channel's daemon in the
foreground. No root privileges or system service are required; LaunchAgent and
other background-service packaging remain post-v1 work.

Each channel has exactly one leader:

1. Startup securely opens a fixed owner-only, non-symlink runtime lock and
   takes a nonblocking OS advisory lock for the lifetime of the daemon.
2. Under that lock, it increments a monotonic leader epoch in SQLite.
3. Every ticket runner, phase run, and effect claim records that epoch plus a
   monotonic per-ticket runner epoch.
4. State/effect completion uses compare-and-swap on ticket version and runner
   epoch. A stale worker may report an observation but cannot advance state.
5. Pause, take, cancel, and leader recovery increment the ticket runner epoch
   before draining or adopting work.

A second daemon, stale socket, replaced lock path, or stale worker is a typed
refusal. The OS lock prevents two live effect executors; database epochs prevent
late results from a superseded runner from being interpreted as current.

#### Durable workflow engine

The custom Go state engine over the application SQLite schema is selected for
v1. The DBOS Go spike was run as a bounded design check and rejected for the
local contention/recovery contract; it is not a v1 production dependency.
DBOS documents local SQLite configuration, recovery from the last completed
workflow step, workflow communication, and transactions that combine
application writes with workflow checkpoints:

- [SQLite configuration](https://docs.dbos.dev/golang/reference/configuration)
- [Workflow recovery and step semantics](https://docs.dbos.dev/golang/tutorials/workflow-tutorial)
- [Application data source](https://docs.dbos.dev/golang/reference/datasources)
- [Messages and events](https://docs.dbos.dev/golang/tutorials/workflow-communication)

DBOS steps are at-least-once. The rejected spike therefore does not remove the
need to reconcile Git, provider, or GitHub effects; the selected custom engine
owns those checkpoints and reconciliations in v1.

##### One-day DBOS proof gate

The spike passes only if one small Go program proves all of the following:

1. It runs with a pure local SQLite database and no external server.
2. A forced daemon kill resumes after the last completed checkpoint.
3. A completed local event is not duplicated after restart.
4. A fake remote mutation whose response is lost is reconciled without a
   duplicate mutation.
5. Pause, take, and resume signals work from another process.
6. Cancellation kills the complete descendant process group.
7. Stable and dev databases cannot see or resume each other's workflows.
8. A workflow-code version change returns either a safe migration or an exact
   incompatible-version message.
9. The event projection remains readable and ordered.
10. Tests can inject a crash at every boundary deterministically.
11. Injected SQLITE_BUSY/locked operations obey an explicit operation deadline,
    return the planned typed error/pause, and leave no background retry running.
12. The proof records the database connection limit, WAL and busy-timeout
    ownership, migration owner, DBOS system-table ownership, and daemon leader
    behavior.
13. A crash after the application records queued to planning but before the
    workflow is durably owned cannot strand or duplicate the run. The selected
    engine must prove either one atomic transaction or deterministic startup
    reconciliation/re-enqueue using a stable workflow ID; an irreconcilable
    observation becomes a typed blocked state.

The spike failed its SQLite contention requirement. The decision is final for
v1: implement and use a small explicit Go state engine over the same schema.
The workflow-facing application interface remains the same, so the rest of the
plan does not depend on DBOS types. v1 carries one production runtime, not two.

#### SQLite

Use the pure-Go modernc SQLite driver unless the DBOS spike requires another
supported pure-Go path. The driver avoids a C toolchain dependency:
[modernc SQLite](https://pkg.go.dev/modernc.org/sqlite).

SQLite runs in WAL mode with a busy timeout, foreign keys enabled, explicit
transactions, and bounded migrations. Each stable release snapshots the
database before a schema migration. A binary that cannot understand the stored
schema or active workflow version refuses to start and prints the compatible
version or rollback command. The selected workflow engine owns the connection
pool after startup; one migration process owns schema changes before workflow
recovery begins. Every database operation has a context deadline. A runtime
whose internal retry can outlive that deadline fails the engine proof gate.

#### Provider runner

Providers implement one narrow internal contract:

~~~text
Run(ctx, PhaseInput) -> PhaseResult

PhaseInput:
  ticket ID, role, prompt, repository/worktree, allowed paths,
  provider/model, timeout, environment policy, structured-output schema

PhaseResult:
  outcome, structured artifact, redacted transcript path,
  provider/model/version, usage if available, changed-file inventory
~~~

There is no dynamic portfolio language. Defaults are code-owned and
configuration may override them:

- Planner, Builder, verification, and final review use separately qualified
  Codex invocations for their logical roles.
- Provider selection and cross-provider fallback are post-v1 work.
- Bounded retries are controlled by the phase policy; exhausted work pauses or
  blocks with a typed next action.
- Autonomous no-human merge is post-v1.

The provider runner creates a dedicated process group, gives it a minimal
environment, streams structured output through a redactor, and enforces soft
and hard timeouts. TERM is followed by a bounded drain and KILL. The worktree
is retained after pause, failure, cancellation, or takeover until an explicit
cleanup policy applies.

Provider authentication is adapter-specific and allowlisted:

- auth login launches the official interactive login in the operator's
  terminal; the daemon never accepts a pasted token.
- Before a paid phase, the adapter checks authentication in the same
  foreground environment that will run the task.
- Each attempt receives an isolated temporary provider home containing only
  the minimum validated authentication material that provider supports.
- Provider authentication may reach that provider. GitHub credentials, product
  secrets, the operator's general environment, SSH agents, and unrelated
  provider homes do not enter the attempt.
- If authentication cannot be isolated or expires, the phase does not start.
  Doctor prints the exact auth login command.

Repository verification/review commands do not run inside the daemon's
credential-bearing environment. They use a separate credential-free command
executor under the guarded baseline described below; autonomous eligibility
also requires that executor to pass the stronger native profile.

#### GitHub integration

v1 is intentionally GitHub-specific. GitHub behavior lives in one internal
package for testability, but it is not a public forge abstraction.

The official gh CLI is required and is invoked only with explicit,
non-interactive arguments and machine-readable output. Doctor checks binary
version and authenticated account without printing tokens. The CLI supports
authentication status, PR operations, automatic merge, and matching the
expected head commit:

- [gh authentication status](https://cli.github.com/manual/gh_auth_status)
- [gh pull request commands](https://cli.github.com/manual/gh_pr)
- [gh command reference](https://cli.github.com/manual/gh_help_reference)

No command calls gh auth token. Credentials remain owned by gh and the macOS
credential store.

## Safety boundary

v1 runs only explicitly trusted repositories. Docker and Colima are not
required. The trusted-repository boundary does not claim that an arbitrary
same-UID hostile executable is harmless; the selected provider CLI and
registered base repository are part of the local trust base.

For each ticket:

1. sf creates one worktree beneath the selected channel root.
2. sf records and repeatedly authenticates the repository, unique branch,
   worktree .git pointer, Git common directory, origin, base ref, and Git
   configuration identity.
3. The provider starts with the worktree as its current directory, an isolated
   provider home, a scrubbed environment, and provider-specific restrictions.
4. Every enabled provider meets the guarded baseline: isolated authentication,
   scrubbed environment, provider restrictions, process supervision, immutable
   Git identity, and bounded local guarded qualification probes. This baseline
   protects factory integrity but, because the provider and repository run as
   the same macOS user, it is not advertised as containment against arbitrary
   hostile same-UID code. Real-provider hostile-fixture evidence is a separate
   manual stable-promotion gate and is not produced by `providers qualify` or
   `make test-all`.
5. Repository test/verification commands run in a separate credential-free
   baseline executor. Their argv and command-policy digest
   are snapshotted from registered configuration before the ticket starts.
   Builder changes to Makefiles, package scripts, or helpers therefore execute
   without daemon/provider/GitHub credentials and cannot update the active
   command policy. Network and package installation are denied wherever the
   selected macOS enforcement primitive can prove that denial; otherwise the
   trusted-repository boundary is explicit and autonomous eligibility fails.
6. Provider and repository-command processes must not invoke mutating Git,
   gh, merge, release, or deployment operations. Read-only Git needed for
   context is explicitly allowlisted.
7. sf validates symlinks, changed paths, file types, Git control-plane
   identity, and the full Git diff immediately before committing.
8. sf invokes Git with an isolated HOME, scrubbed GIT_* environment, explicit
   worktree/common-directory identity, hooks disabled, and the certified
   origin. v1 refuses submodules and nested repositories.
9. The guarded policy never grants project-command network or package
   installation and exposes no factory/GitHub credential. An observed attempt
   blocks and a ticket that needs new dependencies pauses for operator
   takeover. Where native enforcement is unavailable, hostile same-UID bypass
   remains inside the declared trusted-repository assumption; that profile is
   ineligible for autonomous mode. Autonomous execution must prove the denial.
10. sf owns the process group and scans for escaped descendants before it
    validates, commits, takes over, resumes, or cleans a worktree.

Codex has a documented workspace sandbox, Claude documents sandbox and
permission controls, and Cursor documents allow/deny policies:

- [Codex local safety](https://openai.com/index/running-codex-safely/)
- [Claude Code security](https://docs.anthropic.com/en/docs/claude-code/security)
- [Cursor CLI permissions](https://docs.cursor.com/cli/reference/permissions)

Because those protections are not equivalent, provider eligibility has two
levels:

- qualified_guarded: the trusted-provider/repository baseline above passes.
  It does not claim hostile same-UID containment and is sufficient only for
  manual and guarded mode.
- autonomous_eligible: qualified_guarded plus an OS-enforced filesystem,
  credential, Git-control, process, and network capability contract passes for
  both provider tools and repository commands on the exact provider/version and
  target macOS release.

The Phase 0 native-profile spike may accept only a documented provider-native
OS sandbox or a macOS enforcement primitive whose exact executable, profile,
entitlements/arguments, supported OS range, and child inheritance are recorded
and regression-tested. Cursor allow/deny rules alone are not an OS boundary.
The spike attempts parent/home/Keychain/SSH/GitHub reads, .git writes, arbitrary
network, changed-command exfiltration, setsid/double-fork, and launchd escape.
If no candidate passes, guarded development continues but autonomous mode is
disabled and the post-v1 autonomous roadmap stops for an explicit product
decision; Docker/Colima is not silently introduced.

The hostile fixture attempts a parent/home/Keychain write/read, symlink escape,
secret read, network command, changed test-script exfiltration, background
child, setsid/double-fork escape, destructive shell command, .git/config/hook
mutation, and git/gh push. A fixture failure disables that provider/version
entirely. Merely lacking the stronger enforceable boundary limits it to
guarded/manual mode.

If TERM/KILL cannot prove that every writer is gone, the ticket becomes
blocked_process. sf does not grant takeover, resume, commit, or cleanup. It
prints the observed process identity and a safe host-repair/reboot prerequisite
followed by sf recover; it never waits forever or guesses that the writer died.

A future high-risk mode may use containers for untrusted repositories. It is a
separate product capability because a Linux VM adds image, mount, credential,
network, cache, and takeover complexity and cannot execute Xcode/macOS builds.

## Ticket contract

Tickets are local Markdown. sf stores the original bytes and digest in the
channel database; the source file does not become mutable workflow state.
The daemon assigns an SF identifier unique within that channel. An identical nonterminal
submission for the same project and source digest returns the existing ticket
instead of creating a duplicate. Repeating a terminal ticket requires
sf submit --new and creates a new ID.

Each ticket receives one persisted remote branch identity:

~~~text
refs/heads/sf/<channel>/<project>/<ticket>-<random-suffix>
~~~

The suffix is generated once and never rediscovered from branch listings.
Stable and dev therefore cannot collide in Git even when they independently
assign the same ticket ID. Every CLI view prints channel and project; the
channel-specific binary plus a channel-scoped ID make sf start SF-<id>
unambiguous.

Minimum accepted input:

~~~markdown
# Fix duplicate reminders

Users occasionally receive the same scheduled reminder twice.

## Acceptance

- One reminder is delivered for one schedule occurrence.
- A regression test demonstrates the previous duplicate.
~~~

Optional YAML front matter may set:

~~~yaml
type: bug
merge: guarded
priority: normal
max_duration: 90m
max_cost_usd: 20
~~~

Repository, base branch, provider defaults, commands, and safety policy come
from registration/configuration rather than being repeated in every ticket.
Only title and nonempty problem text are required at submission. Missing
acceptance detail does not cause a parser maze: Planner either produces
testable criteria or pauses with a bounded list of concrete operator questions.

Supported types:

| Type | Required pre-build proof | Merge behavior |
| --- | --- | --- |
| bug | Independent red regression test | Normal |
| feature | Independent black-box/acceptance test when feasible; otherwise an executable proof plan | Normal |
| refactor | Characterization baseline plus unchanged behavior assertions | Normal |
| infrastructure/config | Validate/dry-run proof and explicit rollback check | Normal |
| documentation | Link, example, lint, or other executable documentation check | Normal |
| spike | Time-box and report questions | Never merge automatically; no PR unless explicitly requested |

Ticket dependencies and initiatives remain prose/operator concerns in v1.
Sofia starts prerequisite tickets explicitly.

## Verification-first role flow

There are three logical roles, not six:

### Planner

- Converts the ticket into a bounded, typed implementation plan.
- Names acceptance criteria, affected surfaces, risks, test commands, allowed
  paths, and proof type.
- May ask the operator a small set of material questions.
- Cannot edit product code.

### Reviewer, verification invocation

- Runs as a fresh invocation, preferably from a model family different from
  Builder.
- Critiques the plan before implementation.
- Writes the independent regression, acceptance, characterization, validation,
  or documentation proof when feasible.
- Demonstrates the expected pre-build state: red for a bug, a failing or
  missing acceptance behavior for a feature, or a captured baseline for a
  refactor.
- sf validates the changed paths and pre-build result, creates a separate
  verification checkpoint commit, and then seals its intent, commit, proof
  digest, and owned files in SQLite before Builder starts. The red checkpoint
  remains local until an implementation makes the branch publishable.

### Builder

- Receives the ticket, accepted plan, verification intent, and worktree.
- Implements the smallest change that satisfies the proof.
- May add unit, integration, or implementation tests.
- Cannot silently delete, weaken, skip, or rewrite independent verification.
- Returns structured changed-file and command evidence; sf revalidates the Git
  control plane, reruns the proof in the credential-free command sandbox, and
  commits the implementation as a descendant of the verification checkpoint.

### Reviewer, final invocation

- Is a fresh process and context, not the pre-build Reviewer conversation.
- Reviews the exact committed diff, verification intent, test results,
  security-sensitive changes, and GitHub checks.
- Returns pass, repair with an explicit owner, or needs-operator.
- Cannot mutate the worktree.

If an independent test is wrong, Builder requests an amendment. A new Reviewer
invocation either rejects the request or records the exact amendment and reason
before sf creates a new verification checkpoint and Builder continues. Tests
are protected from silent weakening, not
declared immutable forever.

The amendment request itself moves `building` to `verifying` without
invalidating the current proof. A rejection returns to a new Builder attempt
with the original verification intent still authoritative. Only an accepted
amendment records a new checkpoint and invalidates the prior proof gates.

This implements verification-first development while avoiding the legacy
global Git chronology rules. It follows the useful TDD principle of defining a
failing behavior before implementation without treating commit order as the
product requirement:
[Martin Fowler on TDD](https://martinfowler.com/bliki/TestDrivenDevelopment.html).

## Lifecycle

The [normative state-machine artifact](2026-08-29-software-factory-v1-state-machine.json)
is the sole implementation source for states and transitions. Any
state/trigger pair it does not list is forbidden. The diagram below is only a
human summary:

~~~text
queued -> planning -> verifying -> building -> publishing
              ^          |            |          |
              |          |            |          v
              +----------+------------+      waiting_ci
              repair/amendment                   |
                                                 v
                                             reviewing
                                          /      |       \
                                    manual    guarded    post-v1
                                      |          |           |
                          waiting_manual_merge   |      merging or blocked
                                                 v           |
                                        waiting_approval     |
                                                 \           /
                                                  v         v
                                                    merging
                                                       |
                                                       v
                                                  reconciling
                                                       |
                                                       v
                                                      done

active/wait -> stopping -> paused -> classified resume
active/wait -> cancelling -> cancelled
typed unsafe prerequisite -> blocked -> sf recover
manual mismatched external merge -> external_merged
spike review pass -> done(report_only)
~~~

### Durable transition rules

- State changes occur in a database transaction with one event row, expected
  ticket version, current daemon leader epoch, and current ticket runner epoch.
- A phase run has a unique ticket, phase, and attempt number.
- A provider completion is accepted only for its exact active phase run and
  worktree/base identity and current runner epoch.
- At most two automatic correction cycles are allowed for planning,
  verification, building, or review. The next failure pauses.
- Operator rejection is not an automatic correction and does not silently
  consume that counter; it is an explicit new Builder request still bounded by
  the ticket's time/cost ceiling.
- One semantic merge-conflict refresh is attempted. A remaining conflict
  pauses with the worktree and exact files listed.
- A provider gets one configured fallback attempt. The next provider failure
  pauses.
- Any base refresh, candidate/source-head change, operator source commit, or
  accepted verification amendment creates a new candidate snapshot and
  invalidates prior proof result, GitHub checks, final review, and approval.
  Verification intent is additionally invalidated when its owned proof changes.
  The relevant proof, checks, and final review always rerun; there is no
  subjective as-needed reuse.
- A crash resumes the last durable phase; a completed phase is never rerun
  merely because the process response was lost.
- Pause/take/cancel first invalidate the runner epoch. They do not grant the
  worktree while an external mutation is executing; the effect is observed and
  reconciled first.
- Cancellation and takeover wait for process/effect drain and prove that no
  writer remains before granting or cleaning the worktree.
- blocked is distinct from waiting_approval. It carries a closed reason code,
  typed prerequisites, and a recovery action. approve is never valid from
  blocked.

## External-effect protocol

The selected workflow engine can replay an interrupted step. Every mutating
operation therefore uses one application effects table, not multiple ledgers.

Each effect has a unique key derived from:

~~~text
channel + project + ticket + phase + attempt + effect kind + semantic target
~~~

Effect states are planned, executing, confirmed, uncertain, and failed. Each
row also records ticket_version, leader_epoch, runner_epoch, claim_epoch,
claimed_at, request_digest, and the observed remote identity.

Protocol:

1. The current channel leader transactionally inserts or claims the effect
   using its semantic key and a monotonically increasing claim epoch.
2. Immediately before crossing the external-call boundary, compare-and-swap
   verifies the daemon leader, ticket version, runner epoch, and effect claim.
3. Execute the external operation without holding a SQLite transaction.
4. Record the returned remote identity and confirmation only if the same
   epochs remain current. A late stale response becomes an observation and
   cannot advance the ticket.
5. If the process or response is lost, executing remains durable and recovery
   first classifies it as uncertain. Time alone never authorizes a second live
   executor.
6. After proving the prior leader/runner is dead, recovery makes a read-only
   observation using the semantic target.
7. Confirm an already-applied effect or create a new fenced claim and retry
   only when semantic absence is proven.

Required reconciliation:

| Effect | Idempotency/reconciliation rule |
| --- | --- |
| Commit | Recompute expected tree and inspect local branch |
| Push | Freshly observe the exact remote ref, then use an ordinary explicit-ref fast-forward push; v1 never uses force or force-with-lease |
| Draft PR | Bind host, repository, head repository, head ref, base ref, and head OID; adopt only the unique sf-owned open PR |
| PR update/ready | Read PR identity and current head before mutation |
| Merge request | Invoke gh with the reviewed source-head SHA and configured squash/merge/rebase method; never use admin bypass, GitHub --auto, or a merge queue |
| Merge | Read PR and protected branch; never repeat an uncertain merge blindly |
| Worktree creation/removal | Verify registered Git worktree identity and cleanliness |

The reviewed identity is the PR source-head SHA. The resulting protected-branch
merge commit may differ according to the configured method, but GitHub must
report that exact source head as merged and the protected branch must contain
the resulting merge. A queue/enqueued merge response is unsupported and
becomes blocked, not success.

## Data model

Application tables remain deliberately small:

| Table | Purpose |
| --- | --- |
| schema_migrations | Database and workflow compatibility |
| daemon_instances | Current leader epoch and process identity |
| projects | Canonical trusted repository, base branch, commands, policy |
| tickets | Immutable ticket input plus state, version, runner epoch, resume state |
| plans | Typed Planner artifact and acceptance digest |
| verifications | Independent proof intent, owned files, amendment history |
| phase_runs | Role/provider/model/version, attempt, timestamps, outcome, usage |
| effects | Idempotent local/Git/GitHub mutations and reconciliation |
| approvals | Human approval/rejection bound to an exact reviewed SHA |
| events | Ordered typed events used to derive readable output |
| leases | Ticket admission and global/repository/provider capacity |

Normative constraints include:

- One project row per canonical repository in a channel.
- One active nonterminal ticket per project/source digest unless --new is
  explicit.
- One active phase_run per ticket and one unique
  (ticket_id, phase, attempt) tuple.
- One effects row per complete semantic idempotency key and one current claim
  epoch per effect.
- One current runner lease/epoch per ticket.
- One approval per ticket/reviewed-source-head/operator decision; only the
  latest noninvalidated approval may authorize merge.
- One unique remote branch and worktree identity per ticket.
- Foreign-key and check constraints for every state, disposition, outcome,
  merge mode, and effect state.

Concurrent insert/update tests must prove each constraint rather than relying
only on application checks.

The rejected DBOS spike used separate implementation tables; v1 has no DBOS
system tables or second workflow authority. Provider transcripts and large command
logs are bounded files addressed by digest from phase_runs; they are not stored
as large SQLite blobs.

The event projector writes append-only NDJSON for humans and support tooling.
Deleting it loses no authority; sf can regenerate it from events.

## Concurrency

Defaults:

- Global active tickets: 2.
- Per repository: 2.
- Per provider/account route: 1 unless explicitly qualified higher.
- GitHub publication/merge operations per repository: 1.
- One active phase per ticket.

All limits are configurable within validated positive bounds. There is no
unlimited value. Admission is a short SQLite transaction. Long provider runs
do not hold a database or global file lock.

Two tickets may build and review concurrently in separate worktrees.
Repository publication is serialized only around the short refresh/push/merge
effect. If protected main moves, the ticket gets one automatic refresh and
mandatory proof, checks, and fresh review over the new candidate; an unresolved
conflict pauses only that ticket.

## Merge modes

The mode is selected by project default and may be narrowed per ticket.

### manual

- sf creates and updates the draft PR.
- It reports when review and checks are green.
- Sofia owns marking ready and merging on GitHub.
- sf observes the merge. It reaches done only when the merged PR source head is
  the exact reviewed head and required checks were green. A different manual
  head becomes the terminal external_merged outcome with a prominent
  unverified warning.

### guarded, default

- sf creates the PR, performs independent review, and waits for green required
  checks.
- sf pauses at waiting_approval.
- sf approve --operator <identity> binds Sofia's approval to the exact reviewed
  source-head SHA.
- A head change invalidates approval and requires fresh review.
- sf requests the configured immediate exact-head merge after approval. It
  never uses admin bypass, GitHub --auto, or a merge queue.

### Post-v1 autonomous (not available in v1)

- sf merges automatically only when verification, independent final review,
  required GitHub checks, branch protection, budget, safety qualification, and
  exact-head conditions all pass.
- Any reduced-independence condition, ambiguous GitHub result, changed head,
  unqualified execution profile, policy exception, or operator pause becomes a
  typed blocked state. approve is suppressed. The operator may restore the
  missing gate or run sf recover <ticket> --mode guarded --operator <identity>.
  Narrowing is permitted only when project policy allows guarded mode and
  routes through a fresh candidate/proof/check/review cycle before
  waiting_approval.
- Autonomous mode never bypasses GitHub protection.
- Autonomous implementation is integrated only after the guarded walking
  skeleton and guarded pilot pass.

Autonomous mode is deliberately unavailable in v1 because the tested native
execution profile is not eligible. A later release may enable it only after the
guarded pilot and a stronger OS-enforced containment proof. A v1 ticket may
choose manual or guarded mode but cannot exceed the project's maximum
automation.

## Friendly CLI

Primary verbs:

~~~text
sf submit <ticket.md> --project <name>
sf start <ticket>
sf status [ticket] [--watch]
sf show <ticket> [--json]
sf logs <ticket> [--follow] [--phase <name>]
sf pause <ticket> --operator <identity>
sf resume <ticket> --operator <identity>
sf recover <ticket> [--mode guarded] [--operator <identity>]
sf cancel <ticket> --operator <identity>
sf retry <ticket>
sf take <ticket> --operator <identity>
sf approve <ticket> --operator <identity>
sf reject <ticket> --operator <identity> --reason <text>
sf doctor [--repo <path>]
~~~

The daemon authenticates the caller from owner-only socket peer credentials.
--operator defaults to that effective macOS username; when supplied it must
match the authenticated username or a locally configured alias. It is never an
unverified free-form identity. Setup/maintenance commands such as init, auth,
providers qualify, config, daemon, version, update, and rollback are secondary
and do not appear in the normal ticket loop. retry
is accepted only for a typed retryable pause and prints the exact gate it will
re-evaluate; it is not a generic bypass.

Example status:

~~~text
SF-123  Fix duplicate reminders
Channel: stable
State: waiting_approval
Project: nysa (nysa-app)
Head: 9e1c... (reviewed and green)
PR: https://github.com/nysa-company/nysa-app/pull/842
Merge mode: guarded

Proof
  regression: passed
  focused tests: passed
  required GitHub checks: 7/7 passed
  independent review: passed by claude/...

Next: sf approve SF-123 --operator sofia
Alternative: sf take SF-123 --operator sofia
~~~

Mode-aware command validity:

| State/mode | approve | reject | take | external GitHub merge |
| --- | --- | --- | --- | --- |
| waiting_manual_merge/manual | invalid | valid, returns to Builder | valid | expected |
| waiting_approval/guarded | valid | valid | valid | observed; mismatch is external_merged |
| post-v1 autonomous and all gates green | not requested | operator pause must win before merge effect | only before merge effect begins | unexpected but observed |
| blocked, any mode | invalid | invalid unless the blocker names it | valid only after process/effect safety | never treated as approval |
| merging/reconciling | invalid | invalid | request waits for effect reconciliation | observed exactly |

### Takeover

sf take:

1. Records a takeover baseline containing channel, project, worktree, unique
   branch, local head/tree, remote head, base head, candidate/proof digest,
   command-policy digest, and current effect/runner epochs.
2. Invalidates the current runner epoch and stops new phase work.
3. If a push/PR/merge effect is executing, waits in stopping while sf observes
   and reconciles it; the operator does not receive the worktree yet.
4. Terminates and drains the provider/command process group and proves no
   writer remains. An escaped process becomes blocked_process.
5. Marks the ticket paused with its resume state and prints the absolute
   worktree path, branch/head, dirty-file list, last completed proof, and safe
   commands.

The operator may edit, test, and commit. sf resume classifies the handback:

| Handback | Result |
| --- | --- |
| No source/head change | Resume the recorded phase |
| Valid source commit, verification files unchanged | Adopt checkpoint, rerun proof/checks/final review |
| Verification-owned file changed | Enter fresh amendment review before Builder |
| Base, remote, branch, .git, origin, config, hook, submodule, or worktree identity mismatch | blocked; no automatic adoption |
| Live/escaped writer or uncertain external mutation | Remain stopping/blocked until sf recover proves safety |

No commit message grants workflow authority. Any source-head change invalidates
prior proof result, checks, review, and approval. If the operator finishes
outside the factory, manual mode observes the merge; a mismatched source head
is reported as external_merged rather than done.

### Exit codes

- 0: requested operation succeeded.
- 2: invalid command or ticket input.
- 3: needs operator action; output includes next_action.
- 4: temporary external wait/unavailability.
- 5: policy or safety refusal.
- 6: daemon/channel/version incompatibility.
- 7: internal invariant failure.

Every command supports structured JSON without changing semantic fields.
Errors carry a stable code, short message, ticket when applicable, and one
next_action object.

Normative response envelope:

~~~json
{
  "schema": "sf.response/v1",
  "ok": false,
  "code": "provider_auth_missing",
  "message": "Codex authentication is unavailable in the dev daemon environment.",
  "channel": "dev",
  "project": "nysa",
  "ticket": "SF-...",
  "mutation": {"performed": false, "summary": "No phase or external effect started."},
  "next_action": {
    "argv": ["sf-dev", "auth", "login", "codex"],
    "preconditions": [],
    "mutates": "provider credential store only"
  }
}
~~~

Golden fixtures cover daemon unavailable, provider auth missing, correction
limit, stale head, uncertain merge, blocked process, corrupt database, and
schema mismatch. next_action is executable argv, not a shell string. A blocker
that first requires host repair names that prerequisite and then sf recover;
approve is never suggested for a missing safety gate.

## Project and channel configuration

sf init registers the canonical repository path and optionally writes a small
committable .sf/config.toml:

~~~toml
base_branch = "main"
merge_mode = "guarded"
merge_method = "squash"
max_concurrent_tickets = 2

[commands]
verify = ["make test-focused"]
review = ["make test"]

[providers]
planner = ["cursor", "claude", "codex"]
builder = ["cursor", "claude", "codex"]
reviewer = ["claude", "codex", "cursor"]
~~~

Command arrays are argv, never shell strings. Repository configuration cannot
raise machine safety policy, enable unrestricted provider flags, expose
credentials, or exceed channel caps. Each ticket snapshots the committed or
registered configuration bytes and digest at start. A Builder change to
.sf/config.toml or a referenced script cannot change the current ticket's
command authority; an operator must finish active tickets or explicitly
re-register configuration. Stable and dev keep separate snapshots even though
the repository file is shared.

macOS channel roots:

~~~text
~/Library/Application Support/sf/stable/
  sf.sqlite
  run/sf.sock
  logs/
  events/
  worktrees/
  backups/

~/Library/Application Support/sf/dev/
  sf.sqlite
  run/sf.sock
  logs/
  events/
  worktrees/
  backups/
~~~

The sf and sf-dev binaries have different application identifiers, roots,
sockets, databases, worktrees, logs, and backups. A channel is part of every
internal identity and effect key. Both are foreground local daemons in v1;
LaunchAgent labels and service installation are deferred.

## Versions, promotion, and rollback

- Use SemVer releases and ordinary GitHub release artifacts.
- `make build VERSION=<semver>` creates a local stable-channel candidate; it
  does not install or promote that candidate.
- sf-dev may run an arbitrary development commit without touching stable
  state.
- Stable promotion, update, and rollback are explicit later gates; the current
  CLI reserves update/rollback and returns `not_configured`.
- There is no per-product sealed release or qualification generation.
- Release validation must run unit, race, integration, crash,
  security-fixture, upgrade, and stable/dev isolation suites, including a
  simultaneous-daemon isolation acceptance test.
- A disposable local GitHub test repository proves the complete guarded path
  before a release candidate is promoted.
- A future promotion operation must install an already-built, checksummed
  candidate as stable and never copy the dev database.
- A future rollback operation must bind the previous binary to its matching
  database backup and refuse incompatible active workflows rather than perform
  partial recovery.
- Nysa chooses stability by continuing to invoke sf. Factory development uses
  sf-dev.

## Failure behavior

| Failure | Automatic action | Operator result |
| --- | --- | --- |
| Provider unavailable | One configured fallback | Pause after fallback fails |
| Provider timeout | Drain process group; record attempt | Retry once if policy permits, then pause |
| Invalid model output | Reject structured result | One repair attempt, then pause |
| Wrong/weak verification | New Reviewer verification | Pause after two corrections |
| Builder breaks proof | Return exact failures to Builder | Pause after two corrections |
| Reviewer requests changes | Return named findings to Builder | Pause after two corrections |
| Required CI pending | Poll with bounded backoff | Visible wait; no provider cost |
| Required CI red | One diagnosed repair loop | Pause after correction limit |
| Push response lost | Observe exact remote head | Confirm or pause; never blind retry |
| PR creation response lost | Query unique head/base PR | Adopt or pause on ambiguity |
| Protected main moved | One refresh and re-verification | Pause on conflict |
| Merge response lost | Observe PR and protected branch | Confirm or pause; never repeat blindly |
| Daemon crash | Selected engine resumes checkpoint under a new leader epoch | Completed phases stay completed |
| Second daemon/stale worker | OS leader lock and epoch CAS refuse it | Repair stale path only after Doctor proves no leader |
| SQLite busy | Bounded retry | Ticket-independent error if exhausted |
| Disk full | Stop new work and preserve state | Doctor gives cleanup guidance |
| Operator take | Invalidate epoch, reconcile effect, drain writers, preserve worktree | Print takeover instructions only after safe handoff |
| Escaped/unkillable writer | Quarantine as blocked_process | Repair/reboot host, then sf recover |
| Repository command needs network/package install | Refuse credential-free sandbox escalation | Operator takeover |
| Git control-plane drift | Block before commit/push | Restore exact repository identity or cancel |
| Merge queue detected | Refuse sf merge | Use manual mode or change repository policy |
| Budget/time ceiling | Stop before next paid phase | Explicit override or cancel; cost is enforced only when trusted usage is available |
| Provider fails safety fixture | Disable provider/version entirely | Doctor names provider and failed probe |
| Provider lacks enforceable autonomous profile | Retain qualified_guarded at most | Use guarded/manual or another provider |
| Binary/schema mismatch | Refuse daemon start | Print required version/restore action |

No recovery loop runs indefinitely. No uncertain external mutation is converted
to success without observation.

## Observability and support

- sf status is a database projection, not a log parser.
- sf logs streams bounded redacted phase and command output.
- Every state transition and effect produces a typed event with ticket,
  timestamp, phase, attempt, outcome, and non-secret correlation identifiers.
- Event and error schemas are versioned.
- Raw provider output is never printed by doctor.
- Doctor checks filesystem ownership/mode, disk space, SQLite integrity,
  daemon/socket/version, Git, gh authentication, repository registration,
  provider binaries/versions/structured output, sandbox qualification,
  command configuration, and stable/dev isolation.
- Support bundles are explicit, redacted, locally generated, and omit ticket
  source unless the operator asks to include it.

## Success criteria

The factory is ready for stable Nysa use only after:

1. The DBOS proof decision is recorded and the custom Go engine passes its
   equivalent recovery gates.
2. Every deterministic and integration gate in the verification plan passes.
3. Ten consecutive representative Nysa tickets reach their intended terminal
   state without database edits, worktree surgery, or controller-code repair.
4. The set includes at least two bugs, two features, one refactor, one
   infrastructure/configuration change, one documentation change, one paused
   takeover, and one two-ticket concurrency run.
5. Manual external merge observation and guarded automatic merge after human
   approval each complete on an exact reviewed head.
6. A forced crash is injected in planning, provider execution, push, PR
   creation, and merge reconciliation without duplicate effects.
7. sf take yields a usable worktree and instructions within 30 seconds after a
   cooperative provider and within the documented hard-kill bound otherwise.
8. No provider process remains after cancellation tests.
9. Stable and dev cannot see, resume, mutate, or clean each other's ticket
   state or worktrees.
10. An operator unfamiliar with internals can recover every intentionally
    paused fixture using only the next action shown by the CLI.

## Implementation strategy

Implementation began after this plan was approved. The repository remains
local; no remote is created or pushed until the repository name and open-source
metadata are approved.

### Phase 0 — risk spikes and contracts

- Create the Go module, architecture decision records, and interface skeletons.
- Record the completed one-day DBOS proof and the selected custom-engine
  fallback.
- Run the bounded native execution-profile capability proof. Its failure
  disables autonomy, not the guarded walking skeleton.
- Freeze the public ticket, normative state-machine, provider, effect-fencing,
  Git-identity, execution-profile, and CLI response contracts.
- Build fake provider, fake gh, and crash-injection harnesses before production
  workflow code.
- Write and continuously execute the source-build/foreground first-ticket
  tutorial.

Exit: engine choice is final and the skeleton test suite is green.

### Phase 1 — single-authority local foundation

- Implement configuration, SQLite migrations, daemon/socket protocol, event
  projection, CLI shell, leader lock/epochs, and stable/dev roots.
- Implement projects, tickets, phase runs, fenced effects, leases, concrete
  constraints, and the normative typed state transitions.

Exit: two fake tickets can advance deterministically without agents or GitHub.

### Phase 2 — guarded execution and Git substrate

- Implement the guarded provider/repository-command baseline, the proven native
  profile adapter when available, provider restrictions, process supervision,
  escaped-process quarantine, isolated authentication homes, and hostile
  fixtures.
- Implement unique channel-prefixed branches, Git control-plane authentication,
  worktree management, diff/path validation, deterministic commits, ordinary
  fast-forward pushes, and effect reconciliation.

Exit: fake commands receive no factory/GitHub/product credentials; Git identity
and effect fences cannot be bypassed; detected escapes quarantine the ticket;
and the native-profile verdict precisely states which stronger denials are
actually proven. No live provider is required.

### Phase 3 — guarded walking skeleton

- Enable one qualified Builder provider and one independent Reviewer provider.
- Implement Planner, pre-build Reviewer verification checkpoint, Builder,
  draft PR, GitHub checks, fresh final Reviewer, guarded approval, immediate
  exact-head merge, and Done reconciliation.
- Implement pause, resume, cancel, take, approve, reject, status, logs, Doctor,
  and the foreground first-ticket documentation.

Exit: a disposable GitHub repository completes the guarded path and takeover
using a foreground dev daemon. This is the first usable beta and a hard review
checkpoint before adding providers or autonomy.

### Phase 4 — resilience and channels

- Complete daemon/effect crash recovery, schema upgrade/rollback, and all
  supported blocked-state recovery.
- Qualify the exact Codex runtime used by both logical roles; additional
  providers and configured fallback remain post-v1.
- Implement bounded two-ticket concurrency and one-refresh conflict behavior.
- Validate separate sf/sf-dev identities, roots, foreground mode, and release
  candidate artifacts. Service installation, promotion, update, and rollback
  remain explicit later gates.

Exit: every normative transition and supported fault has a deterministic test,
and stable/dev isolation is proven.

### Post-v1 roadmap — autonomous merge and Nysa promotion

- After the independent architecture, DX, and adversarial reviews, run ten
  representative Nysa tickets in guarded mode.
- Only after that guarded gate, a later release may integrate autonomous mode
  for exact
  provider/command profiles that passed the stronger OS-enforced eligibility
  proof.
- Qualify autonomous merge in the disposable GitHub repository, then consider
  a later stable promotion with Sofia's approval.
- Enabling autonomous mode on even one Nysa ticket remains a separate explicit
  post-qualification approval; it is not implied by plan or release approval.

Exit: success criteria pass and Sofia approves stable use.

## Delegation and integration

Ultra remains the coordinator and owns interface decisions, plan changes,
security/merge invariants, cross-lane integration, and final review. Bulk
implementation is delegated:

| Lane | Preferred agent | Ownership |
| --- | --- | --- |
| Engine/data | Terra | DBOS spike, SQLite, workflow abstraction, effects |
| Git/provider | Terra | process supervisor, worktrees, Git, gh integration |
| CLI/DX | Luna | Cobra commands, socket client, status/next-action rendering |
| Fixtures/docs | Luna | fake providers/gh, hostile fixtures, docs, examples |
| Integration | Ultra with rotating Terra/Luna review | contracts, merge modes, release gates |

Implementation uses isolated Git worktrees. No two agents edit the same package
in a wave. Shared interface files land before dependent lanes start. Each lane
must:

1. Rebase/merge the current integration branch before starting.
2. Implement only its task-owned packages.
3. Add focused tests in the same change.
4. Run the required focused and race checks.
5. Submit a small reviewable commit.
6. Receive independent review before integration.

The complete dependency graph and acceptance conditions are in the JSONL task
artifact.

## Parallel worktree waves

~~~text
Wave 0 (serial, Ultra)
  contracts + state machine + repository skeleton
              |
Wave 1
  Terra: DBOS spike ------ Luna: fake boundaries + first-run docs
              |                       |
              +-----------+-----------+
                          |
Wave 2
  Terra A: engine/fencing    Terra B: containment/Git identity
  Luna: CLI/socket/events
                          |
Wave 3
  Terra: worktree/effects    Luna: fake gh + guarded CLI UX
                          |
Wave 4 — guarded integration checkpoint
  one Builder provider + one Reviewer provider + guarded E2E
                          |
Wave 5
  Terra: recovery/concurrency/providers
  Luna: stable/dev/doctor/release/docs
                          |
Wave 6
  Terra/Luna independent challenge + full provider qualification
                          |
Wave 7 (serial guarded gate)
  Ultra: guarded Nysa pilot + fixes
                          |
Wave 8 (serial autonomy gate)
  Ultra/Terra: autonomous invariants + disposable qualification
                          |
Wave 9
  stable promotion + separate legacy-retirement preview
~~~

The four-slot execution limit means at most three subagents run beside the
coordinator. Agent labels describe intended model tier, not permanent people.

## Documentation deliverables

The current repository ships:

- README: repository overview and local build/test entry points.
- docs/tutorials/source-build-foreground.md: first local Nysa ticket tutorial.
- docs/cli.md: commands, JSON, exit codes, examples.
- docs/configuration.md: machine and project configuration.
- docs/releases.md: stable/dev promotion and rollback.
- AGENTS.md.
- Architecture decision records for the workflow engine and native execution
  profile.

## Open taste decisions that do not block local implementation

- Final GitHub repository name. The working local directory and module may use
  sf until a remote is created.
- Open-source license. Apache-2.0 is recommended for its explicit patent grant;
  Sofia chooses before any remote/public release.
- Homebrew distribution versus a signed direct binary after the Nysa pilot.
- Whether a later high-risk mode containerizes only Cursor or all providers.
- Whether a later version adds GitHub Issues as an optional ticket source.

None changes the v1 state machine or implementation boundaries.

## Risks and mitigations

| Risk | Mitigation |
| --- | --- |
| DBOS Go behavior does not satisfy local recovery needs | One-day proof gate and frozen custom fallback |
| Trusted-provider controls are not hostile same-UID containment | Native-profile qualification, trusted repositories only, no autonomous v1 claim |
| SQLite contention at two tickets | Short transactions, WAL, no long locks, race/load test |
| External effect duplicates after crash | Unique effect keys and read-before-retry reconciliation |
| Agent weakens acceptance to make tests pass | Separate verification invocation and explicit amendment approval |
| Human takeover corrupts workflow identity | Drain first, validate exact worktree/head/diff on resume |
| GitHub CLI output or behavior changes | Supported-version range, JSON contract tests, Doctor |
| Autonomous merge surprises operator | Guarded default, project maximum, exact-head rules, visible mode |
| Dev build corrupts stable Nysa work | Completely separate binary identity, root, DB, socket, logs, worktrees |
| Rewrite grows into another platform | Enforced not-in-scope list and task-level acceptance |
| Open-source users expect hostile-code safety | Documentation and runtime refusal for untrusted repos |

## Rollout and retirement

1. Tag the current legacy repository as the final legacy line.
2. Keep its current stable installation untouched while building sf-dev.
3. Build the new factory in its own repository and roots.
4. Run all fake, crash, security, and disposable-GitHub validation.
5. Pilot only sf-dev against selected Nysa tickets.
6. Promote a SemVer release to sf after the ten-ticket gate.
7. Stop submitting new tickets to the legacy factory.
8. Preserve the legacy repository and local state read-only for a bounded
   observation period.
9. Remove legacy background services only through a separately reviewed
   cleanup after the new stable line has proven reliable.

No legacy ticket is imported. No running legacy ticket is resumed in v1.

## Approval checkpoint

Approval of this plan authorizes creation of the new local repository and
implementation of the listed tasks. It does not authorize:

- Creating or pushing a remote repository.
- Changing the Nysa repository.
- Enabling autonomous merge on Nysa.
- Merging or deleting the legacy factory.
- Installing or removing background services outside the new factory's
  development channel.

Those mutations remain explicit later checkpoints.

## GSTACK REVIEW REPORT

Review mode: Engineering manager plan review

Scope decision: selective replacement; full local v1, no laptop-off operation

Review verdict: GO for Sofia's approval

Architecture status: load-bearing decisions resolved and independently challenged

Implementation status: release-candidate hardening and validation in progress

Plan author: root Codex agent; analysis delegated to Terra/Luna agents

| Review | Runs | Status | Findings |
| --- | ---: | --- | --- |
| Engineering plan review | 1 | CLEAR (PLAN) | 23 unique architecture, recovery, safety, test, and DX findings folded |
| Independent architecture | 2 | GO | Atomic workflow start, state authority, fencing, and task DAG verified |
| Independent operator/DX | 2 | GO | First run, provider pair, identity, takeover, and channel UX verified |
| Independent adversarial safety | 3 | GO | Guarded trust boundary, native autonomy proof, Git/process/network invariants verified |

Resolved architecture: new Go repository and blank state; foreground local daemon and
Markdown tickets; Planner/Builder/Reviewer with independent verification before
build; SQLite authority with the selected custom Go state engine;
GitHub through gh; stable/dev isolation; bounded concurrency two; manual and
guarded automatic merge after human approval; autonomous exact-head mode is
post-v1 pending the guarded Nysa pilot and native execution-profile proof.
Docker and Colima remain unnecessary.

Artifact validation: 62 unique tasks, no missing dependencies, no cycles, no
phase/wave inversions, valid JSON/JSONL, and no unknown state references.

VERDICT: ENG REVIEW CLEARED — implementation is in release-candidate
hardening and validation. Remaining release gates are verification evidence,
not plan approval.

NO UNRESOLVED DECISIONS
