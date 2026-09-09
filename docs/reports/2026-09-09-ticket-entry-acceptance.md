# Ticket entry acceptance

Status: focused and full factory acceptance passed on the implementation
candidate. This report is a documentation-only follow-up.
Candidate: `c41345b640e00ff4ee5fff2f7cc982db0de77784` on
`feat/ticket-entry-experience`, based on main `64c5414`.

## Delivered surface

- `sf ticket new [file] --multiline`: offline title/problem/acceptance prompts,
  optional safe title-derived filename, full source preview and explicit save.
- `sf home --project app`: project-scoped navigation to draft creation, saved
  draft submission/start, ticket listing, and exact-head approval selection.
- `sf ticket import https://github.com/OWNER/REPO/issues/NUMBER`: read-only `gh`
  fetch, exact response identity checks, quoted source attribution, operator
  acceptance criteria and explicit local save. `--json` only previews.

Use `sf-dev` instead of `sf` when running the development-channel bundle.
No new daemon state, provider behavior, schema, runtime or approval authority
was introduced. Nothing is installed or changed in a live pilot by these tests.

## Evidence

[Focused GitHub run](https://github.com/nysa-company/sf/actions/runs/34356101825)
passed all steps on macOS at the prior `83c31ef` source. The final
[explicit-consent candidate focused run](https://github.com/nysa-company/sf/actions/runs/34357643516)
also passed every step, including a home test proving plain `run` never supplies
estimated-cost consent and `run estimates` explicitly supplies the existing flag.
The timings below are recorded measurements from that final candidate run:

| Requirement | Evidence |
| --- | --- |
| Multiline drafting and safe optional name | `TestTicketEntryMultilineSuggestedFilenameJourney`, `TestTicketEntryFilenameAndPasteBounds` |
| Preview, cancellation and no overwrite | Existing `TestNewTicket*` tests in full CLI suite; new home/import cancellation and preview-drift tests |
| Project-scoped navigation | `TestTicketEntryHomeDraftAndScopedView`, cancellation/JSON refusal tests |
| No implicit start or blind replay | `TestTicketEntryHomeRunRequiresExactPreviewAndDoesNotRetry` |
| Exact-head approval retained | `TestTicketEntryHomeApprovalStillBindsHead`, existing decision-selection suite |
| Exact issue identity and read-only transport | `TestTicketEntryIssueURLRefusals`, response-refusal matrix, production adapter argv/timeout test |
| Attribution and duplicate local import refusal | `TestTicketEntryIssueImportPreviewSaveAndDuplicate` |
| Bounded import output and cancelled writes | `TestTicketEntryIssueOutputBufferBound`, `TestTicketEntryImportCancelledNeverWrites` |
| Existing CLI behavior | Entire CLI normal suite: 3.792s; race suite: 72.488s; `go vet ./internal/cli` passed |
| Documentation/repository/secrets | `scripts/docs-smoke`, `scripts/repo-check`, `scripts/secret-scan` passed |

The scripted offline draft/validate journey took **8.039792 ms** with predefined
answers. This satisfies the automated two-minute target, but excludes human
typing/decision time, installation, authentication, provider qualification,
network fetch, model execution and ticket delivery. It is not a human usability
trial or a claim that a fresh developer can complete setup in that time.

[Full factory acceptance](https://github.com/nysa-company/sf/actions/runs/34358150167)
passed on the same candidate: **21/21 jobs succeeded**, including the required
`SF acceptance` aggregate, all normal/runtime integration shards, full race
coverage, compiled E2E, crash, security, upgrade and static validation. No local
tests or builds were used for this goal. Later report-only commits do not change
the tested implementation; use the candidate SHA above when identifying the
full acceptance source.

## Review and limitations

A bounded source self-review checked command dispatch, confirmation binding,
exclusive file creation, imported-text treatment, output/time bounds and JSON
refusal paths. It is not an independent external audit.

Import tests use a controlled `gh` executable on GitHub runners, not a live
account or real issue mutation. Local source-named file collision protection
does not deduplicate drafts copied or renamed into other directories. Imported
text is not a scope/feasibility verdict; operator criteria and normal planning
remain necessary. Basic drafting requires no daemon or paid model.

Human usability trials, AI-assisted drafting, installers, provider/runtime
expansion, issue tracker synchronization and live Relay trials are outside this
goal. Existing noninteractive explicit commands remain available.

## Preserved first-run failures

The initial focused run `34355905158` failed at a 128-byte menu path bound and
two inaccurate test response shapes (missing envelope request ID and an emitted
null PR field). The correction supports bounded 4096-byte menu input, fixes the
fixtures without weakening response validation, and adds an actual adapter
timeout test. Superseded full run `34355910397` was cancelled, not passed.
