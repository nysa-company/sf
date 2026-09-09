# GitHub HTTPS credentials on macOS

Use the normal `sf auth login github` flow. GitHub CLI owns the credential in
the macOS Keychain; SF does not require `gh auth login --insecure-storage` or
copy a token into project configuration.

Before starting a ticket, run:

```sh
sf doctor --repo /absolute/path/to/project
```

Doctor reports separate facts:

- `github_auth`: GitHub API authentication.
- `https_credentials`: the packaged HTTPS credential bridge can retrieve a
  nonempty credential using the execution environment, including the validated
  operator home needed by Keychain. No credential bytes are shown or saved.
- `repository_access`: remains untested by this credential-only probe. Login
  and retrieval do not prove read or write permission on a particular project.

If `https_credentials` fails, ensure the macOS login Keychain is unlocked and
the operator session can use its GitHub credential. Run `sf auth login github`
if login has expired, then rerun Doctor. Check that the matching credential
helper is installed alongside the selected SF executable. A missing helper,
unsafe authentication directory, timeout, or unusable response fails the check;
the diagnostic intentionally does not echo subprocess output.

`GH_CONFIG_DIR` and `XDG_CONFIG_HOME` select the same configuration location
for Doctor and daemon startup. Restart the foreground daemon after changing
its authentication configuration or upgrading the helper bundle. A current
Doctor check does not prove an already-running older daemon has been upgraded.

If a ticket remains in planning before worktree/provider creation,
`repository_preflight_failed` in `sf status` means repository/base readiness
could not be established. Run Doctor for that repository. This is different
from `stale` (superseded workflow authority) and `worktree_identity_failed`
(registered checkout identity could not be authenticated). Network, remote-base
and identity failures must not be assumed to be bad credentials.

## Security boundary

Only the trusted HTTPS credential child receives the authenticated operator
HOME. Git itself keeps an isolated home; providers and repository tests keep
their credential-free environments. There is no project HOME override or
general environment forwarding. GitHub CLI still owns credential storage and
Keychain access control. This is the existing trusted-repository/same-user
boundary, not a claim to contain arbitrary hostile same-UID processes.

SSH Git transport uses its separate agent path; GitHub API authentication is
still required. HTTPS fetch or push in a mixed-transport project still needs
the HTTPS bridge check.
