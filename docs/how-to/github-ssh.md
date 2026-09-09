# Use GitHub SSH with SF

SSH authenticates Git fetch/push. SF still uses the official GitHub CLI's API
login for pull requests, checks, and merge operations. An SSH key alone cannot
replace that login. HTTPS remains supported.

## Existing-key setup

1. Use an existing SSH key registered with your GitHub account. Load it into
   your SSH agent with `ssh-add /absolute/path/to/your/key` if needed. SF never
   reads, copies, creates, or uploads your private key.
2. Run `sf auth login github --git-protocol ssh`. This delegates browser login
   to `gh`, skips key creation/upload, and explicitly changes gh's github.com
   Git protocol preference for all accounts in the selected gh configuration.
   It does not rewrite any repository remote. If API login already works,
   this step is optional for an existing SSH checkout.
3. Run `sf init --project my-app --repo /absolute/path/to/my-app` and
   `sf doctor --repo /absolute/path/to/my-app`.
4. Start `sf daemon run` from a terminal with the working `SSH_AUTH_SOCK`.
   Qualify your providers and run tickets as usual.

`sf` may be the stable binary or the source-checkout shortcut; the shortcut
retains the development channel. Do not switch channels to repair authentication.

## Supported origins

The repository must already have one of these exact GitHub origins:

```text
git@github.com:OWNER/REPO.git
ssh://git@github.com/OWNER/REPO.git
ssh://git@github.com:22/OWNER/REPO.git
ssh://git@ssh.github.com:443/OWNER/REPO.git
```

The `.git` suffix is required. SF preserves the configured spelling as part of
repository identity. All four forms use SF's packaged helper and pinned
`ssh.github.com:443` endpoint. Host aliases, custom SSH config, arbitrary ports,
proxies, and GitHub Enterprise hosts are not supported by this GitHub.com path.
Host-key checking is never disabled.

Do not change an origin beneath an active ticket. The origin is authenticated
workflow evidence; changing HTTPS to SSH mid-ticket is identity drift, not an
automatic migration. Finish/cancel existing work and use the supported project
registration flow for a new checkout instead of editing SF's database.

## Diagnose separately

- `sf auth status`: GitHub API login, not SSH key acceptance.
- `sf doctor --repo /absolute/path`: local transport, packaged helper and host
  keys, and whether the local agent reports identities. It does not contact
  GitHub to prove repository permissions.
- A loaded key may still lack access to the repository or organization SSO.
  Repair that access through GitHub; SF does not silently fall back to HTTPS.
- If the socket changes after reboot/login, restart the foreground daemon from
  the terminal with the current agent. Stop the exact daemon with Ctrl-C first.
- Missing SSH helpers require a complete matching-channel bundle, not copying
  a helper from another build.

Private keys remain with your agent. The agent is available only to factory
Git transport, not model or repository-test subprocesses. This retains SF's
trusted-repository boundary; it is not hostile same-UID isolation.
