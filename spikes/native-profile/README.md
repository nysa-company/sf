# Native execution-profile capability spike

This is a bounded macOS capability probe, not an `sf` runtime and not a
provider qualification. It uses only synthetic, non-secret sentinels in a
temporary operating-system scratch directory that is removed on exit. It never
contacts GitHub, starts a LaunchAgent, installs packages, invokes a provider,
or changes Nysa.

Run it from the repository root:

```sh
spikes/native-profile/run.sh
```

The script detects `sandbox-exec`, generates a restrictive Seatbelt profile,
and tests synthetic home/SSH/Keychain/GitHub/provider-home reads, `.git`
writes, arbitrary loopback networking, a modified repository command, and
`launchctl`/`installer` execution. It also exercises `setsid` plus double fork
without leaving a child alive. It checks real sensitive locations only with
metadata-only `test -r` calls; it never reads host credential bytes or persists
raw command output. `--require-autonomous` returns nonzero unless every required
capability is proven.

The result is deliberately conservative: passing individual filesystem and
network denials does not prove autonomous eligibility if a child can detach and
write to the worktree after its supervising process returns. On the tested
machine, that process-lifecycle condition fails, so the spike records
`autonomous_eligible=false`. Guarded mode remains the documented
trusted-provider/repository baseline. The tested profile also needs broad
system-library reads to launch standard macOS tools, so it is not evidence of a
complete arbitrary-filesystem boundary.
