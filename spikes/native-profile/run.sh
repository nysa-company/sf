#!/bin/sh
# Bounded, credential-free capability probe for a candidate macOS execution
# profile. It intentionally has no provider, GitHub, repository, or daemon
# dependency. A successful probe does not enable autonomy; it records exactly
# which primitive was observed and whether the process-lifecycle boundary held.
set -eu

# This directory is always removed. Keeping it outside the repository lets the
# probe deny the real user home without also denying its synthetic worktree.
scratch=$(mktemp -d "${TMPDIR:-/tmp}/sf-native-profile.XXXXXX")
scratch=$(CDPATH= cd -- "$scratch" && pwd -P)
keep=0

cleanup() {
  if [ "$keep" -eq 0 ]; then
    rm -rf "$scratch"
  else
    printf '%s\n' "native-profile scratch retained at $scratch" >&2
  fi
}
trap cleanup EXIT HUP INT TERM

if [ "${1:-}" = "--keep-scratch" ]; then
  keep=1
elif [ "${1:-}" = "--require-autonomous" ]; then
  require_autonomous=1
elif [ -n "${1:-}" ]; then
  printf '%s\n' "usage: $0 [--keep-scratch|--require-autonomous]" >&2
  exit 2
fi
require_autonomous=${require_autonomous:-0}

if ! command -v sandbox-exec >/dev/null 2>&1; then
  printf '%s\n' "primitive=sandbox-exec" "available=false" "verdict=autonomous_eligible=false"
  printf '%s\n' "reason=sandbox-exec is unavailable; no native enforcement primitive was proven"
  [ "$require_autonomous" -eq 0 ] && exit 0
  exit 1
fi

work="$scratch/worktree"
parent="$scratch/synthetic-home-parent"
mkdir -p "$work/.git" "$parent/home/.ssh" "$parent/home/Library/Keychains" \
  "$parent/home/.config/gh" "$parent/provider-home"
printf '%s\n' "synthetic non-secret sentinel" > "$parent/home/.ssh/sentinel"
printf '%s\n' "synthetic non-secret sentinel" > "$parent/home/Library/Keychains/sentinel"
printf '%s\n' "synthetic non-secret sentinel" > "$parent/home/.config/gh/hosts.yml"
printf '%s\n' "synthetic non-secret sentinel" > "$parent/provider-home/sentinel"
printf '%s\n' "synthetic git control" > "$work/.git/config"

quote_profile() {
  printf '%s' "$1" | sed 's/\\/\\\\/g; s/"/\\"/g'
}

work_profile=$(quote_profile "$work")
parent_profile=$(quote_profile "$parent")
home_profile=$(quote_profile "$HOME")
scratch_profile=$(quote_profile "$scratch")
profile="$scratch/seatbelt.sb"
cat > "$profile" <<EOF
(version 1)
(deny default)
(allow process-exec)
(allow process-fork)
(deny file-read* (subpath "$parent_profile"))
(deny file-read* (subpath "$home_profile"))
(deny file-read* (subpath "/Library/Keychains"))
(deny network*)
(deny process-exec (literal "/bin/launchctl"))
(deny process-exec (literal "/usr/sbin/installer"))
(allow file-read*)
(allow file-write* (subpath "$scratch_profile"))
(deny file-write* (subpath "$work_profile/.git"))
(allow file-write* (literal "/dev/null"))
EOF

helper="$work/changed-command-helper.sh"
cat > "$helper" <<EOF
#!/bin/sh
if /bin/cat "$parent/home/.ssh/sentinel" >/dev/null 2>&1; then exit 61; fi
if /bin/cat "$parent/home/Library/Keychains/sentinel" >/dev/null 2>&1; then exit 62; fi
if /bin/cat "$parent/home/.config/gh/hosts.yml" >/dev/null 2>&1; then exit 63; fi
if /bin/cat "$parent/provider-home/sentinel" >/dev/null 2>&1; then exit 64; fi
if /usr/bin/curl --silent --show-error --connect-timeout 1 --max-time 1 http://127.0.0.1:9/ >/dev/null 2>&1; then exit 65; fi
exit 0
EOF
chmod 700 "$helper"

pass=0
fail=0
results="$scratch/results"
: > "$results"

# The repository lives below the real home directory, which the test profile
# denies. Do not inherit it as the sandboxed process current directory.
cd /

case_result() {
  name=$1
  expected=$2
  command=$3
  set +e
  (sandbox-exec -f "$profile" /bin/sh -c "$command" >"$scratch/$name.stdout" 2>"$scratch/$name.stderr") 2>/dev/null
  status=$?
  set -e
  actual=fail
  if [ "$expected" = "success" ] && [ "$status" -eq 0 ]; then actual=pass; fi
  if [ "$expected" = "denied" ] && [ "$status" -ne 0 ]; then actual=pass; fi
  if [ "$actual" = pass ]; then pass=$((pass + 1)); else fail=$((fail + 1)); fi
  printf '%s|%s|%s\n' "$name" "$expected" "$actual" >> "$results"
}

case_result write_inside_worktree success "printf allowed > '$work/allowed.txt'"
case_result write_git_control denied "printf blocked > '$work/.git/control'"
case_result read_synthetic_ssh denied "/bin/cat '$parent/home/.ssh/sentinel' >/dev/null"
case_result read_synthetic_keychain denied "/bin/cat '$parent/home/Library/Keychains/sentinel' >/dev/null"
case_result read_synthetic_github denied "/bin/cat '$parent/home/.config/gh/hosts.yml' >/dev/null"
case_result read_synthetic_provider_home denied "/bin/cat '$parent/provider-home/sentinel' >/dev/null"
case_result read_actual_ssh_metadata denied "test -r '$HOME/.ssh'"
case_result read_actual_keychain_metadata denied "test -r '$HOME/Library/Keychains'"
case_result read_actual_github_metadata denied "test -r '$HOME/.config/gh'"
case_result read_system_keychain_metadata denied "test -r /Library/Keychains"
case_result arbitrary_network denied "/usr/bin/curl --silent --show-error --connect-timeout 1 --max-time 1 http://127.0.0.1:9/ >/dev/null"
set +e
(sandbox-exec -f "$profile" /bin/sh "$helper" >"$scratch/changed-command-exfiltration.stdout" 2>"$scratch/changed-command-exfiltration.stderr") 2>/dev/null
helper_status=$?
set -e
case "$helper_status" in
  61|62|63|64|65) helper_result=fail; fail=$((fail + 1)) ;;
  *) helper_result=pass; pass=$((pass + 1)) ;;
esac
printf '%s|%s|%s\n' "changed_command_exfiltration" "no_sentinel_or_network_success" "$helper_result" >> "$results"
case_result launchd_exec denied "/bin/launchctl print-disabled gui/$UID >/dev/null"
case_result package_installer_exec denied "/usr/sbin/installer -help >/dev/null"

# This probe does not leave a process behind: the grandchild writes a marker and
# exits quickly. Its presence proves that sandbox-exec did not itself prevent a
# child from detaching from the caller's process group while retaining worktree
# write access. That is a hard blocker for autonomous eligibility.
process_code=$(cat <<EOF
import os
import time
p = os.fork()
if p:
    os.waitpid(p, 0)
else:
    os.setsid()
    q = os.fork()
    if q:
        os._exit(0)
    time.sleep(0.15)
    open('$work/escaped-writer.txt', 'w').write('marker')
    os._exit(0)
EOF
)
set +e
(sandbox-exec -f "$profile" /usr/bin/python3 -c "$process_code" >"$scratch/process_escape.stdout" 2>"$scratch/process_escape.stderr") 2>/dev/null
escape_status=$?
set -e
sleep 1
if [ -e "$work/escaped-writer.txt" ]; then
  process_result=fail
  fail=$((fail + 1))
else
  process_result=pass
  pass=$((pass + 1))
fi
printf '%s|%s|%s\n' "setsid_double_fork_worktree_writer" "denied_or_contained" "$process_result" >> "$results"

printf '%s\n' "primitive=sandbox-exec" "available=true" "macos=$(sw_vers -productVersion 2>/dev/null || echo unknown)" "kernel=$(uname -r)" "results:"
while IFS='|' read -r name expected actual; do
  printf '  %s expected=%s result=%s\n' "$name" "$expected" "$actual"
done < "$results"

if [ "$fail" -eq 0 ] && [ "$process_result" = pass ]; then
  verdict=true
  reason="all bounded capability probes passed"
else
  verdict=false
  reason="sandbox-exec did not prove all required restrictions; see setsid_double_fork_worktree_writer"
fi
printf '%s\n' "verdict=autonomous_eligible=$verdict" "reason=$reason" "passed=$pass" "failed=$fail"

if [ "$require_autonomous" -eq 1 ] && [ "$verdict" != true ]; then
  exit 1
fi
