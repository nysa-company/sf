#!/usr/bin/env python3
"""Partition hosted acceptance suites without omitting tests.

CI runs every `other`, `runtime-race`, and Store shard on isolated macOS runners. Local
`make test-race` remains the unpartitioned reference command.
Normal workflow-runtime integration has separate disjoint shards; the other
integration packages remain in the Makefile's integration-other lane.
Crash shards preserve the reference Makefile's exact top-level selection.
"""

import argparse
import math
import re
import subprocess
import sys

STORE = "github.com/nysa-company/sf/internal/store"
RUNTIME = "github.com/nysa-company/sf/internal/workflowruntime"
FLAGS = ["-race", "-count=1", "-shuffle=off", "-p", "1", "-timeout", "60m"]
INTEGRATION_FLAGS = ["-count=1", "-shuffle=off", "-p", "1", "-timeout", "30m"]
CRASH_PATTERN = "(^Test.*Crash|Crash|Recovery|Recover|Rearm|Quarantine)"
# Scheduling hints only, from macOS run 34416329428 (2026-09-09). The live
# inventory is always authoritative: new names receive weight 1, never skip.
PACKAGE_SECONDS = {
    "github.com/nysa-company/sf/" + name: seconds for name, seconds in {
        "cmd/sf": 214, "internal/daemon": 618, "internal/git": 391,
        "internal/publication": 229, "internal/worktreecoord": 225,
        "internal/providercoord": 219, "internal/daemon/runtimecontrol": 109,
        "internal/cli": 99, "internal/localruntime": 89,
        "internal/processsupervisor": 56, "internal/workflowworker": 46,
        "internal/ghrunner": 45, "internal/github": 43, "internal/engine": 43,
        "internal/codexprovider": 29, "internal/bundle": 16,
        "internal/mergeproof": 12,
    }.items()
}
RUNTIME_SECONDS = {
    "TestPostbuildAmendmentCandidateFinalizationRecovery": 564,
    "TestRepositoryMaterializerPostbuildAmendmentPreparedIndexRecovery": 542,
    "TestPostbuildRepairCandidateFinalizationRecovery": 291,
    "TestRepositoryMaterializerPostbuildAmendmentRealEndToEnd": 217,
    "TestRepositoryMaterializerPostbuildRepairRealEndToEnd": 84,
    "TestRepositoryMaterializerPreparePostbuildRepairRealBoundary": 74,
    "TestRepositoryMaterializerRealSourceResumePreparedObservationLoss": 68,
    "TestRepositoryMaterializerRealStoreGitReplay": 43,
}


def race_packages(packages, mode):
    if mode not in ("other", "runtime-race"):
        raise ValueError("invalid package partition mode")
    if (not packages or len(set(packages)) != len(packages)
            or any(not p or p.strip() != p or any(c.isspace() for c in p) for p in packages)):
        raise ValueError("empty, duplicate, or malformed package inventory")
    if packages.count(STORE) != 1 or packages.count(RUNTIME) != 1:
        raise ValueError("Store or workflow runtime missing or duplicated in package inventory")
    selected = [p for p in packages if (p == RUNTIME if mode == "runtime-race"
                                      else p not in (STORE, RUNTIME))]
    if not selected:
        raise ValueError("empty race package partition")
    return selected


def partition(names, index, count):
    if not 1 <= count <= 64 or not 0 <= index < count:
        raise ValueError("invalid shard index/count")
    if not names or len(set(names)) != len(names):
        raise ValueError("empty or duplicate test inventory")
    selected = sorted(names)[index::count]
    if not selected:
        raise ValueError("empty shard")
    return selected


def balanced_partition(names, index, count, weights):
    # Validate the complete live inventory first. Longest-first greedy packing
    # is deterministic, complete and disjoint even with missing/stale timings.
    partition(names, index, count)
    if any(not isinstance(w, (int, float)) or not math.isfinite(w) or w <= 0
           for w in weights.values()):
        raise ValueError("invalid scheduling weight")
    shards, totals = [[] for _ in range(count)], [0] * count
    for name in sorted(names, key=lambda n: (-weights.get(n, 1), n)):
        destination = min(range(count), key=lambda i: (totals[i], i))
        shards[destination].append(name)
        totals[destination] += weights.get(name, 1)
    return sorted(shards[index])


def inventory(output):
    # go test -list also emits its package summary. Benchmarks are not run by
    # ordinary go test; examples and fuzz seeds are, so include both here.
    names = []
    for line in output.splitlines():
        if re.fullmatch(r"(?:Test|Example|Fuzz)\w*", line):
            names.append(line)
        elif re.match(r"^(?:ok|\?)\s+", line) or line.startswith("Benchmark") or not line:
            continue
        else:
            raise ValueError("unexpected test inventory output: " + line)
    return names


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=["other", "runtime-race", "store", "runtime-integration", "crash-other", "crash-runtime"])
    parser.add_argument("--index", type=int, default=0)
    parser.add_argument("--count", type=int, default=1)
    parser.add_argument("--list-only", action="store_true")
    args = parser.parse_args()
    if args.mode in ("other", "crash-other"):
        packages = subprocess.check_output(["go", "list", "./..."], text=True).splitlines()
        selected = race_packages(packages, "other")
        if args.mode == "other":
            selected = balanced_partition(selected, args.index, args.count, PACKAGE_SECONDS)
            command = ["go", "test", *FLAGS, *selected]
        else:
            selected = [p for p in packages if p != RUNTIME]
            command = ["go", "test", *INTEGRATION_FLAGS, *selected, "-run", CRASH_PATTERN]
        print(f"{args.mode} shard {args.index + 1}/{args.count}: " + ", ".join(selected), flush=True)
    else:
        package = STORE if args.mode == "store" else RUNTIME
        race = args.mode in ("store", "runtime-race")
        flags = FLAGS if race else INTEGRATION_FLAGS
        inventory_flags = ["-race"] if race else []
        output = subprocess.check_output(
            ["go", "test", *inventory_flags, "-list", ".", package], text=True
        )
        names = inventory(output)
        if args.mode == "crash-runtime":
            names = [n for n in names if re.search(CRASH_PATTERN, n)]
        selected = (partition(names, args.index, args.count) if args.mode == "store"
                    else balanced_partition(names, args.index, args.count, RUNTIME_SECONDS))
        print(f"{package} shard {args.index + 1}/{args.count}: {len(selected)}/{len(names)} tests", flush=True)
        command = ["go", "test", *flags, "-v", package,
                   "-run", "^(?:" + "|".join(re.escape(n) for n in selected) + ")$"]
    if args.list_only:
        print("\n".join(selected))
        return 0
    return subprocess.call(command)


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (ValueError, subprocess.CalledProcessError) as exc:
        print(str(exc), file=sys.stderr)
        sys.exit(1)
