#!/usr/bin/env python3
"""Partition hosted acceptance suites without omitting tests.

CI runs `other`, `runtime-race`, and every Store shard on isolated macOS runners. Local
`make test-race` remains the unpartitioned reference command.
Normal workflow-runtime integration has separate disjoint shards; the other
integration packages remain in the Makefile's integration-other lane.
"""

import argparse
import re
import subprocess
import sys

STORE = "github.com/nysa-company/sf/internal/store"
RUNTIME = "github.com/nysa-company/sf/internal/workflowruntime"
FLAGS = ["-race", "-count=1", "-shuffle=off", "-p", "1", "-timeout", "60m"]
INTEGRATION_FLAGS = ["-count=1", "-shuffle=off", "-p", "1", "-timeout", "30m"]


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
    parser.add_argument("mode", choices=["other", "runtime-race", "store", "runtime-integration"])
    parser.add_argument("--index", type=int, default=0)
    parser.add_argument("--count", type=int, default=8)
    parser.add_argument("--list-only", action="store_true")
    args = parser.parse_args()
    if args.mode in ("other", "runtime-race"):
        packages = subprocess.check_output(["go", "list", "./..."], text=True).splitlines()
        selected = race_packages(packages, args.mode)
        command = ["go", "test", *FLAGS, *selected]
    else:
        package = STORE if args.mode == "store" else RUNTIME
        flags = FLAGS if args.mode == "store" else INTEGRATION_FLAGS
        inventory_flags = ["-race"] if args.mode == "store" else []
        output = subprocess.check_output(
            ["go", "test", *inventory_flags, "-list", ".", package], text=True
        )
        names = inventory(output)
        selected = partition(names, args.index, args.count)
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
