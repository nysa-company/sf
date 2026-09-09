import importlib.util
import itertools
from pathlib import Path
import re
import subprocess
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location("ci_race", Path(__file__).with_name("ci-race.py"))
ci = importlib.util.module_from_spec(spec)
spec.loader.exec_module(ci)


class RacePartitionTest(unittest.TestCase):
    def test_workflow_runs_every_acceptance_lane_and_shard(self):
        root = Path(__file__).resolve().parent.parent
        workflow = (root / ".github/workflows/repository-baseline.yml").read_text()
        targets = re.search(r"target: \[([^]]+)\]", workflow).group(1).split(", ")
        self.assertEqual(set(targets), {"race-other", "runtime-race", "test-integration-other", "test-crash",
                                      "test-security", "test-upgrade", "test-compiled-e2e", "verify-static"})
        shards = re.search(r"shard: \[([^]]+)\]", workflow).group(1).split(", ")
        self.assertEqual([int(i) for i in shards], list(range(8)))
        self.assertIn('--count 8', (root / "Makefile").read_text())
        runtime = workflow.split("  runtime-integration:\n", 1)[1]
        runtime_shards = re.search(r"shard: \[([^]]+)\]", runtime).group(1).split(", ")
        self.assertEqual([int(i) for i in runtime_shards], list(range(4)))
        makefile = (root / "Makefile").read_text()
        self.assertIn('runtime-race) python3 scripts/run-bounded --timeout 65m -- python3 scripts/ci-race.py runtime-race', makefile)
        self.assertIn('runtime-integration --index "$$SHARD" --count 4', makefile)
        reference = re.search(r"\ntest-integration:\n\t([^\n]+)", makefile).group(1).split()
        other = re.search(r"\ntest-integration-other:\n\t([^\n]+)", makefile).group(1).split()
        self.assertEqual([part for part in reference if part != "./internal/workflowruntime"], other)
        self.assertNotIn("continue-on-error", workflow)

    def test_required_acceptance_gate_is_fail_closed(self):
        workflow = (Path(__file__).resolve().parent.parent /
                    ".github/workflows/repository-baseline.yml").read_text()
        gate = workflow.split("  acceptance:\n", 1)[1].split("  baseline:\n", 1)[0]
        self.assertIn("name: SF acceptance", gate)
        self.assertIn("if: ${{ always() }}", gate)
        self.assertIn("needs: [baseline, store-race, runtime-integration]", gate)
        self.assertIn("${{ needs.baseline.result }}", gate)
        self.assertIn("${{ needs.store-race.result }}", gate)
        self.assertIn("${{ needs.runtime-integration.result }}", gate)
        self.assertIn('test "$BASELINE_RESULT" = success', gate)
        self.assertIn('test "$STORE_RACE_RESULT" = success', gate)
        self.assertIn('test "$RUNTIME_INTEGRATION_RESULT" = success', gate)
        commands = gate.split("        run: |\n", 1)[1]
        for baseline, store, runtime in itertools.product(("success", "failure", "cancelled", "skipped", ""), repeat=3):
            with self.subTest(baseline=baseline, store=store, runtime=runtime):
                result = subprocess.run(
                    ["bash", "--noprofile", "--norc", "-e", "-c", commands],
                    env={"BASELINE_RESULT": baseline, "STORE_RACE_RESULT": store,
                         "RUNTIME_INTEGRATION_RESULT": runtime},
                    capture_output=True, timeout=5)
                self.assertEqual(result.returncode == 0,
                                 baseline == store == runtime == "success")

    def test_complete_disjoint_stable_partition(self):
        names = [f"TestCase{i}" for i in range(541)] + ["ExampleStore", "FuzzDecode"]
        shards = [ci.partition(names, i, 8) for i in range(8)]
        flat = [name for shard in shards for name in shard]
        self.assertEqual(sorted(names), sorted(flat))
        self.assertEqual(len(flat), len(set(flat)))
        self.assertEqual(shards[2], ci.partition(list(reversed(names)), 2, 8))

    def test_invalid_inventory_or_shard_refused(self):
        for names, index, count in [([], 0, 8), (["TestA"] * 2, 0, 1),
                                    (["TestA"], 1, 8), (["TestA"], -1, 8),
                                    (["TestA"], 0, 0), (["TestA"], 8, 8)]:
            with self.assertRaises(ValueError):
                ci.partition(names, index, count)
        with self.assertRaises(ValueError):
            ci.inventory("unexpected fixture output")

    def test_inventory_keeps_examples_and_fuzz_seeds(self):
        self.assertEqual(ci.inventory("TestA\nExampleB\nFuzzC\nBenchmarkD\nok  \tpackage 1s\n"),
                         ["TestA", "ExampleB", "FuzzC"])

    def test_store_failure_is_propagated_and_pattern_is_exact(self):
        with patch("sys.argv", ["ci-race.py", "store", "--count", "1"]), \
             patch.object(ci.subprocess, "check_output", return_value="TestA\nTestAB\n"), \
             patch.object(ci.subprocess, "call", return_value=1) as run:
            self.assertEqual(ci.main(), 1)
            self.assertEqual(run.call_args.args[0][-1], "^(?:TestA|TestAB)$")
            self.assertIn("-race", run.call_args.args[0])

    def test_other_runs_every_package_outside_store_and_runtime(self):
        with patch("sys.argv", ["ci-race.py", "other"]), \
             patch.object(ci.subprocess, "check_output", return_value=f"first\n{ci.STORE}\n{ci.RUNTIME}\nlast\n"), \
             patch.object(ci.subprocess, "call", return_value=0) as run:
            self.assertEqual(ci.main(), 0)
            self.assertEqual(run.call_args.args[0][-2:], ["first", "last"])
            self.assertNotIn(ci.STORE, run.call_args.args[0])
            self.assertNotIn(ci.RUNTIME, run.call_args.args[0])

    def test_runtime_race_runs_whole_package_with_unchanged_flags(self):
        with patch("sys.argv", ["ci-race.py", "runtime-race"]), \
             patch.object(ci.subprocess, "check_output", return_value=f"first\n{ci.STORE}\n{ci.RUNTIME}\nlast\n"), \
             patch.object(ci.subprocess, "call", return_value=1) as run:
            self.assertEqual(ci.main(), 1)
            self.assertEqual(run.call_args.args[0], ["go", "test", *ci.FLAGS, ci.RUNTIME])
            self.assertNotIn("-run", run.call_args.args[0])

    def test_race_package_lanes_are_complete_and_disjoint(self):
        packages = ["first", ci.STORE, ci.RUNTIME, "last"]
        other = ci.race_packages(packages, "other")
        runtime = ci.race_packages(packages, "runtime-race")
        self.assertEqual(set(other + runtime + [ci.STORE]), set(packages))
        self.assertEqual(len(other + runtime + [ci.STORE]), len(packages))
        self.assertEqual(runtime, [ci.RUNTIME])

    def test_race_package_lanes_refuse_malformed_inventory(self):
        for packages in ([], ["first", ci.STORE], ["first", ci.RUNTIME],
                         ["first", ci.STORE, ci.RUNTIME, ci.STORE],
                         ["first", ci.STORE, ci.RUNTIME, ci.RUNTIME],
                         ["first", "first", ci.STORE, ci.RUNTIME],
                         ["", ci.STORE, ci.RUNTIME],
                         [" bad", ci.STORE, ci.RUNTIME],
                         ["bad package", ci.STORE, ci.RUNTIME]):
            for mode in ("other", "runtime-race"):
                with self.subTest(packages=packages, mode=mode), self.assertRaises(ValueError):
                    ci.race_packages(packages, mode)
        with self.assertRaises(ValueError):
            ci.race_packages([ci.STORE, ci.RUNTIME], "other")

    def test_runtime_integration_keeps_normal_flags_and_all_seed_kinds(self):
        with patch("sys.argv", ["ci-race.py", "runtime-integration", "--count", "1"]), \
             patch.object(ci.subprocess, "check_output", return_value="TestA\nExampleB\nFuzzC\n") as listing, \
             patch.object(ci.subprocess, "call", return_value=1) as run:
            self.assertEqual(ci.main(), 1)
            self.assertEqual(listing.call_args.args[0], ["go", "test", "-list", ".", ci.RUNTIME])
            command = run.call_args.args[0]
            self.assertIn(ci.RUNTIME, command)
            self.assertIn("30m", command)
            self.assertNotIn("-race", command)
            self.assertEqual(command[-1], "^(?:ExampleB|FuzzC|TestA)$")


if __name__ == "__main__":
    unittest.main()
