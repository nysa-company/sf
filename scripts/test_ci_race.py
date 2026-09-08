import importlib.util
from pathlib import Path
import re
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
        self.assertEqual(set(targets), {"race-other", "test-integration", "test-crash",
                                      "test-security", "test-upgrade", "test-compiled-e2e", "verify-static"})
        shards = re.search(r"shard: \[([^]]+)\]", workflow).group(1).split(", ")
        self.assertEqual([int(i) for i in shards], list(range(8)))
        self.assertIn('--count 8', (root / "Makefile").read_text())
        self.assertNotIn("continue-on-error", workflow)

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

    def test_other_runs_every_non_store_package(self):
        with patch("sys.argv", ["ci-race.py", "other"]), \
             patch.object(ci.subprocess, "check_output", return_value=f"first\n{ci.STORE}\nlast\n"), \
             patch.object(ci.subprocess, "call", return_value=0) as run:
            self.assertEqual(ci.main(), 0)
            self.assertEqual(run.call_args.args[0][-2:], ["first", "last"])
            self.assertNotIn(ci.STORE, run.call_args.args[0])


if __name__ == "__main__":
    unittest.main()
