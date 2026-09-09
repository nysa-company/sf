"""Portable authored tests; execute in hosted CI, not the native trial."""
import copy
import importlib.util
import json
import os
from pathlib import Path
import tempfile
import unittest

spec = importlib.util.spec_from_file_location("proof_eval", Path(__file__).with_name("eval-proof-consistency.py"))
ev = importlib.util.module_from_spec(spec)
spec.loader.exec_module(ev)


class ProofConsistencyEvalTest(unittest.TestCase):
    def setUp(self):
        self.corpus = ev.load_corpus()
        self.instruction = ev.amendment_instruction()
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()

    def answer(self, case):
        proof = case["proposed_proof"] if case["expected_decision"] == "accept" else case["original_proof"]
        return {"case_id": case["id"], "decision": case["expected_decision"],
                "selected_proof_sha256": ev.digest(proof.encode()),
                "acceptance_preserved": True, "frozen_command": case["frozen_command"],
                "rationale": "Synthetic scorer fixture only, not a model decision."}

    def test_corpus_and_production_instruction_are_bound(self):
        self.assertEqual(len(self.corpus["cases"]), 3)
        self.assertIn("Independently check the alleged contradiction", self.instruction)
        for case in self.corpus["cases"]:
            prompt = ev.prompt(case, self.instruction)
            self.assertIn(self.instruction, prompt)
            self.assertNotIn("expected_decision", prompt)
            self.assertNotIn("immutable_sha256", prompt)
            self.assertNotIn("sf.proof-consistency-score", prompt)

    def test_corpus_count_schema_unknown_and_immutable_tamper_refuse(self):
        for kind in ("count", "schema", "unknown", "input", "label", "large"):
            with self.subTest(kind=kind):
                corpus = copy.deepcopy(self.corpus)
                if kind == "count": corpus["cases"].pop()
                if kind == "schema": corpus["schema"] = "future"
                if kind == "unknown": corpus["extra"] = True
                if kind == "input": corpus["cases"][0]["acceptance"] += "changed"
                if kind == "label": corpus["cases"][0]["expected_decision"] = "reject"
                if kind == "large": corpus["cases"][0]["builder_reason"] = "x" * ev.MAX_CORPUS
                path = self.root / "corpus.json"
                path.write_bytes(ev.canonical(corpus))
                with self.assertRaises(ValueError): ev.load_corpus(path)

    def test_instruction_extraction_refuses_ambiguity_and_unsupported_escape(self):
        for text in ('amendmentInstruction = "one"\namendmentInstruction = "two"\n',
                     'amendmentInstruction = `raw`\n', 'amendmentInstruction = "\\x41"\n',
                     'unrelated = "value"\n'):
            path = self.root / "source.go"
            path.write_text(text)
            with self.assertRaises(ValueError): ev.amendment_instruction(path)
        path.write_text("amendmentInstruction = " + json.dumps('line\nquoted "value"') + "\n")
        self.assertEqual(ev.amendment_instruction(path), 'line\nquoted "value"')

    def test_strict_answers_reject_unknown_duplicate_oversize_wrong_case(self):
        case = self.corpus["cases"][0]
        valid = self.answer(case)
        bad = []
        extra = dict(valid, extra=True)
        bad.append(ev.canonical(extra))
        bad.append(ev.canonical(valid)[:-1] + b',"decision":"accept"}')
        bad.append(b" " * (ev.MAX_ANSWER + 1))
        bad.append(ev.canonical(dict(valid, case_id="unknown")))
        bad.append(ev.canonical(dict(valid, acceptance_preserved=1)))
        bad.append(ev.canonical(dict(valid, rationale="")))
        bad.append(b'{"rationale":NaN}')
        for raw in bad:
            with self.subTest(raw_size=len(raw)), self.assertRaises(ValueError):
                ev.score_answer(case, raw)

    def test_wrong_decision_hash_command_or_preservation_fails(self):
        case = self.corpus["cases"][0]
        answer = self.answer(case)
        self.assertTrue(ev.score_answer(case, ev.canonical(answer))["passed"])
        for field, value in (("decision", "reject"), ("selected_proof_sha256", "0" * 64),
                             ("frozen_command", ["true"]), ("acceptance_preserved", False)):
            with self.subTest(field=field):
                self.assertFalse(ev.score_answer(case, ev.canonical(dict(answer, **{field: value})))["passed"])

    def test_prepare_and_score_roundtrip_is_private_and_does_not_echo_rationale(self):
        results = self.root / "results"
        ev.prepare(results, self.corpus, self.instruction)
        self.assertEqual(results.stat().st_mode & 0o777, 0o700)
        self.assertEqual((results / "manifest.json").stat().st_mode & 0o777, 0o600)
        with self.assertRaises(FileExistsError): ev.prepare(results, self.corpus, self.instruction)
        with self.assertRaises(OSError): ev.score(results, self.corpus, self.instruction)
        for case in self.corpus["cases"]:
            (results / (case["id"] + ".answer.json")).write_bytes(ev.canonical(self.answer(case)))
        report = ev.score(results, self.corpus, self.instruction)
        self.assertTrue(report["passed"])
        self.assertEqual(len(report["cases"]), 3)
        self.assertNotIn("Synthetic scorer fixture", json.dumps(report))
        with self.assertRaises(ValueError): ev.score(results, self.corpus, self.instruction + "changed")
        (results / "case-01.prompt.txt").write_text("changed")
        with self.assertRaises(ValueError): ev.score(results, self.corpus, self.instruction)

    def test_schema_tamper_nonprivate_directory_and_symlink_refuse(self):
        results = self.root / "results"
        ev.prepare(results, self.corpus, self.instruction)
        (results / "output-schema.json").write_text("{}")
        with self.assertRaises(ValueError): ev.score(results, self.corpus, self.instruction)
        os.chmod(results, 0o755)
        with self.assertRaises(ValueError): ev.private_results(results)
        os.chmod(results, 0o700)
        alias = self.root / "alias"
        alias.symlink_to(results, target_is_directory=True)
        with self.assertRaises(ValueError): ev.private_results(alias)
        source = self.root / "answer"
        source.write_text("{}")
        link = self.root / "link"
        link.symlink_to(source)
        with self.assertRaises(OSError): ev.bounded_read(link, ev.MAX_ANSWER)


if __name__ == "__main__":
    unittest.main()
