import importlib.util
import json
import pathlib
import subprocess
import tempfile
import unittest
from unittest import mock


spec = importlib.util.spec_from_file_location(
    "pilot_evidence", pathlib.Path(__file__).with_name("pilot-evidence.py")
)
pilot = importlib.util.module_from_spec(spec)
spec.loader.exec_module(pilot)


class EvidenceCompletenessTests(unittest.TestCase):
    def test_go_test_outcomes(self):
        cases = [
            ("complete", [("pass", "TestPilot/child"), ("pass", "TestPilot")], 0, None),
            ("skipped child", [("skip", "TestPilot/child"), ("pass", "TestPilot")], 0, "TestPilot/child"),
            ("skipped parent", [("skip", "TestPilot")], 0, "TestPilot"),
            ("failed child", [("fail", "TestPilot/child"), ("pass", "TestPilot")], 1, "TestPilot/child"),
            ("missing parent", [("pass", "TestDifferent")], 0, "TestPilot"),
            ("command failed", [("pass", "TestPilot")], 1, "Pilot evidence failed"),
        ]
        for name, events, code, error in cases:
            with self.subTest(name=name), tempfile.TemporaryDirectory() as directory:
                root = pathlib.Path(directory)
                (root / "go.mod").write_text("module example.test/pilot\n\ngo 1.26\ntoolchain go1.26.6\n")
                output = "\n".join(json.dumps({
                    "Action": action,
                    "Package": "github.com/digitaldrywood/detent/internal/hubserver",
                    "Test": test,
                }) for action, test in events)
                result = subprocess.CompletedProcess([], code, output, "")
                with mock.patch.object(pilot, "SUITES", {"hubserver": ["TestPilot"]}), \
                     mock.patch.object(pilot.subprocess, "run", return_value=result), \
                     mock.patch.object(pilot.subprocess, "check_output", return_value="fixture"), \
                     mock.patch("builtins.print"):
                    if error:
                        with self.assertRaisesRegex(RuntimeError, error):
                            pilot.collect(root, False)
                    else:
                        evidence = pilot.collect(root, False)
                        self.assertEqual(evidence["required_tests"], 1)
                        self.assertEqual(evidence["passed_tests_and_subtests"], 2)


if __name__ == "__main__":
    unittest.main()
