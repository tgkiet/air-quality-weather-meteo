import os
import subprocess
import tempfile
import unittest
from pathlib import Path


START_SCRIPT = Path(__file__).resolve().parents[1] / "airflow" / "start.sh"


class AirflowStartTest(unittest.TestCase):
    def run_start(self, **settings):
        with tempfile.TemporaryDirectory() as directory:
            temp = Path(directory)
            stub = temp / "airflow"
            stub.write_text(
                """#!/usr/bin/env bash
printf '%s\n' "$*" >> "$CALLS"
case "$1 $2" in
    'db migrate') exit "${MIGRATION_STATUS:-0}" ;;
    'users list')
        if [[ -n "${USERS_STATUS:-}" ]]; then exit "$USERS_STATUS"; fi
        printf '%s\n' '[{"username": "meteo"}]'
        ;;
    'scheduler ') exit 0 ;;
    *) exec sleep 30 ;;
esac
"""
            )
            stub.chmod(0o755)
            calls = temp / "calls"
            env = os.environ | {
                "PATH": f"{temp}:{os.environ['PATH']}",
                "CALLS": str(calls),
                "_AIRFLOW_WWW_USER_USERNAME": "meteo",
            } | settings
            result = subprocess.run(
                ["bash", str(START_SCRIPT)], env=env, capture_output=True, text=True, timeout=5
            )
            return result, calls.read_text().splitlines()

    def test_failed_migration_starts_no_components(self):
        result, calls = self.run_start(MIGRATION_STATUS="42")
        self.assertEqual(result.returncode, 42)
        self.assertEqual(calls, ["db migrate"])

    def test_failed_user_lookup_does_not_attempt_to_create_admin(self):
        result, calls = self.run_start(USERS_STATUS="43")
        self.assertEqual(result.returncode, 43)
        self.assertEqual(calls, ["db migrate", "users list --output json"])

    def test_supervisor_fails_if_a_component_exits_successfully(self):
        result, calls = self.run_start()
        self.assertEqual(result.returncode, 1, result.stderr)
        self.assertEqual(calls[:2], ["db migrate", "users list --output json"])
        self.assertIn("scheduler", calls)
