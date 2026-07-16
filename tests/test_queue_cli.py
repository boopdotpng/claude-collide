from __future__ import annotations

import io
import json
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from unittest.mock import patch

import queue_cli
from queue_client import QueueClientError


class QueueCliTest(unittest.TestCase):
    def test_queue_submits_shell_program_with_client_identity(self) -> None:
        response = {
            "job_id": "a" * 32,
            "output_file": "/tmp/output",
            "position": 0,
            "estimated_wait_sec": 0,
            "repeat": 2,
            "timeout": 30,
        }
        with (
            patch("queue_cli.post", return_value=response) as post,
            redirect_stdout(io.StringIO()) as stdout,
        ):
            code = queue_cli.main([
                "--json", "--client-id", "agent-test", "queue",
                "--cwd", "/tmp", "--timeout", "30", "--repeat", "2",
                "--env", "ARCH_NAME=blackhole", "--", "printf 'hello' | cat",
            ])
        self.assertEqual(code, 0)
        self.assertEqual(json.loads(stdout.getvalue())["job_id"], "a" * 32)
        post.assert_called_once_with("http://127.0.0.1:5741", "/queue", {
            "cmd": "printf 'hello' | cat",
            "cwd": "/tmp",
            "repeat": 2,
            "mode": "run",
            "env": {"ARCH_NAME": "blackhole"},
            "client_id": "agent-test",
            "timeout": 30,
        })

    def test_run_waits_for_output_and_returns_job_exit_code(self) -> None:
        submitted = {"job_id": "job1", "position": 0}
        done = {
            "status": "done", "exit_code": 7, "elapsed": 0.2,
            "output_file": "/tmp/output", "timed_out": False,
        }
        with (
            patch("queue_cli.post", return_value=submitted),
            patch("queue_cli.get", return_value=done),
            patch("queue_cli.read_all_logs", return_value=("failure\n", False)),
            redirect_stdout(io.StringIO()) as stdout,
            redirect_stderr(io.StringIO()) as stderr,
        ):
            code = queue_cli.main(["run", "--cwd", "/tmp", "--", "exit 7"])
        self.assertEqual(code, 7)
        self.assertEqual(stdout.getvalue(), "failure\n")
        self.assertIn("job failed", stderr.getvalue())

    def test_result_wait_timeout_does_not_kill_job(self) -> None:
        with (
            patch("queue_cli.get", return_value={"status": "running"}),
            redirect_stderr(io.StringIO()) as stderr,
        ):
            code = queue_cli.main(["result", "job1", "--wait-timeout", "0"])
        self.assertEqual(code, 124)
        self.assertIn("job is still active", stderr.getvalue())

    def test_queue_python_removes_generated_script_if_submission_fails(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            with (
                patch("queue_cli._script_directory", return_value=Path(directory)),
                patch("queue_cli.post", side_effect=QueueClientError("no queue")),
                patch("sys.stdin", io.StringIO("print('hello')\n")),
                redirect_stderr(io.StringIO()),
            ):
                code = queue_cli.main(["queue-python", "-"])
            self.assertEqual(code, 1)
            self.assertEqual(list(Path(directory).glob("*.py")), [])

    def test_status_human_format_highlights_device_state(self) -> None:
        status = {
            "worker": {"alive": True},
            "device": {"state": "dead", "dead_since": "now", "dead_reason": "test"},
            "current": None,
            "pending": [],
            "recent": [],
        }
        with (
            patch("queue_cli.get", return_value=status),
            redirect_stdout(io.StringIO()) as stdout,
        ):
            code = queue_cli.main(["status"])
        self.assertEqual(code, 0)
        self.assertIn("DEVICE DEAD", stdout.getvalue())
        self.assertIn("RUNNING: (idle)", stdout.getvalue())


if __name__ == "__main__":
    unittest.main()
