import json
import inspect
import shlex
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

import mcp_server


class McpQueueTest(unittest.IsolatedAsyncioTestCase):
  async def test_queue_returns_after_enqueueing(self):
    queue_result = {
      "job_id": "queued-job",
      "output_file": "/tmp/queued-output",
      "position": 1,
      "estimated_wait_sec": 20,
      "estimated_run_sec": 10,
    }

    with patch("mcp_server._post", new=AsyncMock(return_value=queue_result)) as mock_post:
      response = await mcp_server.queue(
        cmd="python test.py",
        cwd="/repo",
        timeout=90,
        repeat=3,
      )

    result = json.loads(response)
    self.assertEqual(result["job_id"], "queued-job")
    self.assertEqual(result["position"], 1)
    self.assertEqual(result["estimated_wait_sec"], 20)
    self.assertEqual(result["repeat"], 3)
    self.assertIn("result(job_id)", result["hint"])
    mock_post.assert_awaited_once_with("/queue", {
      "cmd": "python test.py",
      "cwd": "/repo",
      "timeout": 90,
      "repeat": 3,
      "mode": "run",
      "client_id": mcp_server.CLIENT_ID,
    })

  def test_blocking_run_tool_is_removed(self):
    self.assertFalse(hasattr(mcp_server, "run"))

  def test_queue_tool_does_not_expose_env_argument(self):
    self.assertNotIn("env", inspect.signature(mcp_server.queue).parameters)
    self.assertNotIn("env", inspect.signature(mcp_server.open_forever).parameters)

  async def test_queue_python_writes_script_and_queues_file(self):
    queue_result = {
      "job_id": "python-job",
      "output_file": "/tmp/python-output",
      "position": 1,
      "estimated_wait_sec": 0,
      "estimated_run_sec": 10,
    }

    with tempfile.TemporaryDirectory() as temp_dir:
      with (
        patch("mcp_server.SCRIPT_DIR", Path(temp_dir)),
        patch("mcp_server._post", new=AsyncMock(return_value=queue_result)) as mock_post,
      ):
        response = await mcp_server.queue_python(
          script="print('hello')",
          cwd="/repo",
          timeout=30,
          python="python3",
          args=["--flag"],
        )

      result = json.loads(response)
      script_file = Path(result["script_file"])
      self.assertTrue(script_file.exists())
      self.assertEqual(script_file.read_text(), "print('hello')\n")

      payload = mock_post.await_args.args[1]
      cmd_parts = shlex.split(payload["cmd"])
      self.assertEqual(cmd_parts, ["python3", str(script_file), "--flag"])
      self.assertEqual(payload["cwd"], "/repo")
      self.assertEqual(payload["timeout"], 30)
      self.assertEqual(payload["mode"], "run")
      self.assertEqual(payload["client_id"], mcp_server.CLIENT_ID)


class McpResetTest(unittest.IsolatedAsyncioTestCase):
  async def test_reset_reports_failing_job_instead_of_queueing(self):
    reset_result = {
      "action": "scheduled",
      "device_state": "healthy",
      "reset_epoch": 3,
      "hint": "Reset will run before the next job.",
    }

    with (
      patch("mcp_server._post", new=AsyncMock(return_value=reset_result)) as mock_post,
      patch("mcp_server._wait_for_job", new=AsyncMock()) as mock_wait_for_job,
    ):
      response = await mcp_server.reset(job_id="failing1")

    result = json.loads(response)
    self.assertEqual(result["action"], "scheduled")
    self.assertEqual(result["reset_epoch"], 3)
    mock_post.assert_awaited_once_with("/reset", {
      "client_id": mcp_server.CLIENT_ID,
      "job_id": "failing1",
    })
    mock_wait_for_job.assert_not_awaited()

  async def test_reset_without_job_id_omits_field(self):
    reset_result = {"action": "joined", "device_state": "resetting", "reset_epoch": 0}

    with patch("mcp_server._post", new=AsyncMock(return_value=reset_result)) as mock_post:
      response = await mcp_server.reset()

    result = json.loads(response)
    self.assertEqual(result["action"], "joined")
    mock_post.assert_awaited_once_with("/reset", {"client_id": mcp_server.CLIENT_ID})


class McpCancelTest(unittest.IsolatedAsyncioTestCase):
  async def test_cancel_posts_job_id(self):
    cancel_result = {"cancelled": {"id": "victim12", "cmd": "sleep 99", "client": "a"}}

    with patch("mcp_server._post", new=AsyncMock(return_value=cancel_result)) as mock_post:
      response = await mcp_server.cancel(job_id="victim12")

    self.assertIn("victim12", response)
    mock_post.assert_awaited_once_with("/cancel", {"job_id": "victim12"})


class McpStatusBannerTest(unittest.IsolatedAsyncioTestCase):
  async def test_status_shows_dead_device_banner(self):
    status_payload = {
      "current": None,
      "pending": [],
      "recent": [],
      "device": {
        "state": "dead",
        "reset_epoch": 2,
        "reset_pending": False,
        "dead_since": "2026-06-09 14:32:00",
        "dead_reason": "DEVICE UNRECOVERABLE: reboot required",
      },
    }

    with patch("mcp_server._get", new=AsyncMock(return_value=status_payload)):
      response = await mcp_server.status()

    self.assertIn("DEVICE DEAD", response)
    self.assertIn("reboot required", response)

  async def test_status_shows_resetting_banner_and_clients(self):
    status_payload = {
      "current": {
        "id": "job1", "cmd": "python x.py", "client": "agent-a",
        "running_sec": 1.0, "repeat": 1,
      },
      "pending": [
        {"id": "job2", "cmd": "python y.py", "client": "agent-b",
         "waiting_sec": 0.5, "repeat": 1},
      ],
      "recent": [],
      "device": {"state": "resetting", "reset_epoch": 0, "reset_pending": False},
    }

    with patch("mcp_server._get", new=AsyncMock(return_value=status_payload)):
      response = await mcp_server.status()

    self.assertIn("DEVICE RESET in progress", response)
    self.assertIn("(agent-a)", response)
    self.assertIn("(agent-b)", response)


if __name__ == "__main__":
  unittest.main()
