import json
import unittest
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
        env={"TT_USB": "1"},
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
      "env": {"TT_USB": "1"},
    })

  def test_blocking_run_tool_is_removed(self):
    self.assertFalse(hasattr(mcp_server, "run"))


class McpResetTest(unittest.IsolatedAsyncioTestCase):
  async def test_reset_returns_after_queueing(self):
    queue_result = {
      "job_id": "reset-job",
      "output_file": "/tmp/reset-output",
      "position": 2,
      "estimated_wait_sec": 45,
      "estimated_run_sec": 30,
    }

    with (
      patch("mcp_server._post", new=AsyncMock(return_value=queue_result)) as mock_post,
      patch("mcp_server._wait_for_job", new=AsyncMock()) as mock_wait_for_job,
    ):
      response = await mcp_server.reset(device=1)

    result = json.loads(response)
    self.assertEqual(result["status"], "queued")
    self.assertEqual(result["message"], "Reset for device 1 was queued.")
    self.assertEqual(result["job_id"], "reset-job")
    self.assertEqual(result["position"], 2)
    self.assertEqual(result["estimated_wait_sec"], 45)
    self.assertIn("result(job_id)", result["hint"])
    mock_post.assert_awaited_once_with("/queue", {
      "cmd": f"{mcp_server.TT_SMI} -r 1",
      "cwd": "",
      "timeout": 30,
    })
    mock_wait_for_job.assert_not_awaited()


if __name__ == "__main__":
  unittest.main()
