from __future__ import annotations

import json
import os
import shlex
import signal
import socket
import subprocess
import sys
import tempfile
import time
import unittest
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
SERVER = REPO_ROOT / "server.py"
POLL = 0.02


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


class ServerTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.port = free_port()
        self.env = os.environ.copy()
        self.env.update({
            "TT_DEVICE_PORT": str(self.port),
            "TT_DEVICE_LOG_DIR": self.temp.name,
            "TT_DEVICE_DEFAULT_TIMEOUT": "5",
            "TT_DEVICE_MAX_TIMEOUT": "30",
            "TT_DEVICE_STOP_GRACE_SEC": "0.2",
            "TT_DEVICE_PROCESS_POLL_INTERVAL": "0.02",
            "TT_DEVICE_MAX_LOG_BYTES": "4096",
            "TT_DEVICE_MAX_REQUEST_BYTES": "512",
            "TT_DEVICE_RESET_CMD": "/bin/true",
            "TT_DEVICE_HEALTH_CHECK_CMD": "/bin/true",
            "TT_DEVICE_DEEP_RESET_CMD": "",
            "PYTHONUNBUFFERED": "1",
        })
        self.start()

    def tearDown(self) -> None:
        self.stop()
        self.temp.cleanup()

    def start(self) -> None:
        self.proc = subprocess.Popen(
            [sys.executable, str(SERVER)], cwd=REPO_ROOT, env=self.env,
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
        )
        self.base = f"http://127.0.0.1:{self.port}"
        deadline = time.time() + 5
        while time.time() < deadline:
            try:
                self.get("/status")
                return
            except Exception:
                if self.proc.poll() is not None:
                    break
                time.sleep(POLL)
        output = self.proc.stdout.read() if self.proc.stdout else ""
        self.fail(f"server failed to start: {output}")

    def stop(self) -> None:
        if getattr(self, "proc", None) and self.proc.poll() is None:
            self.proc.terminate()
            try:
                self.proc.wait(3)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait(3)
        if getattr(self, "proc", None) and self.proc.stdout:
            self.proc.stdout.close()

    def get(self, path: str) -> dict:
        with urllib.request.urlopen(self.base + path, timeout=3) as response:
            return json.loads(response.read())

    def post(self, path: str, payload: dict) -> dict:
        request = urllib.request.Request(
            self.base + path, data=json.dumps(payload).encode(),
            headers={"Content-Type": "application/json"}, method="POST",
        )
        with urllib.request.urlopen(request, timeout=3) as response:
            return json.loads(response.read())

    def post_status(self, path: str, payload) -> tuple[int, dict]:
        request = urllib.request.Request(
            self.base + path, data=json.dumps(payload).encode(),
            headers={"Content-Type": "application/json"}, method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=3) as response:
                return response.status, json.loads(response.read())
        except urllib.error.HTTPError as exc:
            try:
                return exc.code, json.loads(exc.read())
            finally:
                exc.close()

    def submit(self, cmd: str, **values) -> dict:
        payload = {"cmd": cmd, "cwd": str(REPO_ROOT)}
        payload.update(values)
        return self.post("/queue", payload)

    def wait_done(self, job_id: str, timeout: float = 8) -> dict:
        deadline = time.time() + timeout
        while time.time() < deadline:
            result = self.get(f"/result/{job_id}")
            if result["status"] == "done":
                return result
            time.sleep(POLL)
        self.fail(f"job {job_id} did not finish")

    def wait_state(self, state: str, timeout: float = 8) -> dict:
        deadline = time.time() + timeout
        while time.time() < deadline:
            status = self.get("/status")
            if status["device"]["state"] == state:
                return status
            time.sleep(POLL)
        self.fail(f"device did not reach {state}")

    def test_repeat_and_persisted_result(self) -> None:
        job = self.submit("printf 'ok\\n'", repeat=3)
        self.assertEqual(len(job["job_id"]), 32)
        result = self.wait_done(job["job_id"])
        self.assertEqual(result["exit_code"], 0)
        self.assertEqual(result["repeat_completed"], 3)
        logs = self.get(f"/logs/{job['job_id']}?offset=0&limit=4096")
        self.assertEqual(logs["content"].count("ok"), 3)
        self.assertTrue(logs["complete"])

        self.stop()
        self.start()
        persisted = self.get(f"/job/{job['job_id']}")
        self.assertEqual(persisted["status"], "done")
        self.assertEqual(persisted["exit_code"], 0)

    def test_failure_stops_repeats(self) -> None:
        count = Path(self.temp.name) / "count"
        inner = (
            f"n=$(cat {shlex.quote(str(count))} 2>/dev/null || echo 0); "
            f"n=$((n+1)); echo $n > {shlex.quote(str(count))}; test $n -lt 2"
        )
        job = self.submit(f"/bin/sh -c {shlex.quote(inner)}", repeat=5)
        result = self.wait_done(job["job_id"])
        self.assertNotEqual(result["exit_code"], 0)
        self.assertEqual(result["repeat_completed"], 1)
        self.assertEqual(count.read_text().strip(), "2")

    def test_default_timeout_kills_job(self) -> None:
        self.env["TT_DEVICE_DEFAULT_TIMEOUT"] = "1"
        self.stop()
        self.start()
        job = self.submit("sleep 10")
        result = self.wait_done(job["job_id"])
        self.assertTrue(result["timed_out"])
        self.assertEqual(result["exit_code"], -9)
        self.assertLess(result["elapsed"], 3)

    def test_round_robin(self) -> None:
        gate = Path(self.temp.name) / "gate"
        sequence = Path(self.temp.name) / "sequence"
        blocker = self.submit(
            f"while [ ! -e {shlex.quote(str(gate))} ]; do sleep .02; done",
            client_id="a",
        )
        while self.get(f"/job/{blocker['job_id']}")["status"] != "running":
            time.sleep(POLL)

        def marker(name: str, client: str) -> dict:
            return self.submit(
                f"echo {name} >> {shlex.quote(str(sequence))}", client_id=client
            )

        a1, a2, b1 = marker("a1", "a"), marker("a2", "a"), marker("b1", "b")
        gate.touch()
        for item in (blocker, a1, a2, b1):
            self.wait_done(item["job_id"])
        self.assertEqual(sequence.read_text().split(), ["a1", "b1", "a2"])

    def test_log_file_is_bounded(self) -> None:
        job = self.submit(
            f"{shlex.quote(sys.executable)} -c {shlex.quote('print(\"x\" * 20000)')}"
        )
        result = self.wait_done(job["job_id"])
        self.assertTrue(result["log_truncated"])
        output = Path(result["output_file"])
        self.assertLessEqual(output.stat().st_size, 4096)
        logs = self.get(f"/logs/{job['job_id']}?offset=0&limit=4096")
        self.assertIn("Output truncated", logs["content"])

    def test_malformed_types_are_400(self) -> None:
        cases = [
            {"cmd": ["true"]}, {"cmd": "true", "repeat": 1.5},
            {"cmd": "true", "cwd": 42}, {"cmd": "true", "env": []},
        ]
        for payload in cases:
            with self.subTest(payload=payload):
                code, body = self.post_status("/queue", payload)
                self.assertEqual(code, 400)
                self.assertIn("error", body)
        code, body = self.post_status("/queue", ["not", "an", "object"])
        self.assertEqual(code, 400)

    def test_oversized_request_is_413(self) -> None:
        code, body = self.post_status("/queue", {"cmd": "x" * 600})
        self.assertEqual(code, 413)
        self.assertIn("too large", body["error"])

    def test_reset_preempts_running_job(self) -> None:
        running = self.submit("sleep 10", timeout=20)
        while self.get(f"/job/{running['job_id']}")["status"] != "running":
            time.sleep(POLL)
        reset = self.post("/reset", {"job_id": running["job_id"], "client_id": "tester"})
        self.assertEqual(reset["action"], "scheduled")
        result = self.wait_done(running["job_id"])
        self.assertNotEqual(result["exit_code"], 0)
        state = self.wait_state("healthy")
        deadline = time.time() + 5
        while state["device"]["reset_epoch"] < 1 and time.time() < deadline:
            time.sleep(POLL)
            state = self.get("/status")
        self.assertEqual(state["device"]["reset_epoch"], 1)

    def test_concurrent_reset_requests_coalesce(self) -> None:
        count = Path(self.temp.name) / "reset-count"
        inner = f"echo x >> {shlex.quote(str(count))}; sleep .2"
        self.env["TT_DEVICE_RESET_CMD"] = f"/bin/sh -c {shlex.quote(inner)}"
        self.stop()
        self.start()
        with ThreadPoolExecutor(max_workers=8) as pool:
            responses = list(pool.map(lambda _: self.post("/reset", {}), range(8)))
        actions = [response["action"] for response in responses]
        self.assertEqual(actions.count("scheduled"), 1)
        self.assertEqual(actions.count("joined"), 7)
        deadline = time.time() + 5
        while time.time() < deadline:
            status = self.get("/status")
            if status["device"]["state"] == "healthy" and status["device"]["reset_epoch"] == 1:
                break
            time.sleep(POLL)
        self.assertEqual(count.read_text().splitlines(), ["x"])

    def test_failed_health_check_marks_dead_drains_queue_and_survives_restart(self) -> None:
        reset_count = Path(self.temp.name) / "reset-count"
        health_count = Path(self.temp.name) / "health-count"
        reset = Path(self.temp.name) / "reset.sh"
        health = Path(self.temp.name) / "health.sh"
        reset.write_text(f"#!/bin/sh\necho x >> {shlex.quote(str(reset_count))}\n")
        health.write_text(
            f"#!/bin/sh\necho x >> {shlex.quote(str(health_count))}\nexit 1\n"
        )
        reset.chmod(0o755)
        health.chmod(0o755)
        self.env["TT_DEVICE_RESET_CMD"] = str(reset)
        self.env["TT_DEVICE_HEALTH_CHECK_CMD"] = str(health)
        self.env["TT_DEVICE_RESET_RETRIES"] = "1"
        self.stop()
        self.start()

        failed = self.submit("exit 7", client_id="reporter")
        self.assertEqual(self.wait_done(failed["job_id"])["exit_code"], 7)
        running = self.submit("sleep 10", client_id="active")
        while self.get(f"/job/{running['job_id']}")["status"] != "running":
            time.sleep(POLL)
        pending = self.submit("printf 'must-not-run\\n'", client_id="waiting")

        response = self.post("/reset", {
            "job_id": failed["job_id"], "client_id": "simulated-device-test",
        })
        self.assertEqual(response["action"], "scheduled")
        state = self.wait_state("dead")
        self.assertIn("DEVICE UNRECOVERABLE", state["device"]["dead_reason"])
        self.assertEqual(len(reset_count.read_text().splitlines()), 2)
        self.assertEqual(len(health_count.read_text().splitlines()), 2)

        queued_result = self.wait_done(pending["job_id"])
        self.assertEqual(queued_result["exit_code"], -1)
        self.assertIn("reboot or operator recovery", queued_result["error"])
        queued_logs = self.get(f"/logs/{pending['job_id']}?offset=0&limit=4096")
        self.assertNotIn("must-not-run", queued_logs["content"])
        self.assertIn("reboot or operator recovery", queued_logs["content"])

        self.stop()
        self.start()
        self.assertEqual(self.get("/status")["device"]["state"], "dead")
        code, body = self.post_status("/queue", {"cmd": "true"})
        self.assertEqual(code, 503)
        self.assertIn("reboot or operator recovery", body["error"])

    def test_deep_reset_escalation_requires_post_reset_health(self) -> None:
        ready = Path(self.temp.name) / "device-ready"
        reset_count = Path(self.temp.name) / "reset-count"
        deep_count = Path(self.temp.name) / "deep-count"
        health_count = Path(self.temp.name) / "health-count"
        reset = Path(self.temp.name) / "reset.sh"
        deep = Path(self.temp.name) / "deep.sh"
        health = Path(self.temp.name) / "health.sh"
        reset.write_text(
            "#!/bin/sh\n"
            f"echo x >> {shlex.quote(str(reset_count))}\n"
            f"test -e {shlex.quote(str(ready))}\n"
        )
        deep.write_text(
            "#!/bin/sh\n"
            f"echo x >> {shlex.quote(str(deep_count))}\n"
            f"touch {shlex.quote(str(ready))}\n"
        )
        health.write_text(
            "#!/bin/sh\n"
            f"echo x >> {shlex.quote(str(health_count))}\n"
            f"test -e {shlex.quote(str(ready))}\n"
        )
        for script in (reset, deep, health):
            script.chmod(0o755)
        self.env["TT_DEVICE_RESET_CMD"] = str(reset)
        self.env["TT_DEVICE_DEEP_RESET_CMD"] = str(deep)
        self.env["TT_DEVICE_HEALTH_CHECK_CMD"] = str(health)
        self.env["TT_DEVICE_RESET_RETRIES"] = "1"
        self.stop()
        self.start()

        response = self.post("/reset", {"client_id": "deep-reset-test"})
        self.assertEqual(response["action"], "scheduled")
        state = self.wait_state("healthy")
        deadline = time.time() + 5
        while state["device"]["reset_epoch"] < 1 and time.time() < deadline:
            time.sleep(POLL)
            state = self.get("/status")
        self.assertEqual(state["device"]["reset_epoch"], 1)
        self.assertEqual(len(reset_count.read_text().splitlines()), 3)
        self.assertEqual(len(deep_count.read_text().splitlines()), 1)
        self.assertEqual(len(health_count.read_text().splitlines()), 1)

    def test_dead_state_survives_same_boot_restart(self) -> None:
        self.env["TT_DEVICE_RESET_CMD"] = "/bin/false"
        self.env["TT_DEVICE_HEALTH_CHECK_CMD"] = "/bin/false"
        self.stop()
        self.start()
        self.post("/reset", {})
        self.wait_state("dead")
        self.stop()
        self.start()
        status = self.get("/status")
        self.assertEqual(status["device"]["state"], "dead")
        code, _ = self.post_status("/queue", {"cmd": "true"})
        self.assertEqual(code, 503)

    def test_shutdown_during_reset_retries_after_restart(self) -> None:
        self.env["TT_DEVICE_RESET_CMD"] = "sleep 10"
        self.stop()
        self.start()
        self.post("/reset", {})
        self.wait_state("resetting")
        self.stop()

        self.env["TT_DEVICE_RESET_CMD"] = "/bin/true"
        self.start()
        deadline = time.time() + 5
        while time.time() < deadline:
            status = self.get("/status")
            if status["device"]["state"] == "healthy" and status["device"]["reset_epoch"] == 1:
                break
            time.sleep(POLL)
        self.assertEqual(status["device"]["state"], "healthy")
        self.assertEqual(status["device"]["reset_epoch"], 1)

    def test_queued_job_recovers_after_crash(self) -> None:
        pid_file = Path(self.temp.name) / "child.pid"
        running = self.submit(f"echo $$ > {pid_file}; exec sleep 20", timeout=30, client_id="a")
        deadline = time.time() + 5
        while not pid_file.exists() and time.time() < deadline:
            time.sleep(POLL)
        queued = self.submit("printf 'recovered\\n'", client_id="b")

        self.proc.kill()
        self.proc.wait(3)
        child_pid = int(pid_file.read_text())
        deadline = time.time() + 2
        while time.time() < deadline:
            try:
                os.kill(child_pid, 0)
            except ProcessLookupError:
                break
            time.sleep(POLL)
        else:
            try:
                os.killpg(child_pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            self.fail("job process group survived queue-server death")
        if self.proc.stdout:
            self.proc.stdout.close()
        self.start()

        interrupted = self.wait_done(running["job_id"])
        self.assertEqual(interrupted["exit_code"], -1)
        recovered = self.wait_done(queued["job_id"])
        self.assertEqual(recovered["exit_code"], 0)
        logs = self.get(f"/logs/{queued['job_id']}?offset=0&limit=4096")
        self.assertIn("recovered", logs["content"])

    def test_status_exposes_worker_health(self) -> None:
        status = self.get("/status")
        self.assertTrue(status["worker"]["alive"])
        self.assertIsNone(status["worker"]["degraded_reason"])


if __name__ == "__main__":
    unittest.main()
