from __future__ import annotations

import json
import os
import socket
import subprocess
import sys
import tempfile
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path


REPO_ROOT = Path(os.environ.get(
    "TT_DEVICE_STRESS_SERVER_ROOT", Path(__file__).resolve().parents[1]
)).resolve()
JOB_COUNT = int(os.environ.get("TT_DEVICE_STRESS_JOBS", "900"))
CLIENT_COUNT = int(os.environ.get("TT_DEVICE_STRESS_CLIENTS", "64"))


def request(base: str, method: str, path: str, payload: dict | None = None) -> dict:
    data = json.dumps(payload).encode() if payload is not None else None
    req = urllib.request.Request(
        base + path,
        data=data,
        headers={"Content-Type": "application/json"},
        method=method,
    )
    with urllib.request.urlopen(req, timeout=10) as response:
        return json.loads(response.read())


def main() -> None:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    with tempfile.TemporaryDirectory() as directory:
        env = os.environ.copy()
        env.update({
            "TT_DEVICE_PORT": str(port),
            "TT_DEVICE_LOG_DIR": directory,
            "TT_DEVICE_MAX_QUEUED_JOBS": str(JOB_COUNT + 1),
            "TT_DEVICE_RESET_CMD": "/bin/true",
            "TT_DEVICE_HEALTH_CHECK_CMD": "/bin/true",
            "TT_DEVICE_DEEP_RESET_CMD": "",
            "INVOCATION_ID": "stress-test-systemd-mode",
            "PYTHONUNBUFFERED": "1",
        })
        process = subprocess.Popen(
            [sys.executable, str(REPO_ROOT / "server.py")],
            cwd=REPO_ROOT,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
        base = f"http://127.0.0.1:{port}"
        started = time.monotonic()
        peak_rss_kib = 0
        try:
            deadline = time.monotonic() + 5
            while True:
                try:
                    request(base, "GET", "/status")
                    break
                except Exception:
                    if process.poll() is not None or time.monotonic() >= deadline:
                        output = process.stdout.read() if process.stdout else ""
                        raise RuntimeError(f"server failed to start: {output}")
                    time.sleep(0.02)

            gate = Path(directory) / "gate"
            blocker = request(base, "POST", "/queue", {
                "cmd": f"while [ ! -e {gate} ]; do sleep .01; done",
                "client_id": "blocker",
            })
            while request(base, "GET", f"/job/{blocker['job_id']}")["status"] != "running":
                time.sleep(0.01)

            def submit(index: int) -> str:
                result = request(base, "POST", "/queue", {
                    "cmd": "/bin/true",
                    "client_id": f"client-{index % CLIENT_COUNT}",
                })
                return result["job_id"]

            with ThreadPoolExecutor(max_workers=CLIENT_COUNT) as pool:
                job_ids = list(pool.map(submit, range(JOB_COUNT)))
            if len(set(job_ids)) != JOB_COUNT:
                raise AssertionError("stress submissions produced duplicate job IDs")

            status = request(base, "GET", "/status")
            if len(status["pending"]) != JOB_COUNT:
                raise AssertionError(
                    f"expected queue depth {JOB_COUNT}, got {len(status['pending'])}"
                )
            gate.touch()

            deadline = time.monotonic() + 180
            while time.monotonic() < deadline:
                process_status = Path(f"/proc/{process.pid}/status").read_text()
                rss_line = next(line for line in process_status.splitlines() if line.startswith("VmRSS:"))
                peak_rss_kib = max(peak_rss_kib, int(rss_line.split()[1]))
                status = request(base, "GET", "/status")
                if status["current"] is None and not status["pending"]:
                    break
                time.sleep(0.05)
            else:
                raise AssertionError("queue did not drain before the stress timeout")

            for job_id in job_ids + [blocker["job_id"]]:
                result = request(base, "GET", f"/job/{job_id}")
                if result["status"] != "done" or result["exit_code"] != 0:
                    raise AssertionError(f"job {job_id} failed: {result}")

            final = request(base, "GET", "/status")
            if final["pending"] or not final.get("worker", {"alive": True})["alive"]:
                raise AssertionError(f"queue did not drain cleanly: {final}")
            elapsed = time.monotonic() - started
            fields = Path(f"/proc/{process.pid}/stat").read_text().split()
            cpu_seconds = (int(fields[13]) + int(fields[14])) / os.sysconf("SC_CLK_TCK")
            print(
                f"PASS: {JOB_COUNT + 1} jobs completed through {CLIENT_COUNT} concurrent clients "
                f"in {elapsed:.2f}s; server CPU {cpu_seconds:.2f}s; "
                f"peak RSS {peak_rss_kib / 1024:.1f} MiB"
            )
        finally:
            process.terminate()
            try:
                process.wait(5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(5)
            if process.stdout:
                process.stdout.close()


if __name__ == "__main__":
    main()
