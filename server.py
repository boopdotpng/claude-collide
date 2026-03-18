#!/usr/bin/env python3
"""
tt-device-queue server — serializes access to the Tenstorrent device.

HTTP API on localhost:5741. Jobs run one at a time (FIFO).
Output is saved to /tmp/tt-device-logs/<job_id>/output.

Endpoints:
  POST /queue   {"cmd": "...", "cwd": "...", "timeout": 120, "agent": "..."}
                -> {"job_id", "output_file", "position", "estimated_wait_sec"}

  GET  /result/<job_id>
                -> {"status": "queued|running|done", "position", "estimated_wait_sec"}
                   or {"status": "done", "exit_code", "output_file", "elapsed"}

  GET  /status  -> {"current", "pending", "recent"}
"""

import json
import os
import subprocess
import threading
import time
import uuid
from http.server import HTTPServer, BaseHTTPRequestHandler
from pathlib import Path
from queue import Queue
from dataclasses import dataclass, field

HOST = os.environ.get("TT_DEVICE_HOST", "127.0.0.1")
PORT = int(os.environ.get("TT_DEVICE_PORT", "5741"))
DEFAULT_TIMEOUT = int(os.environ.get("TT_DEVICE_TIMEOUT", "120"))
LOG_DIR = Path(os.environ.get("TT_DEVICE_LOG_DIR", "/tmp/tt-device-logs"))
ESTIMATE_PER_JOB = 15  # seconds assumed per queued job

LOG_DIR.mkdir(parents=True, exist_ok=True)


@dataclass
class Job:
  id: str
  cmd: str
  cwd: str
  timeout: int
  agent: str
  submitted: float = field(default_factory=time.time)
  # Filled in by worker
  status: str = "queued"        # queued -> running -> done
  exit_code: int | None = None
  elapsed: float | None = None
  output_file: str = ""
  started_at: float | None = None


class DeviceQueue:
  def __init__(self):
    self._queue: Queue[Job] = Queue()
    self._jobs: dict[str, Job] = {}          # all jobs by id
    self._pending_ids: list[str] = []        # ordered list of queued job ids
    self._current: Job | None = None
    self._history: list[dict] = []
    self._lock = threading.Lock()

  def submit(self, cmd: str, cwd: str, timeout: int, agent: str) -> Job:
    job_id = uuid.uuid4().hex[:8]
    output_dir = LOG_DIR / job_id
    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = str(output_dir / "output")

    job = Job(
      id=job_id, cmd=cmd, cwd=cwd, timeout=timeout,
      agent=agent, output_file=output_file,
    )

    with self._lock:
      self._jobs[job_id] = job
      self._pending_ids.append(job_id)

    self._queue.put(job)
    return job

  def get_job(self, job_id: str) -> Job | None:
    return self._jobs.get(job_id)

  def position_of(self, job_id: str) -> int:
    """0-indexed position in the pending queue. -1 if not pending."""
    with self._lock:
      try:
        return self._pending_ids.index(job_id)
      except ValueError:
        return -1

  def queue_length(self) -> int:
    with self._lock:
      return len(self._pending_ids)

  def status(self) -> dict:
    with self._lock:
      current = None
      if self._current:
        j = self._current
        current = {
          "id": j.id, "cmd": j.cmd[:120], "agent": j.agent,
          "running_sec": round(time.time() - (j.started_at or j.submitted), 1),
        }
      pending = []
      for jid in self._pending_ids:
        j = self._jobs[jid]
        pending.append({
          "id": j.id, "cmd": j.cmd[:120], "agent": j.agent,
          "waiting_sec": round(time.time() - j.submitted, 1),
        })
      return {
        "current": current,
        "pending": pending,
        "recent": self._history[-10:],
      }

  def worker_loop(self):
    """Runs forever in a dedicated thread. Processes jobs one at a time."""
    while True:
      job = self._queue.get()

      with self._lock:
        job.status = "running"
        job.started_at = time.time()
        self._current = job
        if job.id in self._pending_ids:
          self._pending_ids.remove(job.id)

      print(f"[{job.id}] Running: {job.cmd[:100]}")

      try:
        with open(job.output_file, "w") as out_f:
          proc = subprocess.run(
            job.cmd, shell=True,
            stdout=out_f, stderr=subprocess.STDOUT,
            cwd=job.cwd or None,
            timeout=job.timeout,
          )
          exit_code = proc.returncode
      except subprocess.TimeoutExpired:
        exit_code = -9
        with open(job.output_file, "a") as out_f:
          out_f.write(f"\n[tt-device-queue] Timed out after {job.timeout}s — killed\n")
      except Exception as e:
        exit_code = -1
        with open(job.output_file, "a") as out_f:
          out_f.write(f"\n[tt-device-queue] Error: {e}\n")

      elapsed = round(time.time() - job.started_at, 2)

      with self._lock:
        job.status = "done"
        job.exit_code = exit_code
        job.elapsed = elapsed
        self._current = None
        self._history.append({
          "id": job.id, "cmd": job.cmd[:120], "agent": job.agent,
          "exit_code": exit_code, "elapsed": elapsed,
          "finished": time.strftime("%H:%M:%S"),
          "output_file": job.output_file,
        })
        if len(self._history) > 50:
          self._history = self._history[-50:]

      # Write metadata alongside output
      meta_path = Path(job.output_file).parent / "meta.json"
      with open(meta_path, "w") as f:
        json.dump({
          "id": job.id, "cmd": job.cmd, "cwd": job.cwd, "agent": job.agent,
          "exit_code": exit_code, "elapsed": elapsed,
          "started": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(job.started_at)),
          "finished": time.strftime("%Y-%m-%d %H:%M:%S"),
          "output_file": job.output_file,
        }, f, indent=2)

      status = "OK" if exit_code == 0 else f"FAIL({exit_code})"
      print(f"[{job.id}] {status} in {elapsed}s -> {job.output_file}")
      self._queue.task_done()


dq = DeviceQueue()


class Handler(BaseHTTPRequestHandler):
  def log_message(self, fmt, *args):
    # Suppress default access log noise
    pass

  def _json_response(self, code: int, data: dict):
    body = json.dumps(data).encode()
    self.send_response(code)
    self.send_header("Content-Type", "application/json")
    self.send_header("Content-Length", str(len(body)))
    self.end_headers()
    self.wfile.write(body)

  def do_GET(self):
    path = self.path.rstrip("/")

    if path == "/status":
      self._json_response(200, dq.status())
      return

    if path.startswith("/result/"):
      job_id = path[len("/result/"):]
      job = dq.get_job(job_id)
      if not job:
        self._json_response(404, {"error": f"Unknown job: {job_id}"})
        return

      if job.status == "done":
        self._json_response(200, {
          "status": "done",
          "exit_code": job.exit_code,
          "output_file": job.output_file,
          "elapsed": job.elapsed,
        })
      elif job.status == "running":
        running_for = round(time.time() - (job.started_at or job.submitted), 1)
        self._json_response(200, {
          "status": "running",
          "position": 0,
          "estimated_wait_sec": max(0, ESTIMATE_PER_JOB - running_for),
        })
      else:
        pos = dq.position_of(job_id)
        self._json_response(200, {
          "status": "queued",
          "position": pos + 1,  # 1-indexed for humans
          "estimated_wait_sec": (pos + 1) * ESTIMATE_PER_JOB,
        })
      return

    self._json_response(404, {"error": "Not found"})

  def do_POST(self):
    path = self.path.rstrip("/")

    if path == "/queue":
      length = int(self.headers.get("Content-Length", 0))
      if length == 0:
        self._json_response(400, {"error": "Empty body"})
        return
      try:
        body = json.loads(self.rfile.read(length))
      except json.JSONDecodeError:
        self._json_response(400, {"error": "Invalid JSON"})
        return

      cmd = body.get("cmd", "").strip()
      if not cmd:
        self._json_response(400, {"error": "Missing 'cmd'"})
        return

      job = dq.submit(
        cmd=cmd,
        cwd=body.get("cwd", ""),
        timeout=body.get("timeout", DEFAULT_TIMEOUT),
        agent=body.get("agent", "unknown"),
      )

      pos = dq.position_of(job.id)
      # pos is 0-indexed in pending list; add 1 for the currently running job if any
      jobs_ahead = pos + (1 if dq._current else 0)

      self._json_response(200, {
        "job_id": job.id,
        "output_file": job.output_file,
        "position": jobs_ahead,
        "estimated_wait_sec": jobs_ahead * ESTIMATE_PER_JOB,
      })
      return

    self._json_response(404, {"error": "Not found"})


def main():
  # Start worker thread
  worker = threading.Thread(target=dq.worker_loop, daemon=True)
  worker.start()

  server = HTTPServer((HOST, PORT), Handler)
  print(f"tt-device-queue listening on http://{HOST}:{PORT}")
  print(f"Default timeout: {DEFAULT_TIMEOUT}s")
  print(f"Output dir: {LOG_DIR}")
  try:
    server.serve_forever()
  except KeyboardInterrupt:
    print("\nShutting down...")
    server.shutdown()


if __name__ == "__main__":
  main()
