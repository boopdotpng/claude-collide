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

  GET  /job/<job_id>
                -> full job metadata including repeat progress, timestamps, and
                   queue position when still pending

  GET  /status  -> {"current", "pending", "recent"}
"""

import json
import os
import signal
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
ESTIMATE_PER_JOB = 10  # seconds assumed per queued job

LOG_DIR.mkdir(parents=True, exist_ok=True)


def _format_timestamp(ts: float | None) -> str | None:
  if ts is None:
    return None
  return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ts))


@dataclass
class Job:
  id: str
  cmd: str
  cwd: str
  timeout: int
  repeat: int
  agent: str
  submitted: float = field(default_factory=time.time)
  # Filled in by worker
  status: str = "queued"        # queued -> running -> done
  exit_code: int | None = None
  elapsed: float | None = None
  output_file: str = ""
  started_at: float | None = None
  finished_at: float | None = None
  repeat_current: int = 0
  repeat_completed: int = 0


class DeviceQueue:
  def __init__(self):
    self._queue: Queue[Job] = Queue()
    self._jobs: dict[str, Job] = {}          # all jobs by id
    self._pending_ids: list[str] = []        # ordered list of queued job ids
    self._current: Job | None = None
    self._current_proc: subprocess.Popen | None = None
    self._history: list[dict] = []
    self._lock = threading.Lock()

  def submit(self, cmd: str, cwd: str, timeout: int, repeat: int, agent: str) -> tuple["Job", int, int]:
    """Submit a job. Returns (job, position, estimated_wait_sec) computed atomically."""
    if repeat < 1:
      raise ValueError("repeat must be >= 1")

    job_id = uuid.uuid4().hex[:8]
    output_dir = LOG_DIR / job_id
    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = str(output_dir / "output")

    job = Job(
      id=job_id, cmd=cmd, cwd=cwd, timeout=timeout, repeat=repeat,
      agent=agent, output_file=output_file,
    )

    with self._lock:
      self._jobs[job_id] = job
      self._pending_ids.append(job_id)
      # Compute position while still holding the lock, before the worker can dequeue
      pos = self._pending_ids.index(job_id)
      jobs_ahead = pos + (1 if self._current else 0)

    self._queue.put(job)
    return job, jobs_ahead, jobs_ahead * ESTIMATE_PER_JOB

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
          "repeat": j.repeat,
          "repeat_current": j.repeat_current,
          "repeat_completed": j.repeat_completed,
        }
      pending = []
      for jid in self._pending_ids:
        j = self._jobs[jid]
        pending.append({
          "id": j.id, "cmd": j.cmd[:120], "agent": j.agent,
          "waiting_sec": round(time.time() - j.submitted, 1),
          "repeat": j.repeat,
        })
      return {
        "current": current,
        "pending": pending,
        "recent": self._history[-10:],
      }

  def snapshot(self, job: Job) -> dict:
    with self._lock:
      position = None
      estimated_wait_sec = None
      running_sec = None
      if job.status == "queued":
        try:
          pos = self._pending_ids.index(job.id)
        except ValueError:
          pos = -1
        position = pos + 1
        estimated_wait_sec = (pos + 1) * ESTIMATE_PER_JOB
      elif job.status == "running":
        running_sec = round(time.time() - (job.started_at or job.submitted), 1)
        position = 0
        estimated_wait_sec = max(0, ESTIMATE_PER_JOB - running_sec)

      data = {
        "job_id": job.id,
        "status": job.status,
        "cmd": job.cmd,
        "cwd": job.cwd,
        "timeout": job.timeout,
        "repeat": job.repeat,
        "repeat_current": job.repeat_current,
        "repeat_completed": job.repeat_completed,
        "agent": job.agent,
        "submitted_at": _format_timestamp(job.submitted),
        "started_at": _format_timestamp(job.started_at),
        "finished_at": _format_timestamp(job.finished_at),
        "output_file": job.output_file,
        "exit_code": job.exit_code,
        "elapsed": job.elapsed,
      }

      if position is not None:
        data["position"] = position
      if estimated_wait_sec is not None:
        data["estimated_wait_sec"] = estimated_wait_sec
      if running_sec is not None:
        data["running_sec"] = running_sec

      return data

  def kill_current(self) -> dict | None:
    """Kill the currently running job. Returns info about the killed job, or None."""
    with self._lock:
      proc = self._current_proc
      job = self._current
      if not proc or not job:
        return None
      info = {"id": job.id, "cmd": job.cmd[:120], "agent": job.agent}

    # Kill the entire process group
    try:
      os.killpg(proc.pid, signal.SIGKILL)
    except (ProcessLookupError, PermissionError):
      try:
        proc.kill()
      except (ProcessLookupError, PermissionError):
        pass
    return info

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
        deadline = job.started_at + job.timeout if job.timeout > 0 else None
        exit_code = 0
        with open(job.output_file, "w") as out_f:
          for iteration in range(1, job.repeat + 1):
            with self._lock:
              job.repeat_current = iteration

            if job.repeat > 1:
              out_f.write(f"\n[claude-collide] Repeat {iteration}/{job.repeat}\n")
              out_f.flush()

            proc = subprocess.Popen(
              job.cmd, shell=True,
              stdout=out_f, stderr=subprocess.STDOUT,
              cwd=job.cwd or None,
              start_new_session=True,  # own process group for clean kills
            )
            with self._lock:
              self._current_proc = proc

            try:
              wait_timeout = None if deadline is None else max(0, deadline - time.time())
              if wait_timeout == 0:
                raise subprocess.TimeoutExpired(job.cmd, job.timeout)
              proc.wait(timeout=wait_timeout)
              exit_code = proc.returncode
            except subprocess.TimeoutExpired:
              os.killpg(proc.pid, signal.SIGKILL)
              proc.wait()
              exit_code = -9
              out_f.write(f"\n[claude-collide] Timed out after {job.timeout}s — killed\n")
            finally:
              out_f.flush()

            if exit_code != 0:
              break

            with self._lock:
              job.repeat_completed = iteration

          if exit_code == 0:
            with self._lock:
              job.repeat_completed = job.repeat
      except Exception as e:
        exit_code = -1
        with open(job.output_file, "a") as f:
          f.write(f"\n[claude-collide] Error: {e}\n")

      elapsed = round(time.time() - job.started_at, 2)

      with self._lock:
        job.status = "done"
        job.exit_code = exit_code
        job.elapsed = elapsed
        job.finished_at = time.time()
        self._current = None
        self._current_proc = None
        self._history.append({
          "id": job.id, "cmd": job.cmd[:120], "agent": job.agent,
          "exit_code": exit_code, "elapsed": elapsed,
          "finished": time.strftime("%H:%M:%S"),
          "output_file": job.output_file,
          "repeat": job.repeat,
          "repeat_completed": job.repeat_completed,
        })
        if len(self._history) > 50:
          self._history = self._history[-50:]

      # Write metadata alongside output
      meta_path = Path(job.output_file).parent / "meta.json"
      with open(meta_path, "w") as f:
        json.dump({
          "id": job.id, "cmd": job.cmd, "cwd": job.cwd, "agent": job.agent,
          "exit_code": exit_code, "elapsed": elapsed, "repeat": job.repeat,
          "repeat_completed": job.repeat_completed,
          "started": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(job.started_at)),
          "finished": _format_timestamp(job.finished_at),
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

    if path.startswith("/job/"):
      job_id = path[len("/job/"):]
      job = dq.get_job(job_id)
      if not job:
        self._json_response(404, {"error": f"Unknown job: {job_id}"})
        return
      self._json_response(200, dq.snapshot(job))
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
          "repeat": job.repeat,
          "repeat_completed": job.repeat_completed,
          "started_at": _format_timestamp(job.started_at),
          "finished_at": _format_timestamp(job.finished_at),
        })
      elif job.status == "running":
        running_for = round(time.time() - (job.started_at or job.submitted), 1)
        self._json_response(200, {
          "status": "running",
          "position": 0,
          "estimated_wait_sec": max(0, ESTIMATE_PER_JOB - running_for),
          "repeat": job.repeat,
          "repeat_current": job.repeat_current,
          "repeat_completed": job.repeat_completed,
          "started_at": _format_timestamp(job.started_at),
        })
      else:
        pos = dq.position_of(job_id)
        self._json_response(200, {
          "status": "queued",
          "position": pos + 1,  # 1-indexed for humans
          "estimated_wait_sec": (pos + 1) * ESTIMATE_PER_JOB,
          "repeat": job.repeat,
          "repeat_current": job.repeat_current,
          "repeat_completed": job.repeat_completed,
          "submitted_at": _format_timestamp(job.submitted),
        })
      return

    self._json_response(404, {"error": "Not found"})

  def _read_json_body(self) -> dict | None:
    length = int(self.headers.get("Content-Length", 0))
    if length == 0:
      return {}
    try:
      return json.loads(self.rfile.read(length))
    except json.JSONDecodeError:
      self._json_response(400, {"error": "Invalid JSON"})
      return None

  def do_POST(self):
    path = self.path.rstrip("/")

    if path == "/kill":
      killed = dq.kill_current()
      if killed:
        self._json_response(200, {"killed": killed})
      else:
        self._json_response(200, {"error": "Nothing running"})
      return

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

      job, jobs_ahead, wait_sec = dq.submit(
        cmd=cmd,
        cwd=body.get("cwd", ""),
        timeout=body.get("timeout", DEFAULT_TIMEOUT),
        repeat=body.get("repeat", 1),
        agent=body.get("agent", "unknown"),
      )

      self._json_response(200, {
        "job_id": job.id,
        "output_file": job.output_file,
        "position": jobs_ahead,
        "estimated_wait_sec": wait_sec,
        "repeat": job.repeat,
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
