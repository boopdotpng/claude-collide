#!/usr/bin/env python3
"""HTTP entry point for the hardened Tenstorrent device queue."""

from __future__ import annotations

import json
import os
import resource
import signal
import socket
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from socketserver import ThreadingMixIn
from typing import Any
from urllib.parse import parse_qs, urlparse

from queue_core import (
    DEFAULT_CLIENT_ID,
    DeviceDeadError,
    DeviceQueue,
    JobStore,
    QueueConfig,
    QueueUnavailable,
)


REPO_ROOT = Path(__file__).resolve().parent


class BoundedThreadingHTTPServer(ThreadingMixIn, HTTPServer):
    """Threaded HTTP with a hard bound on live handler threads."""

    daemon_threads = True
    block_on_close = False
    request_queue_size = 64
    allow_reuse_address = True

    def __init__(self, address, handler, max_workers: int = 16):
        self._slots = threading.BoundedSemaphore(max(1, max_workers))
        super().__init__(address, handler)

    def process_request(self, request, client_address):
        self._slots.acquire()
        try:
            super().process_request(request, client_address)
        except Exception:
            self._slots.release()
            raise

    def process_request_thread(self, request, client_address):
        try:
            super().process_request_thread(request, client_address)
        finally:
            self._slots.release()


class Handler(BaseHTTPRequestHandler):
    queue: DeviceQueue
    config: QueueConfig
    server_version = "tt-device-queue/2"
    protocol_version = "HTTP/1.1"

    def setup(self) -> None:
        super().setup()
        self.connection.settimeout(10.0)

    def log_message(self, fmt: str, *args: Any) -> None:
        # Keep the journal useful; job lifecycle and exceptional request errors
        # are logged explicitly instead of logging every poll.
        return

    def _send_json(self, code: int, data: dict[str, Any]) -> None:
        body = json.dumps(data, separators=(",", ":"), ensure_ascii=False).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        try:
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError):
            pass

    def _read_json(self, *, required: bool = False) -> dict[str, Any] | None:
        raw_length = self.headers.get("Content-Length")
        if raw_length is None:
            if required:
                self._send_json(411, {"error": "Content-Length is required"})
                return None
            return {}
        try:
            length = int(raw_length)
        except ValueError:
            self._send_json(400, {"error": "Invalid Content-Length"})
            return None
        if length < 0:
            self._send_json(400, {"error": "Invalid Content-Length"})
            return None
        if length > self.config.max_request_bytes:
            self._send_json(413, {"error": "Request body is too large"})
            self.close_connection = True
            return None
        if length == 0:
            if required:
                self._send_json(400, {"error": "Empty body"})
                return None
            return {}
        try:
            payload = json.loads(self.rfile.read(length))
        except (json.JSONDecodeError, UnicodeDecodeError):
            self._send_json(400, {"error": "Invalid JSON"})
            return None
        if not isinstance(payload, dict):
            self._send_json(400, {"error": "JSON body must be an object"})
            return None
        return payload

    @staticmethod
    def _job_id_from(path: str, prefix: str) -> str:
        return path[len(prefix):]

    def do_GET(self) -> None:
        try:
            self._do_get()
        except (BrokenPipeError, ConnectionResetError, socket.timeout):
            return
        except Exception as exc:
            print(f"HTTP GET {self.path!r} failed: {type(exc).__name__}: {exc}")
            self._send_json(500, {"error": "Internal server error"})

    def _do_get(self) -> None:
        parsed = urlparse(self.path)
        path = parsed.path.rstrip("/") or "/"
        query = parse_qs(parsed.query)
        if path == "/status":
            self._send_json(200, self.queue.status())
            return
        if path == "/breakage":
            self._send_json(200, {
                "last_breakage": self.queue.status()["device"].get("last_breakage")
            })
            return
        if path.startswith("/job/"):
            job_id = self._job_id_from(path, "/job/")
            job = self.queue.get_job(job_id)
            if not job:
                self._send_json(404, {"error": f"Unknown job: {job_id}"})
                return
            self._send_json(200, self.queue.snapshot(job))
            return
        if path.startswith("/logs/"):
            job_id = self._job_id_from(path, "/logs/")
            job = self.queue.get_job(job_id)
            if not job:
                self._send_json(404, {"error": f"Unknown job: {job_id}"})
                return
            try:
                offset = int(query.get("offset", ["0"])[0])
                limit = int(query.get("limit", [str(self.config.max_log_read)])[0])
            except ValueError:
                self._send_json(400, {"error": "offset and limit must be integers"})
                return
            self._send_json(200, self.queue.read_logs(job, offset, limit))
            return
        if path.startswith("/result/"):
            job_id = self._job_id_from(path, "/result/")
            job = self.queue.get_job(job_id)
            if not job:
                self._send_json(404, {"error": f"Unknown job: {job_id}"})
                return
            snap = self.queue.snapshot(job)
            if job.status == "done":
                keys = (
                    "status", "exit_code", "output_file", "elapsed", "mode",
                    "estimated_remaining_sec", "repeat", "repeat_completed",
                    "started_at", "finished_at", "timed_out", "timeout_message",
                    "log_size", "log_truncated", "dropped_log_bytes", "error",
                )
            else:
                keys = (
                    "status", "mode", "position", "estimated_wait_sec",
                    "estimated_remaining_sec", "repeat", "repeat_current",
                    "repeat_completed", "first_iteration_elapsed",
                    "per_iter_estimate_sec", "submitted_at", "started_at",
                    "timed_out", "log_size", "log_truncated",
                )
            self._send_json(200, {key: snap.get(key) for key in keys if key in snap})
            return
        self._send_json(404, {"error": "Not found"})

    def do_POST(self) -> None:
        try:
            self._do_post()
        except (BrokenPipeError, ConnectionResetError, socket.timeout):
            return
        except Exception as exc:
            print(f"HTTP POST {self.path!r} failed: {type(exc).__name__}: {exc}")
            self._send_json(500, {"error": "Internal server error"})

    def _do_post(self) -> None:
        path = urlparse(self.path).path.rstrip("/")
        if path == "/queue":
            body = self._read_json(required=True)
            if body is None:
                return
            try:
                job, jobs_ahead, wait = self.queue.submit(
                    cmd=body.get("cmd"), cwd=body.get("cwd", ""),
                    timeout=body.get("timeout"), repeat=body.get("repeat", 1),
                    mode=body.get("mode", "run"), env=body.get("env", {}),
                    client_id=body.get("client_id", DEFAULT_CLIENT_ID),
                )
            except DeviceDeadError as exc:
                self._send_json(503, {"error": str(exc), "device_state": "dead"})
                return
            except QueueUnavailable as exc:
                self._send_json(503, {"error": str(exc)})
                return
            except ValueError as exc:
                self._send_json(400, {"error": str(exc)})
                return
            self._send_json(200, {
                "job_id": job.id, "output_file": job.output_file,
                "position": jobs_ahead, "estimated_wait_sec": wait,
                "estimated_run_sec": round(job.repeat * job.per_iter_estimate_sec),
                "repeat": job.repeat, "mode": job.mode, "timeout": job.timeout,
            })
            return
        if path == "/cancel":
            body = self._read_json()
            if body is None:
                return
            job_id = body.get("job_id")
            if not isinstance(job_id, str) or not job_id.strip():
                self._send_json(400, {"error": "Missing 'job_id'"})
                return
            try:
                cancelled = self.queue.cancel_job(job_id.strip())
            except KeyError:
                self._send_json(404, {"error": f"Unknown job: {job_id}"})
                return
            except ValueError as exc:
                self._send_json(409, {"error": str(exc)})
                return
            except QueueUnavailable as exc:
                self._send_json(503, {"error": str(exc)})
                return
            self._send_json(200, {"cancelled": cancelled})
            return
        if path == "/kill":
            body = self._read_json()
            if body is None:
                return
            raw_job_id = body.get("job_id", "")
            if not isinstance(raw_job_id, str):
                self._send_json(400, {"error": "job_id must be a string"})
                return
            try:
                stopped = self.queue.stop_job(raw_job_id.strip() or None)
            except ValueError as exc:
                self._send_json(409, {"error": str(exc)})
                return
            self._send_json(200, {"killed": stopped} if stopped else {"error": "Nothing running"})
            return
        if path == "/reset":
            body = self._read_json()
            if body is None:
                return
            raw_job_id = body.get("job_id", "")
            if not isinstance(raw_job_id, str):
                self._send_json(400, {"error": "job_id must be a string"})
                return
            try:
                result = self.queue.request_reset(
                    raw_job_id.strip() or None,
                    body.get("client_id", DEFAULT_CLIENT_ID),
                )
            except KeyError:
                self._send_json(404, {"error": f"Unknown job: {raw_job_id}"})
                return
            except DeviceDeadError as exc:
                self._send_json(503, {"error": str(exc), "device_state": "dead"})
                return
            except QueueUnavailable as exc:
                self._send_json(503, {"error": str(exc)})
                return
            self._send_json(200, result)
            return
        self._send_json(404, {"error": "Not found"})


def raise_nofile_limit(target: int = 65536) -> None:
    try:
        soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        wanted = target if hard == resource.RLIM_INFINITY else min(target, hard)
        if soft < wanted:
            resource.setrlimit(resource.RLIMIT_NOFILE, (wanted, hard))
    except (ValueError, OSError) as exc:
        print(f"Could not raise RLIMIT_NOFILE: {exc}")


def build_runtime() -> tuple[QueueConfig, JobStore, DeviceQueue]:
    config = QueueConfig.from_env(REPO_ROOT)
    store = JobStore(config.db_path)
    return config, store, DeviceQueue(config, store)


def main() -> None:
    raise_nofile_limit()
    config, _store, queue = build_runtime()
    Handler.queue = queue
    Handler.config = config
    host = os.environ.get("TT_DEVICE_HOST", "127.0.0.1")
    port = int(os.environ.get("TT_DEVICE_PORT", "5741"))
    max_workers = int(os.environ.get("TT_DEVICE_HTTP_WORKERS", "16"))
    httpd = BoundedThreadingHTTPServer((host, port), Handler, max_workers=max_workers)
    queue.start()

    stopping = threading.Event()

    def stop(_signum=None, _frame=None) -> None:
        if stopping.is_set():
            return
        stopping.set()
        queue.initiate_shutdown()
        # BaseServer.shutdown must be called from a thread other than the one
        # running serve_forever.
        threading.Thread(target=httpd.shutdown, name="http-shutdown", daemon=True).start()

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    print(f"tt-device-queue listening on http://{host}:{port}")
    print(f"Default/max timeout: {config.default_timeout}s/{config.max_timeout}s")
    print(f"Output dir: {config.log_dir}")
    try:
        httpd.serve_forever(poll_interval=0.2)
    finally:
        stop()
        httpd.server_close()
        queue.join(timeout=config.stop_grace_sec + 5)


if __name__ == "__main__":
    main()
