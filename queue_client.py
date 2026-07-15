#!/usr/bin/env python3
"""Small synchronous client shared by CLI-style callers and the MCP adapter."""

from __future__ import annotations

import json
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any
from urllib.parse import urlencode


class QueueClientError(RuntimeError):
    pass


def read_output_file(output_file: str, maximum: int = 1 << 20) -> str:
    """Compatibility helper with a hard memory bound and binary-safe decoding."""
    if not output_file:
        return ""
    try:
        with open(Path(output_file), "rb") as stream:
            data = stream.read(maximum + 1)
    except (FileNotFoundError, PermissionError, OSError):
        return f"(could not read {output_file})"
    truncated = len(data) > maximum
    text = data[:maximum].decode("utf-8", errors="replace")
    if truncated:
        text += f"\n[Output truncated at {maximum} bytes.]"
    return text


def _decode_response(response) -> dict[str, Any]:
    raw = response.read()
    try:
        result = json.loads(raw)
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise QueueClientError("Queue server returned an invalid JSON response") from exc
    if not isinstance(result, dict):
        raise QueueClientError("Queue server returned a non-object JSON response")
    return result


def _http_error(exc: urllib.error.HTTPError) -> QueueClientError:
    try:
        result = _decode_response(exc)
        message = result.get("error", f"HTTP {exc.code}")
    except QueueClientError:
        message = f"HTTP {exc.code}: {exc.reason}"
    finally:
        exc.close()
    return QueueClientError(str(message))


def get(base: str, path: str) -> dict[str, Any]:
    try:
        with urllib.request.urlopen(f"{base}{path}", timeout=10) as response:
            return _decode_response(response)
    except urllib.error.HTTPError as exc:
        raise _http_error(exc) from None
    except (urllib.error.URLError, TimeoutError) as exc:
        raise QueueClientError(f"tt-device-queue server is unavailable: {exc}") from exc


def post(base: str, path: str, data: dict[str, Any]) -> dict[str, Any]:
    request = urllib.request.Request(
        f"{base}{path}", data=json.dumps(data).encode(),
        headers={"Content-Type": "application/json"}, method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            return _decode_response(response)
    except urllib.error.HTTPError as exc:
        raise _http_error(exc) from None
    except (urllib.error.URLError, TimeoutError) as exc:
        raise QueueClientError(f"tt-device-queue server is unavailable: {exc}") from exc


def read_all_logs(
    base: str, job_id: str, *, maximum: int = 1 << 20, chunk_size: int = 64 << 10,
) -> tuple[str, bool]:
    offset = 0
    pieces: list[str] = []
    truncated = False
    while offset < maximum:
        limit = min(chunk_size, maximum - offset)
        query = urlencode({"offset": offset, "limit": limit})
        result = get(base, f"/logs/{job_id}?{query}")
        pieces.append(result.get("content", ""))
        next_offset = int(result.get("next_offset", offset))
        if result.get("complete"):
            truncated = bool(result.get("log_truncated"))
            break
        if next_offset <= offset:
            break
        offset = next_offset
    else:
        truncated = True
    if offset >= maximum:
        truncated = True
    return "".join(pieces), truncated


def wait_for_job(
    base: str, job_id: str, poll_interval: float = 0.5,
    output_limit: int = 1 << 20,
) -> dict[str, Any]:
    interval = 0.05
    while True:
        result = get(base, f"/result/{job_id}")
        if result["status"] == "done":
            output, truncated = read_all_logs(base, job_id, maximum=output_limit)
            return {
                "exit_code": result["exit_code"], "elapsed": result["elapsed"],
                "output_file": result.get("output_file", ""), "output": output,
                "output_truncated": truncated,
                "timed_out": bool(result.get("timed_out")),
                "timeout_message": result.get("timeout_message"),
                "error": result.get("error"),
            }
        time.sleep(interval)
        interval = min(interval * 2, poll_interval)
