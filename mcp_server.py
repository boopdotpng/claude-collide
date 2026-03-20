#!/usr/bin/env python3
"""
MCP server for tt-device-queue.

Exposes the device queue as MCP tools so Claude Code agents can submit jobs
and retrieve results without dumb polling. The agent calls device_submit()
to enqueue a command (returns immediately), then calls device_result() when
it actually needs the output (blocks until done).

Tools:
  device_submit  — Submit a command to the device queue. Returns immediately.
  device_result  — Wait for a job to finish and return its full output.
  device_run     — Submit + wait in one call (convenience, blocks until done).
  device_status  — Show what's running, queued, and recently completed.
  device_reset   — Queue a device reset (tt-smi -r).

Talks to the existing tt-device-queue HTTP server on localhost:5741.
"""

import asyncio
import json
import os
import shlex
from pathlib import Path

import httpx
from mcp.server.fastmcp import FastMCP

HOST = "127.0.0.1"
PORT = int(os.environ.get("TT_DEVICE_PORT", "5741"))
BASE = f"http://{HOST}:{PORT}"
DEFAULT_TIMEOUT = 60
AGENT = os.environ.get("TT_AGENT", "mcp-server")

# Poll interval when waiting for job completion — tight because it's localhost
POLL_INTERVAL = 0.5

server = FastMCP(
    "claude-collide",
    instructions=(
        "FIFO queue for commands that touch the GPU/device. Other agents may be "
        "using the device concurrently — all device commands MUST go through these "
        "tools, never through Bash directly. This includes: running Python scripts "
        "that use the device (ttnn, tt-metal, CUDA, etc.), pytest/tests that touch "
        "hardware, benchmarks, tt-smi, firmware tools, and anything that could "
        "conflict with another agent's device access."
    ),
)


class DeviceQueueError(Exception):
    pass


def _wrap_repeat_cmd(cmd: str, repeat: int) -> str:
    if repeat < 1:
        raise DeviceQueueError("repeat must be >= 1")
    if repeat == 1:
        return cmd

    script = (
        'i=1; '
        'while [ "$i" -le "$1" ]; do '
        'printf "\\n[claude-collide] Repeat %s/%s\\n" "$i" "$1"; '
        'eval "$2" || exit $?; '
        'i=$((i + 1)); '
        'done'
    )
    return " ".join([
        "/bin/sh",
        "-c",
        shlex.quote(script),
        "sh",
        shlex.quote(str(repeat)),
        shlex.quote(cmd),
    ])


async def _post(client: httpx.AsyncClient, path: str, data: dict) -> dict:
    try:
        resp = await client.post(f"{BASE}{path}", json=data, timeout=10)
        result = resp.json()
        if resp.status_code != 200:
            raise DeviceQueueError(result.get("error", f"HTTP {resp.status_code}"))
        return result
    except httpx.ConnectError:
        raise DeviceQueueError(
            "tt-device-queue server is not running. "
            "Start it: python ~/tenstorrent/tt-device-queue/server.py &"
        )


async def _get(client: httpx.AsyncClient, path: str) -> dict:
    try:
        resp = await client.get(f"{BASE}{path}", timeout=10)
        result = resp.json()
        if resp.status_code == 404:
            raise DeviceQueueError(result.get("error", "Not found"))
        return result
    except httpx.ConnectError:
        raise DeviceQueueError(
            "tt-device-queue server is not running. "
            "Start it: python ~/tenstorrent/tt-device-queue/server.py &"
        )


async def _wait_for_job(client: httpx.AsyncClient, job_id: str) -> dict:
    """Poll until the job is done. Returns the full result with output contents.

    Uses fast initial polls (50ms) to catch instant failures, then backs off
    to POLL_INTERVAL (500ms) for longer-running jobs.
    """
    interval = 0.05  # start fast for instant failures
    while True:
        result = await _get(client, f"/result/{job_id}")
        if result["status"] == "done":
            # Read the full output file
            output_file = result.get("output_file", "")
            output_text = ""
            if output_file:
                try:
                    output_text = Path(output_file).read_text()
                except (FileNotFoundError, PermissionError):
                    output_text = f"(could not read {output_file})"

            return {
                "exit_code": result["exit_code"],
                "elapsed": result["elapsed"],
                "output_file": output_file,
                "output": output_text,
            }
        await asyncio.sleep(interval)
        interval = min(interval * 2, POLL_INTERVAL)  # backoff to 500ms


@server.tool()
async def device_submit(
    cmd: str,
    cwd: str = "",
    timeout: int = DEFAULT_TIMEOUT,
    repeat: int = 1,
    agent: str = AGENT,
) -> str:
    """Submit a command to the device queue and return immediately with a job_id.

    Use this instead of Bash for ANY command that uses the GPU/device (python
    scripts using ttnn/tt-metal/CUDA, pytest, benchmarks, tt-smi, etc.). Other
    agents may be using the device — the queue prevents conflicts.

    Returns immediately. Call device_result(job_id) when you need the output.
    Do other work (read files, write code, plan) in the meantime. If you have
    nothing else to do, use device_run() instead.

    Args:
        cmd: Shell command to run (e.g. "pytest tests/" or "python train.py")
        cwd: Working directory for the command
        timeout: Max execution time in seconds (default 120)
        repeat: Run the command this many times sequentially inside one queued job;
            all output is appended to the same output file and execution stops on
            the first failure
        agent: Tag identifying this agent
    """
    queued_cmd = _wrap_repeat_cmd(cmd, repeat)
    async with httpx.AsyncClient() as client:
        result = await _post(client, "/queue", {
            "cmd": queued_cmd, "cwd": cwd, "timeout": timeout, "agent": agent,
        })

    return json.dumps({
        "job_id": result["job_id"],
        "output_file": result["output_file"],
        "position": result["position"],
        "estimated_wait_sec": result["estimated_wait_sec"],
        "repeat": repeat,
        "hint": "Call device_result(job_id) when you need the output. Repeat runs still use one job_id and append into one output file.",
    }, indent=2)


@server.tool()
async def device_result(job_id: str) -> str:
    """Wait for a previously submitted device job to finish and return its
    full output. Blocks until the job completes.

    Only call this when you actually need the result. If you have other work
    to do (reading files, writing code, planning next steps), do that first
    and call this after — the job runs in the background regardless.

    Args:
        job_id: The job_id returned by device_submit()
    """
    async with httpx.AsyncClient() as client:
        result = await _wait_for_job(client, job_id)

    exit_code = result["exit_code"]
    status = "OK" if exit_code == 0 else f"FAILED (exit code {exit_code})"

    lines = [
        f"Status: {status}",
        f"Elapsed: {result['elapsed']}s",
        f"Output file: {result['output_file']}",
        "",
        "--- Command Output ---",
        result["output"],
    ]
    return "\n".join(lines)


@server.tool()
async def device_run(
    cmd: str,
    cwd: str = "",
    timeout: int = DEFAULT_TIMEOUT,
    repeat: int = 1,
    agent: str = AGENT,
) -> str:
    """Submit a command to the device queue and wait for it to complete.

    Use this instead of Bash for ANY command that uses the GPU/device (python
    scripts using ttnn/tt-metal/CUDA, pytest, benchmarks, tt-smi, etc.). Other
    agents may be using the device — the queue prevents conflicts.

    Blocks until done. Use this when you have nothing else to do while waiting.
    If you want to do other work while the command runs, use device_submit()
    instead and call device_result() later.

    Args:
        cmd: Shell command to run (e.g. "pytest tests/" or "python train.py")
        cwd: Working directory for the command
        timeout: Max execution time in seconds (default 120)
        repeat: Run the command this many times sequentially inside one queued job;
            all output is appended to the same output file and execution stops on
            the first failure
        agent: Tag identifying this agent
    """
    queued_cmd = _wrap_repeat_cmd(cmd, repeat)
    async with httpx.AsyncClient() as client:
        submit_result = await _post(client, "/queue", {
            "cmd": queued_cmd, "cwd": cwd, "timeout": timeout, "agent": agent,
        })

        job_id = submit_result["job_id"]
        result = await _wait_for_job(client, job_id)

    exit_code = result["exit_code"]
    status = "OK" if exit_code == 0 else f"FAILED (exit code {exit_code})"

    lines = [
        f"Job: {job_id}",
        f"Status: {status}",
        f"Elapsed: {result['elapsed']}s",
        f"Output file: {result['output_file']}",
        "",
        "--- Command Output ---",
        result["output"],
    ]
    return "\n".join(lines)


@server.tool()
async def device_status() -> str:
    """Show what's currently running, queued, and recently completed on the device.
    Use this to check if the device is busy before submitting work, or to see
    the history of recent jobs."""
    async with httpx.AsyncClient() as client:
        data = await _get(client, "/status")

    lines = []

    current = data.get("current")
    if current:
        lines.append(f"RUNNING: [{current['id']}] {current['cmd']}")
        lines.append(f"         agent={current['agent']}  {current['running_sec']}s")
    else:
        lines.append("RUNNING: (idle)")

    pending = data.get("pending", [])
    if pending:
        lines.append(f"\nQUEUED ({len(pending)}):")
        for p in pending:
            lines.append(f"  [{p['id']}] {p['cmd']}")
            lines.append(f"           agent={p['agent']}  waiting {p['waiting_sec']}s")
    else:
        lines.append("\nQUEUED: (empty)")

    recent = data.get("recent", [])
    if recent:
        lines.append(f"\nRECENT:")
        for r in recent:
            tag = "OK" if r.get("exit_code", 1) == 0 else f"FAIL({r.get('exit_code')})"
            lines.append(f"  [{r['id']}] {tag} {r.get('elapsed', '?')}s  {r['cmd']}")

    return "\n".join(lines)


@server.tool()
async def device_kill() -> str:
    """Kill the currently running device job immediately. Use this when a
    command is hung or you need to abort it. The job will be marked as failed
    and the next queued job will start.
    """
    async with httpx.AsyncClient() as client:
        result = await _post(client, "/kill", {})

    killed = result.get("killed")
    if killed:
        return f"Killed job [{killed['id']}] {killed['cmd']} (agent={killed['agent']})"
    return "Nothing running to kill."


TT_SMI = os.path.expanduser("~/tenstorrent/.venv/bin/tt-smi")


@server.tool()
async def device_reset(device: int = 0, agent: str = AGENT) -> str:
    """Reset the Tenstorrent device via tt-smi. Queued through the FIFO like
    any other command — waits for running jobs to finish first, then resets.

    Use this when the device is in a bad state (hangs, errors, firmware
    issues, NaN outputs). Blocks until the reset completes.

    Args:
        device: Device number to reset (default 0)
        agent: Tag identifying this agent
    """
    cmd = f"{TT_SMI} -r {device}"
    async with httpx.AsyncClient() as client:
        submit_result = await _post(client, "/queue", {
            "cmd": cmd, "cwd": "", "timeout": 30, "agent": f"{agent}/reset",
        })
        job_id = submit_result["job_id"]
        result = await _wait_for_job(client, job_id)

    exit_code = result["exit_code"]
    status = "OK" if exit_code == 0 else f"FAILED (exit code {exit_code})"

    lines = [
        f"Reset device {device}: {status}",
        f"Elapsed: {result['elapsed']}s",
        "",
        result["output"],
    ]
    return "\n".join(lines)


if __name__ == "__main__":
    server.run(transport="stdio")
