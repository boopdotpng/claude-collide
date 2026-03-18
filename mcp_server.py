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
from pathlib import Path

import httpx
from mcp.server.fastmcp import FastMCP

HOST = "127.0.0.1"
PORT = int(os.environ.get("TT_DEVICE_PORT", "5741"))
BASE = f"http://{HOST}:{PORT}"
DEFAULT_TIMEOUT = 120
AGENT = os.environ.get("TT_AGENT", "mcp-server")

# Poll interval when waiting for job completion — tight because it's localhost
POLL_INTERVAL = 0.5

server = FastMCP(
    "tt-device-queue",
    instructions=(
        "Tools for running commands on the Tenstorrent device. "
        "The device can only run one command at a time — commands are queued FIFO. "
        "Use device_submit() to enqueue, then do other work, then call device_result() "
        "when you need the output. Or use device_run() to submit and wait in one shot."
    ),
)


class DeviceQueueError(Exception):
    pass


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
    """Poll until the job is done. Returns the full result with output contents."""
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
        await asyncio.sleep(POLL_INTERVAL)


@server.tool()
async def device_submit(
    cmd: str,
    cwd: str = "",
    timeout: int = DEFAULT_TIMEOUT,
    agent: str = AGENT,
) -> str:
    """Submit a command to the Tenstorrent device queue. Returns immediately with
    a job_id. The command will run when its turn comes in the FIFO queue.

    Call device_result(job_id) later to get the output. This lets you do other
    work (read files, write code, plan) while the device job runs.

    Args:
        cmd: Shell command to run (e.g. "PYTHONPATH=. uv run examples/matmul.py")
        cwd: Working directory for the command
        timeout: Max execution time in seconds (default 120)
        agent: Tag identifying this agent (default "mcp-server")
    """
    async with httpx.AsyncClient() as client:
        result = await _post(client, "/queue", {
            "cmd": cmd, "cwd": cwd, "timeout": timeout, "agent": agent,
        })

    return json.dumps({
        "job_id": result["job_id"],
        "output_file": result["output_file"],
        "position": result["position"],
        "estimated_wait_sec": result["estimated_wait_sec"],
        "hint": "Call device_result(job_id) when you need the output. You can do other work in the meantime.",
    }, indent=2)


@server.tool()
async def device_result(job_id: str) -> str:
    """Wait for a device job to finish and return its full output.

    Blocks until the job completes — only call this when you actually need the
    result. If you have other work to do, do it before calling this.

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
    agent: str = AGENT,
) -> str:
    """Submit a command and wait for it to complete. Returns the full output.

    This is a convenience tool that combines device_submit + device_result.
    Use this when you have nothing else to do while waiting. If you want to
    do other work while the command runs, use device_submit() instead.

    Args:
        cmd: Shell command to run
        cwd: Working directory for the command
        timeout: Max execution time in seconds (default 120)
        agent: Tag identifying this agent (default "mcp-server")
    """
    async with httpx.AsyncClient() as client:
        submit_result = await _post(client, "/queue", {
            "cmd": cmd, "cwd": cwd, "timeout": timeout, "agent": agent,
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
    """Show what's currently running, queued, and recently completed on the device."""
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
async def device_reset(agent: str = AGENT) -> str:
    """Queue a device reset (tt-smi -r). Returns immediately — the reset runs
    when its turn comes in the queue.

    Args:
        agent: Tag identifying this agent
    """
    async with httpx.AsyncClient() as client:
        result = await _post(client, "/queue", {
            "cmd": "tt-smi -r",
            "cwd": "",
            "timeout": 30,
            "agent": f"{agent}/reset",
        })

    return json.dumps({
        "job_id": result["job_id"],
        "position": result["position"],
        "estimated_wait_sec": result["estimated_wait_sec"],
        "hint": "Reset has been queued. Call device_result(job_id) to confirm it completed.",
    }, indent=2)


if __name__ == "__main__":
    server.run(transport="stdio")
