#!/usr/bin/env python3
"""
MCP server for tt-device-queue.
"""

import asyncio
import json
import os
import shlex
import uuid
from pathlib import Path

from mcp.server.fastmcp import FastMCP

from queue_client import post, get, run_tt_smi_snapshot, wait_for_job

HOST = "127.0.0.1"
PORT = int(os.environ.get("TT_DEVICE_PORT", "5741"))
BASE = f"http://{HOST}:{PORT}"
DEFAULT_TIMEOUT = 120
DEFAULT_OPEN_TIMEOUT = 180
SCRIPT_DIR = Path(
    os.environ.get(
        "TT_DEVICE_SCRIPT_DIR",
        str(Path(__file__).resolve().parent / "logs" / "mcp-scripts"),
    )
)

# Poll interval when waiting for job completion — tight because it's localhost
POLL_INTERVAL = 0.5

server = FastMCP(
    "tt-device-queue",
    instructions=(
        "Use only for Tenstorrent device commands. Non-Tenstorrent work should use normal shell."
    ),
)


async def _wait_for_job(job_id: str) -> dict:
    return await asyncio.to_thread(wait_for_job, BASE, job_id, POLL_INTERVAL)


async def _post(path: str, data: dict) -> dict:
    return await asyncio.to_thread(post, BASE, path, data)


async def _get(path: str) -> dict:
    return await asyncio.to_thread(get, BASE, path)


def _write_python_script(script: str) -> Path:
    SCRIPT_DIR.mkdir(parents=True, exist_ok=True)
    script_path = SCRIPT_DIR / f"{uuid.uuid4().hex[:8]}.py"
    if not script.endswith("\n"):
        script += "\n"
    script_path.write_text(script)
    return script_path


@server.tool(name="queue")
async def queue(
    cmd: str,
    cwd: str = "",
    timeout: int = DEFAULT_TIMEOUT,
    repeat: int = 1,
) -> str:
    """Queue device command. Returns job_id. PYTHONPATH includes "."."""
    result = await _post("/queue", {
        "cmd": cmd, "cwd": cwd, "timeout": timeout, "repeat": repeat,
        "mode": "run",
    })

    return json.dumps({
        "job_id": result["job_id"],
        "output_file": result["output_file"],
        "position": result["position"],
        "estimated_wait_sec": result["estimated_wait_sec"],
        "estimated_run_sec": result.get("estimated_run_sec"),
        "repeat": repeat,
        "hint": "Use result(job_id) for output.",
    }, indent=2)


@server.tool(name="open_forever")
async def open_forever(
    cmd: str,
    cwd: str = "",
    timeout: int = DEFAULT_OPEN_TIMEOUT,
) -> str:
    """Queue long-running device command. Stop with kill(job_id)."""
    result = await _post("/queue", {
        "cmd": cmd, "cwd": cwd, "timeout": timeout, "repeat": 1,
        "mode": "open",
    })

    return json.dumps({
        "job_id": result["job_id"],
        "output_file": result["output_file"],
        "position": result["position"],
        "estimated_wait_sec": result["estimated_wait_sec"],
        "estimated_run_sec": result.get("estimated_run_sec"),
        "mode": result.get("mode", "open"),
        "timeout": result.get("timeout", timeout),
        "hint": "Use job/logs, then kill(job_id).",
    }, indent=2)


@server.tool(name="queue_python")
async def queue_python(
    script: str,
    cwd: str = "",
    timeout: int = DEFAULT_TIMEOUT,
    repeat: int = 1,
    python: str = "python3",
    args: list[str] | None = None,
) -> str:
    """Write Python script file, then queue it."""
    script_path = await asyncio.to_thread(_write_python_script, script)
    cmd = shlex.join([python, str(script_path), *(args or [])])
    result = await _post("/queue", {
        "cmd": cmd, "cwd": cwd, "timeout": timeout, "repeat": repeat,
        "mode": "run",
    })

    return json.dumps({
        "job_id": result["job_id"],
        "script_file": str(script_path),
        "output_file": result["output_file"],
        "position": result["position"],
        "estimated_wait_sec": result["estimated_wait_sec"],
        "estimated_run_sec": result.get("estimated_run_sec"),
        "repeat": repeat,
        "hint": "Use result(job_id) for output.",
    }, indent=2)


@server.tool(name="job")
async def job(job_id: str) -> str:
    """Get job status."""
    result = await _get(f"/job/{job_id}")

    return json.dumps(result, indent=2)


@server.tool(name="logs")
async def logs(job_id: str, offset: int = 0, limit: int = 16384) -> str:
    """Read job output chunk."""
    result = await _get(f"/logs/{job_id}?offset={offset}&limit={limit}")

    return json.dumps(result, indent=2)


@server.tool(name="tt_smi_status")
async def tt_smi_status() -> str:
    """Run tt-smi snapshot outside the queue."""
    return await asyncio.to_thread(run_tt_smi_snapshot)


@server.tool(name="result")
async def result(job_id: str) -> str:
    """Wait for job and return output."""
    result = await _wait_for_job(job_id)

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


@server.tool(name="status")
async def status() -> str:
    """Show queue status."""
    data = await _get("/status")

    lines = []

    current = data.get("current")
    if current:
        lines.append(f"RUNNING: [{current['id']}] {current['cmd']}")
        if current.get("mode") == "open":
            lines.append("         mode open")
        repeat = current.get("repeat", 1)
        if repeat > 1:
            progress = f"  repeat {current.get('repeat_current', 0)}/{repeat}"
        else:
            progress = ""
        eta = current.get("estimated_remaining_sec")
        eta_text = f"  eta ~{eta}s" if eta is not None else ""
        lines.append(f"         {current['running_sec']}s{progress}{eta_text}")
    else:
        lines.append("RUNNING: (idle)")

    pending = data.get("pending", [])
    if pending:
        lines.append(f"\nQUEUED ({len(pending)}):")
        for p in pending:
            lines.append(f"  [{p['id']}] {p['cmd']}")
            if p.get("mode") == "open":
                lines.append("           mode open")
            repeat = f"  repeat {p['repeat']}x" if p.get('repeat', 1) > 1 else ""
            eta = p.get("estimated_wait_sec")
            eta_text = f"  eta ~{eta}s" if eta is not None else ""
            lines.append(f"           waiting {p['waiting_sec']}s{repeat}{eta_text}")
    else:
        lines.append("\nQUEUED: (empty)")

    recent = data.get("recent", [])
    if recent:
        lines.append(f"\nRECENT:")
        for r in recent:
            tag = "OK" if r.get("exit_code", 1) == 0 else f"FAIL({r.get('exit_code')})"
            repeat = r.get("repeat", 1)
            suffix = f"  repeat {r.get('repeat_completed', 0)}/{repeat}" if repeat > 1 else ""
            lines.append(f"  [{r['id']}] {tag} {r.get('elapsed', '?')}s  {r['cmd']}{suffix}")

    return "\n".join(lines)


@server.tool(name="kill")
async def kill(job_id: str = "") -> str:
    """Stop running job."""
    payload = {"job_id": job_id} if job_id else {}
    result = await _post("/kill", payload)

    killed = result.get("killed")
    if killed:
        signal_name = killed.get("signal", "SIGINT")
        return f"Sent {signal_name} to job [{killed['id']}] {killed['cmd']}"
    return "Nothing running to kill."


TT_SMI = os.path.expanduser("~/tenstorrent/blackhole-py/tt-smi.py")


@server.tool(name="reset")
async def reset(device: int = 0) -> str:
    """Queue device reset."""
    cmd = f"{TT_SMI} -r {device}"
    result = await _post("/queue", {
        "cmd": cmd, "cwd": "", "timeout": 30,
    })

    return json.dumps({
        "status": "queued",
        "message": f"Reset for device {device} was queued.",
        "job_id": result["job_id"],
        "output_file": result["output_file"],
        "position": result["position"],
        "estimated_wait_sec": result["estimated_wait_sec"],
        "estimated_run_sec": result.get("estimated_run_sec"),
        "hint": "Use result(job_id) for output.",
    }, indent=2)


if __name__ == "__main__":
    server.run(transport="stdio")
