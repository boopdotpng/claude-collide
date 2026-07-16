#!/usr/bin/env python3
"""Command-line client for the tt-device-queue service."""

from __future__ import annotations

import argparse
import json
import os
import shlex
import sys
import time
import uuid
from pathlib import Path
from typing import Any, Sequence

from queue_client import QueueClientError, get, post, read_all_logs


DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 5741
DEFAULT_RESULT_BYTES = 1 << 20
POLL_INTERVAL = 0.5
AGENT_PROCESS_NAMES = {"pi", "codex", "claude", "claude-code"}
REPO_ROOT = Path(__file__).resolve().parent


def _agent_process_id() -> str | None:
    """Find the nearest agent harness on Linux so calls share a fair client queue."""
    pid = os.getppid()
    seen: set[int] = set()
    while pid > 1 and pid not in seen:
        seen.add(pid)
        proc = Path("/proc") / str(pid)
        try:
            name = (proc / "comm").read_text().strip()
            stat = (proc / "stat").read_text()
            parent = int(stat.rsplit(")", 1)[1].split()[1])
        except (OSError, ValueError, IndexError):
            return None
        if name in AGENT_PROCESS_NAMES:
            return f"agent-{name}-{pid}"
        pid = parent
    return None


def default_client_id() -> str:
    configured = os.environ.get("TT_QUEUE_CLIENT_ID", "").strip()
    if configured:
        return configured
    detected = _agent_process_id()
    if detected:
        return detected
    try:
        return f"cli-{os.getuid()}"
    except AttributeError:
        return "cli"


def _base_url(args: argparse.Namespace) -> str:
    configured = os.environ.get("TT_DEVICE_URL", "").rstrip("/")
    if configured:
        return configured
    return f"http://{args.host}:{args.port}"


def _json(data: Any) -> None:
    print(json.dumps(data, indent=2, sort_keys=True))


def _command(parts: Sequence[str]) -> str:
    values = list(parts)
    if values and values[0] == "--":
        values.pop(0)
    if not values:
        raise ValueError("a command is required")
    # A single quoted argument is an intentional shell program. Multiple
    # arguments are treated as argv and safely reconstructed for bash.
    return values[0] if len(values) == 1 else shlex.join(values)


def _environment(values: Sequence[str]) -> dict[str, str]:
    result: dict[str, str] = {}
    for value in values:
        name, separator, contents = value.partition("=")
        if not separator or not name:
            raise ValueError(f"invalid environment entry {value!r}; expected NAME=VALUE")
        result[name] = contents
    return result


def _submission(args: argparse.Namespace, command: str) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "cmd": command,
        "cwd": str(Path(args.cwd).expanduser().resolve()),
        "repeat": args.repeat,
        "mode": "run",
        "env": _environment(args.env),
        "client_id": args.client_id,
    }
    if args.timeout is not None:
        payload["timeout"] = args.timeout
    return payload


def _print_submission(result: dict[str, Any], as_json: bool) -> None:
    if as_json:
        _json(result)
        return
    for key in (
        "job_id", "script_file", "output_file", "position", "estimated_wait_sec",
        "estimated_run_sec", "repeat", "timeout",
    ):
        if key in result:
            print(f"{key}={result[key]}")


def _exit_code(value: Any) -> int:
    if not isinstance(value, int):
        return 1
    if value < 0:
        return min(255, 128 + abs(value))
    return min(255, value)


def _wait(
    base: str, job_id: str, *, wait_timeout: float | None,
    output_limit: int,
) -> tuple[dict[str, Any], str, bool]:
    deadline = None if wait_timeout is None else time.monotonic() + wait_timeout
    interval = 0.05
    while True:
        result = get(base, f"/result/{job_id}")
        if result.get("status") == "done":
            output, truncated = read_all_logs(base, job_id, maximum=output_limit)
            return result, output, truncated
        if deadline is not None and time.monotonic() >= deadline:
            raise TimeoutError(
                f"timed out waiting for job {job_id}; the queued job is still active"
            )
        time.sleep(interval)
        interval = min(interval * 2, POLL_INTERVAL)


def _print_result(
    metadata: dict[str, Any], output: str, truncated: bool, *, as_json: bool,
    output_limit: int,
) -> None:
    if as_json:
        data = dict(metadata)
        data["output"] = output
        data["output_truncated"] = truncated
        _json(data)
        return
    if output:
        sys.stdout.write(output)
        if not output.endswith("\n"):
            sys.stdout.write("\n")
    status = "timeout" if metadata.get("timed_out") else (
        "ok" if metadata.get("exit_code") == 0 else "failed"
    )
    print(
        f"[tt-device-queue] job {status}; exit_code={metadata.get('exit_code')} "
        f"elapsed={metadata.get('elapsed')}s output_file={metadata.get('output_file', '')}",
        file=sys.stderr,
    )
    if truncated:
        print(
            f"[tt-device-queue] output truncated at {output_limit} bytes; "
            "use the logs command to read bounded chunks",
            file=sys.stderr,
        )


def _result(args: argparse.Namespace, base: str, job_id: str) -> int:
    metadata, output, truncated = _wait(
        base, job_id, wait_timeout=args.wait_timeout, output_limit=args.output_limit,
    )
    _print_result(
        metadata, output, truncated, as_json=args.json, output_limit=args.output_limit,
    )
    return _exit_code(metadata.get("exit_code"))


def _script_directory() -> Path:
    configured = os.environ.get("TT_DEVICE_CLI_SCRIPT_DIR")
    return Path(configured).expanduser() if configured else REPO_ROOT / "logs-v2" / "cli-scripts"


def _write_python_script(source: str) -> Path:
    directory = _script_directory()
    directory.mkdir(mode=0o700, parents=True, exist_ok=True)
    cutoff = time.time() - 7 * 86400
    try:
        for old in directory.glob("*.py"):
            if old.stat().st_mtime < cutoff:
                old.unlink(missing_ok=True)
    except OSError:
        pass
    path = directory / f"{uuid.uuid4().hex}.py"
    path.write_text(source if source.endswith("\n") else source + "\n")
    path.chmod(0o600)
    return path


def _python_command(args: argparse.Namespace) -> tuple[str, Path | None]:
    generated: Path | None = None
    if args.script == "-":
        generated = _write_python_script(sys.stdin.read())
        script = generated
    else:
        script = Path(args.script).expanduser().resolve()
        if not script.is_file():
            raise ValueError(f"Python script does not exist: {script}")
    return shlex.join([args.python, str(script), *args.python_arg]), generated


def _format_status(data: dict[str, Any]) -> str:
    lines: list[str] = []
    worker = data.get("worker") or {}
    if not worker.get("alive", True) or worker.get("degraded_reason"):
        lines.append(
            f"!!! QUEUE DEGRADED — {worker.get('degraded_reason') or 'worker is not alive'}"
        )
    device = data.get("device") or {}
    if device.get("state") == "dead":
        lines.append(
            f"!!! DEVICE DEAD since {device.get('dead_since')} — {device.get('dead_reason')}"
        )
    elif device.get("state") == "resetting" or device.get("reset_pending"):
        lines.append("!!! DEVICE RESET in progress — jobs are held")
    current = data.get("current")
    if current:
        client = f" ({current['client']})" if current.get("client") else ""
        lines.append(f"RUNNING: [{current['id']}]{client} {current['cmd']}")
        lines.append(
            f"         {current['running_sec']}s  "
            f"eta ~{current.get('estimated_remaining_sec', '?')}s"
        )
    else:
        lines.append("RUNNING: (idle)")
    pending = data.get("pending", [])
    if pending:
        lines.append(f"\nQUEUED ({len(pending)}):")
        for item in pending:
            client = f" ({item['client']})" if item.get("client") else ""
            lines.append(f"  [{item['id']}]{client} {item['cmd']}")
            lines.append(
                f"           waiting {item['waiting_sec']}s  "
                f"eta ~{item.get('estimated_wait_sec', '?')}s"
            )
    else:
        lines.append("\nQUEUED: (empty)")
    recent = data.get("recent", [])
    if recent:
        lines.append("\nRECENT:")
        for item in recent:
            tag = "TIMEOUT" if item.get("timed_out") else (
                "OK" if item.get("exit_code") == 0 else f"FAIL({item.get('exit_code')})"
            )
            lines.append(
                f"  [{item['id']}] {tag} {item.get('elapsed', '?')}s  {item['cmd']}"
            )
    return "\n".join(lines)


def _add_submission_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--cwd", default=os.getcwd(), help="job working directory")
    parser.add_argument("--timeout", type=int, help="job timeout in seconds")
    parser.add_argument("--repeat", type=int, default=1)
    parser.add_argument(
        "--env", action="append", default=[], metavar="NAME=VALUE",
        help="add or override a job environment variable (repeatable)",
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="tt-device-queue",
        description="Serialize commands that access the local Tenstorrent device.",
    )
    parser.add_argument("--host", default=os.environ.get("TT_DEVICE_HOST", DEFAULT_HOST))
    parser.add_argument("--port", type=int, default=int(os.environ.get("TT_DEVICE_PORT", DEFAULT_PORT)))
    parser.add_argument("--client-id", default=default_client_id())
    parser.add_argument("--json", action="store_true", help="emit machine-readable JSON")
    subparsers = parser.add_subparsers(dest="action", required=True)

    queue = subparsers.add_parser("queue", help="submit a shell command and return immediately")
    _add_submission_arguments(queue)
    queue.add_argument("command", nargs=argparse.REMAINDER)

    run = subparsers.add_parser("run", help="submit a shell command and wait for its output")
    _add_submission_arguments(run)
    run.add_argument("--wait-timeout", type=float, help="stop waiting without stopping the job")
    run.add_argument("--output-limit", type=int, default=int(os.environ.get(
        "TT_DEVICE_RESULT_BYTES", DEFAULT_RESULT_BYTES
    )))
    run.add_argument("command", nargs=argparse.REMAINDER)

    queue_python = subparsers.add_parser(
        "queue-python", help="submit a Python file, or '-' to read a generated script from stdin"
    )
    _add_submission_arguments(queue_python)
    queue_python.add_argument("--python", default="python3")
    queue_python.add_argument("--python-arg", action="append", default=[], metavar="ARG")
    queue_python.add_argument("--wait", action="store_true")
    queue_python.add_argument("--wait-timeout", type=float)
    queue_python.add_argument("--output-limit", type=int, default=int(os.environ.get(
        "TT_DEVICE_RESULT_BYTES", DEFAULT_RESULT_BYTES
    )))
    queue_python.add_argument("script", help="script path or '-' for stdin")

    job = subparsers.add_parser("job", help="show structured job metadata")
    job.add_argument("job_id")

    logs = subparsers.add_parser("logs", help="read one bounded output chunk")
    logs.add_argument("job_id")
    logs.add_argument("--offset", type=int, default=0)
    logs.add_argument("--limit", type=int, default=16384)

    result = subparsers.add_parser("result", help="wait for a job and print bounded output")
    result.add_argument("job_id")
    result.add_argument("--wait-timeout", type=float, help="stop waiting without stopping the job")
    result.add_argument("--output-limit", type=int, default=int(os.environ.get(
        "TT_DEVICE_RESULT_BYTES", DEFAULT_RESULT_BYTES
    )))

    subparsers.add_parser("status", help="show queue and device status")

    kill = subparsers.add_parser("kill", help="stop the running job")
    kill.add_argument("job_id", nargs="?", default="")

    reset = subparsers.add_parser("reset", help="schedule a coalesced device reset")
    reset.add_argument("job_id", nargs="?", default="")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    base = _base_url(args)
    try:
        if args.action in {"queue", "run"}:
            response = post(base, "/queue", _submission(args, _command(args.command)))
            if args.action == "queue":
                _print_submission(response, args.json)
                return 0
            if not args.json:
                print(
                    f"[tt-device-queue] submitted job {response['job_id']} "
                    f"(position {response.get('position', '?')})",
                    file=sys.stderr,
                )
            return _result(args, base, response["job_id"])

        if args.action == "queue-python":
            command, generated = _python_command(args)
            try:
                response = post(base, "/queue", _submission(args, command))
            except Exception:
                if generated is not None:
                    generated.unlink(missing_ok=True)
                raise
            response["script_file"] = str(generated or Path(args.script).expanduser().resolve())
            if args.wait:
                if not args.json:
                    print(
                        f"[tt-device-queue] submitted job {response['job_id']} "
                        f"(position {response.get('position', '?')})",
                        file=sys.stderr,
                    )
                return _result(args, base, response["job_id"])
            _print_submission(response, args.json)
            return 0

        if args.action == "job":
            _json(get(base, f"/job/{args.job_id}"))
            return 0

        if args.action == "logs":
            data = get(base, f"/logs/{args.job_id}?offset={args.offset}&limit={args.limit}")
            sys.stdout.write(data.get("content", ""))
            if not data.get("complete"):
                print(
                    f"[tt-device-queue] more logs available; "
                    f"next_offset={data.get('next_offset')}",
                    file=sys.stderr,
                )
            return 0

        if args.action == "result":
            return _result(args, base, args.job_id)

        if args.action == "status":
            data = get(base, "/status")
            if args.json:
                _json(data)
            else:
                print(_format_status(data))
            return 0

        if args.action == "kill":
            payload = {"job_id": args.job_id} if args.job_id else {}
            _json(post(base, "/kill", payload))
            return 0

        if args.action == "reset":
            payload = {"job_id": args.job_id} if args.job_id else {}
            _json(post(base, "/reset", payload))
            return 0
    except QueueClientError as exc:
        print(f"tt-device-queue: {exc}", file=sys.stderr)
        return 1
    except TimeoutError as exc:
        print(f"tt-device-queue: {exc}", file=sys.stderr)
        return 124
    except (OSError, ValueError) as exc:
        print(f"tt-device-queue: {exc}", file=sys.stderr)
        return 2
    except KeyboardInterrupt:
        print("tt-device-queue: interrupted; the queued job was not stopped", file=sys.stderr)
        return 130
    parser.error(f"unknown action: {args.action}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
