# tt-device-queue Spec

This document describes the behavior currently implemented in this repository.
It is intended to be the source of truth for feature scope and runtime semantics.

## Purpose

- Serialize access to a shared hardware resource through a local FIFO queue.
- Provide two user-facing surfaces:
  - an HTTP queue server in `server.py`
  - an MCP wrapper in `mcp_server.py`
- Provide one direct non-queued power telemetry path via `tt-smi.py --snapshot`.

## Process Model

- `server.py` runs a single worker thread and executes queued jobs one at a time.
- The queue server listens on `TT_DEVICE_HOST` / `TT_DEVICE_PORT` and defaults to `127.0.0.1:5741`.
- Jobs are persisted in memory only for the lifetime of the server process.
- The installed systemd user service runs `server.py`, not `mcp_server.py`.
- `mcp_server.py` runs over stdio and is started by the MCP client on demand.

## Job Model

Each queued job has:

- `job_id`: 8 hex chars, unique per server lifetime
- `cmd`: shell command string executed with `shell=True`
- `cwd`: working directory passed to `subprocess.Popen`
- `timeout`: total timeout budget for the whole job, not per repeat iteration
- `repeat`: number of sequential executions inside the same job, minimum `1`
- `env`: per-job environment variables merged into the subprocess environment
- `mode`: `run` for normal jobs, `open` for intentionally long-running jobs
- `status`: `queued`, `running`, or `done`
- `output_file`: `./logs/<job_id>/output` by default, overridable via `TT_DEVICE_LOG_DIR`
- timestamps: `submitted_at`, `started_at`, `finished_at`
- results: `exit_code`, `elapsed`
- repeat progress: `repeat_current`, `repeat_completed`, `first_iteration_elapsed`, `per_iter_estimate_sec`

## Repeat Semantics

- `repeat=1` behaves like a normal single command execution.
- `repeat>1` executes the same command sequentially inside one queued job.
- `mode=open` keeps the queue slot occupied until the process exits, times out, or is explicitly stopped.
- `mode=open` requires `repeat=1`.
- The queue creates exactly one `job_id` and one output file for the entire repeated run.
- Before each repeated iteration, the server appends a marker line:
  - `[tt-device-queue] Repeat N/M`
- If any iteration exits non-zero, the job stops immediately and later iterations are not run.
- If the job times out, the server sends `SIGKILL` to the process group immediately. Timed-out jobs end with exit code `-9`.
- Explicit stop requests send Ctrl+C first, then escalate to `SIGKILL` if needed.
- Timeout applies to the full repeated job, not to each iteration independently.

## Output and Metadata Files

- Each job writes stdout/stderr combined into `output_file`.
- The same output bytes are appended live into `./logs/jobs.sqlite3` by default.
- Repeated runs append all iterations to the same `output_file`.
- When a job finishes, `meta.json` is written next to the output file.
- `meta.json` contains final execution metadata including repeat progress and timing estimates.
- Completed job metadata and log bytes remain queryable from SQLite after a server restart.

## Estimation Semantics

- Initial per-iteration estimate is `10` seconds.
- Initial submit-time `estimated_run_sec` is `repeat * 10`.
- Initial queue wait is the sum of estimated remaining runtimes of jobs ahead, including the currently running job.
- After the first successful repeat iteration, `per_iter_estimate_sec` is updated to that iteration's runtime.
- Remaining runtime for a running repeat job is estimated as:
  - remaining time in the current iteration, plus
  - `per_iter_estimate_sec * remaining_iterations_after_current`
- Completed jobs report `estimated_remaining_sec = 0`.

## HTTP API

### POST `/queue`

Request JSON:

```json
{
  "cmd": "python3 script.py",
  "cwd": "/path/to/repo",
  "timeout": 120,
  "repeat": 1,
  "mode": "run",
  "env": {"TT_USB": "1"}
}
```

Behavior:

- `cmd` is required and must be non-empty after stripping.
- `repeat` must be `>= 1`.
- `mode` defaults to `run` and must be either `run` or `open`.
- `mode=open` defaults to `180s` timeout when one is not provided.
- `env` defaults to `{}` and must be an object whose names and values are strings.
- Returns HTTP 200 with:
  - `job_id`
  - `output_file`
  - `position` where `0` means starts immediately if idle/current slot is free
  - `estimated_wait_sec`
  - `estimated_run_sec`
  - `repeat`
  - `mode`
  - `timeout`

### GET `/result/<job_id>`

Returns one of:

- queued:
  - `status=queued`
  - `mode`
  - `position`
  - `estimated_wait_sec`
  - `estimated_remaining_sec`
  - repeat metadata
- running:
  - `status=running`
  - `mode`
  - `position=0`
  - `estimated_wait_sec=0`
  - `estimated_remaining_sec`
  - repeat metadata
- done:
  - `status=done`
  - `mode`
  - `exit_code`
  - `output_file`
  - `elapsed`
  - `estimated_remaining_sec=0`
  - repeat metadata

Unknown jobs return HTTP 404.

### GET `/job/<job_id>`

Returns structured metadata for the job, including:

- lifecycle status and timestamps
- command, cwd, timeout
- repeat progress and ETA fields
- queue position when queued
- `running_sec` when running

Unknown jobs return HTTP 404.

### GET `/logs/<job_id>?offset=<n>&limit=<n>`

Behavior:

- Reads from SQLite using byte offsets, with a file fallback for legacy rows.
- `offset < 0` is clamped to `0`.
- `limit` is clamped to `[1, 65536]`.
- Returns:
  - `job_id`
  - `status`
  - `output_file`
  - `offset`
  - `next_offset`
  - `content`
  - `truncated`
  - `complete`
- `complete` means job is done and the returned chunk reaches EOF.

Unknown jobs return HTTP 404.

### GET `/status`

Returns global queue state:

- `current`: currently running job summary or `null`
- `pending`: queued jobs in FIFO order with wait and run estimates
- `recent`: last 10 completed jobs from persistent SQLite history

### POST `/kill`

Behavior:

- Accepts optional JSON body `{"job_id": "..."}`.
- If the requested job is currently running, sends Ctrl+C to its process group.
- The worker waits up to a short grace window, then escalates to `SIGKILL` if needed.
- Returns `{"killed": {...}}` when something was killed.
- Returns HTTP 409 when the requested job is not the currently running job.
- Returns `{"error": "Nothing running"}` when idle.

## MCP Surface

Implemented tools in `mcp_server.py`:

- `queue(cmd, cwd, timeout, repeat)`
- `open_forever(cmd, cwd, timeout)`
- `queue_python(script, cwd, timeout, repeat, python, args)`
- `job(job_id)`
- `logs(job_id, offset, limit)`
- `tt_smi_status()`
- `result(job_id)`
- `status()`
- `kill(job_id="")`
- `reset(device=0)`

MCP behavior notes:

- Queue-backed tools call the HTTP server through shared code in `queue_client.py`.
- The MCP server is only for commands that touch Tenstorrent hardware. CPU-only
  and general development work should use normal shell/tools instead.
- The HTTP server automatically adds `.` to `PYTHONPATH`; MCP callers do not
  need a separate env field for the common `PYTHONPATH=.` case.
- Leading shell assignments such as `MATMUL_PROFILE=1 python3 ...` work.
- `queue_python` stores large one-off Python snippets as files before queueing,
  keeping queue metadata readable.
- `result` waits until completion, then returns the full output text.
- `open_forever` is only for long-running Tenstorrent hardware work, not
  ordinary local dev servers or CPU-only log streams.
- `tt_smi_status` does not use the queue and can run concurrently with queued jobs.
- `reset` is queued work; it is not a direct bypass path.

## Shared Client Layer

`queue_client.py` provides shared helper behavior for MCP:

- HTTP GET/POST with uniform error translation
- blocking wait/poll loop for jobs
- output-file reading for completed results
- direct `~/tenstorrent/blackhole-py/tt-smi.py --snapshot` invocation

This file keeps HTTP client behavior and direct telemetry invocation separate from MCP transport code.

## TT-SMI Status

The `tt_smi_status` path invokes the same `tt-smi.py` file used for reset:

- command: `~/tenstorrent/blackhole-py/tt-smi.py --snapshot`
- source: Blackhole PCIe telemetry as rendered by `tt-smi`
- output includes the snapshot's thermals, power, clocks, status, and raw telemetry

If no Blackhole PCIe device is found or telemetry fails, the command exits non-zero.

## Installation and Service Behavior

- `install.sh` creates `.venv` and installs `mcp`.
- `install.sh` removes legacy `tt-device-queue` CLI symlinks from `~/.local/bin`.
- `install.sh` installs and enables the user systemd service from `tt-device-queue.service`.
- The systemd unit starts only the HTTP queue server.
- Updating `server.py` requires restarting the service.
- Updating `mcp_server.py` requires reconnecting/restarting the MCP client session.

## Test Coverage Currently Present

Implemented tests cover:

- repeated success writes one job and one output file
- repeated failure stops on first failing iteration
- job timeout kills the repeated run and stops later iterations
- per-job queued/running/done metadata
- log chunk reading with offsets and completion detection
- repeat-aware ETA initialization and refinement after first iteration
- `repeat` defaulting to `1`
- `tt_smi_status` snapshot invocation and error handling in `queue_client.py`

## Not Guaranteed by the Current Implementation

- Queued or running jobs do not resume after a server restart; they are marked done with exit code `-1`.
- No authentication or remote access controls beyond binding to localhost by default
- No queue prioritization; ordering is strict FIFO
- No cancellation of queued-but-not-yet-running jobs
- No streaming MCP transport for partial `result` output
- No server-side enforcement that a command actually targets device hardware
