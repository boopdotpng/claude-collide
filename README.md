# tt-device-queue

FIFO job queue that serializes access to a shared resource (a GPU, a dev board, a serial port, etc.) so multiple AI agents — or humans — don't collide.

## Why

AI coding agents cannot use `flock` correctly. They forget the lock, release it early, hold it across unrelated work, or simply ignore it when told to use it. After enough wasted debugging sessions watching Claude trample its own device state, we gave up on teaching it and built a queue server instead. If the agent can only run commands by submitting them to a FIFO, it is physically impossible to collide.

## Components

- **server.py** — HTTP server (localhost:5741) that runs a FIFO job queue. Commands execute one at a time via a single worker thread. Output is saved to `./logs/<job_id>/output` and mirrored into `./logs/jobs.sqlite3`.
- **mcp_server.py** — MCP (Model Context Protocol) server that wraps the HTTP API as native tools for AI coding agents. Runs over stdio.

## Architecture

```
┌─────────────┐    stdio/MCP     ┌────────────────┐    HTTP     ┌────────────┐
│  AI Agent   │ ◄──────────────► │  mcp_server.py │ ──────────► │ server.py  │
│  (claude,   │                  │                │             │ :5741      │
│   codex,    │                  │  queue         │             │            │
│   opencode) │                  │  result        │             │  FIFO      │──► shared
│             │                  │  status        │             │  worker    │    resource
│             │                  │  tt_smi_status │             │            │
└─────────────┘                  │  reset         │             └────────────┘
                                 └────────────────┘
```

The MCP server enables an **async two-tool pattern**: the agent calls `queue` to enqueue a command (returns immediately), does other work (reads files, writes code, plans), then calls `result` when it actually needs the output. This avoids blocking the agent during device execution.

## MCP Tools

The MCP server is only for commands that touch Tenstorrent hardware. Agents
should use normal shell/tools for CPU-only or general development work such as
reading files, editing code, installing packages, building non-device projects,
starting ordinary local dev servers, or running tests that do not touch the
device.

| Tool | Blocks? | Description |
|---|---|---|
| `queue(cmd, cwd, timeout, repeat)` | No | Enqueue a command, get back a `job_id` immediately |
| `open_forever(cmd, cwd, timeout)` | No | Enqueue an intentionally long-running Tenstorrent hardware job that keeps the queue occupied until stopped |
| `queue_python(script, cwd, timeout, repeat, python, args)` | No | Write a Python snippet to a script file, then enqueue that script |
| `job(job_id)` | No | Fetch structured per-job status, timestamps, repeat progress, and queue position |
| `logs(job_id, offset, limit)` | No | Read current or persisted job output by byte offset without blocking |
| `tt_smi_status()` | No | Print a one-shot `tt-smi --snapshot` telemetry view without consuming a queue slot |
| `result(job_id)` | Yes | Wait for a job to finish, return full output |
| `status()` | No | Show running, queued, and recent jobs |
| `kill(job_id="")` | No | Stop the running job, sending Ctrl+C first and force-killing only if needed |
| `reset()` | No | Queue a device reset command |

`repeat` defaults to `1`. When set higher, the server runs the same command sequentially inside a single queued job, appends all iterations into the same output file, and still returns one `job_id` for the agent to track. It stops immediately on the first failing iteration and exposes repeat progress through `job` and `status`. Initial ETA scales with `repeat`, then refines after the first successful iteration by reusing that iteration's runtime as the per-repeat estimate.

The server automatically prepends `.` to `PYTHONPATH` for queued jobs, so agents do not need to add `PYTHONPATH=.`. Normal leading shell assignments such as `MATMUL_PROFILE=1 python3 examples/matmul_peak.py` work as expected.

Use `queue_python` instead of large `python -c` strings or heredocs. The MCP wrapper writes the snippet into `logs/mcp-scripts/` and queues a short command that runs the generated file.

`open_forever` is for Tenstorrent hardware commands that are intentionally meant to stay alive for a while, like device-facing profiler UIs or hardware log streams. It is not for ordinary local dev servers or CPU-only logs. These jobs still use the same FIFO queue and stdout file, but they keep the queue slot occupied until they exit or the agent calls `kill(job_id)`. Manual `kill` sends Ctrl+C first and only escalates to SIGKILL if the process ignores it; timeouts send SIGKILL immediately. The default timeout for `open_forever` jobs is 180 seconds.

Logs are persistent by default. The server stores compatibility output files in `./logs/<job_id>/output` and appends the same bytes to `./logs/jobs.sqlite3` as they are produced. Completed jobs remain available through `job`, `logs`, `result`, and `status` after the server restarts. The whole `./logs/` directory is ignored by git.

## Setup

```bash
git clone https://github.com/boopdotpng/tt-device-queue.git
cd tt-device-queue
./install.sh
```

The install script creates a venv, installs dependencies, starts a systemd user service, and removes any legacy CLI symlink from `~/.local/bin`. At the end it prints the commands to register the MCP server with your agent.

### Manual setup

```bash
# Install dependencies (or: python3 -m venv .venv && .venv/bin/pip install mcp)
uv venv .venv
uv pip install mcp

# Start the queue server
python server.py &

# Or install as a systemd service
cp tt-device-queue.service ~/.config/systemd/user/
systemctl --user enable --now tt-device-queue
```

## Registering the MCP server

The MCP server command is:
```
/path/to/tt-device-queue/.venv/bin/python3 /path/to/tt-device-queue/mcp_server.py
```

### Claude Code

```bash
claude mcp add -s user tt-device-queue -- /path/to/tt-device-queue/.venv/bin/python3 /path/to/tt-device-queue/mcp_server.py
```

### Codex

```bash
codex mcp add tt-device-queue -- /path/to/tt-device-queue/.venv/bin/python3 /path/to/tt-device-queue/mcp_server.py
```

### OpenCode

Run `opencode mcp add` and follow the interactive prompts. Use transport `stdio` and the command above.

### Project-scoped (any tool)

Drop a `.mcp.json` in your project root:
```json
{
  "mcpServers": {
    "tt-device-queue": {
      "command": "/path/to/tt-device-queue/.venv/bin/python3",
      "args": ["mcp_server.py"],
      "cwd": "/path/to/tt-device-queue",
      "timeout": 300
    }
  }
}
```

## License

MIT — Copyright (c) 2026 Claude
