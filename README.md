# claude-collide

Device access serialization for Tenstorrent hardware. Ensures only one command touches the device at a time via a FIFO job queue, so multiple AI agents (or humans) don't collide.

## Components

- **server.py** — HTTP server (localhost:5741) that runs a FIFO job queue. Commands execute one at a time via a single worker thread. Output is saved to `/tmp/tt-device-logs/<job_id>/output`.
- **mcp_server.py** — MCP (Model Context Protocol) server that wraps the HTTP API as native tools for AI coding agents. Runs over stdio.
- **claude-collide** — CLI client for submitting jobs and checking results from the shell.

## Architecture

```
┌─────────────┐    stdio/MCP     ┌────────────────┐    HTTP     ┌────────────┐
│  AI Agent   │ ◄──────────────► │  mcp_server.py │ ──────────► │ server.py  │
│  (claude,   │                  │                │             │ :5741      │
│   codex,    │                  │  device_submit │             │            │
│   opencode) │                  │  device_result │             │  FIFO      │──► Tenstorrent
│             │                  │  device_run    │             │  worker    │    device
│             │                  │  device_status │             │            │
└─────────────┘                  │  device_reset  │             └────────────┘
                                 └────────────────┘
```

The MCP server enables an **async two-tool pattern**: the agent calls `device_submit` to enqueue a command (returns immediately), does other work (reads files, writes code, plans), then calls `device_result` when it actually needs the output. This avoids blocking the agent during device execution.

## MCP Tools

| Tool | Blocks? | Description |
|---|---|---|
| `device_submit(cmd, cwd, timeout)` | No | Enqueue a command, get back a `job_id` immediately |
| `device_result(job_id)` | Yes | Wait for a job to finish, return full output |
| `device_run(cmd, cwd, timeout)` | Yes | Submit + wait in one call (convenience) |
| `device_status()` | No | Show running, queued, and recent jobs |
| `device_reset()` | No | Queue a `tt-smi -r` device reset |

## Setup

```bash
# Install dependencies
uv venv .venv
uv pip install mcp

# Start the queue server (or use systemd)
python server.py &

# Install systemd service (optional)
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

## CLI Usage

```bash
# Submit and block until done
claude-collide exec PYTHONPATH=. uv run examples/matmul.py

# Submit and get job_id back immediately
claude-collide queue PYTHONPATH=. uv run examples/matmul.py

# Check result
claude-collide result <job_id>

# View queue
claude-collide status

# Reset device
claude-collide reset
```

## License

MIT — Copyright (c) 2026 Claude
