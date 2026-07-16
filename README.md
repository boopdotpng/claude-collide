# tt-device-queue

A durable, bounded queue that serializes access to one Tenstorrent device. A
small HTTP service owns scheduling and recovery; users and coding agents invoke
it through the `tt-device-queue` command-line client.

## CLI

`./install.sh` puts `tt-device-queue` on `~/.local/bin` and installs the bundled
Pi/Agent Skill globally. The standard shell configuration on this host already
includes that directory in `PATH`.

Run a device command and wait for its output:

```bash
tt-device-queue run --cwd "$PWD" -- \
  'ARCH_NAME=blackhole python examples/device_test.py'
```

Pass the complete shell program as one quoted argument when it contains shell
syntax such as environment assignments, pipes, redirects, or `&&`.

Submit without waiting and inspect the result later:

```bash
tt-device-queue --json queue --cwd "$PWD" -- 'python examples/device_test.py'
tt-device-queue job JOB_ID
tt-device-queue logs JOB_ID --offset 0 --limit 16384
tt-device-queue result JOB_ID
```

Other operations:

```bash
tt-device-queue status
tt-device-queue --json status
tt-device-queue kill [JOB_ID]
tt-device-queue reset [SUSPECT_JOB_ID]
tt-device-queue queue-python --cwd "$PWD" --wait script.py
```

Global connection options (`--host`, `--port`, `--client-id`, and `--json`) go
before the subcommand. `TT_DEVICE_URL`, `TT_DEVICE_PORT`,
`TT_QUEUE_CLIENT_ID`, and `TT_DEVICE_RESULT_BYTES` provide environment-based
overrides.

`run` and `result` return the queued command's exit status. Their
`--wait-timeout` only stops the local wait; it does not kill the queued job.
Use `kill` explicitly when cancellation is intended.

## Agent usage

The bundled `skills/tt-device-queue/SKILL.md` tells Pi and other Agent Skills
compatible harnesses to route every command that may initialize or access a
Tenstorrent accelerator through this CLI. CPU-only work can continue to use the
normal shell. The installer links the skill into
`~/.agents/skills/tt-device-queue` so it is available from sibling repositories.

The CLI uses `TT_QUEUE_CLIENT_ID` when set. Otherwise, on Linux it identifies
the nearest Pi, Codex, or Claude process so repeated calls from one agent retain
the queue's FIFO-per-client and round-robin fairness behavior.

## HTTP service

The service provides `POST /queue`, `/kill`, `/reset` and `GET /status`,
`/job/<id>`, `/logs/<id>`, `/result/<id>`. The CLI is the supported user-facing
interface; the HTTP API remains useful for tests and custom local integrations.

## What changed internally

- SQLite is the durable source for job/device metadata. Output files are the
  sole log source, avoiding duplicate BLOB storage and per-chunk transactions.
- Only queued and running jobs live in memory. Completed jobs are loaded on
  demand and never cached indefinitely.
- Queued jobs recover after a server crash. The interrupted running job is
  failed clearly; it is never silently replayed.
- Device state, reset epoch, pending reset, and boot ID are durable. A same-boot
  service restart cannot clear a dead device. A real host reboot can.
- Every job has a 128-bit UUID, a default runtime ceiling, an output cap, and
  validated request fields.
- Reset requests can interrupt a hung current job, then run before further
  dispatch. Reset success requires a separate health-check command to pass.
- The worker is supervised internally. Persistence failures fail closed, retain
  dirty transitions for retry, and appear in `/status` rather than leaving a
  silently dead worker behind a healthy HTTP process.
- Request bodies, concurrent handlers, queue depth, log reads, CLI result
  output, environment size, repeats, and timeouts are bounded.
- Completed metadata/logs and generated CLI scripts have retention policies.

## Setup for development

```bash
cd ~/tenstorrent/tt-device-queue
./install.sh
PYTHONPATH=. .venv/bin/python3 -m unittest discover -s tests -p 'test_*.py'
```

Run an isolated development server on a non-production port:

```bash
TT_DEVICE_PORT=5742 TT_DEVICE_LOG_DIR=/tmp/tt-device-queue-test \
  .venv/bin/python3 server.py
TT_DEVICE_PORT=5742 tt-device-queue status
```

## Important defaults

| Setting | Default | Environment variable |
|---|---:|---|
| Job timeout | 3600 seconds | `TT_DEVICE_DEFAULT_TIMEOUT` |
| Absolute timeout limit | 86400 seconds | `TT_DEVICE_MAX_TIMEOUT` |
| Queue depth | 1000 | `TT_DEVICE_MAX_QUEUED_JOBS` |
| Output per job | 16 MiB | `TT_DEVICE_MAX_LOG_BYTES` |
| CLI `result` output | 1 MiB | `TT_DEVICE_RESULT_BYTES` |
| Request body | 1 MiB | `TT_DEVICE_MAX_REQUEST_BYTES` |
| Concurrent HTTP handlers | 16 | `TT_DEVICE_HTTP_WORKERS` |
| Metadata retention | 30 days / 10,000 jobs | `TT_DEVICE_RETENTION_DAYS`, `TT_DEVICE_MAX_COMPLETED_JOBS` |

Raw HTTP `timeout: 0` and omitted timeout both select the configured default;
unbounded jobs are intentionally not supported.

## Reset configuration and privilege boundary

The defaults are:

```text
TT_DEVICE_RESET_CMD=~/tenstorrent/.venv/bin/tt-smi -r
TT_DEVICE_HEALTH_CHECK_CMD=~/tenstorrent/.venv/bin/tt-smi -s
TT_DEVICE_DEEP_RESET_CMD=
```

The normal recovery path is unprivileged: `tt-smi -r` is retried, then
`tt-smi -s` must independently confirm that the device is available. If that
does not recover the device, the queue enters durable dead state, aborts queued
work, and requires a host reboot or operator recovery. The optional deep-reset
command remains empty in this installation.

This queue is an operational serialization mechanism, not a sandbox for
hostile commands. Strong enforcement requires running jobs and the reset
controller under separate OS identities or isolation domains.

## Storage

The service stores durable state in `logs-v2/`. The pre-v2 SQLite schema is
incompatible; do not point the new server at an old log directory.
