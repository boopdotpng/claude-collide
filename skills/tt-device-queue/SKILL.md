---
name: tt-device-queue
description: Serializes commands that access a local Tenstorrent accelerator. Use for tt-smi, TT hardware tests, accelerator examples, firmware/device inspection, resets, and any command that could initialize or use a Tenstorrent device.
compatibility: Requires the tt-device-queue service and CLI on the same Linux host.
allowed-tools: bash
---

# Tenstorrent Device Queue

All commands that may access a Tenstorrent device **must** go through `tt-device-queue`. Never run those commands directly with the shell. CPU-only builds, formatting, file operations, and unit tests that do not initialize hardware may use normal shell commands.

The CLI automatically assigns a stable client identity for the current Pi/Codex/Claude process. `TT_QUEUE_CLIENT_ID` can override it.

## Run and wait

Pass the complete shell program as one quoted argument so pipes, redirects, environment assignments, and `&&` retain their meaning:

```bash
tt-device-queue run --cwd "$PWD" -- 'ARCH_NAME=blackhole python examples/device_test.py'
```

The command prints bounded job output and exits with the queued command's exit status. Interrupting the local wait does not stop the queued job.

Options must precede the quoted command:

```bash
tt-device-queue run --cwd "$PWD" --timeout 1800 --repeat 3 \
  --env ARCH_NAME=blackhole -- 'python examples/device_test.py --size 1024'
```

## Submit asynchronously

```bash
tt-device-queue --json queue --cwd "$PWD" -- 'long-running-device-command'
tt-device-queue job JOB_ID
tt-device-queue result JOB_ID
tt-device-queue logs JOB_ID --offset 0 --limit 16384
```

Use asynchronous submission when a wait should span multiple agent turns. Save the returned `job_id`. `result` waits for completion; `job` only checks current metadata.

## Python snippets

Queue an existing script:

```bash
tt-device-queue queue-python --cwd "$PWD" --wait path/to/script.py
```

Queue source from stdin:

```bash
tt-device-queue queue-python --cwd "$PWD" --wait - <<'PY'
import torch
print(torch.__version__)
PY
```

## Queue control

```bash
tt-device-queue status
tt-device-queue --json status
tt-device-queue kill JOB_ID
tt-device-queue reset JOB_ID
```

Do not kill another client's job merely to reduce wait time. Request a reset only for suspected device breakage or recovery; resets may interrupt the current job. Include the suspected job ID when known.

## Connection settings

The defaults are `127.0.0.1:5741`. Override with `TT_DEVICE_URL`, or with global `--host` and `--port` options placed before the subcommand.
