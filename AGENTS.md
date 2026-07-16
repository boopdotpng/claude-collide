# Agent Notes

- Route every command that may initialize or access a Tenstorrent device through `tt-device-queue`; use `tt-device-queue run --cwd "$PWD" -- 'COMMAND'` rather than running it directly.
- Run focused server tests with `PYTHONPATH=. .venv/bin/python3 tests/test_queue_server.py`.
- Run focused CLI tests with `PYTHONPATH=. .venv/bin/python3 tests/test_queue_cli.py`.
- Run all tests with `PYTHONPATH=. .venv/bin/python3 -m unittest discover -s tests -p 'test_*.py'`.
- Run the isolated stress test with `PYTHONPATH=. .venv/bin/python3 tests/stress_queue.py`.
- After changing `server.py`, `queue_core.py`, or the unit, reinstall and restart it with `./install.sh` only after confirming the queue is idle.
