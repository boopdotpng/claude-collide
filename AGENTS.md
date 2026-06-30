# Agent Notes

- Keep changes small and avoid rewriting unrelated dirty files.
- Run focused tests first: `python tests/test_queue_server.py`.
- Run the full suite with the repo venv: `PYTHONPATH=. .venv/bin/python3 -m unittest discover -s tests -p 'test_*.py'`.
- `python -m unittest tests.test_*` does not work because `tests/` is not a package.
- Queue refusal is controlled by `TT_DEVICE_LSMOD_CMD`; tests mock it with a fake `lsmod` command.
- If `tt-device-queue.service` or `server.py` changes and the user service is installed, refresh and restart it:
  `cp tt-device-queue.service ~/.config/systemd/user/tt-device-queue.service && systemctl --user daemon-reload && systemctl --user restart tt-device-queue`.
