#!/usr/bin/env bash
set -euo pipefail

REPO_DIR="$(cd "$(dirname "$0")" && pwd)"
VENV="$REPO_DIR/.venv"

if command -v uv >/dev/null 2>&1; then
  [ -d "$VENV" ] || uv venv "$VENV"
  uv pip install --python "$VENV/bin/python3" -r "$REPO_DIR/requirements.txt"
else
  [ -d "$VENV" ] || python3 -m venv "$VENV"
  "$VENV/bin/pip" install -r "$REPO_DIR/requirements.txt"
fi

PYTHONPATH="$REPO_DIR" "$VENV/bin/python3" -m unittest discover \
  -s "$REPO_DIR/tests" -p 'test_*.py'

mkdir -p "$HOME/.config/systemd/user"
cp "$REPO_DIR/tt-device-queue.service" "$HOME/.config/systemd/user/tt-device-queue.service"
systemctl --user daemon-reload
systemctl --user enable tt-device-queue.service
systemctl --user restart tt-device-queue.service

echo "Environment tested and tt-device-queue.service installed."
