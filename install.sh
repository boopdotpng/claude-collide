#!/usr/bin/env bash
set -euo pipefail

REPO_DIR="$(cd "$(dirname "$0")" && pwd)"
VENV="$REPO_DIR/.venv"

if command -v uv >/dev/null 2>&1; then
  [ -d "$VENV" ] || uv venv "$VENV"
else
  [ -d "$VENV" ] || python3 -m venv "$VENV"
fi

PYTHONPATH="$REPO_DIR" "$VENV/bin/python3" -m unittest discover \
  -s "$REPO_DIR/tests" -p 'test_*.py'

mkdir -p "$HOME/.local/bin" "$HOME/.agents/skills"
ln -sfnT "$REPO_DIR/tt-device-queue" "$HOME/.local/bin/tt-device-queue"
ln -sfnT "$REPO_DIR/skills/tt-device-queue" "$HOME/.agents/skills/tt-device-queue"

mkdir -p "$HOME/.config/systemd/user"
cp "$REPO_DIR/tt-device-queue.service" "$HOME/.config/systemd/user/tt-device-queue.service"
systemctl --user daemon-reload
systemctl --user enable tt-device-queue.service
systemctl --user restart tt-device-queue.service

echo "Environment tested; CLI, agent skill, and tt-device-queue.service installed."
