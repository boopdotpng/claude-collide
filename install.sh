#!/usr/bin/env bash
set -euo pipefail

REPO_DIR="$(cd "$(dirname "$0")" && pwd)"
VENV="$REPO_DIR/.venv"

echo "=== tt-device-queue installer ==="
echo "Repo: $REPO_DIR"
echo ""

# 1. Create venv and install dependencies
echo "[1/4] Setting up Python venv..."
if command -v uv &>/dev/null; then
  [ -d "$VENV" ] || uv venv "$VENV"
  uv pip install --python "$VENV/bin/python3" mcp
else
  echo "  (uv not found, falling back to python3 -m venv + pip)"
  [ -d "$VENV" ] || python3 -m venv "$VENV"
  "$VENV/bin/pip" install mcp
fi

# 2. Remove legacy CLI symlinks
echo "[2/4] Removing legacy CLI symlinks..."
if [ -L ~/.local/bin/tt-device-queue ]; then
  rm ~/.local/bin/tt-device-queue
  echo "  -> removed ~/.local/bin/tt-device-queue"
fi
if [ -L ~/.local/bin/claude-collide ]; then
  rm ~/.local/bin/claude-collide
  echo "  -> removed legacy ~/.local/bin/claude-collide"
fi

# 3. Install and start systemd service
echo "[3/4] Installing systemd service..."
mkdir -p ~/.config/systemd/user
cp "$REPO_DIR/tt-device-queue.service" ~/.config/systemd/user/
systemctl --user disable --now claude-collide.service || true
rm -f ~/.config/systemd/user/claude-collide.service
systemctl --user daemon-reload
systemctl --user enable --now tt-device-queue
echo "  -> tt-device-queue.service enabled and started"

# 4. Done — print MCP registration instructions
echo "[4/4] Done!"
echo ""
echo "=== Register the MCP server with your agent ==="
echo ""
echo "Claude Code:"
echo "  claude mcp add -s user tt-device-queue -- $VENV/bin/python3 $REPO_DIR/mcp_server.py"
echo ""
echo "Codex:"
echo "  codex mcp add tt-device-queue -- $VENV/bin/python3 $REPO_DIR/mcp_server.py"
echo ""
echo "OpenCode:"
echo "  opencode mcp add  (follow prompts, use stdio transport)"
echo "  Command: $VENV/bin/python3 $REPO_DIR/mcp_server.py"
echo ""
echo "Or drop a .mcp.json in any project root — see README.md for details."
