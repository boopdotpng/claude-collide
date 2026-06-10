#!/usr/bin/env bash
# Installs the privileged deep-reset helper used by tt-device-queue.
#
# This is the ONLY root-level access the service gets: a single fixed-path
# script that does a PCI remove + rescan of the Tenstorrent device. The helper
# itself refuses sudo calls unless they come directly from the queue server's
# reset worker; agent shells should report breakage via reset(job_id) instead.
# The sudoers rule is scoped to exactly that path, NOPASSWD, nothing else.
#
# Run manually:  sudo ./install-deep-reset.sh
set -euo pipefail

REPO_DIR="$(cd "$(dirname "$0")" && pwd)"
HELPER_SRC="$REPO_DIR/tt-pci-deep-reset"
HELPER_DST="/usr/local/sbin/tt-pci-deep-reset"
SUDOERS_DST="/etc/sudoers.d/tt-device-queue"

if [ "$(id -u)" -ne 0 ]; then
  echo "Run as root:  sudo $0" >&2
  exit 1
fi

# The user the tt-device-queue service runs as (the invoking user via sudo).
SERVICE_USER="${SUDO_USER:-}"
if [ -z "$SERVICE_USER" ] || [ "$SERVICE_USER" = "root" ]; then
  echo "Could not determine the service user (run via sudo, not as root login)." >&2
  exit 1
fi

echo "[1/2] Installing $HELPER_DST (root-owned, 0755)"
install -o root -g root -m 0755 "$HELPER_SRC" "$HELPER_DST"

echo "[2/2] Installing sudoers rule for user '$SERVICE_USER'"
TMP="$(mktemp)"
trap 'rm -f "$TMP"' EXIT
printf '%s ALL=(root) NOPASSWD: %s\n' "$SERVICE_USER" "$HELPER_DST" > "$TMP"
# Validate before installing — a broken sudoers file can lock out sudo.
visudo -c -f "$TMP"
install -o root -g root -m 0440 "$TMP" "$SUDOERS_DST"

echo ""
echo "Done. Verify as $SERVICE_USER with:"
echo "  sudo -n $HELPER_DST --help 2>&1 | head -1       # should NOT ask for a password"
echo "  sudo -n $HELPER_DST 2>&1 | head -1              # should be refused outside the queue"
echo ""
echo "The queue server picks this up automatically (no config needed) the next"
echo "time the service restarts. It is only used as an escalation when the"
echo "tt-smi -r reset fails its probe, and never while a job is running."
