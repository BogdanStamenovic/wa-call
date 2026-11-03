#!/usr/bin/env bash
# Undo setup. The session (linked-device keys) and call log are user data: kept unless asked.
set -euo pipefail
here="$(cd "$(dirname "$0")/.." && pwd)"
state="${XDG_STATE_HOME:-$HOME/.local/state}/wa-call"
unit="${XDG_CONFIG_HOME:-$HOME/.config}/systemd/user/wa-call.service"
systemctl --user disable --now wa-call.service >/dev/null 2>&1 || true
if [ -t 0 ] && [ -d "$state" ]; then
  read -r -p "Unlink wa-call from your WhatsApp account and delete $state? [y/N] " a
  if [ "$a" = y ] || [ "$a" = Y ]; then
    "$here/target/release/wa-call" unlink || echo "unlink failed; remove it on the phone (Linked devices)"
    rm -rf -- "$state"
  fi
else
  echo "kept $state (session + call log); no TTY to ask. 'wa-call unlink' before removing to free the device slot."
fi
rm -f -- "$unit"
systemctl --user daemon-reload || true
