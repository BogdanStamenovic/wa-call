#!/usr/bin/env bash
# Fresh clone -> working daemon. Safe to rerun (it is also the update path).
set -euo pipefail
here="$(cd "$(dirname "$0")/.." && pwd)"
cd "$here"

for dep in ffmpeg pw-cat; do
  command -v "$dep" >/dev/null || echo "warning: $dep not found; file playback (ffmpeg) or live audio (pw-cat) will fail" >&2
done

# rust-toolchain.toml pins the nightly whatsapp-rust is tested on; rustup fetches it per-user.
if ! command -v cargo >/dev/null && [ ! -x "$HOME/.cargo/bin/cargo" ]; then
  echo "installing rustup (user-level, ~/.cargo, no PATH edits)"
  curl -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal --default-toolchain none --no-modify-path
fi
export PATH="$HOME/.cargo/bin:$PATH"
cargo build --release --locked 2>&1 | tail -3

unit_dir="${XDG_CONFIG_HOME:-$HOME/.config}/systemd/user"
mkdir -p "$unit_dir"
sed "s#@BIN@#$here/target/release/wa-call#" systemd/wa-call.service.in > "$unit_dir/wa-call.service.new"
mv -f "$unit_dir/wa-call.service.new" "$unit_dir/wa-call.service"
systemctl --user daemon-reload
systemctl --user enable wa-call.service >/dev/null 2>&1 || true
# Restart only if it was running: a not-yet-linked install has nothing to run.
if systemctl --user is-active --quiet wa-call.service; then
  systemctl --user restart wa-call.service
  echo "wa-call daemon restarted"
else
  state="${XDG_STATE_HOME:-$HOME/.local/state}/wa-call"
  if [ -e "$state/linked" ]; then
    systemctl --user start wa-call.service && echo "wa-call daemon started"
  else
    echo "not linked yet: run 'wa-call link --phone +NUMBER' (or 'wa link'), then 'systemctl --user start wa-call'"
  fi
fi
"$here/target/release/wa-call" --version
