# wa-call

WhatsApp voice and video calls from the command line: a daemon that links to your account as
a companion device, plus a CLI that dials, answers, rejects and hangs up through it.

It is built on [whatsapp-rust](https://github.com/oxidezap/whatsapp-rust), which as of
November 2026 is the only open-source client that both places and answers 1:1 calls
end to end. The call engine (`src/engine.rs`, `src/video.rs`) is derived from that project's
`examples/voip-cli` (MIT, `LICENSE.whatsapp-rust`). What is new here is the
audio plumbing and the daemon/control-socket split.

## How it works

```
wa-call dial … ──┐
wa-call answer … ├─ JSON lines over $XDG_RUNTIME_DIR/wa-call.sock ─▶ wa-call daemon ─▶ WhatsApp
wa-call events ──┘                                                   │
                                                  ffmpeg / pw-cat ◀──┘ 16 kHz mono, 60 ms frames
```

- **The daemon** (`wa-call daemon`, run by the `wa-call.service` user unit) keeps the
  session open and owns every call. Incoming offers are handled by a policy: `ring` (the default)
  emits a `ringing` event and waits for `answer`/`reject`, while `answer` and `reject` act on
  their own.
- **Audio** goes through child processes rather than a linked audio stack, so the daemon runs
  on a headless box:
  - `--play FILE` is decoded by ffmpeg and paced in-process at one frame every 60 ms. ffmpeg's own
    `-re` sends the first ~0.5 s in a burst, and the peer hears that as a stutter.
  - `--live` uses `pw-cat` for capture and playback.
  - `--record` writes a streaming WAV. Its header is patched on close, so a crash still leaves
    readable audio.
  - With neither `--play` nor `--live`, the call sends real-time silence.
- **Video** uses upstream's ffmpeg H.264 path. The input is `--video-input testsrc|FILE|/dev/videoN`.
  The peer's video goes to a file by default, since the daemon has no screen.
- **Codec:** MLOW, WhatsApp's own codec, is the default. `WA_AUDIO_CODEC=opus` in the daemon's
  environment switches to Opus.

## Install

```sh
ownbox install wa-call
```

Setup does the following:
- installs rustup per-user if cargo is missing (into `~/.cargo`, no PATH edits);
- builds against the pinned whatsapp-rust revision;
- writes and enables `~/.config/systemd/user/wa-call.service`;
- starts the service once a session is linked.

`ownbox update wa-call` rebuilds and restarts the service if it was running. The session in
`~/.local/state/wa-call/` is outside the checkout and is never touched. `ownbox uninstall`
stops the service and, when it can ask you, offers to unlink the device and delete the session.

Runtime needs: `ffmpeg` (files, video) and `pw-cat` (PipeWire, for `--live`).

## Usage

```sh
wa-call link --phone +381641234567   # prints an 8-char code: phone → Linked devices → Link with phone number
systemctl --user start wa-call

wa-call dial +381601112233 --play greeting.mp3 --record reply.wav --hangup-after-play --wait
wa-call dial +381601112233 --live                       # talk through PipeWire
wa-call dial +381601112233 --video --video-input testsrc

wa-call events                                          # NDJSON: ringing, dialing, connected, ended, …
wa-call answer --record call.wav                        # the only ringing call, or pass its id
wa-call reject
wa-call hangup
wa-call policy answer --play voicemail.mp3 --record msg.wav --hangup-after-play   # an answering machine
wa-call status
wa-call unlink                                          # stop the daemon first
```

Every event is also appended to `~/.local/state/wa-call/calls.jsonl`.

| Event | When |
|---|---|
| `online` / `unlinked` / `logged-out` | session state |
| `ringing` | incoming offer (`call_id`, `peer`, `name`, `video`) |
| `dialing` / `answered` | we placed or accepted a call |
| `connected` | relay allocated, media flowing |
| `ended` | `by: peer\|us`, `unanswered`, `rejected`, `note` |
| `media-ended` | the media plane for a call is torn down |
| `missed` | an offer delivered after the fact (on reconnect) |
| `failed` | media setup failed after accepting |

## Limitations

- **Unofficial client.** WhatsApp's terms forbid it, and accounts using reverse-engineered clients
  have been suspended, sometimes permanently, including low-volume ones (whatsmeow #810, #989).
  The whatsapp-rust README specifically warns that placing calls raises that risk.
- **The protocol is reverse-engineered and moves weekly.** The whatsapp-rust revision is pinned in
  `Cargo.toml`. Bumping it is a deliberate step that should be followed by a live test call.
- **A linked companion may break the phone's own calls on some accounts.** whatsmeow #1231 reports
  calls stuck at "Connecting…". It is not established whether this applies to whatsapp-rust
  companions. Unlinking fixes it immediately.
- **One device slot.** wa-call is a linked device of its own, separate from any messaging client
  you link (such as `wa`/wacli). WhatsApp allows 4.
- **1:1 calls only.** Group calls are experimental upstream and not exposed here.
- **Recording captures the other side only**, mono. What we send is not mixed in.
- **Video options are per call, but codec settings are not.** Size, fps and bitrate come from the
  daemon's `WA_VIDEO_*` environment.
- **Not yet verified against a live call from this tool.** The engine is upstream's, and that
  upstream has working two-way calls. The first live test should check that a call lasts longer
  than 30 s, because one other stack reports calls being cut at around 20 s.
