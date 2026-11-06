//! wa-call: WhatsApp voice and video calls from the command line.
//!
//! `wa-call daemon` holds a linked-device session and owns every call; the other subcommands
//! are thin clients of its control socket, so scripts can dial, answer and hang up while the
//! daemon keeps the connection (and the media) alive.

mod audio;
mod daemon;
mod engine;
mod video;

use std::path::PathBuf;
use std::process::ExitCode;

use anyhow::{Context, Result, bail};
use clap::{Args, Parser, Subcommand};
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;

use audio::AudioSpec;
use engine::{CallSpec, IncomingPolicy};

#[derive(Parser)]
#[command(name = "wa-call", version, about = "WhatsApp calls from the command line")]
struct Cli {
    /// Print raw JSON responses instead of a short summary.
    #[arg(long, global = true)]
    json: bool,
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Link this device to your WhatsApp account (QR, or a code with --phone).
    Link {
        /// Your number in international format; prints an 8-character code to type on the phone.
        #[arg(long)]
        phone: Option<String>,
    },
    /// Remove this device from your account (stop the daemon first).
    Unlink,
    /// Run the call daemon in the foreground (systemd runs this).
    Daemon {
        /// What to do with incoming calls until told otherwise.
        #[arg(long, value_enum, default_value = "ring")]
        incoming: IncomingPolicy,
        #[command(flatten)]
        media: Media,
    },
    /// Daemon state: link status, live and ringing calls.
    Status,
    /// Place a call.
    Dial {
        /// Phone number (international format) or JID.
        to: String,
        #[command(flatten)]
        media: Media,
        /// Block until the call ends, printing its events.
        #[arg(long)]
        wait: bool,
    },
    /// Answer a ringing call (the only one, or by id).
    Answer {
        call_id: Option<String>,
        #[command(flatten)]
        media: Media,
        #[arg(long)]
        wait: bool,
    },
    /// Decline a ringing call.
    Reject { call_id: Option<String> },
    /// End a live call.
    Hangup { call_id: Option<String> },
    /// Toggle video on a live call (start, accept the peer's request, or stop).
    Video {
        call_id: Option<String>,
        #[command(flatten)]
        media: Media,
    },
    /// Stream daemon events as NDJSON (ringing, connected, ended, ...).
    Events,
    /// Set the daemon's incoming-call policy and the media it answers with.
    Policy {
        #[arg(value_enum)]
        incoming: IncomingPolicy,
        #[command(flatten)]
        media: Media,
    },
}

#[derive(Args, Clone, Default)]
struct Media {
    /// Send this audio file/URL (any ffmpeg input) instead of the microphone.
    #[arg(long)]
    play: Option<String>,
    /// Loop --play.
    #[arg(long = "loop")]
    loop_play: bool,
    /// Hang up when --play finishes.
    #[arg(long)]
    hangup_after_play: bool,
    /// Record the other side to this WAV file.
    #[arg(long)]
    record: Option<PathBuf>,
    /// Use PipeWire for live audio (capture unless --play, and playback).
    #[arg(long)]
    live: bool,
    /// PipeWire node to capture from.
    #[arg(long)]
    mic: Option<String>,
    /// PipeWire node to play to.
    #[arg(long)]
    speaker: Option<String>,
    /// Exchange raw s16le 16 kHz mono audio with a program listening on this Unix socket
    /// (instead of --play/--live): the peer is written to it, what it writes is sent.
    #[arg(long, value_name = "PATH")]
    socket: Option<PathBuf>,
    /// Video call / answer with video.
    #[arg(long)]
    video: bool,
    /// Video input: testsrc, a file/URL, or /dev/videoN.
    #[arg(long)]
    video_input: Option<String>,
    /// Peer video: file (default), window or none.
    #[arg(long)]
    video_sink: Option<String>,
}

impl Media {
    fn spec(&self) -> CallSpec {
        let abs = |p: &PathBuf| std::path::absolute(p).unwrap_or_else(|_| p.clone());
        // The daemon resolves paths from its own cwd, so make them absolute here.
        let play = self.play.as_ref().map(|p| {
            if p.contains("://") {
                p.clone()
            } else {
                abs(&PathBuf::from(p)).to_string_lossy().into_owned()
            }
        });
        CallSpec {
            audio: AudioSpec {
                play,
                loop_play: self.loop_play,
                hangup_after_play: self.hangup_after_play,
                record: self.record.as_ref().map(abs),
                live: self.live,
                capture_target: self.mic.clone(),
                playback_target: self.speaker.clone(),
                socket: self.socket.as_ref().map(abs),
            },
            video: self.video,
            video_input: self.video_input.clone(),
            video_sink: self.video_sink.clone(),
        }
    }
}

#[tokio::main]
async fn main() -> ExitCode {
    env_logger::Builder::from_env(
        env_logger::Env::default().default_filter_or("warn,wa_call=info"),
    )
    .init();
    let cli = Cli::parse();
    match run(cli).await {
        Ok(code) => code,
        Err(e) => {
            eprintln!("wa-call: {e:#}");
            ExitCode::from(1)
        }
    }
}

async fn run(cli: Cli) -> Result<ExitCode> {
    let json_out = cli.json;
    let req = match cli.cmd {
        Cmd::Link { phone } => {
            daemon::link(phone).await?;
            return Ok(ExitCode::SUCCESS);
        }
        Cmd::Unlink => {
            daemon::unlink().await?;
            return Ok(ExitCode::SUCCESS);
        }
        Cmd::Daemon { incoming, media } => {
            daemon::run(incoming, media.spec()).await?;
            return Ok(ExitCode::SUCCESS);
        }
        Cmd::Events => return stream_events(None).await,
        Cmd::Status => json!({"op": "status"}),
        Cmd::Dial { to, media, wait } => {
            let r = request(json!({"op": "dial", "to": to, "spec": media.spec()})).await?;
            return finish(r, wait, json_out).await;
        }
        Cmd::Answer {
            call_id,
            media,
            wait,
        } => {
            let r = request(json!({"op": "answer", "call_id": call_id, "spec": media.spec()})).await?;
            return finish(r, wait, json_out).await;
        }
        Cmd::Reject { call_id } => json!({"op": "reject", "call_id": call_id}),
        Cmd::Hangup { call_id } => json!({"op": "hangup", "call_id": call_id}),
        Cmd::Video { call_id, media } => {
            json!({"op": "video", "call_id": call_id, "spec": media.spec()})
        }
        Cmd::Policy { incoming, media } => {
            json!({"op": "policy", "policy": incoming, "spec": media.spec()})
        }
    };
    let r = request(req).await?;
    print_response(&r, json_out);
    Ok(ExitCode::SUCCESS)
}

async fn finish(r: Value, wait: bool, json_out: bool) -> Result<ExitCode> {
    print_response(&r, json_out);
    if wait {
        let cid = r["call_id"].as_str().context("no call id")?.to_string();
        return stream_events(Some(cid)).await;
    }
    Ok(ExitCode::SUCCESS)
}

fn print_response(r: &Value, json_out: bool) {
    if json_out {
        println!("{r}");
    } else if let Some(cid) = r["call_id"].as_str() {
        println!("{cid}");
    } else {
        let mut r = r.clone();
        if let Some(o) = r.as_object_mut() {
            o.remove("ok");
        }
        println!("{}", serde_json::to_string_pretty(&r).unwrap_or_default());
    }
}

async fn connect() -> Result<UnixStream> {
    let sock = daemon::socket_path();
    UnixStream::connect(&sock).await.with_context(|| {
        format!(
            "no daemon at {} (start it: systemctl --user start wa-call, or wa-call daemon)",
            sock.display()
        )
    })
}

async fn request(req: Value) -> Result<Value> {
    let stream = connect().await?;
    let (r, mut w) = stream.into_split();
    w.write_all(format!("{req}\n").as_bytes()).await?;
    let mut lines = BufReader::new(r).lines();
    let line = lines.next_line().await?.context("daemon closed the connection")?;
    let v: Value = serde_json::from_str(&line)?;
    if v["ok"] != json!(true) {
        bail!("{}", v["error"].as_str().unwrap_or("request failed"));
    }
    Ok(v)
}

/// Print events (optionally only one call's) until that call ends.
async fn stream_events(only: Option<String>) -> Result<ExitCode> {
    let stream = connect().await?;
    let (r, mut w) = stream.into_split();
    w.write_all(b"{\"op\":\"events\"}\n").await?;
    let mut lines = BufReader::new(r).lines();
    lines.next_line().await?;
    while let Some(line) = lines.next_line().await? {
        let v: Value = serde_json::from_str(&line).unwrap_or(json!({"raw": line}));
        if let Some(cid) = &only {
            if v["call_id"].as_str() != Some(cid.as_str()) {
                continue;
            }
            println!("{v}");
            if matches!(v["event"].as_str(), Some("ended" | "media-ended" | "failed")) {
                return Ok(ExitCode::SUCCESS);
            }
        } else {
            println!("{v}");
        }
    }
    Ok(ExitCode::SUCCESS)
}
