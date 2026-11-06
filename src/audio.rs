//! Audio endpoints for a call. The engine speaks 16 kHz mono i16 in 960-sample (60 ms) frames;
//! everything here adapts files and PipeWire to that, through child processes (ffmpeg, pw-cat)
//! rather than linking an audio stack, so the daemon runs on a headless box with no ALSA device.

use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use anyhow::{Context, Result};
use log::{debug, info, warn};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::process::{Child, Command};

pub const FRAME_SAMPLES: usize = 960;
pub const RATE: u32 = 16_000;
const FRAME_BYTES: usize = FRAME_SAMPLES * 2;
const FRAME_PERIOD: Duration = Duration::from_millis(60);

/// What a call sends and where what it receives goes. Default: send silence, discard audio.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct AudioSpec {
    /// Any file or URL ffmpeg can read; sent at real-time pace, then silence.
    pub play: Option<String>,
    /// Loop `play` instead of falling silent at its end.
    #[serde(default)]
    pub loop_play: bool,
    /// Hang up once `play` has finished (ignored with `loop_play`).
    #[serde(default)]
    pub hangup_after_play: bool,
    /// Write the peer's audio to this WAV file (16 kHz mono s16).
    pub record: Option<PathBuf>,
    /// Use PipeWire: capture from the default (or `capture_target`) source when nothing is
    /// played, and play the peer on the default (or `playback_target`) sink.
    #[serde(default)]
    pub live: bool,
    pub capture_target: Option<String>,
    pub playback_target: Option<String>,
    /// Exchange audio with another program over this Unix stream socket, which that program is
    /// listening on: the daemon connects when the call's media is set up, writes the peer to it
    /// and reads what to send from it, both as raw s16le, 16 kHz, mono. Takes precedence over
    /// `play`/`live`. The other side keeps the clock, so nothing read from it is paced here.
    #[serde(default)]
    pub socket: Option<PathBuf>,
}

/// Source frames for the engine. `done` fires when a non-looping `play` file has ended.
pub struct Source {
    pub frames: async_channel::Receiver<Vec<i16>>,
    pub done: tokio::sync::oneshot::Receiver<()>,
}

/// Both ends of a call's audio: what the engine sends, and where what it receives goes.
pub fn spawn_endpoints(spec: &AudioSpec) -> Result<(Source, async_channel::Sender<Vec<i16>>)> {
    if let Some(path) = &spec.socket {
        return spawn_socket(path, spec.record.clone());
    }
    Ok((spawn_source(spec)?, spawn_sink(spec)?))
}

/// The `socket` mode. One connection carries both directions, so the far program cannot mix up
/// which call a stream belongs to.
fn spawn_socket(
    path: &Path,
    record: Option<PathBuf>,
) -> Result<(Source, async_channel::Sender<Vec<i16>>)> {
    let std_stream = std::os::unix::net::UnixStream::connect(path)
        .with_context(|| format!("connecting the audio socket {}", path.display()))?;
    std_stream.set_nonblocking(true)?;
    let stream = tokio::net::UnixStream::from_std(std_stream)?;
    let (mut reader, mut writer) = stream.into_split();
    info!("audio over socket {}", path.display());

    // Small and lossy on purpose. Before the peer answers nothing drains the source, and a deep
    // queue would then hold stale audio that every later frame queues behind: a constant delay
    // for the rest of the call. Two frames bound that at 120 ms; extra frames are dropped.
    let (tx, rx) = async_channel::bounded::<Vec<i16>>(2);
    let (done_tx, done_rx) = tokio::sync::oneshot::channel();
    tokio::spawn(async move {
        let mut buf = vec![0u8; FRAME_BYTES];
        let mut dropped: u64 = 0;
        loop {
            match reader.read_exact(&mut buf).await {
                Ok(_) => {
                    let frame = buf
                        .chunks_exact(2)
                        .map(|b| i16::from_le_bytes([b[0], b[1]]))
                        .collect();
                    match tx.try_send(frame) {
                        Ok(()) => {}
                        Err(async_channel::TrySendError::Full(_)) => dropped += 1,
                        Err(async_channel::TrySendError::Closed(_)) => return,
                    }
                }
                Err(e) => {
                    debug!("audio socket closed its sending side: {e}");
                    break;
                }
            }
        }
        if dropped > 0 {
            debug!("audio socket: dropped {dropped} frame(s) nobody was ready for");
        }
        let _ = done_tx.send(());
        let mut tick = tokio::time::interval(FRAME_PERIOD);
        loop {
            tick.tick().await;
            if tx.send(vec![0; FRAME_SAMPLES]).await.is_err() {
                return;
            }
        }
    });

    let (sink_tx, sink_rx) = async_channel::bounded::<Vec<i16>>(16);
    tokio::spawn(async move {
        let mut wav = match &record {
            Some(path) => WavWriter::create(path).await.ok(),
            None => None,
        };
        let mut open = true;
        while let Ok(frame) = sink_rx.recv().await {
            let bytes: Vec<u8> = frame.iter().flat_map(|s| s.to_le_bytes()).collect();
            if let Some(w) = wav.as_mut()
                && w.write(&bytes).await.is_err()
            {
                wav = None;
            }
            if open && writer.write_all(&bytes).await.is_err() {
                warn!("audio socket closed its receiving side");
                open = false;
            }
        }
        if let Some(w) = wav {
            let _ = w.finish().await;
        }
        let _ = writer.shutdown().await;
    });
    Ok((
        Source {
            frames: rx,
            done: done_rx,
        },
        sink_tx,
    ))
}

pub fn spawn_source(spec: &AudioSpec) -> Result<Source> {
    let (tx, rx) = async_channel::bounded::<Vec<i16>>(4);
    let (done_tx, done_rx) = tokio::sync::oneshot::channel();
    let child = if let Some(input) = &spec.play {
        let mut cmd = Command::new("ffmpeg");
        // No `-re`: ffmpeg front-loads ~0.5 s before throttling, which the peer would hear as a
        // burst then a gap. Files are paced below instead; ffmpeg just decodes ahead.
        cmd.args(["-nostdin", "-loglevel", "error"]);
        if spec.loop_play {
            cmd.args(["-stream_loop", "-1"]);
        }
        cmd.args(["-i", input, "-f", "s16le", "-ar", "16000", "-ac", "1", "-"]);
        Some(spawn_reader(cmd).with_context(|| format!("ffmpeg for {input}"))?)
    } else if spec.live {
        let mut cmd = pw_cat("--record", spec.capture_target.as_deref());
        cmd.arg("-");
        Some(spawn_reader(cmd).context("pw-record (is PipeWire running?)")?)
    } else {
        None
    };
    // A capture device runs on its own clock; pacing it here too would make the two drift.
    let paced = spec.play.is_some();
    tokio::spawn(async move {
        if let Some(mut child) = child {
            let mut out = child.stdout.take().expect("piped stdout");
            let mut buf = vec![0u8; FRAME_BYTES];
            let mut tick = tokio::time::interval(FRAME_PERIOD);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                if paced {
                    tick.tick().await;
                }
                match out.read_exact(&mut buf).await {
                    Ok(_) => {
                        let frame = buf
                            .chunks_exact(2)
                            .map(|b| i16::from_le_bytes([b[0], b[1]]))
                            .collect();
                        if tx.send(frame).await.is_err() {
                            let _ = child.kill().await;
                            return;
                        }
                    }
                    Err(e) => {
                        debug!("audio source ended: {e}");
                        break;
                    }
                }
            }
            let _ = child.wait().await;
            let _ = done_tx.send(());
        }
        // The engine paces nothing itself: a source that stops producing reads as a dead mic, so
        // after the input ends keep the call fed with real-time silence until it is torn down.
        let mut tick = tokio::time::interval(FRAME_PERIOD);
        loop {
            tick.tick().await;
            if tx.send(vec![0; FRAME_SAMPLES]).await.is_err() {
                return;
            }
        }
    });
    Ok(Source {
        frames: rx,
        done: done_rx,
    })
}

/// Sink for the peer's audio: fans out to a WAV recording and/or PipeWire playback.
pub fn spawn_sink(spec: &AudioSpec) -> Result<async_channel::Sender<Vec<i16>>> {
    let (tx, rx) = async_channel::bounded::<Vec<i16>>(16);
    let mut player = if spec.live {
        let mut cmd = pw_cat("--playback", spec.playback_target.as_deref());
        cmd.arg("-").stdin(Stdio::piped()).stdout(Stdio::null());
        Some(cmd.spawn().context("pw-play (is PipeWire running?)")?)
    } else {
        None
    };
    let record = spec.record.clone();
    tokio::spawn(async move {
        let mut wav = match &record {
            Some(path) => match WavWriter::create(path).await {
                Ok(w) => Some(w),
                Err(e) => {
                    warn!("cannot record to {}: {e:#}", path.display());
                    None
                }
            },
            None => None,
        };
        let mut stdin = player.as_mut().and_then(|p| p.stdin.take());
        while let Ok(frame) = rx.recv().await {
            let bytes: Vec<u8> = frame.iter().flat_map(|s| s.to_le_bytes()).collect();
            if let Some(w) = wav.as_mut()
                && let Err(e) = w.write(&bytes).await
            {
                warn!("recording write failed, stopping recording: {e}");
                wav = None;
            }
            if let Some(s) = stdin.as_mut()
                && s.write_all(&bytes).await.is_err()
            {
                warn!("PipeWire playback closed");
                stdin = None;
            }
        }
        if let Some(w) = wav {
            match w.finish().await {
                Ok(path) => info!("recording saved: {}", path.display()),
                Err(e) => warn!("finalizing recording failed: {e}"),
            }
        }
        drop(stdin);
        if let Some(mut p) = player {
            let _ = p.kill().await;
        }
    });
    Ok(tx)
}

fn pw_cat(mode: &str, target: Option<&str>) -> Command {
    let mut cmd = Command::new("pw-cat");
    cmd.args([mode, "--rate", "16000", "--channels", "1", "--format", "s16", "--raw"]);
    if let Some(t) = target {
        cmd.args(["--target", t]);
    }
    cmd.kill_on_drop(true);
    cmd
}

fn spawn_reader(mut cmd: Command) -> Result<Child> {
    cmd.stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .kill_on_drop(true);
    Ok(cmd.spawn()?)
}

/// Streaming WAV writer: header first with zero sizes, patched on `finish`, so a crash still
/// leaves the samples on disk (ffmpeg/sox read a zero-size header fine).
struct WavWriter {
    file: tokio::fs::File,
    path: PathBuf,
    data_len: u32,
}

impl WavWriter {
    async fn create(path: &Path) -> Result<Self> {
        if let Some(dir) = path.parent()
            && !dir.as_os_str().is_empty()
        {
            tokio::fs::create_dir_all(dir).await?;
        }
        let mut file = tokio::fs::File::create(path).await?;
        file.write_all(&wav_header(0)).await?;
        Ok(Self {
            file,
            path: path.to_path_buf(),
            data_len: 0,
        })
    }

    async fn write(&mut self, bytes: &[u8]) -> std::io::Result<()> {
        self.file.write_all(bytes).await?;
        self.data_len = self.data_len.saturating_add(bytes.len() as u32);
        Ok(())
    }

    async fn finish(mut self) -> Result<PathBuf> {
        self.file.seek(std::io::SeekFrom::Start(0)).await?;
        self.file.write_all(&wav_header(self.data_len)).await?;
        self.file.flush().await?;
        Ok(self.path)
    }
}

fn wav_header(data_len: u32) -> [u8; 44] {
    let mut h = [0u8; 44];
    let byte_rate = RATE * 2;
    h[0..4].copy_from_slice(b"RIFF");
    h[4..8].copy_from_slice(&(36 + data_len).to_le_bytes());
    h[8..12].copy_from_slice(b"WAVE");
    h[12..16].copy_from_slice(b"fmt ");
    h[16..20].copy_from_slice(&16u32.to_le_bytes());
    h[20..22].copy_from_slice(&1u16.to_le_bytes());
    h[22..24].copy_from_slice(&1u16.to_le_bytes());
    h[24..28].copy_from_slice(&RATE.to_le_bytes());
    h[28..32].copy_from_slice(&byte_rate.to_le_bytes());
    h[32..34].copy_from_slice(&2u16.to_le_bytes());
    h[34..36].copy_from_slice(&16u16.to_le_bytes());
    h[36..40].copy_from_slice(b"data");
    h[40..44].copy_from_slice(&data_len.to_le_bytes());
    h
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn wav_roundtrip_has_patched_sizes() {
        let dir = std::env::temp_dir().join(format!("wa-call-test-{}", std::process::id()));
        let path = dir.join("t.wav");
        let mut w = WavWriter::create(&path).await.unwrap();
        w.write(&[1, 0, 2, 0]).await.unwrap();
        w.finish().await.unwrap();
        let b = std::fs::read(&path).unwrap();
        assert_eq!(b.len(), 48);
        assert_eq!(u32::from_le_bytes(b[40..44].try_into().unwrap()), 4);
        assert_eq!(u32::from_le_bytes(b[4..8].try_into().unwrap()), 40);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn socket_mode_carries_both_directions() {
        let dir = std::env::temp_dir().join(format!("wa-call-sock-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("a.sock");
        let listener = tokio::net::UnixListener::bind(&path).unwrap();
        let spec = AudioSpec {
            socket: Some(path.clone()),
            ..Default::default()
        };
        let (src, sink) = spawn_endpoints(&spec).unwrap();
        let (mut far, _) = listener.accept().await.unwrap();
        // What the far program sends arrives as engine frames...
        let mut said = Vec::new();
        for i in 0..FRAME_SAMPLES as i16 {
            said.extend_from_slice(&i.to_le_bytes());
        }
        far.write_all(&said).await.unwrap();
        let got = src.frames.recv().await.unwrap();
        assert_eq!(got.len(), FRAME_SAMPLES);
        assert_eq!(got[5], 5);
        // ...and what the peer says arrives at the far program as raw s16le.
        sink.send(vec![7i16; FRAME_SAMPLES]).await.unwrap();
        let mut heard = vec![0u8; FRAME_BYTES];
        far.read_exact(&mut heard).await.unwrap();
        assert_eq!(i16::from_le_bytes([heard[0], heard[1]]), 7);
        // Closing the far side ends the source, which then keeps the call fed with silence.
        drop(far);
        let mut src = src;
        tokio::time::timeout(Duration::from_secs(2), &mut src.done).await.unwrap().unwrap();
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn socket_mode_drops_rather_than_queues() {
        let dir = std::env::temp_dir().join(format!("wa-call-sockq-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("q.sock");
        let listener = tokio::net::UnixListener::bind(&path).unwrap();
        let spec = AudioSpec {
            socket: Some(path.clone()),
            ..Default::default()
        };
        let (src, _sink) = spawn_endpoints(&spec).unwrap();
        let (mut far, _) = listener.accept().await.unwrap();
        // Ten frames while nothing consumes: only the first two may be kept.
        for n in 0..10i16 {
            far.write_all(&vec![n as u8, 0].repeat(FRAME_SAMPLES)).await.unwrap();
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(src.frames.len(), 2);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn silence_source_is_paced() {
        let src = spawn_source(&AudioSpec::default()).unwrap();
        let t = std::time::Instant::now();
        for _ in 0..4 {
            assert_eq!(src.frames.recv().await.unwrap().len(), FRAME_SAMPLES);
        }
        // First tick is immediate, then 3 × 60 ms.
        assert!(t.elapsed() >= Duration::from_millis(170));
    }

    #[tokio::test]
    async fn file_source_decodes_then_signals_done() {
        let dir = std::env::temp_dir().join(format!("wa-call-src-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let wav = dir.join("tone.wav");
        let ok = std::process::Command::new("ffmpeg")
            .args(["-y", "-loglevel", "error", "-f", "lavfi", "-i", "sine=frequency=440:duration=0.6"])
            .arg(&wav)
            .status()
            .map(|s| s.success())
            .unwrap_or(false);
        if !ok {
            eprintln!("ffmpeg unavailable; skipping");
            return;
        }
        let spec = AudioSpec {
            play: Some(wav.to_string_lossy().into_owned()),
            ..Default::default()
        };
        let mut src = spawn_source(&spec).unwrap();
        let t = std::time::Instant::now();
        let mut loud = 0;
        for _ in 0..10 {
            let f = src.frames.recv().await.unwrap();
            assert_eq!(f.len(), FRAME_SAMPLES);
            if f.iter().any(|s| s.unsigned_abs() > 1000) {
                loud += 1;
            }
        }
        // 0.6 s of tone = 10 frames of 60 ms, delivered in real time (first tick is immediate).
        assert!(loud >= 9, "only {loud} frames carried the tone");
        assert!(t.elapsed() >= Duration::from_millis(530), "not paced: {:?}", t.elapsed());
        tokio::time::timeout(Duration::from_secs(2), &mut src.done).await.unwrap().unwrap();
        // ...and the source keeps going with silence afterwards.
        assert!(src.frames.recv().await.unwrap().iter().all(|s| *s == 0));
        std::fs::remove_dir_all(dir).unwrap();
    }
}
