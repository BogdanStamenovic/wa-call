//! Call engine: the media/signaling half of wa-call. Derived from whatsapp-rust's
//! `examples/voip-cli` (MIT, see LICENSE.whatsapp-rust); the cpal audio bridge, stdin UI and
//! argv parsing were replaced by `audio.rs` (pluggable sources/sinks) and `daemon.rs` (control
//! socket), so the code below only keeps the call state machine and codec plumbing.
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, OnceLock};

use anyhow::{Result, anyhow, bail};
#[cfg(feature = "voip-opus")]
use bytes::Bytes;
use log::{debug, error, info, warn};
use wacore::types::call::{CallAction, IncomingCall};
use wacore::types::events::{Event, EventHandler};
use wacore::voip::CallEvent;
#[cfg(all(feature = "voip-opus", feature = "voip-mlow"))]
use wacore::voip::MlowDecoder;
#[cfg(all(feature = "voip-opus", feature = "voip-mlow"))]
use wacore::voip::rtp::RTP_PAYLOAD_TYPE_MLOW_RED;
use whatsapp_rust::prelude::*;
#[cfg(feature = "voip-opus")]
use whatsapp_rust::voip::KeyframeUrgency;
use whatsapp_rust::voip::audio::{WaOpusDecoder, WaOpusEncoder};
#[cfg(feature = "voip-opus")]
use whatsapp_rust::voip::{AudioCodec, EncodedAudioFrame};
use whatsapp_rust::voip::{
    AudioFormat, CallHandle, CallTermination, VideoState, VideoUpgradeToken,
};

use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::audio::AudioSpec;
use crate::daemon::emit;
use crate::video::{self, VideoOpts};


#[cfg(not(any(feature = "voip-mlow", feature = "voip-opus")))]
compile_error!("enable at least one of voip-mlow or voip-opus");

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AudioMode {
    #[cfg(feature = "voip-mlow")]
    Mlow,
    #[cfg(feature = "voip-opus")]
    OpusMlow,
    #[cfg(feature = "voip-opus")]
    OpusNative,
    #[cfg(feature = "voip-opus")]
    OpusRfc7587,
}

impl AudioMode {
    pub fn from_env() -> Result<Self> {
        let configured = std::env::var("WA_AUDIO_CODEC").ok();
        match configured
            .as_deref()
            .map(str::to_ascii_lowercase)
            .as_deref()
        {
            #[cfg(feature = "voip-mlow")]
            None | Some("mlow") => Ok(Self::Mlow),
            #[cfg(all(not(feature = "voip-mlow"), feature = "voip-opus"))]
            None => Self::opus_from_env(),
            #[cfg(feature = "voip-opus")]
            Some("opus") => Self::opus_from_env(),
            #[cfg(feature = "voip-opus")]
            Some("opus-pt111") => Ok(Self::OpusRfc7587),
            Some(other) => bail!(
                "WA_AUDIO_CODEC={other:?} is unavailable in this build; compile the matching \
                 voip-mlow/voip-opus feature"
            ),
        }
    }

    #[cfg(feature = "voip-opus")]
    fn opus_from_env() -> Result<Self> {
        match std::env::var("WA_AUDIO_PROFILE")
            .ok()
            .as_deref()
            .map(str::to_ascii_lowercase)
            .as_deref()
        {
            None | Some("native") | Some("standard") | Some("pt120") => Ok(Self::OpusNative),
            Some("rfc7587") | Some("pt111") => Ok(Self::OpusRfc7587),
            Some("mlow") | Some("mlow-escape") => Ok(Self::OpusMlow),
            Some(other) => {
                bail!(
                    "unknown WA_AUDIO_PROFILE={other:?}; expected native (default), rfc7587, or mlow"
                )
            }
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            #[cfg(feature = "voip-mlow")]
            Self::Mlow => "mlow",
            #[cfg(feature = "voip-opus")]
            Self::OpusMlow => "opus/mlow-escape",
            #[cfg(feature = "voip-opus")]
            Self::OpusNative => "opus-native/pt120",
            #[cfg(feature = "voip-opus")]
            Self::OpusRfc7587 => "opus-rfc7587/pt111",
        }
    }

    fn format(self) -> AudioFormat {
        match self {
            #[cfg(feature = "voip-mlow")]
            Self::Mlow => AudioFormat::MLOW_16KHZ_60MS,
            #[cfg(feature = "voip-opus")]
            Self::OpusMlow => AudioFormat::OPUS_MLOW_16KHZ_60MS,
            #[cfg(feature = "voip-opus")]
            Self::OpusNative => AudioFormat::OPUS_16KHZ_60MS,
            #[cfg(feature = "voip-opus")]
            Self::OpusRfc7587 => AudioFormat::OPUS_RFC7587_16KHZ_60MS,
        }
    }

    fn signaling_rate(self) -> u32 {
        self.format().signaling_rate
    }
}

// ===================== live call / listen =====================

#[cfg(feature = "voip-opus")]
fn spawn_opus_bridge(
    mic: async_channel::Receiver<Vec<i16>>,
    speaker: async_channel::Sender<Vec<i16>>,
    audio: AudioMode,
) -> Result<(
    async_channel::Receiver<Bytes>,
    async_channel::Sender<EncodedAudioFrame>,
)> {
    let mlow_escape = audio == AudioMode::OpusMlow;
    let mut encoder = if mlow_escape {
        WaOpusEncoder::new_mlow_escape()?
    } else {
        WaOpusEncoder::new()?
    };
    let mut decoder = WaOpusDecoder::new()?;
    let (encoded_tx, encoded_rx) = async_channel::bounded::<Bytes>(3);
    let (playout_tx, playout_rx) = async_channel::bounded::<EncodedAudioFrame>(3);

    tokio::task::spawn_blocking(move || {
        while let Ok(pcm) = mic.recv_blocking() {
            match encoder.encode(&pcm).map(Bytes::from) {
                Ok(payload) => {
                    if encoded_tx.send_blocking(payload).is_err() {
                        break;
                    }
                }
                Err(error) => {
                    warn!("Opus encode failed: {error}");
                }
            }
        }
    });
    tokio::task::spawn_blocking(move || {
        #[cfg(feature = "voip-mlow")]
        let mut mlow_decoder = MlowDecoder::new();
        #[cfg(not(feature = "voip-mlow"))]
        let mut mlow_warning_logged = false;
        while let Ok(frame) = playout_rx.recv_blocking() {
            match frame.codec {
                AudioCodec::Opus => match if mlow_escape {
                    decoder.decode_mlow_escape(&frame.data)
                } else {
                    decoder.decode(&frame.data)
                } {
                    Ok(pcm) => {
                        let _ = speaker.try_send(pcm.to_vec());
                    }
                    Err(error) => warn!("Opus decode failed: {error}"),
                },
                AudioCodec::Mlow => {
                    #[cfg(feature = "voip-mlow")]
                    {
                        mlow_decoder.set_redundancy(i32::from(
                            frame.payload_type == RTP_PAYLOAD_TYPE_MLOW_RED,
                        ));
                        let pcm = mlow_decoder
                            .decode(&frame.data)
                            .into_iter()
                            .map(|sample| (sample * 32767.0).clamp(-32768.0, 32767.0) as i16)
                            .collect();
                        let _ = speaker.try_send(pcm);
                    }
                    #[cfg(not(feature = "voip-mlow"))]
                    if !mlow_warning_logged {
                        warn!(
                            "peer selected MLOW inbound audio; this Opus-only binary will keep the call but cannot play it"
                        );
                        mlow_warning_logged = true;
                    }
                }
                _ => {}
            }
        }
    });

    Ok((encoded_rx, playout_tx))
}

#[cfg(feature = "voip-opus")]
fn spawn_fallback_opus_decoder(
    speaker: async_channel::Sender<Vec<i16>>,
) -> Option<async_channel::Sender<Bytes>> {
    let mut decoder = match WaOpusDecoder::new() {
        Ok(decoder) => decoder,
        Err(error) => {
            error!("failed to initialize fallback Opus decoder: {error}");
            return None;
        }
    };
    let (payload_tx, payload_rx) = async_channel::bounded::<Bytes>(3);
    tokio::task::spawn_blocking(move || {
        while let Ok(payload) = payload_rx.recv_blocking() {
            match decoder.decode_mlow_escape(&payload) {
                Ok(pcm) => {
                    let _ = speaker.try_send(pcm.to_vec());
                }
                Err(error) => debug!("failed to decode MLOW escape payload: {error}"),
            }
        }
    });
    Some(payload_tx)
}
/// What the daemon does with an incoming offer before anyone tells it otherwise.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, clap::ValueEnum)]
#[serde(rename_all = "lowercase")]
pub enum IncomingPolicy {
    /// Emit a `ringing` event and wait for `answer`/`reject`; the caller gives up on its own.
    Ring,
    Answer,
    Reject,
}

/// Everything a single call is set up with: audio endpoints plus optional video.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct CallSpec {
    #[serde(flatten)]
    pub audio: AudioSpec,
    #[serde(default)]
    pub video: bool,
    /// ffmpeg video input: `testsrc`, a file/URL, or a `/dev/video*` node. Default: webcam.
    pub video_input: Option<String>,
    /// `window`, `file` or `none`. The daemon runs headless, so it defaults to `file`.
    pub video_sink: Option<String>,
}

/// Drives calls off the typed `Event::IncomingCall`: offers are held in `CallState::pending`
/// until the control socket (or the policy) answers or rejects them; the library facade owns
/// the media plane once accepted.
pub struct CallObserver {
    pub client: Arc<Client>,
    pub audio: AudioMode,
    pub policy: Mutex<IncomingPolicy>,
    pub default_spec: Mutex<CallSpec>,
    pub state: Arc<Mutex<CallState>>,
}

#[derive(Default)]
pub struct CallState {
    /// Live calls' handles by call-id.
    handles: HashMap<String, Arc<CallHandle>>,
    /// Registration order; the last live entry is the default target of `hangup`/`video`.
    call_order: Vec<String>,
    /// Each call's view of its video: pending peer request vs our video live.
    video_ui: HashMap<String, VideoUi>,
    starting: HashSet<String>,
    /// Prevents a late startup from resurrecting media after the peer ended the call.
    terminated_during_startup: HashSet<String>,
    /// Offers not yet answered or rejected.
    pending: HashMap<String, IncomingCall>,
    /// Peer and direction of every call we know about, for `status`.
    info: HashMap<String, serde_json::Value>,
}

#[derive(Clone, Copy, PartialEq)]
enum VideoUi {
    /// The peer asked for video (`UpgradeRequestV2`); `v` accepts it.
    PendingPeerRequest(VideoUpgradeToken),
    /// Our video plane is up; `v` downgrades.
    Active,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum InboundMediaFailureStage {
    /// Microphone, speaker, codec bridge, or video endpoints failed before the facade took ownership.
    LocalPreparation,
    /// `accept().start()` ran, so its generation-aware guard owns peer teardown.
    Facade,
}

fn should_reject_failed_media_start(
    stage: InboundMediaFailureStage,
    peer_already_ended: bool,
) -> bool {
    stage == InboundMediaFailureStage::LocalPreparation && !peer_already_ended
}

impl CallObserver {
    pub fn new(client: Arc<Client>, audio: AudioMode, policy: IncomingPolicy, spec: CallSpec) -> Self {
        Self {
            client,
            audio,
            policy: Mutex::new(policy),
            default_spec: Mutex::new(spec),
            state: Arc::new(Mutex::new(CallState::default())),
        }
    }
}

fn lock_call_state(state: &Mutex<CallState>) -> std::sync::MutexGuard<'_, CallState> {
    state
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn begin_call_startup(state: &Arc<Mutex<CallState>>, call_id: &str) {
    let mut st = lock_call_state(state);
    st.starting.insert(call_id.to_string());
    st.terminated_during_startup.remove(call_id);
}

fn complete_call_startup(state: &Arc<Mutex<CallState>>, call_id: &str) -> bool {
    let mut st = lock_call_state(state);
    st.starting.remove(call_id);
    st.terminated_during_startup.remove(call_id)
}

fn record_peer_terminate(state: &Arc<Mutex<CallState>>, call_id: &str) {
    let mut st = lock_call_state(state);
    if st.starting.contains(call_id) {
        st.terminated_during_startup.insert(call_id.to_string());
    }
    st.handles.remove(call_id);
    st.video_ui.remove(call_id);
    st.call_order.retain(|id| id != call_id);
}

fn peer_terminated_during_startup(state: &Arc<Mutex<CallState>>, call_id: &str) -> bool {
    lock_call_state(state)
        .terminated_during_startup
        .contains(call_id)
}

fn mark_call_most_recent(order: &mut Vec<String>, call_id: &str) {
    order.retain(|id| id != call_id);
    order.push(call_id.to_string());
}

/// Register a handle for UI control and remove it when the library-owned call ends.
async fn register_handle(state: &Arc<Mutex<CallState>>, cid: String, handle: Arc<CallHandle>) {
    if complete_call_startup(state, &cid) {
        // The library also sees the terminate, but this closes a handle created after that teardown.
        handle.hangup_local().await;
        info!("◾ discarded media startup for already-ended call {cid}");
        return;
    }
    {
        let mut st = lock_call_state(state);
        st.handles.insert(cid.clone(), handle.clone());
        mark_call_most_recent(&mut st.call_order, &cid);
    }
    let state = state.clone();
    tokio::spawn(async move {
        handle.wait_ended().await;
        {
            let mut st = lock_call_state(&state);
            // Remove only if it is still OUR handle: a same-call-id replacement may now own this slot
            // (its own cleanup will remove it), so we must not delete the live call.
            if st
                .handles
                .get(&cid)
                .is_some_and(|h| Arc::ptr_eq(h, &handle))
            {
                st.handles.remove(&cid);
                st.video_ui.remove(&cid);
                st.call_order.retain(|id| id != &cid);
            }
        }
        info!("◾ call {cid} media ended");
        emit("media-ended", json!({"call_id": cid}));
    });
}

impl EventHandler for CallObserver {
    fn handle_event(&self, event: Arc<Event>) {
        match &*event {
            Event::IncomingCall(call) => match &call.action {
                CallAction::Offer {
                    call_id, is_video, ..
                } => {
                    let call: IncomingCall = (**call).clone();
                    let cid = call_id.clone();
                    let policy = *self.policy.lock().unwrap_or_else(|p| p.into_inner());
                    let peer = json!({
                        "call_id": cid, "peer": call.from.to_string(),
                        "name": call.notify, "video": is_video, "direction": "incoming",
                    });
                    {
                        let mut st = lock_call_state(&self.state);
                        st.pending.insert(cid.clone(), call);
                        st.info.insert(cid.clone(), peer.clone());
                    }
                    info!("📞 incoming call {cid} from {} ({policy:?})", peer["peer"]);
                    emit("ringing", peer);
                    let client = self.client.clone();
                    let state = self.state.clone();
                    let audio = self.audio;
                    match policy {
                        IncomingPolicy::Ring => {}
                        IncomingPolicy::Reject => {
                            tokio::spawn(async move {
                                if let Err(e) = reject_pending(&client, &state, Some(&cid)).await {
                                    warn!("auto-reject failed: {e:#}");
                                }
                            });
                        }
                        IncomingPolicy::Answer => {
                            let spec = self
                                .default_spec
                                .lock()
                                .unwrap_or_else(|p| p.into_inner())
                                .clone();
                            tokio::spawn(async move {
                                if let Err(e) =
                                    answer_pending(&client, audio, &state, Some(&cid), spec).await
                                {
                                    warn!("auto-answer failed: {e:#}");
                                }
                            });
                        }
                    }
                }
                CallAction::Terminate { call_id, .. } => {
                    info!("◀ peer ended call {call_id}");
                    let was_pending = lock_call_state(&self.state).pending.remove(call_id).is_some();
                    record_peer_terminate(&self.state, call_id);
                    emit(
                        "ended",
                        json!({"call_id": call_id, "by": "peer", "unanswered": was_pending}),
                    );
                }
                _ => {}
            },
            Event::MissedCall(mc) => {
                // An offer replayed from the offline queue on reconnect: a dead call, never ring/accept.
                info!("☎ missed call {} from {} (offline-delivered)", mc.call_id, mc.from);
                emit(
                    "missed",
                    json!({"call_id": mc.call_id, "peer": mc.from.to_string()}),
                );
            }
            _ => {}
        }
    }
}

/// Pick the call a command targets: the given id, else the single/most recent candidate.
fn pick<'a, V>(map: &'a HashMap<String, V>, order: &[String], id: Option<&str>) -> Result<String> {
    if let Some(id) = id {
        return map
            .contains_key(id)
            .then(|| id.to_string())
            .ok_or_else(|| anyhow!("no such call {id}"));
    }
    if let Some(id) = order.iter().rev().find(|c| map.contains_key(*c)) {
        return Ok(id.clone());
    }
    match map.len() {
        0 => bail!("no call to act on"),
        1 => Ok(map.keys().next().cloned().unwrap_or_default()),
        _ => bail!("several calls; pass a call id"),
    }
}

pub async fn answer_pending(
    client: &Arc<Client>,
    audio: AudioMode,
    state: &Arc<Mutex<CallState>>,
    id: Option<&str>,
    spec: CallSpec,
) -> Result<String> {
    let call = {
        let mut st = lock_call_state(state);
        let cid = pick(&st.pending, &[], id)?;
        st.pending.remove(&cid).ok_or_else(|| anyhow!("offer vanished"))?
    };
    let CallAction::Offer {
        call_id, is_video, ..
    } = &call.action
    else {
        bail!("not an offer");
    };
    let cid = call_id.clone();
    begin_call_startup(state, &cid);
    if let Err(e) = respond_to_offer(client, &call, true, audio).await {
        complete_call_startup(state, &cid);
        return Err(e.context("call signaling failed"));
    }
    let initial_video = spec.video && *is_video;
    match start_media(client, &call, initial_video, spec.video, audio, state, &spec).await {
        Ok(handle) => {
            register_handle(state, cid.clone(), handle).await;
            emit("answered", json!({"call_id": cid, "spec": spec}));
            Ok(cid)
        }
        Err((stage, e)) => {
            let peer_already_ended = complete_call_startup(state, &cid);
            if should_reject_failed_media_start(stage, peer_already_ended)
                && let Err(reject_error) = client.voip().reject(&call).await
            {
                warn!("failed to reject call after local media setup failed: {reject_error}");
            }
            emit("failed", json!({"call_id": cid, "error": format!("{e:#}")}));
            Err(e.context("inbound media failed"))
        }
    }
}

pub async fn reject_pending(
    client: &Arc<Client>,
    state: &Arc<Mutex<CallState>>,
    id: Option<&str>,
) -> Result<String> {
    let (cid, call) = {
        let mut st = lock_call_state(state);
        let cid = pick(&st.pending, &[], id)?;
        let call = st.pending.remove(&cid).ok_or_else(|| anyhow!("offer vanished"))?;
        (cid, call)
    };
    client
        .voip()
        .reject(&call)
        .await
        .map_err(|e| anyhow!("send reject: {e}"))?;
    emit("ended", json!({"call_id": cid, "by": "us", "rejected": true}));
    Ok(cid)
}

pub async fn dial(
    client: &Arc<Client>,
    audio: AudioMode,
    state: &Arc<Mutex<CallState>>,
    peer: &Jid,
    spec: CallSpec,
) -> Result<String> {
    let handle = place_outgoing_call(client, peer, audio, state, &spec).await?;
    let cid = handle.call_id().to_string();
    lock_call_state(state).info.insert(
        cid.clone(),
        json!({"call_id": cid, "peer": peer.to_string(), "video": spec.video, "direction": "outgoing"}),
    );
    register_handle(state, cid.clone(), handle).await;
    emit("dialing", json!({"call_id": cid, "peer": peer.to_string(), "spec": spec}));
    Ok(cid)
}

pub async fn hangup(state: &Arc<Mutex<CallState>>, id: Option<&str>) -> Result<String> {
    let (cid, handle) = {
        let st = lock_call_state(state);
        let cid = pick(&st.handles, &st.call_order, id)?;
        let h = st.handles.get(&cid).cloned().ok_or_else(|| anyhow!("call vanished"))?;
        (cid, h)
    };
    let note = match handle.terminate().await {
        CallTermination::LocalOnly(e) => format!("peer not confirmed ({e}); ended locally"),
        CallTermination::PartlyNotified {
            notified,
            unconfirmed,
        } => format!("reached {notified} device(s), {unconfirmed} unconfirmed"),
        _ => "ok".into(),
    };
    emit("ended", json!({"call_id": cid, "by": "us", "note": note}));
    Ok(cid)
}

/// Toggle video on a live call: downgrade, accept the peer's pending request, or start an upgrade.
pub async fn toggle_video(
    state: &Arc<Mutex<CallState>>,
    id: Option<&str>,
    spec: &CallSpec,
) -> Result<&'static str> {
    let (cid, handle, ui) = {
        let st = lock_call_state(state);
        let cid = pick(&st.handles, &st.call_order, id)?;
        let h = st.handles.get(&cid).cloned().ok_or_else(|| anyhow!("call vanished"))?;
        let ui = st.video_ui.get(&cid).copied();
        (cid, h, ui)
    };
    match ui {
        Some(VideoUi::Active) => {
            handle.stop_video().await.map_err(|e| anyhow!("stop_video: {e}"))?;
            mark_video(state, &cid, None);
            Ok("video stopped")
        }
        Some(VideoUi::PendingPeerRequest(request)) => {
            accept_peer_video(&handle, request, spec).await?;
            activate_peer_video_request(state, &cid, request);
            Ok("accepted peer's video")
        }
        None => {
            let opts = VideoOpts::from_spec(spec).await?;
            let src = video::spawn_video_source(&opts).await?;
            let sink = video::spawn_video_sink(&opts, &cid, Some(keyframe_recovery(&handle))).await?;
            handle
                .start_video(src, sink)
                .await
                .map_err(|e| anyhow!("start_video: {e}"))?;
            mark_video(state, &cid, Some(VideoUi::Active));
            Ok("video upgrade requested")
        }
    }
}

pub fn status(state: &Arc<Mutex<CallState>>) -> serde_json::Value {
    let st = lock_call_state(state);
    let describe = |cid: &String| st.info.get(cid).cloned().unwrap_or(json!({"call_id": cid}));
    json!({
        "live": st.handles.keys().map(|c| {
            let mut v = describe(c);
            v["video"] = json!(st.video_ui.get(c).map(|u| match u {
                VideoUi::Active => "on",
                VideoUi::PendingPeerRequest(_) => "peer-requested",
            }));
            v
        }).collect::<Vec<_>>(),
        "ringing": st.pending.keys().map(describe).collect::<Vec<_>>(),
    })
}

/// Hang up after a non-looping `play` file ends, if the spec asks for it.
fn hangup_when_played(handle: &Arc<CallHandle>, done: tokio::sync::oneshot::Receiver<()>, spec: &CallSpec) {
    if !spec.audio.hangup_after_play || spec.audio.loop_play || spec.audio.play.is_none() {
        return;
    }
    let handle = handle.clone();
    tokio::spawn(async move {
        if done.await.is_ok() {
            // Let the last frames drain through the jitter buffer before tearing down.
            tokio::time::sleep(std::time::Duration::from_millis(600)).await;
            info!("▶ playback finished; hanging up {}", handle.call_id());
            let _ = handle.terminate().await;
            emit("ended", json!({"call_id": handle.call_id(), "by": "us", "note": "playback finished"}));
        }
    });
}

async fn start_media(
    client: &Arc<Client>,
    call: &IncomingCall,
    initial_video: bool,
    auto_video: bool,
    audio: AudioMode,
    state: &Arc<Mutex<CallState>>,
    spec: &CallSpec,
) -> std::result::Result<Arc<CallHandle>, (InboundMediaFailureStage, anyhow::Error)> {
    let voip = client.voip();
    let (builder, event_speaker, video_recovery, done) = async {
        let source = crate::audio::spawn_source(&spec.audio)?;
        let mic = source.frames;
        let speaker = crate::audio::spawn_sink(&spec.audio)?;
        let event_speaker = speaker.clone();
        info!("🔌 connecting media via client.voip().accept(..)…");
        let (video_endpoints, video_recovery) = if initial_video {
            let opts = VideoOpts::from_spec(spec).await?;
            let cid = call.action.call_id();
            let (on_loss, slot) = deferred_keyframe_recovery();
            (
                Some((
                    video::spawn_video_source(&opts).await?,
                    video::spawn_video_sink(&opts, cid, Some(on_loss)).await?,
                )),
                Some(slot),
            )
        } else {
            (None, None)
        };
        if peer_terminated_during_startup(state, call.action.call_id()) {
            bail!("peer ended the call during media preparation");
        }
        let mut builder = match audio {
            #[cfg(feature = "voip-mlow")]
            AudioMode::Mlow => voip.accept(call).audio(mic, speaker),
            #[cfg(feature = "voip-opus")]
            AudioMode::OpusMlow | AudioMode::OpusNative | AudioMode::OpusRfc7587 => {
                let (source, sink) = spawn_opus_bridge(mic, speaker, audio)?;
                voip.accept(call)
                    .encoded_audio(audio.format(), source, sink)
            }
        };
        if let Some((source, sink)) = video_endpoints {
            builder = builder.video(source, sink);
        }
        Ok::<_, anyhow::Error>((builder, event_speaker, video_recovery, source.done))
    }
    .await
    .map_err(|error| (InboundMediaFailureStage::LocalPreparation, error))?;

    let handle = builder.start().await.map_err(|error| {
        (
            InboundMediaFailureStage::Facade,
            anyhow!("accept media: {error}"),
        )
    })?;
    if let Some(slot) = video_recovery {
        let _ = slot.set(handle.clone());
    }
    info!(
        "🎙  {} media flow live for call {}{}",
        audio.name(),
        handle.call_id(),
        if initial_video { " 🎥 video on." } else { "" }
    );
    let handle = Arc::new(handle);
    if initial_video {
        mark_video(state, handle.call_id(), Some(VideoUi::Active));
    }
    hangup_when_played(&handle, done, spec);
    spawn_call_event_listener(handle.clone(), event_speaker, auto_video, state.clone(), spec.clone());
    Ok(handle)
}

/// Place an outgoing 1:1 call through the library facade. The returned handle is dormant until
/// the server hands back the relay; the facade attaches the engine then.
async fn place_outgoing_call(
    client: &Arc<Client>,
    peer: &Jid,
    audio: AudioMode,
    state: &Arc<Mutex<CallState>>,
    spec: &CallSpec,
) -> Result<Arc<CallHandle>> {
    let video = spec.video;
    let source = crate::audio::spawn_source(&spec.audio)?;
    let mic = source.frames;
    let speaker = crate::audio::spawn_sink(&spec.audio)?;
    let event_speaker = speaker.clone();
    info!("📞 placing call to {peer} via client.voip().call(..)…");
    let voip = client.voip();
    let mut builder = match audio {
        #[cfg(feature = "voip-mlow")]
        AudioMode::Mlow => voip.call(peer).audio(mic, speaker),
        #[cfg(feature = "voip-opus")]
        AudioMode::OpusMlow | AudioMode::OpusNative | AudioMode::OpusRfc7587 => {
            let (source, sink) = spawn_opus_bridge(mic, speaker, audio)?;
            voip.call(peer).encoded_audio(audio.format(), source, sink)
        }
    };
    let mut video_recovery = None;
    if video {
        let opts = VideoOpts::from_spec(spec).await?;
        let (on_loss, slot) = deferred_keyframe_recovery();
        video_recovery = Some(slot);
        builder = builder.video(
            video::spawn_video_source(&opts).await?,
            video::spawn_video_sink(&opts, "outgoing", Some(on_loss)).await?,
        );
    }
    let handle = builder
        .start()
        .await
        .map_err(|e| anyhow!("place call: {e}"))?;
    if let Some(slot) = video_recovery {
        let _ = slot.set(handle.clone());
    }
    info!(
        "☎  {} offer sent for call {} — waiting for the peer's relay to connect media.{}",
        audio.name(),
        handle.call_id(),
        if video { " 🎥 video call." } else { "" }
    );
    let handle = Arc::new(handle);
    if video {
        mark_video(state, handle.call_id(), Some(VideoUi::Active));
    }
    hangup_when_played(&handle, source.done, spec);
    spawn_call_event_listener(handle.clone(), event_speaker, video, state.clone(), spec.clone());
    Ok(handle)
}

/// Update (or clear) the stdin toggle's view of a call's video state.
fn mark_video(state: &Arc<Mutex<CallState>>, call_id: &str, ui: Option<VideoUi>) {
    let mut st = lock_call_state(state);
    match ui {
        Some(ui) => {
            st.video_ui.insert(call_id.to_string(), ui);
        }
        None => {
            st.video_ui.remove(call_id);
        }
    }
}

fn clear_pending_peer_video(state: &Arc<Mutex<CallState>>, call_id: &str) {
    let mut st = lock_call_state(state);
    if matches!(
        st.video_ui.get(call_id),
        Some(VideoUi::PendingPeerRequest(_))
    ) {
        st.video_ui.remove(call_id);
    }
}

fn activate_peer_video_request(
    state: &Arc<Mutex<CallState>>,
    call_id: &str,
    request: VideoUpgradeToken,
) {
    let mut st = lock_call_state(state);
    if let Some(ui) = st.video_ui.get_mut(call_id)
        && *ui == VideoUi::PendingPeerRequest(request)
    {
        *ui = VideoUi::Active;
    }
}

/// Surface call diagnostics and drive the video upgrade handshake.
fn spawn_call_event_listener(
    handle: Arc<CallHandle>,
    speaker: async_channel::Sender<Vec<i16>>,
    auto_video: bool,
    state: Arc<Mutex<CallState>>,
    spec: CallSpec,
) {
    let Some(mut events) = handle.take_events() else {
        warn!("call event receiver already owned by another consumer");
        return;
    };
    #[cfg(feature = "voip-opus")]
    let fallback_opus_tx = spawn_fallback_opus_decoder(speaker.clone());
    tokio::spawn(async move {
        #[cfg(not(feature = "voip-opus"))]
        let mut fallback_warning_logged = false;
        let mut peer_video_receiver_confirmed = false;
        let mut peer_keyframe_request_logged = false;
        while let Ok(ev) = events.recv().await {
            match ev {
                CallEvent::RelayAllocated => {
                    info!("🛰  relay allocate acknowledged — media path live");
                    emit("connected", json!({"call_id": handle.call_id()}));
                }
                CallEvent::RelayAllocateFailed(code) => {
                    warn!("relay rejected the allocate (STUN error {code}) — media never came up")
                }
                CallEvent::RelayAllocateTimedOut => {
                    warn!("relay never acked the allocate (wedged relay) — media never came up")
                }
                CallEvent::ForeignAudio(payload) => {
                    #[cfg(feature = "voip-opus")]
                    if let Some(fallback_opus_tx) = &fallback_opus_tx {
                        let _ = fallback_opus_tx.try_send(payload);
                    }
                    #[cfg(not(feature = "voip-opus"))]
                    if !fallback_warning_logged {
                        warn!(
                            "peer sent a {}-byte MLOW embedded Opus fallback, but this binary has \
                             no Opus decoder",
                            payload.len()
                        );
                        let _ = &speaker;
                        fallback_warning_logged = true;
                    }
                }
                CallEvent::AudioFormatMismatch {
                    expected_rate,
                    received_rates,
                } => {
                    error!(
                        "peer selected incompatible audio rates {received_rates:?}; expected \
                         {expected_rate}"
                    );
                }
                CallEvent::RtcpReceived {
                    packet_types,
                    sender_ssrc,
                    referenced_ssrcs,
                    reports_audio,
                    reports_video,
                    report_blocks,
                    feedback,
                } => {
                    let requests_keyframe = feedback
                        .iter()
                        .any(|item| item.packet_type == 206 && matches!(item.fmt, 1 | 4));
                    let blocks = report_blocks
                        .iter()
                        .map(|report| {
                            format!(
                                "{:#010x}:loss={}/{} highest={} jitter={} lsr={:#010x} dlsr={} ext={}B",
                                report.ssrc,
                                report.fraction_lost,
                                report.cumulative_lost,
                                report.extended_highest_sequence,
                                report.jitter,
                                report.last_sender_report,
                                report.delay_since_last_sender_report,
                                report.profile_extension.len(),
                            )
                        })
                        .collect::<Vec<_>>()
                        .join(",");
                    let feedback = feedback
                        .iter()
                        .map(|item| {
                            let kind = match (item.packet_type, item.fmt) {
                                (205, 1) => "NACK",
                                (206, 1) => "PLI",
                                (206, 4) => "FIR",
                                (206, 15) => "AFB",
                                _ => "unknown",
                            };
                            format!(
                                "{kind}:sender={:#010x}/media={:#010x}/fci={}B:{}",
                                item.sender_ssrc,
                                item.media_ssrc,
                                item.fci.len(),
                                hex::encode(&item.fci),
                            )
                        })
                        .collect::<Vec<_>>()
                        .join(",");
                    if reports_video && !peer_video_receiver_confirmed {
                        info!(
                            "🎥 peer RTCP confirms receipt of our outbound video stream: blocks=[{blocks}] feedback=[{feedback}]"
                        );
                        peer_video_receiver_confirmed = true;
                    }
                    if requests_keyframe && !peer_keyframe_request_logged {
                        info!("🎥 peer requests an outbound keyframe: [{feedback}]");
                        peer_keyframe_request_logged = true;
                    }
                    debug!(
                        "📊 peer RTCP PT={packet_types:?} sender={sender_ssrc:#010x} refs={referenced_ssrcs:x?} reports_audio={reports_audio} reports_video={reports_video} blocks=[{blocks}] feedback=[{feedback}]"
                    );
                }
                CallEvent::OutboundMediaDropped {
                    video_access_units,
                    packets,
                } => {
                    warn!(
                        "🎥 relay-send backpressure: dropped {video_access_units} complete video AUs / {packets} packets"
                    );
                }
                CallEvent::PeerVideoStateChanged { .. } if handle.video_states().is_none() => {
                    debug!("ignoring queued video state for an ended or replaced call");
                }
                CallEvent::PeerVideoStateChanged { source, state, .. }
                    if handle.group_state().is_some() =>
                {
                    // A participant's stop/disable does not change our call-wide video toggle.
                    info!("🎥 participant {source} video {state:?}");
                }
                CallEvent::PeerVideoStateChanged {
                    state: vs,
                    upgrade_token,
                    ..
                } => match vs {
                    VideoState::UpgradeRequest | VideoState::UpgradeRequestV2 => {
                        let Some(request) = upgrade_token else {
                            info!("🎥 concurrent video upgrade resolved automatically");
                            continue;
                        };
                        mark_video(
                            &state,
                            handle.call_id(),
                            Some(VideoUi::PendingPeerRequest(request)),
                        );
                        if auto_video {
                            info!("🎥 peer asked for video — auto-accepting (--video)");
                            let handle = handle.clone();
                            let state = state.clone();
                            let spec = spec.clone();
                            tokio::spawn(async move {
                                if let Err(e) = accept_peer_video(&handle, request, &spec).await {
                                    warn!("accepting peer video failed: {e}");
                                } else {
                                    activate_peer_video_request(&state, handle.call_id(), request);
                                }
                            });
                        } else {
                            info!("🎥 peer asked for video — press `v` + Enter to accept");
                        }
                    }
                    VideoState::UpgradeAccept | VideoState::Enabled => {
                        info!("🎥 peer video {vs:?} — inbound video should start flowing");
                    }
                    VideoState::Stopped => {
                        info!("🎥 peer stopped its video");
                        clear_pending_peer_video(&state, handle.call_id());
                    }
                    VideoState::Disabled
                    | VideoState::UpgradeReject
                    | VideoState::UpgradeRejectByTimeout
                    | VideoState::UpgradeCancel
                    | VideoState::UpgradeCancelByTimeout
                    | VideoState::Error => {
                        info!("🎥 video upgrade ended ({vs:?})");
                        mark_video(&state, handle.call_id(), None);
                    }
                    other => info!("🎥 peer video state: {other:?}"),
                },
                // Outbound video is dropping every frame until an IDR reaches
                // the wire, and only the encoder can make one. This example
                // cannot: its source is a spawned ffmpeg whose GOP is fixed at
                // launch, with no runtime input that forces a keyframe. Saying
                // so beats leaving someone to wonder why their picture froze
                // for up to `GOP_SECONDS`; a real client wires this to its
                // encoder's force-IDR control.
                CallEvent::VideoKeyframeNeeded => info!(
                    "🎥 engine wants a keyframe; this encoder makes one every {}s and cannot be asked",
                    video::GOP_SECONDS
                ),
                // CallEvent is #[non_exhaustive]: ignore variants newer core versions may add.
                _ => {}
            }
        }
    });
}

/// Let a sink ask the peer for the keyframe that ends a shed: an access unit
/// ffplay refused is a loss nothing inside the library can see. Available to the
/// mid-call flows only -- the from-start ones build their sink to hand to the
/// builder that produces the handle, so there is nothing to close over yet.
fn keyframe_recovery(handle: &CallHandle) -> video::OnVideoLoss {
    let handle = handle.clone();
    Arc::new(move || handle.request_peer_keyframe(KeyframeUrgency::Coalesced))
}

/// The same recovery for a flow that builds its sink *before* the handle exists.
///
/// A video-from-start call hands its endpoints to the builder that produces the
/// handle, so there is nothing to close over yet -- but the sink outlives that,
/// and a shed frame after the call is up deserves the same request an upgrade
/// gets. Returns the hook and the slot to fill once the handle is in hand; until
/// it is filled the hook is a no-op, which is the honest answer for a loss that
/// happened before there was a call to ask on.
fn deferred_keyframe_recovery() -> (video::OnVideoLoss, Arc<OnceLock<CallHandle>>) {
    let slot: Arc<OnceLock<CallHandle>> = Arc::new(OnceLock::new());
    let hook = {
        let slot = slot.clone();
        Arc::new(move || {
            if let Some(handle) = slot.get() {
                handle.request_peer_keyframe(KeyframeUrgency::Coalesced);
            }
        }) as video::OnVideoLoss
    };
    (hook, slot)
}

/// Fresh ffmpeg/ffplay endpoints for a mid-call upgrade/accept on `handle`.
async fn accept_peer_video(
    handle: &CallHandle,
    request: VideoUpgradeToken,
    spec: &CallSpec,
) -> Result<()> {
    let opts = VideoOpts::from_spec(spec).await?;
    let src = video::spawn_video_source(&opts).await?;
    let sink =
        video::spawn_video_sink(&opts, handle.call_id(), Some(keyframe_recovery(handle))).await?;
    handle
        .accept_video(request, src, sink)
        .await
        .map_err(|e| anyhow!("accept_video: {e}"))
}
async fn respond_to_offer(
    client: &Arc<Client>,
    call: &IncomingCall,
    accept: bool,
    audio: AudioMode,
) -> Result<()> {
    let CallAction::Offer {
        call_id,
        audio: offered_audio,
        ..
    } = &call.action
    else {
        return Ok(());
    };
    info!(
        "📞 incoming call {call_id} from {} ({})",
        call.from,
        if accept { "accepting" } else { "rejecting" }
    );
    if !accept {
        client
            .voip()
            .reject(call)
            .await
            .map_err(|e| anyhow!("send reject: {e}"))?;
        return Ok(());
    }
    if !offered_audio.is_empty()
        && !offered_audio.iter().any(|codec| {
            codec.enc.eq_ignore_ascii_case("opus") && codec.rate == audio.signaling_rate()
        })
    {
        client
            .voip()
            .reject(call)
            .await
            .map_err(|e| anyhow!("reject incompatible audio offer: {e}"))?;
        bail!(
            "peer did not offer {} signaling rate {}",
            audio.name(),
            audio.signaling_rate()
        );
    }
    Ok(())
}

