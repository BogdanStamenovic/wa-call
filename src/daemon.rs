//! The long-lived half: holds the WhatsApp companion session, owns every call, and serves a
//! JSON-lines control socket. One request per line in, one response per line out; the `events`
//! op instead keeps the connection open and streams every event as it happens.

use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock};

use anyhow::{Context, Result, anyhow, bail};
use log::{info, warn};
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::broadcast;
use wacore::store::DevicePropsOverride;
use whatsapp_rust::pair_code::PairCodeOptions;
use whatsapp_rust::prelude::*;

use crate::engine::{self, AudioMode, CallObserver, CallSpec, IncomingPolicy};

static EVENTS: OnceLock<broadcast::Sender<Value>> = OnceLock::new();
static LOG: OnceLock<PathBuf> = OnceLock::new();

/// Publish an event to socket subscribers and append it to the call log.
pub fn emit(kind: &str, mut v: Value) {
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    if !v.is_object() {
        v = json!({ "data": v });
    }
    v["event"] = json!(kind);
    v["ts"] = json!(ts);
    info!("event {v}");
    if let Some(path) = LOG.get() {
        use std::io::Write;
        if let Ok(mut f) = std::fs::OpenOptions::new().create(true).append(true).open(path) {
            let _ = writeln!(f, "{v}");
        }
    }
    if let Some(tx) = EVENTS.get() {
        let _ = tx.send(v);
    }
}

pub fn state_dir() -> PathBuf {
    std::env::var_os("WA_CALL_STATE_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            let base = std::env::var_os("XDG_STATE_HOME")
                .map(PathBuf::from)
                .unwrap_or_else(|| home().join(".local/state"));
            base.join("wa-call")
        })
}

pub fn socket_path() -> PathBuf {
    if let Some(p) = std::env::var_os("WA_CALL_SOCKET") {
        return PathBuf::from(p);
    }
    match std::env::var_os("XDG_RUNTIME_DIR") {
        Some(d) => PathBuf::from(d).join("wa-call.sock"),
        None => state_dir().join("wa-call.sock"),
    }
}

fn home() -> PathBuf {
    std::env::var_os("HOME").map(PathBuf::from).unwrap_or_else(|| PathBuf::from("/tmp"))
}

/// Accepts `+381 64 123-4567`, `381641234567`, or a full JID.
pub fn parse_peer(s: &str) -> Result<Jid> {
    if s.contains('@') {
        return s.parse::<Jid>().map_err(|e| anyhow!("bad JID {s:?}: {e}"));
    }
    let digits: String = s.chars().filter(|c| c.is_ascii_digit()).collect();
    if digits.len() < 7 {
        bail!("{s:?} is not a phone number in international format (country code first)");
    }
    format!("{digits}@s.whatsapp.net")
        .parse::<Jid>()
        .map_err(|e| anyhow!("bad number {s:?}: {e}"))
}

async fn open_store(dir: &Path) -> Result<SqliteStore> {
    std::fs::create_dir_all(dir).with_context(|| format!("creating {}", dir.display()))?;
    let db = dir.join("whatsapp.db");
    SqliteStore::open(db.to_str().context("non-UTF-8 state dir")?)
        .await
        .map_err(|e| anyhow!("sqlite: {e}"))
}

/// The name the phone shows under Linked devices.
fn device_props() -> DevicePropsOverride {
    let host = std::fs::read_to_string("/etc/hostname").unwrap_or_default();
    let host = host.trim();
    DevicePropsOverride::new().with_os(if host.is_empty() {
        "wa-call".to_string()
    } else {
        format!("wa-call ({host})")
    })
}

/// Pair this device. Exits once the session is logged in.
pub async fn link(phone: Option<String>) -> Result<()> {
    if UnixStream::connect(socket_path()).await.is_ok() {
        bail!("the daemon is running and owns the session; it is already linked (wa-call status)");
    }
    let store = open_store(&state_dir()).await?;
    let (done_tx, mut done_rx) = tokio::sync::mpsc::channel::<()>(1);
    let mut builder = Bot::builder()
        .with_backend(store)
        .with_device_props(device_props())
        .skip_history_sync()
        .on_qr_code(|code, timeout| async move {
            eprintln!("Scan with WhatsApp → Linked devices → Link a device (valid {}s):", timeout.as_secs());
            print_qr(&code);
        })
        .on_pair_code(|code, timeout| async move {
            eprintln!(
                "On the phone: Linked devices → Link a device → Link with phone number instead, \
                 then enter  {code}  (valid {}s)",
                timeout.as_secs()
            );
        })
        .on_connected(move |client| {
            let done_tx = done_tx.clone();
            async move {
                if client.is_logged_in() {
                    let _ = done_tx.send(()).await;
                }
            }
        });
    if let Some(number) = phone {
        let digits: String = number.chars().filter(|c| c.is_ascii_digit()).collect();
        builder = builder.with_pair_code(PairCodeOptions {
            phone_number: digits,
            ..Default::default()
        });
    }
    let bot = builder.build().await.map_err(|e| anyhow!("build bot: {e}"))?;
    tokio::select! {
        _ = bot.run() => bail!("connection ended before linking finished"),
        _ = done_rx.recv() => {}
        _ = tokio::time::sleep(std::time::Duration::from_secs(300)) => bail!("not linked within 5 minutes"),
    }
    // Give the post-pair handshake a moment to persist keys before the process exits.
    tokio::time::sleep(std::time::Duration::from_secs(3)).await;
    let _ = std::fs::write(state_dir().join("linked"), b"");
    eprintln!("linked; session stored in {}", state_dir().display());
    Ok(())
}

/// Remove this companion from the account (the phone's Linked devices list) and forget the keys.
pub async fn unlink() -> Result<()> {
    if UnixStream::connect(socket_path()).await.is_ok() {
        bail!("stop the daemon first (systemctl --user stop wa-call)");
    }
    let store = open_store(&state_dir()).await?;
    let (tx, mut rx) = tokio::sync::mpsc::channel::<Arc<Client>>(1);
    let bot = Bot::builder()
        .with_backend(store)
        .with_device_props(device_props())
        .skip_history_sync()
        .on_qr_code(|_c, _t| async {})
        .on_connected(move |client| {
            let tx = tx.clone();
            async move {
                let _ = tx.send(client).await;
            }
        })
        .build()
        .await
        .map_err(|e| anyhow!("build bot: {e}"))?;
    let client = tokio::select! {
        _ = bot.run() => bail!("connection ended before logout"),
        c = rx.recv() => c.context("no client")?,
        _ = tokio::time::sleep(std::time::Duration::from_secs(60)) => bail!("could not connect within 60s"),
    };
    if !client.is_logged_in() {
        eprintln!("not linked; nothing to remove on the account side");
    } else {
        let report = client.logout().await;
        eprintln!("logout: {:?}", report.deregistration);
    }
    let _ = std::fs::remove_file(state_dir().join("linked"));
    Ok(())
}

fn print_qr(code: &str) {
    let rendered = std::process::Command::new("qrencode")
        .args(["-t", "ansiutf8", code])
        .status()
        .map(|s| s.success())
        .unwrap_or(false);
    if !rendered {
        eprintln!("(install qrencode to see the QR here; or render this payload)\n{code}");
    }
    let _ = std::fs::write(state_dir().join("pairing-qr.txt"), code);
}

#[derive(Deserialize)]
struct Request {
    op: String,
    call_id: Option<String>,
    to: Option<String>,
    policy: Option<IncomingPolicy>,
    #[serde(default)]
    spec: Option<CallSpec>,
}

pub async fn run(policy: IncomingPolicy, default_spec: CallSpec) -> Result<()> {
    let dir = state_dir();
    let _ = LOG.set(dir.join("calls.jsonl"));
    let (tx, _) = broadcast::channel(256);
    let _ = EVENTS.set(tx);

    let sock = socket_path();
    if UnixStream::connect(&sock).await.is_ok() {
        bail!("another wa-call daemon is already serving {}", sock.display());
    }
    let _ = std::fs::remove_file(&sock);
    let listener = UnixListener::bind(&sock).with_context(|| format!("binding {}", sock.display()))?;
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&sock, std::fs::Permissions::from_mode(0o600))?;
    }

    let audio = AudioMode::from_env()?;
    let store = open_store(&dir).await?;
    let bot = Bot::builder()
        .with_backend(store)
        .with_device_props(device_props())
        .skip_history_sync()
        .on_qr_code(|_code, _timeout| async move {
            warn!("not linked: run `wa-call link` first (stop the daemon while linking)");
            emit("unlinked", json!({}));
        })
        .on_connected(|_client| async {
            info!("✅ connected");
            emit("online", json!({}));
        })
        .on_logged_out(|info| async move {
            warn!("logged out by the phone: {info:?}");
            emit("logged-out", json!({}));
        })
        .build()
        .await
        .map_err(|e| anyhow!("build bot: {e}"))?;
    let client = bot.client();
    let observer = Arc::new(CallObserver::new(client.clone(), audio, policy, default_spec));
    let _sub = client.subscribe_handler(observer.clone());
    info!(
        "wa-call daemon on {} (incoming: {policy:?}, audio {})",
        sock.display(),
        audio.name()
    );

    let accept_loop = {
        let observer = observer.clone();
        async move {
            loop {
                match listener.accept().await {
                    Ok((stream, _)) => {
                        let observer = observer.clone();
                        tokio::spawn(async move {
                            if let Err(e) = serve(stream, observer).await {
                                warn!("control connection: {e:#}");
                            }
                        });
                    }
                    Err(e) => warn!("accept: {e}"),
                }
            }
        }
    };

    tokio::select! {
        _ = bot.run() => warn!("WhatsApp connection loop ended"),
        _ = accept_loop => {}
        _ = shutdown() => info!("shutting down"),
    }
    let _ = std::fs::remove_file(&sock);
    Ok(())
}

async fn shutdown() {
    use tokio::signal::unix::{SignalKind, signal};
    let mut term = signal(SignalKind::terminate()).expect("SIGTERM handler");
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        _ = term.recv() => {}
    }
}

async fn serve(stream: UnixStream, obs: Arc<CallObserver>) -> Result<()> {
    let (r, mut w) = stream.into_split();
    let mut lines = BufReader::new(r).lines();
    while let Some(line) = lines.next_line().await? {
        if line.trim().is_empty() {
            continue;
        }
        let req: Request = match serde_json::from_str(&line) {
            Ok(r) => r,
            Err(e) => {
                reply(&mut w, Err(anyhow!("bad request: {e}"))).await?;
                continue;
            }
        };
        if req.op == "events" {
            reply(&mut w, Ok(json!({}))).await?;
            let mut rx = EVENTS.get().context("event bus not ready")?.subscribe();
            loop {
                match rx.recv().await {
                    Ok(ev) => {
                        w.write_all(format!("{ev}\n").as_bytes()).await?;
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        w.write_all(format!("{}\n", json!({"event": "lagged", "missed": n})).as_bytes())
                            .await?;
                    }
                    Err(broadcast::error::RecvError::Closed) => return Ok(()),
                }
            }
        }
        let res = handle(&obs, req).await;
        reply(&mut w, res).await?;
    }
    Ok(())
}

async fn reply(w: &mut tokio::net::unix::OwnedWriteHalf, res: Result<Value>) -> Result<()> {
    let v = match res {
        Ok(mut v) => {
            v["ok"] = json!(true);
            v
        }
        Err(e) => json!({"ok": false, "error": format!("{e:#}")}),
    };
    w.write_all(format!("{v}\n").as_bytes()).await?;
    Ok(())
}

async fn handle(obs: &Arc<CallObserver>, req: Request) -> Result<Value> {
    let id = req.call_id.as_deref();
    let spec = || {
        req.spec
            .clone()
            .unwrap_or_else(|| obs.default_spec.lock().unwrap_or_else(|p| p.into_inner()).clone())
    };
    match req.op.as_str() {
        "status" => {
            let mut v = engine::status(&obs.state);
            v["logged_in"] = json!(obs.client.is_logged_in());
            v["policy"] = json!(*obs.policy.lock().unwrap_or_else(|p| p.into_inner()));
            Ok(v)
        }
        "dial" => {
            let to = req.to.as_deref().context("dial needs `to`")?;
            let peer = parse_peer(to)?;
            if !obs.client.is_logged_in() {
                bail!("not linked/online yet");
            }
            let cid = engine::dial(&obs.client, obs.audio, &obs.state, &peer, spec()).await?;
            Ok(json!({"call_id": cid}))
        }
        "answer" => {
            let cid = engine::answer_pending(&obs.client, obs.audio, &obs.state, id, spec()).await?;
            Ok(json!({"call_id": cid}))
        }
        "reject" => {
            let cid = engine::reject_pending(&obs.client, &obs.state, id).await?;
            Ok(json!({"call_id": cid}))
        }
        "hangup" => {
            let cid = engine::hangup(&obs.state, id).await?;
            Ok(json!({"call_id": cid}))
        }
        "video" => {
            let what = engine::toggle_video(&obs.state, id, &spec()).await?;
            Ok(json!({"result": what}))
        }
        "policy" => {
            if let Some(p) = req.policy {
                *obs.policy.lock().unwrap_or_else(|p| p.into_inner()) = p;
            }
            if let Some(s) = req.spec.clone() {
                *obs.default_spec.lock().unwrap_or_else(|p| p.into_inner()) = s;
            }
            Ok(json!({
                "policy": *obs.policy.lock().unwrap_or_else(|p| p.into_inner()),
                "spec": obs.default_spec.lock().unwrap_or_else(|p| p.into_inner()).clone(),
            }))
        }
        other => bail!("unknown op {other:?}"),
    }
}
