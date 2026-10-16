//! Local CLI control channel.
//!
//! Lets `handy <subcommand>` drive a *running* Handy instance from the command
//! line (see `cli.rs` for the subcommands). The running app listens on a
//! loopback TCP socket bound to an ephemeral port; the port and a per-launch
//! token are written to `cli-control.json` in the app data dir. A CLI process
//! reads that file, connects, sends one line of JSON, and reads one line of
//! JSON back — so every command is acknowledged: the CLI only reports success
//! after the running app has actually applied the change (or reports the exact
//! error and exits nonzero).
//!
//! Why a small socket instead of `tauri_plugin_single_instance` arg-forwarding
//! (which `--toggle-transcription` etc. already use): that path is one-way and
//! cannot acknowledge, so a failed apply would be silently swallowed and
//! `handy model download <id>` could not report whether the download actually
//! succeeded. The socket is loopback-only and token-gated.

use crate::audio_toolkit::audio::list_input_devices;
use crate::managers::audio::AudioRecordingManager;
use crate::managers::model::ModelManager;
use crate::settings::{get_settings, write_settings};
use crate::transcription_coordinator::PipelineStage;
use crate::TranscriptionCoordinator;
use log::{debug, error, warn};
use serde::{Deserialize, Serialize};
use std::io::{BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, Sender};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tauri::{AppHandle, Manager};

/// Tauri bundle identifier — used to locate the (non-portable) app data dir
/// without an `AppHandle`, so the CLI client and the app agree on the path.
const APP_IDENTIFIER: &str = "com.pais.handy";
const CONTROL_FILE: &str = "cli-control.json";

/// Whether microphone listening should end up on, off, or flipped.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, clap::ValueEnum)]
#[serde(rename_all = "lowercase")]
pub enum ListenState {
    On,
    Off,
    Toggle,
}

/// A single command the CLI sends to the running app.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "kebab-case")]
pub enum ControlCommand {
    /// Turn microphone listening on/off (or toggle it). With `submit`, the
    /// stop is followed by the paste and then Handy's own auto-submit key
    /// (even if the global `auto_submit` setting is off); the reply only comes
    /// back once the paste has finished. `submit` is an error unless this
    /// command actually stops a running recording.
    Listen {
        state: ListenState,
        #[serde(default, skip_serializing_if = "is_false")]
        submit: bool,
    },
    /// Report `{listening, stage, mic}` without changing anything.
    Status,
    /// Keep the connection open and stream one JSON line per state change
    /// (see [`ControlEvent`]) until the client disconnects.
    Subscribe,
    /// Select the active input device by its exact name ("default" clears it).
    SetMicrophone { name: String },
    /// Download a transcription model by id.
    DownloadModel { id: String },
}

fn is_false(b: &bool) -> bool {
    !*b
}

#[derive(Serialize, Deserialize)]
struct Request {
    token: String,
    #[serde(flatten)]
    command: ControlCommand,
}

#[derive(Serialize, Deserialize)]
struct Response {
    ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    message: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
    /// Structured payload (e.g. the `status` object), when a command has one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    data: Option<serde_json::Value>,
}

/// Successful result of a command: a human-readable line plus optional data.
struct Reply {
    message: String,
    data: Option<serde_json::Value>,
}

impl Reply {
    fn message(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            data: None,
        }
    }
}

// ---------------------------------------------------------------------------
// State events + one-shot submit request (shared by coordinator, actions,
// clipboard and the control server)
// ---------------------------------------------------------------------------

/// A pipeline state change streamed to `subscribe` clients as one JSON line.
///
/// `event` is one of `recording-started`, `recording-stopped`, `processing`,
/// `idle`, `pasted` (or `ping`, a keepalive that carries no state change).
/// `stage` is the pipeline stage (`idle` | `recording` | `processing`) once the
/// event has happened; `ts` is unix milliseconds.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ControlEvent {
    pub event: String,
    pub stage: String,
    pub ts: u64,
}

impl ControlEvent {
    fn new(event: &str, stage: PipelineStage) -> Self {
        Self {
            event: event.to_string(),
            stage: stage.as_str().to_string(),
            ts: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0),
        }
    }
}

static SUBSCRIBERS: Mutex<Vec<Sender<ControlEvent>>> = Mutex::new(Vec::new());

/// Register a new event listener. Dropping the receiver unsubscribes it (the
/// sender is pruned on the next publish).
fn subscribe() -> Receiver<ControlEvent> {
    let (tx, rx) = mpsc::channel();
    if let Ok(mut subs) = SUBSCRIBERS.lock() {
        subs.push(tx);
    }
    rx
}

fn publish(event: ControlEvent) {
    if let Ok(mut subs) = SUBSCRIBERS.lock() {
        subs.retain(|tx| tx.send(event.clone()).is_ok());
    }
}

/// Called by the transcription coordinator whenever its stage changes.
pub fn publish_stage_change(stage: PipelineStage) {
    match stage {
        PipelineStage::Recording => publish(ControlEvent::new("recording-started", stage)),
        PipelineStage::Processing => {
            publish(ControlEvent::new("recording-stopped", stage));
            publish(ControlEvent::new("processing", stage));
        }
        PipelineStage::Idle => publish(ControlEvent::new("idle", stage)),
    }
}

/// Called after a transcription has been pasted (the pipeline is still in the
/// processing stage at that point; `idle` follows).
pub fn publish_pasted() {
    publish(ControlEvent::new("pasted", PipelineStage::Processing));
}

/// One-shot "send the auto-submit key after the next paste" request, set by
/// `listen off --submit` and consumed by `clipboard::paste`.
static SUBMIT_REQUESTED: AtomicBool = AtomicBool::new(false);

/// Consume the pending submit request (true at most once per request).
pub(crate) fn take_submit_request() -> bool {
    SUBMIT_REQUESTED.swap(false, Ordering::SeqCst)
}

/// What actually happened to a requested submit, recorded by `clipboard::paste`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum SubmitOutcome {
    /// Paste finished and the submit key was sent.
    Sent,
    /// Reliable paste accepted the paste and will send the submit key itself.
    Queued,
    PasteFailed(String),
    SendFailed(String),
}

static SUBMIT_OUTCOME: Mutex<Option<SubmitOutcome>> = Mutex::new(None);

/// Record the outcome of a requested submit; the first one recorded wins.
pub(crate) fn record_submit_outcome(outcome: SubmitOutcome) {
    if let Ok(mut slot) = SUBMIT_OUTCOME.lock() {
        slot.get_or_insert(outcome);
    }
}

/// How long to wait for the stop to take effect, and for the whole
/// stop -> transcribe -> paste pipeline to finish.
const SUBMIT_STOP_TIMEOUT: Duration = Duration::from_secs(5);
const SUBMIT_PIPELINE_TIMEOUT: Duration = Duration::from_secs(120);

/// Wait for a requested stop to run through the pipeline and report what the
/// paste actually did with the submit. Always clears `pending` and `outcome`,
/// so a stop that produced nothing to paste can never leak a submit (or a
/// stale outcome) into the next dictation.
fn await_submit(
    rx: &Receiver<ControlEvent>,
    pending: &AtomicBool,
    outcome: &Mutex<Option<SubmitOutcome>>,
    stop_timeout: Duration,
    pipeline_timeout: Duration,
) -> Result<&'static str, String> {
    let result = wait_for_pipeline(rx, stop_timeout, pipeline_timeout);
    pending.store(false, Ordering::SeqCst);
    let recorded = outcome.lock().ok().and_then(|mut slot| slot.take());
    result?;
    match recorded {
        Some(SubmitOutcome::Sent) => Ok("listening stopped; pasted and submitted"),
        Some(SubmitOutcome::Queued) => Ok("listening stopped; pasted, submit queued"),
        Some(SubmitOutcome::PasteFailed(e)) => Err(format!("paste failed, submit not sent: {e}")),
        Some(SubmitOutcome::SendFailed(e)) => {
            Err(format!("pasted, but sending submit failed: {e}"))
        }
        None => Err("nothing was pasted, so submit was not sent".to_string()),
    }
}

/// Wait for the stop to take effect and the pipeline to return to idle.
fn wait_for_pipeline(
    rx: &Receiver<ControlEvent>,
    stop_timeout: Duration,
    pipeline_timeout: Duration,
) -> Result<(), String> {
    let stopped = wait_for_event(rx, &["recording-stopped", "idle"], stop_timeout)
        .map_err(|_| "stop was not applied (no state change)".to_string())?;
    if stopped.event == "idle" {
        return Err("recording was cancelled before it was transcribed".to_string());
    }
    wait_for_event(rx, &["idle"], pipeline_timeout)
        .map_err(|_| "timed out waiting for the transcription to finish".to_string())?;
    Ok(())
}

fn wait_for_event(
    rx: &Receiver<ControlEvent>,
    names: &[&str],
    timeout: Duration,
) -> Result<ControlEvent, ()> {
    let deadline = std::time::Instant::now() + timeout;
    loop {
        let remaining = deadline.saturating_duration_since(std::time::Instant::now());
        match rx.recv_timeout(remaining) {
            Ok(e) if names.contains(&e.event.as_str()) => return Ok(e),
            Ok(_) => continue,
            Err(_) => return Err(()),
        }
    }
}

// ---------------------------------------------------------------------------
// Control-file path (shared by server and client)
// ---------------------------------------------------------------------------

/// Directory that holds the control file. Portable-aware, and otherwise derived
/// from OS conventions + the bundle identifier so the CLI client — which has no
/// `AppHandle` — resolves the same path the running app wrote.
fn control_dir() -> Option<PathBuf> {
    if let Some(dir) = crate::portable::data_dir() {
        return Some(dir.clone());
    }

    #[cfg(target_os = "macos")]
    {
        let home = std::env::var_os("HOME")?;
        Some(
            PathBuf::from(home)
                .join("Library/Application Support")
                .join(APP_IDENTIFIER),
        )
    }
    #[cfg(target_os = "linux")]
    {
        if let Some(xdg) = std::env::var_os("XDG_DATA_HOME") {
            Some(PathBuf::from(xdg).join(APP_IDENTIFIER))
        } else {
            let home = std::env::var_os("HOME")?;
            Some(
                PathBuf::from(home)
                    .join(".local/share")
                    .join(APP_IDENTIFIER),
            )
        }
    }
    #[cfg(target_os = "windows")]
    {
        let appdata = std::env::var_os("APPDATA")?;
        Some(PathBuf::from(appdata).join(APP_IDENTIFIER))
    }
}

fn control_file_path() -> Option<PathBuf> {
    Some(control_dir()?.join(CONTROL_FILE))
}

// ---------------------------------------------------------------------------
// Server (runs inside the app)
// ---------------------------------------------------------------------------

/// Start the CLI control listener. Binds a loopback TCP socket on an ephemeral
/// port, records the port + a fresh token in the control file, and serves
/// commands on a background thread. Best-effort: a bind/write failure is logged
/// and the app continues normally (the CLI just reports "not running").
pub fn start(app: AppHandle) {
    let listener = match TcpListener::bind(("127.0.0.1", 0)) {
        Ok(l) => l,
        Err(e) => {
            error!("CLI control: failed to bind loopback socket: {e}");
            return;
        }
    };

    let port = match listener.local_addr() {
        Ok(addr) => addr.port(),
        Err(e) => {
            error!("CLI control: failed to read local addr: {e}");
            return;
        }
    };

    let token = match generate_token() {
        Ok(t) => t,
        Err(e) => {
            error!("CLI control: failed to generate token: {e}");
            return;
        }
    };
    if let Err(e) = write_control_file(port, &token) {
        error!("CLI control: failed to write control file: {e}");
        return;
    }

    debug!("CLI control listening on 127.0.0.1:{port}");

    std::thread::spawn(move || {
        for stream in listener.incoming() {
            match stream {
                Ok(stream) => {
                    let app = app.clone();
                    let token = token.clone();
                    std::thread::spawn(move || {
                        if let Err(e) = handle_connection(stream, &app, &token) {
                            warn!("CLI control: connection error: {e}");
                        }
                    });
                }
                Err(e) => warn!("CLI control: accept failed: {e}"),
            }
        }
    });
}

/// Remove the control file (best-effort) on app shutdown so a stale port is not
/// left behind for the next launch to trip over.
pub fn remove_control_file() {
    if let Some(path) = control_file_path() {
        let _ = std::fs::remove_file(path);
    }
}

fn handle_connection(stream: TcpStream, app: &AppHandle, token: &str) -> std::io::Result<()> {
    // Bound the request read so a stalled/malicious client cannot pin a thread.
    // The command work below (which may be a long download) runs with no read
    // timeout, and the response write is unaffected.
    stream.set_read_timeout(Some(Duration::from_secs(10)))?;

    // Requests are a single short JSON line; cap the read so a client cannot
    // stream unbounded data without a newline and pin memory on this thread.
    use std::io::Read;
    const MAX_REQUEST_BYTES: u64 = 64 * 1024;
    let mut reader = BufReader::new(stream.try_clone()?.take(MAX_REQUEST_BYTES));
    let mut line = String::new();
    reader.read_line(&mut line)?;

    let request = serde_json::from_str::<Request>(line.trim());

    // `subscribe` keeps the connection open instead of sending one reply.
    if let Ok(Request {
        token: received,
        command: ControlCommand::Subscribe,
    }) = &request
    {
        if tokens_match(received, token) {
            return serve_events(stream, app);
        }
    }

    let response = match request {
        Ok(req) if tokens_match(&req.token, token) => match dispatch(app, req.command) {
            Ok(reply) => Response {
                ok: true,
                message: Some(reply.message),
                error: None,
                data: reply.data,
            },
            Err(error) => Response {
                ok: false,
                message: None,
                error: Some(error),
                data: None,
            },
        },
        Ok(_) => Response {
            ok: false,
            message: None,
            error: Some("authentication failed".to_string()),
            data: None,
        },
        Err(e) => Response {
            ok: false,
            message: None,
            error: Some(format!("malformed request: {e}")),
            data: None,
        },
    };

    let mut stream = stream;
    stream.set_read_timeout(None)?;
    let mut payload = serde_json::to_string(&response)
        .unwrap_or_else(|_| r#"{"ok":false,"error":"failed to serialize response"}"#.to_string());
    payload.push('\n');
    stream.write_all(payload.as_bytes())?;
    stream.flush()?;
    Ok(())
}

/// How often an idle event stream sends a `ping`, so a vanished client is
/// noticed (the write fails) instead of pinning its thread until the next
/// real event.
const EVENT_KEEPALIVE: Duration = Duration::from_secs(30);

/// Serve a `subscribe` connection: acknowledge with the current status, then
/// write one [`ControlEvent`] JSON line per state change until the client goes
/// away.
fn serve_events(mut stream: TcpStream, app: &AppHandle) -> std::io::Result<()> {
    stream.set_read_timeout(None)?;
    // Subscribe before taking the snapshot so no change falls in the gap.
    let rx = subscribe();
    let snapshot = status_snapshot(app).map_err(std::io::Error::other)?;
    let ack = Response {
        ok: true,
        message: Some("subscribed".to_string()),
        error: None,
        data: Some(snapshot),
    };
    write_line(&mut stream, &ack)?;

    loop {
        let event = match rx.recv_timeout(EVENT_KEEPALIVE) {
            Ok(event) => event,
            Err(RecvTimeoutError::Timeout) => {
                let stage = current_stage(app);
                ControlEvent::new("ping", stage)
            }
            Err(RecvTimeoutError::Disconnected) => return Ok(()),
        };
        write_line(&mut stream, &event)?;
    }
}

fn write_line<T: Serialize>(stream: &mut TcpStream, value: &T) -> std::io::Result<()> {
    let mut payload = serde_json::to_string(value).map_err(std::io::Error::other)?;
    payload.push('\n');
    stream.write_all(payload.as_bytes())?;
    stream.flush()
}

fn current_stage(app: &AppHandle) -> PipelineStage {
    app.try_state::<TranscriptionCoordinator>()
        .map(|c| c.stage())
        .unwrap_or(PipelineStage::Idle)
}

/// `{listening, stage, mic}`: `listening` is whether the microphone is
/// recording right now, `stage` the pipeline stage (`idle` | `recording` |
/// `processing`), `mic` the selected input device name (`null` = system
/// default).
fn status_snapshot(app: &AppHandle) -> Result<serde_json::Value, String> {
    let rm = app
        .try_state::<Arc<AudioRecordingManager>>()
        .ok_or_else(|| "audio manager not initialized".to_string())?;
    Ok(serde_json::json!({
        "listening": rm.is_recording(),
        "stage": current_stage(app).as_str(),
        "mic": get_settings(app).selected_microphone,
    }))
}

/// Apply a command to the running app. Returns a human-readable success message
/// or an error string; the error is relayed verbatim to the CLI, which exits
/// nonzero.
fn dispatch(app: &AppHandle, command: ControlCommand) -> Result<Reply, String> {
    match command {
        ControlCommand::Status => {
            let data = status_snapshot(app)?;
            Ok(Reply {
                message: data.to_string(),
                data: Some(data),
            })
        }
        // Handled in `handle_connection` before dispatch (it streams).
        ControlCommand::Subscribe => Err("subscribe is a streaming command".to_string()),
        ControlCommand::Listen { state, submit } => {
            let rm = app
                .try_state::<Arc<AudioRecordingManager>>()
                .ok_or_else(|| "audio manager not initialized".to_string())?;
            let coordinator = app
                .try_state::<TranscriptionCoordinator>()
                .ok_or_else(|| "transcription coordinator not initialized".to_string())?;

            let recording = rm.is_recording();
            let (should_toggle, message) = match state {
                ListenState::On if recording => (false, "already listening"),
                ListenState::On => (true, "listening started"),
                ListenState::Off if recording => (true, "listening stopped"),
                ListenState::Off => (false, "already stopped"),
                ListenState::Toggle if recording => (true, "listening stopped"),
                ListenState::Toggle => (true, "listening started"),
            };

            let stops = should_toggle && recording;
            if submit && !stops {
                return Err(if recording {
                    "--submit only applies when stopping; listening is already on".to_string()
                } else {
                    "--submit needs an active recording to stop; nothing to submit".to_string()
                });
            }

            if submit {
                // Subscribe and arm the submit request before the stop press so
                // neither the events nor the paste can slip past us.
                let rx = subscribe();
                if let Ok(mut slot) = SUBMIT_OUTCOME.lock() {
                    *slot = None;
                }
                SUBMIT_REQUESTED.store(true, Ordering::SeqCst);
                coordinator.send_input("transcribe", "CLI", true, false);
                return await_submit(
                    &rx,
                    &SUBMIT_REQUESTED,
                    &SUBMIT_OUTCOME,
                    SUBMIT_STOP_TIMEOUT,
                    SUBMIT_PIPELINE_TIMEOUT,
                )
                .map(Reply::message);
            }

            if should_toggle {
                // Mirror the signal/CLI toggle path: a single press event with
                // push_to_talk = false flips the transcribe binding.
                coordinator.send_input("transcribe", "CLI", true, false);
            }
            Ok(Reply::message(message))
        }
        ControlCommand::SetMicrophone { name } => {
            // Resolve the manager before touching persisted settings so a missing
            // manager never leaves a device selection that was never applied.
            let rm = app
                .try_state::<Arc<AudioRecordingManager>>()
                .ok_or_else(|| "audio manager not initialized".to_string())?;

            let previous = get_settings(app).selected_microphone.clone();
            let mut settings = get_settings(app);
            settings.selected_microphone = if name == "default" {
                None
            } else {
                Some(name.clone())
            };
            write_settings(app, settings);

            // Apply live: update_selected_device restarts the cpal stream. We are
            // on a dedicated connection thread, so this blocking call is fine.
            if let Err(e) = rm.update_selected_device() {
                // Roll back so a failed apply does not survive a restart.
                let mut restored = get_settings(app);
                restored.selected_microphone = previous;
                write_settings(app, restored);
                return Err(format!("failed to apply device: {e}"));
            }

            Ok(Reply::message(format!("microphone set to \"{name}\"")))
        }
        ControlCommand::DownloadModel { id } => {
            let model_manager = app
                .try_state::<Arc<ModelManager>>()
                .ok_or_else(|| "model manager not initialized".to_string())?
                .inner()
                .clone();

            // Block this connection thread until the download finishes, so the
            // CLI only reports success once the model is actually on disk.
            tauri::async_runtime::block_on(model_manager.download_model(&id))
                .map_err(|e| format!("download failed: {e}"))?;

            Ok(Reply::message(format!("model \"{id}\" downloaded")))
        }
    }
}

// ---------------------------------------------------------------------------
// Control-file read/write + token
// ---------------------------------------------------------------------------

fn write_control_file(port: u16, token: &str) -> std::io::Result<()> {
    let path = control_file_path().ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "could not resolve control file path",
        )
    })?;
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let json = serde_json::json!({ "port": port, "token": token }).to_string();
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        // Create with 0600 from the outset so the token is never briefly
        // world-readable between the write and a follow-up chmod.
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o600)
            .open(&path)?;
        file.write_all(json.as_bytes())?;
        // An existing file keeps its old mode through OpenOptions, so enforce it.
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))?;
    }
    #[cfg(not(unix))]
    std::fs::write(&path, json)?;
    Ok(())
}

fn read_control_file() -> Result<(u16, String), String> {
    let path =
        control_file_path().ok_or_else(|| "could not resolve control file path".to_string())?;
    let data = std::fs::read_to_string(&path)
        .map_err(|e| format!("control file not readable at {} ({e})", path.display()))?;
    let value: serde_json::Value =
        serde_json::from_str(&data).map_err(|e| format!("control file is not valid JSON: {e}"))?;
    let port = value
        .get("port")
        .and_then(|p| p.as_u64())
        .ok_or_else(|| "control file missing \"port\"".to_string())? as u16;
    let token = value
        .get("token")
        .and_then(|t| t.as_str())
        .ok_or_else(|| "control file missing \"token\"".to_string())?
        .to_string();
    Ok((port, token))
}

/// Generate a per-launch token from the OS CSPRNG (16 random bytes, hex-encoded
/// to 32 chars). The token lives in a 0600 file readable only by the user and
/// gates the loopback socket. Fails loudly if the CSPRNG is unavailable rather
/// than falling back to predictable entropy.
fn generate_token() -> std::io::Result<String> {
    let mut bytes = [0u8; 16];
    getrandom::fill(&mut bytes).map_err(std::io::Error::other)?;
    Ok(bytes.iter().map(|b| format!("{b:02x}")).collect())
}

/// Constant-time token comparison so a timing side channel cannot be used to
/// recover the token byte by byte. `ct_eq` on slices already returns false for
/// mismatched lengths.
fn tokens_match(received: &str, expected: &str) -> bool {
    use subtle::ConstantTimeEq;
    received.as_bytes().ct_eq(expected.as_bytes()).into()
}

// ---------------------------------------------------------------------------
// Client (runs as a short-lived `handy <subcommand>` process)
// ---------------------------------------------------------------------------

/// Exit codes for the CLI client.
const EXIT_OK: i32 = 0;
const EXIT_COMMAND_FAILED: i32 = 1; // app reached, but the command failed
const EXIT_BAD_USAGE: i32 = 2; // client-side validation failed (e.g. mic ambiguity)
const EXIT_NOT_RUNNING: i32 = 3; // could not reach a running app

/// Resolve a microphone query to exactly one device, then set it. If the query
/// matches zero or multiple devices, print every candidate (name + index) to
/// stderr and exit nonzero — never pick arbitrarily.
pub fn run_mic(query: &str) -> i32 {
    if query.eq_ignore_ascii_case("default") {
        return run(&ControlCommand::SetMicrophone {
            name: "default".to_string(),
        });
    }

    let devices = match list_input_devices() {
        Ok(devices) => devices,
        Err(e) => {
            eprintln!("error: failed to enumerate input devices: {e}");
            return EXIT_BAD_USAGE;
        }
    };

    // Prefer an exact (case-insensitive) name match; fall back to substring.
    let exact: Vec<_> = devices
        .iter()
        .filter(|d| d.name.eq_ignore_ascii_case(query))
        .collect();
    let candidates: Vec<_> = if !exact.is_empty() {
        exact
    } else {
        let needle = query.to_lowercase();
        devices
            .iter()
            .filter(|d| d.name.to_lowercase().contains(&needle))
            .collect()
    };

    match candidates.as_slice() {
        [] => {
            eprintln!("error: no input device matches \"{query}\"");
            eprintln!("available input devices:");
            for d in &devices {
                eprintln!("  {}  (index {})", d.name, d.index);
            }
            EXIT_BAD_USAGE
        }
        [only] => run(&ControlCommand::SetMicrophone {
            name: only.name.clone(),
        }),
        many => {
            eprintln!(
                "error: \"{query}\" matches {} input devices; be more specific:",
                many.len()
            );
            for d in many {
                eprintln!("  {}  (index {})", d.name, d.index);
            }
            EXIT_BAD_USAGE
        }
    }
}

/// A client-side failure to reach the running app (connect/write/read/parse).
/// Distinct from a command failure the app reports back in a `Response`.
#[derive(Debug)]
struct TransportError(String);

/// Send one request to the app on `port`/`token` and return its parsed
/// response. Errors here are transport-level (the app was unreachable or spoke
/// a malformed reply); a failed *command* comes back as `Response { ok: false }`.
fn send_request(
    port: u16,
    token: String,
    command: &ControlCommand,
) -> Result<Response, TransportError> {
    let stream = TcpStream::connect(("127.0.0.1", port))
        .map_err(|e| TransportError(format!("could not connect on 127.0.0.1:{port}: {e}")))?;

    let request = Request {
        token,
        command: command.clone(),
    };
    let mut payload =
        serde_json::to_string(&request).map_err(|e| TransportError(format!("encode: {e}")))?;
    payload.push('\n');

    let mut writer = stream
        .try_clone()
        .map_err(|e| TransportError(format!("socket: {e}")))?;
    writer
        .write_all(payload.as_bytes())
        .and_then(|_| writer.flush())
        .map_err(|e| TransportError(format!("send: {e}")))?;

    let mut reader = BufReader::new(stream);
    let mut line = String::new();
    reader
        .read_line(&mut line)
        .map_err(|e| TransportError(format!("read: {e}")))?;

    serde_json::from_str::<Response>(line.trim())
        .map_err(|e| TransportError(format!("malformed response: {e}")))
}

/// Connect to the running app, send one command, and print the acknowledged
/// result. Returns a process exit code.
pub fn run(command: &ControlCommand) -> i32 {
    // Match the running app's control-file path resolution (portable mode is
    // detected from the marker next to this same executable).
    crate::portable::init();

    let (port, token) = match read_control_file() {
        Ok(v) => v,
        Err(e) => {
            eprintln!("error: Handy does not appear to be running: {e}");
            return EXIT_NOT_RUNNING;
        }
    };

    match send_request(port, token, command) {
        Ok(resp) if resp.ok => {
            if let Some(message) = resp.message {
                println!("{message}");
            }
            EXIT_OK
        }
        Ok(resp) => {
            eprintln!(
                "error: {}",
                resp.error.unwrap_or_else(|| "command failed".to_string())
            );
            EXIT_COMMAND_FAILED
        }
        Err(TransportError(e)) => {
            eprintln!("error: could not reach running Handy: {e}");
            EXIT_NOT_RUNNING
        }
    }
}

/// Subscribe to the running app's state events and print each JSON line to
/// stdout (flushed) until the app closes the connection. The first line is the
/// acknowledgment carrying the current status.
pub fn run_events() -> i32 {
    crate::portable::init();

    let (port, token) = match read_control_file() {
        Ok(v) => v,
        Err(e) => {
            eprintln!("error: Handy does not appear to be running: {e}");
            return EXIT_NOT_RUNNING;
        }
    };

    let stream = match TcpStream::connect(("127.0.0.1", port)) {
        Ok(s) => s,
        Err(e) => {
            eprintln!(
                "error: could not reach running Handy: could not connect on 127.0.0.1:{port}: {e}"
            );
            return EXIT_NOT_RUNNING;
        }
    };
    let request = Request {
        token,
        command: ControlCommand::Subscribe,
    };
    let sent = serde_json::to_string(&request)
        .map_err(|e| e.to_string())
        .and_then(|mut payload| {
            payload.push('\n');
            let mut writer = &stream;
            writer
                .write_all(payload.as_bytes())
                .and_then(|_| writer.flush())
                .map_err(|e| e.to_string())
        });
    if let Err(e) = sent {
        eprintln!("error: could not reach running Handy: send: {e}");
        return EXIT_NOT_RUNNING;
    }

    let stdout = std::io::stdout();
    let mut first = true;
    for line in BufReader::new(stream).lines() {
        let Ok(line) = line else {
            break;
        };
        if first {
            first = false;
            match serde_json::from_str::<Response>(&line) {
                Ok(resp) if resp.ok => {}
                Ok(resp) => {
                    eprintln!(
                        "error: {}",
                        resp.error.unwrap_or_else(|| "subscribe failed".to_string())
                    );
                    return EXIT_COMMAND_FAILED;
                }
                Err(e) => {
                    eprintln!("error: malformed response: {e}");
                    return EXIT_NOT_RUNNING;
                }
            }
        }
        let mut out = stdout.lock();
        if writeln!(out, "{line}").and_then(|_| out.flush()).is_err() {
            // Downstream (e.g. a closed pipe) went away; stop quietly.
            return EXIT_OK;
        }
    }
    // The app closed the stream (shutdown): that is not a command failure, but
    // a consumer should know the feed ended.
    if first {
        EXIT_NOT_RUNNING
    } else {
        EXIT_OK
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn control_command_round_trips_through_json() {
        let cmd = ControlCommand::Listen {
            state: ListenState::On,
            submit: false,
        };
        let req = Request {
            token: "abc".to_string(),
            command: cmd,
        };
        let json = serde_json::to_string(&req).unwrap();
        assert!(json.contains("\"action\":\"listen\""));
        assert!(json.contains("\"state\":\"on\""));
        assert!(json.contains("\"token\":\"abc\""));
        // `submit: false` is omitted so the wire format is unchanged for plain
        // `listen` requests.
        assert!(!json.contains("submit"));

        let parsed: Request = serde_json::from_str(&json).unwrap();
        assert!(matches!(
            parsed.command,
            ControlCommand::Listen {
                state: ListenState::On,
                submit: false
            }
        ));
    }

    #[test]
    fn download_model_command_serializes_kebab_action() {
        let json = serde_json::to_string(&Request {
            token: "t".to_string(),
            command: ControlCommand::DownloadModel {
                id: "whisper-small".to_string(),
            },
        })
        .unwrap();
        assert!(json.contains("\"action\":\"download-model\""));
        assert!(json.contains("\"id\":\"whisper-small\""));
    }

    #[test]
    fn token_is_16_bytes_of_hex() {
        let token = generate_token().unwrap();
        assert_eq!(token.len(), 32);
        assert!(token.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn tokens_match_is_true_only_on_equal_tokens() {
        let token = generate_token().unwrap();
        assert!(tokens_match(&token, &token));
        assert!(!tokens_match(&token, "0"));
        assert!(!tokens_match(&token, &format!("{token}extra")));
    }

    /// End-to-end wire test over a real loopback socket: a mock server reads the
    /// request, verifies the token, and replies. Exercises `send_request`'s
    /// connect → write → read → parse round-trip without a Tauri app or GUI.
    #[test]
    fn send_request_round_trips_over_loopback() {
        let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
        let port = listener.local_addr().unwrap().port();

        let server = std::thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            let mut reader = BufReader::new(stream.try_clone().unwrap());
            let mut line = String::new();
            reader.read_line(&mut line).unwrap();
            let req: Request = serde_json::from_str(line.trim()).unwrap();
            assert_eq!(req.token, "secret");
            assert!(matches!(req.command, ControlCommand::DownloadModel { .. }));
            let mut out = stream;
            out.write_all(b"{\"ok\":true,\"message\":\"model \\\"x\\\" downloaded\"}\n")
                .unwrap();
            out.flush().unwrap();
        });

        let resp = send_request(
            port,
            "secret".to_string(),
            &ControlCommand::DownloadModel {
                id: "x".to_string(),
            },
        )
        .expect("transport should succeed");
        assert!(resp.ok);
        assert_eq!(resp.message.as_deref(), Some("model \"x\" downloaded"));
        server.join().unwrap();
    }

    #[test]
    fn send_request_errors_when_nothing_listening() {
        // Bind an ephemeral port, record it, then drop the listener so nothing is
        // bound there — deterministic even in a sandbox where low ports may be in
        // use. connect must then fail as a transport error.
        let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);

        let err = send_request(
            port,
            "t".to_string(),
            &ControlCommand::Listen {
                state: ListenState::Off,
                submit: false,
            },
        );
        assert!(err.is_err());
    }

    #[test]
    fn listen_without_submit_field_still_parses() {
        // What an older client sends: no `submit` key at all.
        let parsed: Request =
            serde_json::from_str(r#"{"token":"t","action":"listen","state":"off"}"#).unwrap();
        assert!(matches!(
            parsed.command,
            ControlCommand::Listen {
                state: ListenState::Off,
                submit: false
            }
        ));
    }

    #[test]
    fn listen_submit_and_new_actions_serialize() {
        let json = serde_json::to_string(&Request {
            token: "t".to_string(),
            command: ControlCommand::Listen {
                state: ListenState::Off,
                submit: true,
            },
        })
        .unwrap();
        assert!(json.contains("\"submit\":true"));

        for (cmd, action) in [
            (ControlCommand::Status, "status"),
            (ControlCommand::Subscribe, "subscribe"),
        ] {
            let json = serde_json::to_string(&Request {
                token: "t".to_string(),
                command: cmd,
            })
            .unwrap();
            assert!(json.contains(&format!("\"action\":\"{action}\"")));
        }
    }

    #[test]
    fn response_data_is_omitted_when_absent_and_parsed_when_present() {
        let plain = serde_json::to_string(&Response {
            ok: true,
            message: Some("m".to_string()),
            error: None,
            data: None,
        })
        .unwrap();
        assert!(!plain.contains("data"));

        let with_data: Response = serde_json::from_str(
            r#"{"ok":true,"message":"x","data":{"listening":false,"stage":"idle","mic":null}}"#,
        )
        .unwrap();
        let data = with_data.data.unwrap();
        assert_eq!(data["stage"], "idle");
        assert_eq!(data["listening"], false);
        // Old-style responses (no `data`) still parse.
        let old: Response = serde_json::from_str(r#"{"ok":true,"message":"x"}"#).unwrap();
        assert!(old.data.is_none());
    }

    fn ev(name: &str, stage: PipelineStage) -> ControlEvent {
        ControlEvent::new(name, stage)
    }

    #[test]
    fn stage_changes_publish_events_in_order() {
        let rx = subscribe();
        publish_stage_change(PipelineStage::Recording);
        publish_stage_change(PipelineStage::Processing);
        publish_pasted();
        publish_stage_change(PipelineStage::Idle);

        let names: Vec<String> = std::iter::from_fn(|| rx.try_recv().ok())
            .map(|e| e.event)
            .collect();
        // Other tests share the global bus, so only assert our subsequence.
        let wanted = [
            "recording-started",
            "recording-stopped",
            "processing",
            "pasted",
            "idle",
        ];
        let mut it = names.iter();
        for w in wanted {
            assert!(it.any(|n| n == w), "missing {w} in {names:?}");
        }
    }

    #[test]
    fn event_serializes_with_stage_and_timestamp() {
        let json = serde_json::to_string(&ev("idle", PipelineStage::Idle)).unwrap();
        let v: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(v["event"], "idle");
        assert_eq!(v["stage"], "idle");
        assert!(v["ts"].as_u64().unwrap() > 0);
    }

    #[test]
    fn submit_succeeds_when_submit_key_was_sent() {
        let (tx, rx) = mpsc::channel();
        let pending = AtomicBool::new(true);
        let outcome = Mutex::new(None);
        tx.send(ev("recording-stopped", PipelineStage::Processing))
            .unwrap();
        tx.send(ev("processing", PipelineStage::Processing))
            .unwrap();
        pending.store(false, Ordering::SeqCst);
        *outcome.lock().unwrap() = Some(SubmitOutcome::Sent);
        tx.send(ev("pasted", PipelineStage::Processing)).unwrap();
        tx.send(ev("idle", PipelineStage::Idle)).unwrap();

        let out = await_submit(
            &rx,
            &pending,
            &outcome,
            Duration::from_secs(1),
            Duration::from_secs(1),
        );
        assert_eq!(out, Ok("listening stopped; pasted and submitted"));
    }

    #[test]
    fn submit_fails_and_clears_request_when_nothing_was_pasted() {
        let (tx, rx) = mpsc::channel();
        let pending = AtomicBool::new(true);
        let outcome = Mutex::new(None);
        tx.send(ev("recording-stopped", PipelineStage::Processing))
            .unwrap();
        tx.send(ev("idle", PipelineStage::Idle)).unwrap();

        let out = await_submit(
            &rx,
            &pending,
            &outcome,
            Duration::from_secs(1),
            Duration::from_secs(1),
        );
        assert!(out.unwrap_err().contains("nothing was pasted"));
        assert!(
            !pending.load(Ordering::SeqCst),
            "an unconsumed request must not leak into the next paste"
        );
    }

    #[test]
    fn submit_reports_cancellation_and_clears_request() {
        let (tx, rx) = mpsc::channel();
        let pending = AtomicBool::new(true);
        let outcome = Mutex::new(None);
        tx.send(ev("idle", PipelineStage::Idle)).unwrap();

        let out = await_submit(
            &rx,
            &pending,
            &outcome,
            Duration::from_secs(1),
            Duration::from_secs(1),
        );
        assert!(out.unwrap_err().contains("cancelled"));
        assert!(!pending.load(Ordering::SeqCst));
    }

    #[test]
    fn submit_times_out_when_stop_never_happens() {
        let (_tx, rx) = mpsc::channel::<ControlEvent>();
        let pending = AtomicBool::new(true);
        let outcome = Mutex::new(None);

        let out = await_submit(
            &rx,
            &pending,
            &outcome,
            Duration::from_millis(30),
            Duration::from_millis(30),
        );
        assert!(out.unwrap_err().contains("stop was not applied"));
        assert!(!pending.load(Ordering::SeqCst));
    }

    #[test]
    fn submit_times_out_when_pipeline_never_finishes() {
        let (tx, rx) = mpsc::channel();
        let pending = AtomicBool::new(true);
        let outcome = Mutex::new(None);
        tx.send(ev("recording-stopped", PipelineStage::Processing))
            .unwrap();

        let out = await_submit(
            &rx,
            &pending,
            &outcome,
            Duration::from_secs(1),
            Duration::from_millis(30),
        );
        assert!(out.unwrap_err().contains("timed out"));
        assert!(!pending.load(Ordering::SeqCst));
    }

    fn run_with_outcome(o: Option<SubmitOutcome>) -> (Result<&'static str, String>, bool) {
        let (tx, rx) = mpsc::channel();
        let pending = AtomicBool::new(true);
        let outcome = Mutex::new(o);
        tx.send(ev("recording-stopped", PipelineStage::Processing))
            .unwrap();
        tx.send(ev("idle", PipelineStage::Idle)).unwrap();
        let out = await_submit(
            &rx,
            &pending,
            &outcome,
            Duration::from_secs(1),
            Duration::from_secs(1),
        );
        let cleared = outcome.lock().unwrap().is_none();
        (out, cleared)
    }

    #[test]
    fn submit_errors_when_paste_failed() {
        let (out, cleared) = run_with_outcome(Some(SubmitOutcome::PasteFailed("boom".into())));
        let err = out.unwrap_err();
        assert!(err.contains("paste failed") && err.contains("boom"));
        assert!(cleared);
    }

    #[test]
    fn submit_errors_when_enter_send_failed() {
        let (out, cleared) = run_with_outcome(Some(SubmitOutcome::SendFailed("nokey".into())));
        let err = out.unwrap_err();
        assert!(err.contains("submit failed") && err.contains("nokey"));
        assert!(cleared);
    }

    #[test]
    fn submit_reports_queued_for_reliable_paste() {
        let (out, _) = run_with_outcome(Some(SubmitOutcome::Queued));
        assert_eq!(out, Ok("listening stopped; pasted, submit queued"));
    }

    #[test]
    fn first_recorded_submit_outcome_wins() {
        let slot = Mutex::new(None);
        slot.lock().unwrap().get_or_insert(SubmitOutcome::Sent);
        slot.lock()
            .unwrap()
            .get_or_insert(SubmitOutcome::PasteFailed("late".into()));
        assert_eq!(*slot.lock().unwrap(), Some(SubmitOutcome::Sent));
    }

    #[test]
    fn take_submit_request_is_one_shot() {
        SUBMIT_REQUESTED.store(true, Ordering::SeqCst);
        assert!(take_submit_request());
        assert!(!take_submit_request());
    }
}
