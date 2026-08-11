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
use crate::TranscriptionCoordinator;
use log::{debug, error, warn};
use serde::{Deserialize, Serialize};
use std::io::{BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
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
    /// Turn microphone listening on/off (or toggle it).
    Listen { state: ListenState },
    /// Select the active input device by its exact name ("default" clears it).
    SetMicrophone { name: String },
    /// Download a transcription model by id.
    DownloadModel { id: String },
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

    let response = match serde_json::from_str::<Request>(line.trim()) {
        Ok(req) if tokens_match(&req.token, token) => match dispatch(app, req.command) {
            Ok(message) => Response {
                ok: true,
                message: Some(message),
                error: None,
            },
            Err(error) => Response {
                ok: false,
                message: None,
                error: Some(error),
            },
        },
        Ok(_) => Response {
            ok: false,
            message: None,
            error: Some("authentication failed".to_string()),
        },
        Err(e) => Response {
            ok: false,
            message: None,
            error: Some(format!("malformed request: {e}")),
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

/// Apply a command to the running app. Returns a human-readable success message
/// or an error string; the error is relayed verbatim to the CLI, which exits
/// nonzero.
fn dispatch(app: &AppHandle, command: ControlCommand) -> Result<String, String> {
    match command {
        ControlCommand::Listen { state } => {
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

            if should_toggle {
                // Mirror the signal/CLI toggle path: a single press event with
                // push_to_talk = false flips the transcribe binding.
                coordinator.send_input("transcribe", "CLI", true, false);
            }
            Ok(message.to_string())
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

            Ok(format!("microphone set to \"{name}\""))
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

            Ok(format!("model \"{id}\" downloaded"))
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn control_command_round_trips_through_json() {
        let cmd = ControlCommand::Listen {
            state: ListenState::On,
        };
        let req = Request {
            token: "abc".to_string(),
            command: cmd,
        };
        let json = serde_json::to_string(&req).unwrap();
        assert!(json.contains("\"action\":\"listen\""));
        assert!(json.contains("\"state\":\"on\""));
        assert!(json.contains("\"token\":\"abc\""));

        let parsed: Request = serde_json::from_str(&json).unwrap();
        assert!(matches!(
            parsed.command,
            ControlCommand::Listen {
                state: ListenState::On
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
            },
        );
        assert!(err.is_err());
    }
}
