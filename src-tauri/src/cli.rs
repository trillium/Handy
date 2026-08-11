use crate::control::ListenState;
use clap::{Parser, Subcommand};
use std::path::PathBuf;

/// Subcommands that drive a *running* Handy instance over the local CLI control
/// channel (see `control.rs`). Unlike the top-level flags below (which the app
/// consumes at its own startup or forwards via single-instance), these run as a
/// short-lived process that connects to the running app, applies the change
/// live, and reports the acknowledged result — exiting nonzero on any failure.
#[derive(Subcommand, Debug, Clone)]
pub enum Command {
    /// Turn microphone listening on/off (or toggle) on the running app.
    Listen {
        /// Desired state: `on`, `off`, or `toggle`.
        #[arg(value_enum)]
        state: ListenState,
    },
    /// Change the active input device on the running app.
    ///
    /// Matches the device by exact name, or by a unique case-insensitive
    /// substring. If the query matches more than one device, every candidate is
    /// printed and the command exits nonzero rather than guessing. Pass
    /// `default` to follow the system default.
    Mic {
        /// Device name or a unique substring (or `default`).
        device: String,
    },
    /// Transcription model operations.
    Model {
        #[command(subcommand)]
        action: ModelCommand,
    },
}

#[derive(Subcommand, Debug, Clone)]
pub enum ModelCommand {
    /// Download a transcription model by id (see `handy --list-models`).
    Download {
        /// Model id, e.g. `whisper-small`.
        name: String,
    },
}

#[derive(Parser, Debug, Clone, Default)]
#[command(name = "handy", about = "Handy - Speech to Text")]
pub struct CliArgs {
    /// Drive a running Handy instance (listen / mic / model). Omit to launch
    /// the app normally.
    #[command(subcommand)]
    pub command: Option<Command>,

    /// Start with the main window hidden
    #[arg(long)]
    pub start_hidden: bool,

    /// Disable the system tray icon
    #[arg(long)]
    pub no_tray: bool,

    /// Toggle transcription on/off (sent to running instance)
    #[arg(long)]
    pub toggle_transcription: bool,

    /// Toggle transcription with post-processing on/off (sent to running instance)
    #[arg(long)]
    pub toggle_post_process: bool,

    /// Cancel the current operation (sent to running instance)
    #[arg(long)]
    pub cancel: bool,

    /// Enable debug mode with verbose logging
    #[arg(long)]
    pub debug: bool,

    /// Transcribe this WAV (16 kHz mono) headlessly and exit. Runs the same
    /// batch transcription path as the app — no mic, no VAD, no download
    /// (the model must already be installed).
    #[arg(short = 'f', long, value_name = "WAV")]
    pub transcribe_file: Option<PathBuf>,

    /// Model id to load for --transcribe-file (default: the selected model).
    #[arg(long)]
    pub model: Option<String>,

    /// Hard-select the compute device for --transcribe-file by its registry
    /// index (see --list-devices). Omit to use the persisted accelerator
    /// setting. transcribe-cpp (whisper-family) models only.
    #[arg(long, value_name = "N")]
    pub device_index: Option<usize>,

    /// List the transcribe-cpp compute devices (with indices) and exit.
    #[arg(long)]
    pub list_devices: bool,

    /// List the available models (with ids) and exit. Pass an id to --model.
    /// Honors --json for machine-readable output.
    #[arg(long)]
    pub list_models: bool,

    /// Repeat the transcription N times (best_ms reports the fastest run).
    #[arg(long, value_name = "N")]
    pub repeat: Option<usize>,

    /// Emit --transcribe-file results as JSON.
    #[arg(long)]
    pub json: bool,
}
