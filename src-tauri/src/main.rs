// Prevents additional console window on Windows in release, DO NOT REMOVE!!
#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

use clap::Parser;
use handy_app_lib::CliArgs;

fn main() {
    let cli_args = CliArgs::parse();

    // CLI control subcommands (`handy listen|mic|model ...`) talk to an
    // already-running instance and exit; they must not boot the app.
    if let Some(code) = handy_app_lib::run_cli_control(&cli_args) {
        std::process::exit(code);
    }

    #[cfg(target_os = "linux")]
    {
        // DMABUF renderer causes crashes on various GPU/display server configurations
        // See: https://github.com/tauri-apps/tauri/issues/9394
        std::env::set_var("WEBKIT_DISABLE_DMABUF_RENDERER", "1");
    }

    handy_app_lib::run(cli_args)
}
