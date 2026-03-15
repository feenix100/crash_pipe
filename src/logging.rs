// Logging setup.
// The app uses `tracing` for structured logs and switches between `info` and
// `debug` verbosity based on the CLI flag.

use tracing_subscriber::{EnvFilter, fmt};

// Initialize global tracing subscriber once at process startup.
pub fn init(verbose: bool) {
    // Choose the log level dynamically from the CLI.
    let filter = if verbose {
        EnvFilter::new("debug")
    } else {
        EnvFilter::new("info")
    };

    fmt().with_env_filter(filter).with_target(false).compact().init();
}
