// Command-line argument definitions for the crashpipe binary.
// Each field maps to a CLI flag and controls where the pipeline reads files,
// where it writes output, how many workers run, and whether fault injection
// or verbose logging is enabled for demos/testing.

use std::path::PathBuf;

use clap::{ArgAction, Parser};

// Derive Clap's `Parser` so this struct can be filled from command-line flags.
#[derive(Debug, Clone, Parser)]
#[command(name = "crashpipe", about = "Minimal file ingest pipeline")]
// Top-level CLI configuration.
pub struct Cli {
    // Folder the program scans/watches for incoming files.
    #[arg(long, default_value = "./inbox")]
    pub inbox: PathBuf,
    // Folder where finished gzip artifacts are written.
    #[arg(long, default_value = "./outbox")]
    pub outbox: PathBuf,
    // SQLite database used to persist checkpoints and job state.
    #[arg(long, default_value = "./state.db")]
    pub db: PathBuf,
    // Run one pass and exit instead of staying alive.
    #[arg(long, action = ArgAction::SetTrue)]
    pub once: bool,
    // Enable filesystem watching for new files after startup.
    #[arg(long, default_value_t = true, action = ArgAction::Set)]
    pub watch: bool,
    // Number of worker threads that compete for jobs.
    #[arg(long, default_value_t = 2)]
    pub workers: usize,
    // How long a worker lock can sit before it is considered stale.
    #[arg(long, default_value_t = 30)]
    pub lock_timeout_secs: u64,
    // Optional step name used to intentionally fail during demos/testing.
    #[arg(long)]
    pub failpoint: Option<String>,
    // Turn on debug logging.
    #[arg(long)]
    pub verbose: bool,
}

impl Cli {
    // Convenience wrapper so `main` reads cleanly.
    pub fn parse_args() -> Self {
        Self::parse()
    }
}
