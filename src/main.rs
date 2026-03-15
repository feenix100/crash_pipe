// Binary entry point.
// `main` wires together the CLI, database, migrations, startup reconciliation,
// watcher thread, and worker threads that actually process files.

use std::fs;
use std::str::FromStr;
use std::thread;

use anyhow::{Context, Result};
use tracing::info;

use crashpipe::cli::Cli;
use crashpipe::db::Database;
use crashpipe::logging;
use crashpipe::pipeline::{Pipeline, PipelineConfig};
use crashpipe::state::Failpoint;
use crashpipe::watcher;

fn main() -> Result<()> {
    // Parse command-line flags first so the rest of startup is configuration-driven.
    let cli = Cli::parse_args();
    // Set up structured logging before doing any real work.
    logging::init(cli.verbose);

    // Ensure the expected folder layout exists.
    fs::create_dir_all(&cli.inbox)?;
    fs::create_dir_all(&cli.outbox)?;

    // Open the SQLite database and bring schema up to date.
    let db = Database::open(&cli.db)?;
    db.run_migrations("migrations")?;

    // Parse the optional fault-injection step name.
    let failpoint = cli
        .failpoint
        .as_deref()
        .map(Failpoint::from_str)
        .transpose()
        .context("invalid --failpoint value")?;

    // Shared pipeline object cloned into worker threads.
    let pipeline = Pipeline::new(
        db.clone(),
        PipelineConfig {
            outbox: cli.outbox.clone(),
            lock_timeout_secs: cli.lock_timeout_secs,
            failpoint,
        },
    );

    // Initial directory scan catches files that already existed before startup.
    let discovered = pipeline.scan_and_enqueue(&cli.inbox)?;
    info!(discovered, "startup scan complete");

    // Repair stale locks / partially finished work from a previous run.
    pipeline.reconcile_startup()?;

    // Watch mode is disabled when `--once` is requested.
    let watch_enabled = cli.watch && !cli.once;
    let _watcher = if watch_enabled {
        Some(watcher::run_watch_loop(db.clone(), cli.inbox.clone(), 750))
    } else {
        None
    };

    // Spawn one or more workers that compete for jobs in the database.
    let mut workers = Vec::new();
    for idx in 0..cli.workers.max(1) {
        let worker_pipeline = pipeline.clone();
        let worker_id = format!("worker-{}", idx + 1);
        let once = cli.once;
        workers.push(thread::spawn(move || worker_pipeline.worker_loop(worker_id, once)));
    }

    // Bubble worker errors back to the main thread.
    for handle in workers {
        let joined = handle.join().map_err(|_| anyhow::anyhow!("worker thread panicked"))?;
        joined?;
    }

    // Keep the process alive when running as a long-lived watcher.
    if watch_enabled {
        loop {
            thread::park();
        }
    }

    Ok(())
}
