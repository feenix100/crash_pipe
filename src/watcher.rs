// Filesystem watcher.
// This module listens for new/modified files in the inbox and enqueues them in
// the database. The worker threads do the actual processing later.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::mpsc::{self, RecvTimeoutError};
use std::thread;
use std::time::{Duration, Instant};

use notify::{Event, EventKind, RecommendedWatcher, RecursiveMode, Watcher};
use tracing::{debug, warn};

use crate::db::Database;

// Spawn the watcher on its own thread so the main thread can keep running workers.
pub fn run_watch_loop(db: Database, inbox: PathBuf, debounce_ms: u64) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        if let Err(err) = watch_forever(db, inbox, debounce_ms) {
            warn!(error = %err, "watch loop stopped");
        }
    })
}

// Long-running watch loop.
// A debounce map prevents the same file event from being enqueued repeatedly.
fn watch_forever(db: Database, inbox: PathBuf, debounce_ms: u64) -> Result<(), anyhow::Error> {
    // Bridge notify's callback API into a blocking receive loop.
    let (tx, rx) = mpsc::channel::<notify::Result<Event>>();
    let mut watcher: RecommendedWatcher = notify::recommended_watcher(move |res| {
        let _ = tx.send(res);
    })?;
    watcher.watch(&inbox, RecursiveMode::NonRecursive)?;

    // Remember the last time we saw each path so bursty editor/file events collapse.
    let mut recent: HashMap<PathBuf, Instant> = HashMap::new();
    let debounce = Duration::from_millis(debounce_ms);
    loop {
        match rx.recv_timeout(Duration::from_millis(500)) {
            Ok(Ok(event)) => {
                if !is_relevant_event(&event.kind) {
                    continue;
                }
                for path in event.paths {
                    if !path.is_file() {
                        continue;
                    }
                    let now = Instant::now();
                    if let Some(last) = recent.get(&path)
                        && now.duration_since(*last) < debounce
                    {
                        continue;
                    }
                    recent.insert(path.clone(), now);
                    match std::fs::metadata(&path) {
                        Ok(meta) => {
                            let src = path.to_string_lossy().to_string();
                            match db.enqueue_file(&src, meta.len() as i64) {
                                Ok(record) => {
                                    debug!(
                                        ingest_id = record.ingest_id,
                                        src_path = record.src_path,
                                        "watcher enqueued file"
                                    );
                                }
                                Err(error) => {
                                    warn!(src_path = %path.display(), error = %error, "watcher enqueue failed");
                                }
                            }
                        }
                        Err(error) => {
                            warn!(src_path = %path.display(), error = %error, "watcher metadata failed");
                        }
                    }
                }
            }
            Ok(Err(error)) => warn!(error = %error, "watch event error"),
            Err(RecvTimeoutError::Timeout) => {
                recent.retain(|_, seen_at| seen_at.elapsed() < Duration::from_secs(30));
            }
            Err(RecvTimeoutError::Disconnected) => break,
        }
    }
    Ok(())
}

// We only care about create/modify-style events that could produce work.
fn is_relevant_event(kind: &EventKind) -> bool {
    matches!(kind, EventKind::Create(_) | EventKind::Modify(_) | EventKind::Any)
}
