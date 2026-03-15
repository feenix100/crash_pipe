// Core pipeline orchestration.
// This module scans for files, reconciles unfinished work after a restart,
// claims jobs from SQLite, and advances them through compress -> hash -> move
// -> record using explicit checkpoints.

use std::fs;
use std::path::{Path, PathBuf};
use std::thread;
use std::time::Duration;

use tracing::{debug, error, info, instrument, warn};

use crate::db::{Database, FileRecord};
use crate::error::PipelineError;
use crate::fs_ops::{self, TempArtifact};
use crate::state::{Failpoint, PipelineStatus, PipelineStep};

// Runtime settings shared by all workers.
#[derive(Debug, Clone)]
pub struct PipelineConfig {
    pub outbox: PathBuf,
    pub lock_timeout_secs: u64,
    pub failpoint: Option<Failpoint>,
}

// Stateless orchestrator around the database handle and config.
#[derive(Clone)]
pub struct Pipeline {
    db: Database,
    config: PipelineConfig,
}

impl Pipeline {
    // Construct a new pipeline controller.
    pub fn new(db: Database, config: PipelineConfig) -> Self {
        Self { db, config }
    }

    // Discover files already present at startup and enqueue them.
    pub fn scan_and_enqueue(&self, inbox: &Path) -> Result<usize, PipelineError> {
        let mut count = 0usize;
        for entry in fs::read_dir(inbox)? {
            let entry = entry?;
            let path = entry.path();
            if !path.is_file() {
                continue;
            }
            let meta = fs::metadata(&path)?;
            let record = self
                .db
                .enqueue_file(&path.to_string_lossy(), meta.len() as i64)?;
            debug!(ingest_id = record.ingest_id, src_path = record.src_path, "startup enqueue");
            count += 1;
        }
        Ok(count)
    }

    // Repair leftover state from an earlier run before workers start.
    pub fn reconcile_startup(&self) -> Result<(), PipelineError> {
        let stale = self.db.clear_stale_locks(self.config.lock_timeout_secs)?;
        if stale > 0 {
            info!(released_stale_locks = stale, "startup stale lock recovery");
        }

        let pending = self.db.load_pending_records()?;
        for record in pending {
            self.reconcile_record(&record)?;
        }
        Ok(())
    }

    // Inspect a partially completed record and decide how to recover it.
    // Examples:
    // - if final output already exists, mark the record Done
    // - if the source disappeared, mark Failed
    // - if a temp file exists during Moving, let a worker resume that move
    fn reconcile_record(&self, record: &FileRecord) -> Result<(), PipelineError> {
        let src = PathBuf::from(&record.src_path);
        let output_path = match &record.output_path {
            Some(path) => PathBuf::from(path),
            None => fs_ops::final_output_path(&self.config.outbox, &src)?,
        };
        let tmp_path = fs_ops::temp_output_path(&self.config.outbox, &record.ingest_id);

        if !src.exists() && record.current_step != PipelineStep::Done {
            self.db
                .mark_failed(&record.ingest_id, "source file missing during reconciliation", None)?;
            warn!(ingest_id = record.ingest_id, src_path = record.src_path, "source missing at startup; marked failed");
            return Ok(());
        }

        if fs_ops::file_exists(&output_path) {
            if record.output_path.is_none() {
                self.db
                    .set_output_path(&record.ingest_id, &output_path.to_string_lossy(), None)?;
            }
            if record.sha256.is_none() && src.exists() {
                let hash = fs_ops::sha256_hex(&src)?;
                self.db.set_sha_if_missing(&record.ingest_id, &hash, None)?;
            }
            if record.status != PipelineStatus::Done || record.current_step != PipelineStep::Done {
                self.db.mark_done_unlocked(&record.ingest_id)?;
                info!(ingest_id = record.ingest_id, src_path = record.src_path, "reconciliation moved record to Done");
            }
        } else if record.current_step == PipelineStep::Moving && tmp_path.exists() {
            debug!(ingest_id = record.ingest_id, "moving step with temp artifact present; will resume move");
        } else if matches!(record.current_step, PipelineStep::Compressing | PipelineStep::Compressed) && !tmp_path.exists() {
            self.db
                .mark_failed(&record.ingest_id, "missing temp artifact after restart", None)?;
        }

        Ok(())
    }

    // Main worker loop: repeatedly claim jobs and process them.
    pub fn worker_loop(&self, worker_id: String, once: bool) -> Result<(), PipelineError> {
        fs_ops::ensure_outbox_layout(&self.config.outbox)?;
        loop {
            let maybe_job = self
                .db
                .claim_next_job(&worker_id, self.config.lock_timeout_secs)?;
            match maybe_job {
                Some(job) => {
                    if let Err(error) = self.process_claimed(job.clone(), &worker_id) {
                        let _ = self
                            .db
                            .mark_failed(&job.ingest_id, &error.to_string(), Some(&worker_id));
                        error!(ingest_id = job.ingest_id, src_path = job.src_path, worker_id, error = %error, "job failed");
                    }
                }
                None => {
                    if once {
                        break;
                    }
                    thread::sleep(Duration::from_millis(500));
                }
            }
        }
        Ok(())
    }

    #[instrument(skip(self, job), fields(ingest_id = %job.ingest_id, src_path = %job.src_path, worker_id))]
    // Process one claimed job from its current checkpoint onward.
    // The logic is intentionally restart-friendly: each major step is recorded
    // in the database so re-running does not duplicate completed work.
    fn process_claimed(&self, job: FileRecord, worker_id: &str) -> Result<(), PipelineError> {
        let src_path = PathBuf::from(&job.src_path);
        let final_path = fs_ops::final_output_path(&self.config.outbox, &src_path)?;
        let temp_path = fs_ops::temp_output_path(&self.config.outbox, &job.ingest_id);

        // Bail out early if the input vanished after it was enqueued.
        if !src_path.exists() {
            self.db
                .mark_failed(&job.ingest_id, "source missing before processing", Some(worker_id))?;
            return Ok(());
        }

        // Idempotency shortcut: if the final artifact already exists, do not rebuild it.
        if fs_ops::file_exists(&final_path) {
            self.db
                .set_output_path(&job.ingest_id, &final_path.to_string_lossy(), Some(worker_id))?;
            if job.sha256.is_none() {
                let hash = fs_ops::sha256_hex(&src_path)?;
                self.db
                    .set_sha_if_missing(&job.ingest_id, &hash, Some(worker_id))?;
            }
            self.db.mark_done_and_release(&job.ingest_id, worker_id)?;
            info!(output_path = %final_path.display(), "final output already exists; marked done");
            return Ok(());
        }

        // Compression runs when the job has not progressed past that phase yet,
        // or when the temp artifact is missing and must be recreated.
        if job.current_step <= PipelineStep::Compressed || !temp_path.exists() {
            self.db
                .checkpoint_start(&job.ingest_id, PipelineStep::Compressing, worker_id)?;
            self.fail_if(PipelineStep::Compressing)?;
            fs_ops::compress_gzip(&src_path, &temp_path)?;
            self.db
                .checkpoint_complete(&job.ingest_id, PipelineStep::Compressed, worker_id)?;
        }

        // Hashing records metadata about the original source file.
        self.db
            .checkpoint_start(&job.ingest_id, PipelineStep::Hashing, worker_id)?;
        self.fail_if(PipelineStep::Hashing)?;
        // Reuse any previously saved hash on retries; otherwise compute it now.
        let hash = if let Some(existing) = self
            .db
            .get_by_ingest_id(&job.ingest_id)?
            .and_then(|r| r.sha256)
        {
            existing
        } else {
            fs_ops::sha256_hex(&src_path)?
        };
        self.db
            .set_sha_if_missing(&job.ingest_id, &hash, Some(worker_id))?;
        self.db
            .checkpoint_complete(&job.ingest_id, PipelineStep::Hashed, worker_id)?;

        // Move the finished temp artifact into its final location atomically.
        self.db
            .checkpoint_start(&job.ingest_id, PipelineStep::Moving, worker_id)?;
        self.fail_if(PipelineStep::Moving)?;
        // `TempArtifact` cleans up automatically if commit never happens.
        let artifact = TempArtifact::new(temp_path.clone());
        artifact.commit(&final_path)?;
        self.db
            .set_output_path(&job.ingest_id, &final_path.to_string_lossy(), Some(worker_id))?;
        self.db
            .checkpoint_complete(&job.ingest_id, PipelineStep::Moved, worker_id)?;

        // Final bookkeeping step: mark the record complete and release the lock.
        self.db
            .checkpoint_start(&job.ingest_id, PipelineStep::Recording, worker_id)?;
        self.fail_if(PipelineStep::Recording)?;
        self.db.mark_done_and_release(&job.ingest_id, worker_id)?;
        info!(output_path = %final_path.display(), step = %PipelineStep::Done.as_str(), "pipeline complete");
        Ok(())
    }

    // Optional fault injection used to demonstrate crash-safe recovery.
    fn fail_if(&self, step: PipelineStep) -> Result<(), PipelineError> {
        if let Some(fp) = self.config.failpoint {
            let hit = matches!(
                (fp, step),
                (Failpoint::Compressing, PipelineStep::Compressing)
                    | (Failpoint::Hashing, PipelineStep::Hashing)
                    | (Failpoint::Moving, PipelineStep::Moving)
                    | (Failpoint::Recording, PipelineStep::Recording)
            );
            if hit {
                return Err(PipelineError::Failpoint(step.as_str()));
            }
        }
        Ok(())
    }
}
