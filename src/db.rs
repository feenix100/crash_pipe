// SQLite persistence layer.
// The database stores one row per source file, including the current step,
// worker lock information, hashes, output paths, timestamps, and last error.
// This is what makes the pipeline resumable across crashes/restarts.

use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::Duration;

use chrono::{DateTime, Utc};
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use thiserror::Error;
use uuid::Uuid;

use crate::state::{ParseEnumError, PipelineStatus, PipelineStep};

// In-memory representation of one row from the `files` table.
#[derive(Debug, Clone)]
pub struct FileRecord {
    pub ingest_id: String,
    pub src_path: String,
    pub status: PipelineStatus,
    pub current_step: PipelineStep,
    pub attempt_count: i64,
    pub sha256: Option<String>,
    pub output_path: Option<String>,
    pub file_size: i64,
    pub created_at: String,
    pub updated_at: String,
    pub last_error: Option<String>,
    pub locked_by: Option<String>,
    pub locked_at: Option<String>,
    pub heartbeat_at: Option<String>,
}

// Thin wrapper around the SQLite file path.
// A fresh connection is opened per operation.
#[derive(Debug, Clone)]
pub struct Database {
    path: PathBuf,
}

// Errors specific to the persistence layer.
#[derive(Debug, Error)]
pub enum DbError {
    #[error("sqlite error: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("invalid migration file name: {0}")]
    InvalidMigrationName(String),
    #[error("invalid enum value in database: {0}")]
    InvalidEnum(String),
}

impl From<ParseEnumError> for DbError {
    fn from(value: ParseEnumError) -> Self {
        Self::InvalidEnum(value.0)
    }
}

impl Database {
    // Create a database handle and verify that a connection can be opened.
    pub fn open(path: &Path) -> Result<Self, DbError> {
        let db = Self {
            path: path.to_path_buf(),
        };
        let _ = db.connect()?;
        Ok(db)
    }

    // Expose the path mostly for diagnostics/tests.
    pub fn path(&self) -> &Path {
        &self.path
    }

    // Open a connection with WAL enabled for better concurrency.
    fn connect(&self) -> Result<Connection, DbError> {
        let conn = Connection::open(&self.path)?;
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.busy_timeout(Duration::from_secs(5))?;
        Ok(conn)
    }

    // Apply schema migrations before the pipeline starts using the DB.
    pub fn run_migrations(&self, migrations_dir: impl AsRef<Path>) -> Result<(), DbError> {
        let mut conn = self.connect()?;
        crate::migrations::run(&mut conn, migrations_dir.as_ref())
    }

    // Insert a new file row, or reset an existing unfinished row back to Queued.
    // If the record is already fully Done, we keep its ingest id and do not reset it.
    pub fn enqueue_file(&self, src_path: &str, file_size: i64) -> Result<FileRecord, DbError> {
        let now = Utc::now().to_rfc3339();
        let mut conn = self.connect()?;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;

        let existing = tx
            .query_row(
                "
                SELECT ingest_id, status, current_step
                FROM files
                WHERE src_path = ?1
                ",
                [src_path],
                |row| {
                    let ingest_id: String = row.get(0)?;
                    let status_raw: String = row.get(1)?;
                    let step_raw: String = row.get(2)?;
                    Ok((ingest_id, status_raw, step_raw))
                },
            )
            .optional()?;

        let ingest_id = match existing {
            Some((ingest_id, status_raw, step_raw)) => {
                let status = PipelineStatus::from_str(&status_raw)?;
                let step = PipelineStep::from_str(&step_raw)?;
                if status == PipelineStatus::Done && step == PipelineStep::Done {
                    ingest_id
                } else {
                    tx.execute(
                        "
                        UPDATE files
                        SET status = ?1,
                            current_step = ?2,
                            file_size = ?3,
                            updated_at = ?4,
                            last_error = NULL,
                            locked_by = NULL,
                            locked_at = NULL,
                            heartbeat_at = NULL
                        WHERE ingest_id = ?5
                        ",
                        params![
                            PipelineStatus::Queued.as_str(),
                            PipelineStep::Queued.as_str(),
                            file_size,
                            now,
                            ingest_id
                        ],
                    )?;
                    ingest_id
                }
            }
            None => {
                let new_id = Uuid::new_v4().to_string();
                tx.execute(
                    "
                    INSERT INTO files(
                        ingest_id, src_path, status, current_step, attempt_count, sha256, output_path, file_size,
                        created_at, updated_at, last_error, locked_by, locked_at, heartbeat_at
                    ) VALUES (?1, ?2, ?3, ?4, 0, NULL, NULL, ?5, ?6, ?6, NULL, NULL, NULL, NULL)
                    ",
                    params![
                        new_id,
                        src_path,
                        PipelineStatus::Queued.as_str(),
                        PipelineStep::Queued.as_str(),
                        file_size,
                        now
                    ],
                )?;
                new_id
            }
        };

        tx.commit()?;
        self.get_by_ingest_id(&ingest_id)?
            .ok_or_else(|| DbError::InvalidEnum("failed to reload enqueued row".to_string()))
    }

    // Atomically claim the next available job for a worker.
    // This also releases stale locks whose timestamps are older than the timeout.
    pub fn claim_next_job(
        &self,
        worker_id: &str,
        lock_timeout_secs: u64,
    ) -> Result<Option<FileRecord>, DbError> {
        let mut conn = self.connect()?;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let now = Utc::now();
        let now_str = now.to_rfc3339();
        let stale_cutoff = (now - chrono::Duration::seconds(lock_timeout_secs as i64)).to_rfc3339();

        tx.execute(
            "
            UPDATE files
            SET status = ?1,
                current_step = ?2,
                locked_by = NULL,
                locked_at = NULL,
                heartbeat_at = NULL,
                last_error = COALESCE(last_error, 'stale lock released'),
                updated_at = ?3
            WHERE status = ?4
              AND locked_at IS NOT NULL
              AND locked_at < ?5
            ",
            params![
                PipelineStatus::Queued.as_str(),
                PipelineStep::Queued.as_str(),
                now_str,
                PipelineStatus::Processing.as_str(),
                stale_cutoff
            ],
        )?;

        let candidate = tx
            .query_row(
                "
                SELECT ingest_id
                FROM files
                WHERE status IN (?1, ?2)
                  AND current_step != ?3
                  AND (locked_at IS NULL OR locked_at < ?4)
                ORDER BY updated_at ASC
                LIMIT 1
                ",
                params![
                    PipelineStatus::Queued.as_str(),
                    PipelineStatus::Processing.as_str(),
                    PipelineStep::Done.as_str(),
                    stale_cutoff
                ],
                |row| row.get::<_, String>(0),
            )
            .optional()?;

        let Some(ingest_id) = candidate else {
            tx.commit()?;
            return Ok(None);
        };

        tx.execute(
            "
            UPDATE files
            SET status = ?1,
                locked_by = ?2,
                locked_at = ?3,
                heartbeat_at = ?3,
                attempt_count = attempt_count + 1,
                updated_at = ?3
            WHERE ingest_id = ?4
            ",
            params![
                PipelineStatus::Processing.as_str(),
                worker_id,
                now_str,
                ingest_id
            ],
        )?;

        let record = query_record_tx(&tx, &ingest_id)?;
        tx.commit()?;
        Ok(record)
    }

    // Refresh the lock heartbeat so other workers know this job is still alive.
    pub fn heartbeat(&self, ingest_id: &str, worker_id: &str) -> Result<(), DbError> {
        let now = Utc::now().to_rfc3339();
        let conn = self.connect()?;
        conn.execute(
            "
            UPDATE files
            SET heartbeat_at = ?1, updated_at = ?1
            WHERE ingest_id = ?2 AND locked_by = ?3
            ",
            params![now, ingest_id, worker_id],
        )?;
        Ok(())
    }

    // Record that a worker is beginning a specific step.
    pub fn checkpoint_start(
        &self,
        ingest_id: &str,
        step: PipelineStep,
        worker_id: &str,
    ) -> Result<(), DbError> {
        let now = Utc::now().to_rfc3339();
        let conn = self.connect()?;
        conn.execute(
            "
            UPDATE files
            SET status = ?1,
                current_step = ?2,
                heartbeat_at = ?3,
                updated_at = ?3
            WHERE ingest_id = ?4
              AND locked_by = ?5
            ",
            params![
                PipelineStatus::Processing.as_str(),
                step.as_str(),
                now,
                ingest_id,
                worker_id
            ],
        )?;
        Ok(())
    }

    // Record completion of a step.
    // When the step is `Done`, the status flips to Done as well.
    pub fn checkpoint_complete(
        &self,
        ingest_id: &str,
        step: PipelineStep,
        worker_id: &str,
    ) -> Result<(), DbError> {
        let now = Utc::now().to_rfc3339();
        let conn = self.connect()?;
        conn.execute(
            "
            UPDATE files
            SET status = CASE WHEN ?1 = ?2 THEN ?3 ELSE ?4 END,
                current_step = ?1,
                heartbeat_at = ?5,
                updated_at = ?5
            WHERE ingest_id = ?6
              AND locked_by = ?7
            ",
            params![
                step.as_str(),
                PipelineStep::Done.as_str(),
                PipelineStatus::Done.as_str(),
                PipelineStatus::Processing.as_str(),
                now,
                ingest_id,
                worker_id
            ],
        )?;
        Ok(())
    }

    // Save the SHA-256 once, but leave it unchanged on retries if already present.
    pub fn set_sha_if_missing(
        &self,
        ingest_id: &str,
        sha256: &str,
        worker_id: Option<&str>,
    ) -> Result<(), DbError> {
        let now = Utc::now().to_rfc3339();
        let conn = self.connect()?;
        if let Some(worker_id) = worker_id {
            conn.execute(
                "
                UPDATE files
                SET sha256 = COALESCE(sha256, ?1),
                    heartbeat_at = ?2,
                    updated_at = ?2
                WHERE ingest_id = ?3
                  AND locked_by = ?4
                ",
                params![sha256, now, ingest_id, worker_id],
            )?;
        } else {
            conn.execute(
                "
                UPDATE files
                SET sha256 = COALESCE(sha256, ?1),
                    updated_at = ?2
                WHERE ingest_id = ?3
                ",
                params![sha256, now, ingest_id],
            )?;
        }
        Ok(())
    }

    // Save the final output path once it is known.
    pub fn set_output_path(
        &self,
        ingest_id: &str,
        output_path: &str,
        worker_id: Option<&str>,
    ) -> Result<(), DbError> {
        let now = Utc::now().to_rfc3339();
        let conn = self.connect()?;
        if let Some(worker_id) = worker_id {
            conn.execute(
                "
                UPDATE files
                SET output_path = ?1,
                    heartbeat_at = ?2,
                    updated_at = ?2
                WHERE ingest_id = ?3
                  AND locked_by = ?4
                ",
                params![output_path, now, ingest_id, worker_id],
            )?;
        } else {
            conn.execute(
                "
                UPDATE files
                SET output_path = ?1,
                    updated_at = ?2
                WHERE ingest_id = ?3
                ",
                params![output_path, now, ingest_id],
            )?;
        }
        Ok(())
    }

    // Mark a job as failed and release any worker lock.
    pub fn mark_failed(&self, ingest_id: &str, error: &str, worker_id: Option<&str>) -> Result<(), DbError> {
        let now = Utc::now().to_rfc3339();
        let conn = self.connect()?;
        if let Some(worker_id) = worker_id {
            conn.execute(
                "
                UPDATE files
                SET status = ?1,
                    last_error = ?2,
                    locked_by = NULL,
                    locked_at = NULL,
                    heartbeat_at = NULL,
                    updated_at = ?3
                WHERE ingest_id = ?4
                  AND locked_by = ?5
                ",
                params![
                    PipelineStatus::Failed.as_str(),
                    error,
                    now,
                    ingest_id,
                    worker_id
                ],
            )?;
        } else {
            conn.execute(
                "
                UPDATE files
                SET status = ?1,
                    last_error = ?2,
                    locked_by = NULL,
                    locked_at = NULL,
                    heartbeat_at = NULL,
                    updated_at = ?3
                WHERE ingest_id = ?4
                ",
                params![PipelineStatus::Failed.as_str(), error, now, ingest_id],
            )?;
        }
        Ok(())
    }

    // Final successful transition for a locked job.
    pub fn mark_done_and_release(&self, ingest_id: &str, worker_id: &str) -> Result<(), DbError> {
        let now = Utc::now().to_rfc3339();
        let conn = self.connect()?;
        conn.execute(
            "
            UPDATE files
            SET status = ?1,
                current_step = ?2,
                locked_by = NULL,
                locked_at = NULL,
                heartbeat_at = NULL,
                last_error = NULL,
                updated_at = ?3
            WHERE ingest_id = ?4
              AND locked_by = ?5
            ",
            params![
                PipelineStatus::Done.as_str(),
                PipelineStep::Done.as_str(),
                now,
                ingest_id,
                worker_id
            ],
        )?;
        Ok(())
    }

    // Final successful transition used during startup reconciliation when no worker owns the row.
    pub fn mark_done_unlocked(&self, ingest_id: &str) -> Result<(), DbError> {
        let now = Utc::now().to_rfc3339();
        let conn = self.connect()?;
        conn.execute(
            "
            UPDATE files
            SET status = ?1,
                current_step = ?2,
                locked_by = NULL,
                locked_at = NULL,
                heartbeat_at = NULL,
                last_error = NULL,
                updated_at = ?3
            WHERE ingest_id = ?4
            ",
            params![
                PipelineStatus::Done.as_str(),
                PipelineStep::Done.as_str(),
                now,
                ingest_id
            ],
        )?;
        Ok(())
    }

    // Release ownership without changing status/step.
    pub fn release_lock(&self, ingest_id: &str, worker_id: &str) -> Result<(), DbError> {
        let now = Utc::now().to_rfc3339();
        let conn = self.connect()?;
        conn.execute(
            "
            UPDATE files
            SET locked_by = NULL,
                locked_at = NULL,
                heartbeat_at = NULL,
                updated_at = ?1
            WHERE ingest_id = ?2
              AND locked_by = ?3
            ",
            params![now, ingest_id, worker_id],
        )?;
        Ok(())
    }

    // Load a row by ingest id.
    pub fn get_by_ingest_id(&self, ingest_id: &str) -> Result<Option<FileRecord>, DbError> {
        let conn = self.connect()?;
        query_record(&conn, ingest_id)
    }

    // Load a row by source path.
    pub fn get_by_src_path(&self, src_path: &str) -> Result<Option<FileRecord>, DbError> {
        let conn = self.connect()?;
        let mut stmt = conn.prepare(
            "
            SELECT ingest_id
            FROM files
            WHERE src_path = ?1
            LIMIT 1
            ",
        )?;
        let ingest = stmt.query_row([src_path], |row| row.get::<_, String>(0)).optional()?;
        match ingest {
            Some(ingest_id) => query_record(&conn, &ingest_id),
            None => Ok(None),
        }
    }

    // Load every row that is not fully finished. Used during startup recovery.
    pub fn load_pending_records(&self) -> Result<Vec<FileRecord>, DbError> {
        let conn = self.connect()?;
        let mut stmt = conn.prepare(
            "
            SELECT ingest_id
            FROM files
            WHERE current_step != ?1
               OR status != ?2
            ORDER BY updated_at ASC
            ",
        )?;
        let mut rows = stmt.query(params![PipelineStep::Done.as_str(), PipelineStatus::Done.as_str()])?;
        let mut out = Vec::new();
        while let Some(row) = rows.next()? {
            let ingest_id: String = row.get(0)?;
            if let Some(record) = query_record(&conn, &ingest_id)? {
                out.push(record);
            }
        }
        Ok(out)
    }

    // Release locks that belong to workers which likely crashed or disappeared.
    pub fn clear_stale_locks(&self, lock_timeout_secs: u64) -> Result<usize, DbError> {
        let conn = self.connect()?;
        let now = Utc::now().to_rfc3339();
        let stale_cutoff = (Utc::now() - chrono::Duration::seconds(lock_timeout_secs as i64)).to_rfc3339();
        let changed = conn.execute(
            "
            UPDATE files
            SET status = ?1,
                current_step = CASE WHEN current_step = ?2 THEN ?2 ELSE ?3 END,
                locked_by = NULL,
                locked_at = NULL,
                heartbeat_at = NULL,
                last_error = COALESCE(last_error, 'stale lock released at startup'),
                updated_at = ?4
            WHERE status = ?5
              AND locked_at IS NOT NULL
              AND locked_at < ?6
            ",
            params![
                PipelineStatus::Queued.as_str(),
                PipelineStep::Done.as_str(),
                PipelineStep::Queued.as_str(),
                now,
                PipelineStatus::Processing.as_str(),
                stale_cutoff
            ],
        )?;
        Ok(changed)
    }
}

// Shared helper to hydrate a `FileRecord` from SQLite text columns.
fn query_record(conn: &Connection, ingest_id: &str) -> Result<Option<FileRecord>, DbError> {
    let mut stmt = conn.prepare(
        "
        SELECT ingest_id, src_path, status, current_step, attempt_count, sha256, output_path, file_size,
               created_at, updated_at, last_error, locked_by, locked_at, heartbeat_at
        FROM files
        WHERE ingest_id = ?1
        ",
    )?;
    let record = stmt
        .query_row([ingest_id], |row| {
            let status_raw: String = row.get(2)?;
            let step_raw: String = row.get(3)?;
            Ok(FileRecord {
                ingest_id: row.get(0)?,
                src_path: row.get(1)?,
                status: PipelineStatus::from_str(&status_raw).map_err(|e| {
                    rusqlite::Error::FromSqlConversionFailure(
                        2,
                        rusqlite::types::Type::Text,
                        Box::new(e),
                    )
                })?,
                current_step: PipelineStep::from_str(&step_raw).map_err(|e| {
                    rusqlite::Error::FromSqlConversionFailure(
                        3,
                        rusqlite::types::Type::Text,
                        Box::new(e),
                    )
                })?,
                attempt_count: row.get(4)?,
                sha256: row.get(5)?,
                output_path: row.get(6)?,
                file_size: row.get(7)?,
                created_at: row.get(8)?,
                updated_at: row.get(9)?,
                last_error: row.get(10)?,
                locked_by: row.get(11)?,
                locked_at: row.get(12)?,
                heartbeat_at: row.get(13)?,
            })
        })
        .optional()?;
    Ok(record)
}

// Same as `query_record`, but runs inside an existing transaction.
fn query_record_tx(tx: &rusqlite::Transaction<'_>, ingest_id: &str) -> Result<Option<FileRecord>, DbError> {
    let mut stmt = tx.prepare(
        "
        SELECT ingest_id, src_path, status, current_step, attempt_count, sha256, output_path, file_size,
               created_at, updated_at, last_error, locked_by, locked_at, heartbeat_at
        FROM files
        WHERE ingest_id = ?1
        ",
    )?;
    let record = stmt
        .query_row([ingest_id], |row| {
            let status_raw: String = row.get(2)?;
            let step_raw: String = row.get(3)?;
            Ok(FileRecord {
                ingest_id: row.get(0)?,
                src_path: row.get(1)?,
                status: PipelineStatus::from_str(&status_raw).map_err(|e| {
                    rusqlite::Error::FromSqlConversionFailure(
                        2,
                        rusqlite::types::Type::Text,
                        Box::new(e),
                    )
                })?,
                current_step: PipelineStep::from_str(&step_raw).map_err(|e| {
                    rusqlite::Error::FromSqlConversionFailure(
                        3,
                        rusqlite::types::Type::Text,
                        Box::new(e),
                    )
                })?,
                attempt_count: row.get(4)?,
                sha256: row.get(5)?,
                output_path: row.get(6)?,
                file_size: row.get(7)?,
                created_at: row.get(8)?,
                updated_at: row.get(9)?,
                last_error: row.get(10)?,
                locked_by: row.get(11)?,
                locked_at: row.get(12)?,
                heartbeat_at: row.get(13)?,
            })
        })
        .optional()?;
    Ok(record)
}

// Parse timestamp text from the database into a chrono UTC value.
pub fn parse_rfc3339(ts: &str) -> Result<DateTime<Utc>, DbError> {
    let parsed = DateTime::parse_from_rfc3339(ts)
        .map_err(|_| DbError::InvalidEnum(format!("invalid timestamp: {ts}")))?;
    Ok(parsed.with_timezone(&Utc))
}
