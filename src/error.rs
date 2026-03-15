// Unified application error type.
// This lets higher-level pipeline code return one error enum even though
// failures may come from the database layer, filesystem layer, I/O, or watcher.

use thiserror::Error;

// Derive Display + Error implementations via `thiserror`.
#[derive(Debug, Error)]
// Errors that can bubble out of pipeline operations.
pub enum PipelineError {
    #[error("database error: {0}")]
    Db(#[from] crate::db::DbError),
    #[error("filesystem error: {0}")]
    Fs(#[from] crate::fs_ops::FsOpsError),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("watch error: {0}")]
    Watch(#[from] notify::Error),
    #[error("failpoint triggered at step: {0}")]
    Failpoint(&'static str),
}
