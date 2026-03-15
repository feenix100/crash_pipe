// Filesystem helpers used by the pipeline.
// This module isolates the "do work on files" operations from the orchestration
// logic so pipeline.rs can focus on state transitions and recovery rules.

use std::fs::{self, File};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

use flate2::Compression;
use flate2::write::GzEncoder;
use sha2::{Digest, Sha256};
use thiserror::Error;

// Error type for filesystem-specific failures.
#[derive(Debug, Error)]
pub enum FsOpsError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("path error: {0}")]
    Path(String),
}

// Create the output folder plus a hidden `.tmp` area for in-progress artifacts.
pub fn ensure_outbox_layout(outbox: &Path) -> Result<(), FsOpsError> {
    fs::create_dir_all(outbox)?;
    fs::create_dir_all(outbox.join(".tmp"))?;
    Ok(())
}

// Temporary file path used while compression is still in progress.
// The ingest id makes the temp artifact stable across retries.
pub fn temp_output_path(outbox: &Path, ingest_id: &str) -> PathBuf {
    outbox.join(".tmp").join(format!("{ingest_id}.gz.tmp"))
}

// Final output name is based on the original file name with `.gz` appended.
pub fn final_output_path(outbox: &Path, src_path: &Path) -> Result<PathBuf, FsOpsError> {
    let file_name = src_path
        .file_name()
        .and_then(|f| f.to_str())
        .ok_or_else(|| FsOpsError::Path(format!("missing file name: {}", src_path.display())))?;
    Ok(outbox.join(format!("{file_name}.gz")))
}

// RAII guard around a temp file.
// If we never call `commit`, Drop removes the file so partial output is cleaned up.
pub struct TempArtifact {
    path: PathBuf,
    committed: bool,
}

impl TempArtifact {
    // Start tracking a temp artifact that may or may not be committed.
    pub fn new(path: PathBuf) -> Self {
        Self {
            path,
            committed: false,
        }
    }

    // Expose the temp path for inspection/debugging.
    pub fn path(&self) -> &Path {
        &self.path
    }

    // Atomically move the temp file into its final destination.
    // Renaming is the key step that avoids exposing half-written output.
    pub fn commit(mut self, final_path: &Path) -> Result<(), FsOpsError> {
        if let Some(parent) = final_path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::rename(&self.path, final_path)?;
        self.committed = true;
        Ok(())
    }
}

// If a panic/error happens before commit, best-effort delete the temp file.
impl Drop for TempArtifact {
    fn drop(&mut self) {
        if !self.committed {
            let _ = fs::remove_file(&self.path);
        }
    }
}

// Read the source file and stream it into a gzip encoder.
// Output is written to the temp path first, not the final path.
pub fn compress_gzip(src_path: &Path, temp_path: &Path) -> Result<(), FsOpsError> {
    if let Some(parent) = temp_path.parent() {
        fs::create_dir_all(parent)?;
    }

    let input = File::open(src_path)?;
    let output = File::create(temp_path)?;
    let mut reader = BufReader::new(input);
    let writer = BufWriter::new(output);
    let mut encoder = GzEncoder::new(writer, Compression::default());
    std::io::copy(&mut reader, &mut encoder)?;
    let mut writer = encoder.finish()?;
    writer.flush()?;
    drop(writer);
    Ok(())
}

// Compute a SHA-256 hash of the original source file.
// The result is stored in the database so the ingest record has stable metadata.
pub fn sha256_hex(src_path: &Path) -> Result<String, FsOpsError> {
    let file = File::open(src_path)?;
    let mut reader = BufReader::new(file);
    let mut hasher = Sha256::new();
    let mut buf = [0u8; 16 * 1024];
    loop {
        let n = reader.read(&mut buf)?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    Ok(hex::encode(hasher.finalize()))
}

// Small helper to make intent explicit at the call site.
pub fn file_exists(path: &Path) -> bool {
    path.is_file()
}
