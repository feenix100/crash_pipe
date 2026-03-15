// Shared enums that describe pipeline progress.
// These enums are stored as strings in SQLite and used by the worker logic to
// decide what has already happened and what still needs to happen.

use std::str::FromStr;

use thiserror::Error;

// High-level status for a record.
// Fault-injection points for testing crash/restart behavior.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PipelineStatus {
    Queued,
    Processing,
    Done,
    Failed,
}

impl PipelineStatus {
    // Convert the enum to the exact string stored in SQLite.
    // Convert the enum to the exact string stored in SQLite.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "Queued",
            Self::Processing => "Processing",
            Self::Done => "Done",
            Self::Failed => "Failed",
        }
    }
}

// Fine-grained pipeline step.
// The derived ordering lets the code compare progress with `<=` and friends.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum PipelineStep {
    Queued,
    Compressing,
    Compressed,
    Hashing,
    Hashed,
    Moving,
    Moved,
    Recording,
    Done,
}

impl PipelineStep {
    // Convert the enum to the exact string stored in SQLite.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "Queued",
            Self::Compressing => "Compressing",
            Self::Compressed => "Compressed",
            Self::Hashing => "Hashing",
            Self::Hashed => "Hashed",
            Self::Moving => "Moving",
            Self::Moved => "Moved",
            Self::Recording => "Recording",
            Self::Done => "Done",
        }
    }
}

// Used when parsing enum values from text fails.
#[derive(Debug, Error)]
#[error("invalid enum value: {0}")]
pub struct ParseEnumError(pub String);

// Parse the database string back into the enum.
impl FromStr for PipelineStatus {
    type Err = ParseEnumError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "Queued" => Ok(Self::Queued),
            "Processing" => Ok(Self::Processing),
            "Done" => Ok(Self::Done),
            "Failed" => Ok(Self::Failed),
            other => Err(ParseEnumError(other.to_string())),
        }
    }
}

// Parse the database string back into the enum.
impl FromStr for PipelineStep {
    type Err = ParseEnumError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "Queued" => Ok(Self::Queued),
            "Compressing" => Ok(Self::Compressing),
            "Compressed" => Ok(Self::Compressed),
            "Hashing" => Ok(Self::Hashing),
            "Hashed" => Ok(Self::Hashed),
            "Moving" => Ok(Self::Moving),
            "Moved" => Ok(Self::Moved),
            "Recording" => Ok(Self::Recording),
            "Done" => Ok(Self::Done),
            other => Err(ParseEnumError(other.to_string())),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Failpoint {
    Compressing,
    Hashing,
    Moving,
    Recording,
}

// Accept a few human-friendly spellings from the CLI.
impl FromStr for Failpoint {
    type Err = ParseEnumError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.to_ascii_lowercase().as_str() {
            "compressing" | "compress" => Ok(Self::Compressing),
            "hashing" | "hash" => Ok(Self::Hashing),
            "moving" | "move" => Ok(Self::Moving),
            "recording" | "record" => Ok(Self::Recording),
            other => Err(ParseEnumError(other.to_string())),
        }
    }
}

// Simple roundtrip tests make sure enum strings stay stable.
#[cfg(test)]
mod tests {
    use super::{PipelineStatus, PipelineStep};
    use std::str::FromStr;

    #[test]
    fn status_roundtrip() {
        let statuses = [
            PipelineStatus::Queued,
            PipelineStatus::Processing,
            PipelineStatus::Done,
            PipelineStatus::Failed,
        ];
        for status in statuses {
            let parsed = PipelineStatus::from_str(status.as_str()).expect("status parse");
            assert_eq!(parsed, status);
        }
    }

    #[test]
    fn step_roundtrip() {
        let steps = [
            PipelineStep::Queued,
            PipelineStep::Compressing,
            PipelineStep::Compressed,
            PipelineStep::Hashing,
            PipelineStep::Hashed,
            PipelineStep::Moving,
            PipelineStep::Moved,
            PipelineStep::Recording,
            PipelineStep::Done,
        ];
        for step in steps {
            let parsed = PipelineStep::from_str(step.as_str()).expect("step parse");
            assert_eq!(parsed, step);
        }
    }
}
