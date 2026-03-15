// Library entry point for the crashpipe crate.
// This file simply re-exports the project modules so the binary can use them
// through `crashpipe::...` paths.

pub mod cli;
pub mod db;
pub mod error;
pub mod fs_ops;
pub mod logging;
pub mod migrations;
pub mod pipeline;
pub mod state;
pub mod watcher;
