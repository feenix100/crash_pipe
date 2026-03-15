# crashpipe

`crashpipe` is a crash-resilient file pipeline in Rust with durable SQLite checkpoints and resumable workers.

It is built to show practical Rust patterns in a real system: explicit state machines, atomic file handling, structured recovery, and idempotent processing.

## What it does

Pipeline flow:

1. enqueue files from a startup scan and optional watcher events
2. claim jobs durably in SQLite with lock ownership
3. run `compress -> hash -> move -> record` with step checkpoints
4. recover conservatively on restart

## Core design

- `status`: `Queued | Processing | Done | Failed`
- `current_step`: `Queued | Compressing | Compressed | Hashing | Hashed | Moving | Moved | Recording | Done`
- unique `src_path` ensures one ingest record per source path
- `ingest_id` is generated once and reused for the path
- deterministic temp path: `outbox/.tmp/<ingest_id>.gz.tmp`
- atomic rename is the commit point (`temp -> final .gz`)

## Why hash original bytes?

SHA-256 is computed from source file bytes. This keeps the content fingerprint stable regardless of gzip container details.

## Quick Start

If you already have Rust installed, you can clone the repo and try `crashpipe` right away.

### Clone the repository

```bash
git clone https://github.com/feenix100/crash_pipe.git
cd crash_pipe-main
```

### Build the project

```bash
cargo build
```

### Run the program

```bash
cargo run -- \
  --inbox ./inbox \
  --outbox ./outbox \
  --db ./state.db \
  --workers 4 \
  --watch true \
  --lock-timeout-secs 30 \
  --verbose
```

### Run a one-shot pass

```bash
cargo run -- --once --verbose
```

## CLI

Flags:

- `--inbox <PATH>` default `./inbox`
- `--outbox <PATH>` default `./outbox`
- `--db <PATH>` default `./state.db`
- `--once` process current queue and exit
- `--watch <bool>` default `true` (active only when `--once` is not set)
- `--workers <N>` default `2`
- `--lock-timeout-secs <SECS>` default `30`
- `--failpoint <STEP>` optional: `compressing|hashing|moving|recording`
- `--verbose`

## Reconciliation rules

At startup `crashpipe`:

1. clears stale locks older than `--lock-timeout-secs`
2. loads non-done rows
3. applies conservative repair:
   - missing source file => mark `Failed`
   - final output exists => set output metadata, fill missing hash, mark `Done`
   - step indicates compression but temp artifact is missing => mark `Failed`
   - moving step with temp artifact present => keep resumable and let worker continue

## Atomic file handling

Compression always writes to deterministic temp artifact:

- `outbox/.tmp/<ingest_id>.gz.tmp`

`TempArtifact` guard removes temp files on drop unless `commit()` is called.
`commit()` does an atomic rename to final output path (same filesystem assumption).

## Migrations

- `001_init.sql` initial files table
- `002_pass2_resumable.sql` adds checkpoint/lock columns + unique `src_path`

## Resume demo (kill and restart)

1. Put files into `inbox/`.
2. Start with a failpoint:

```bash
cargo run -- --once --failpoint moving --verbose
```

3. This intentionally fails before move completion, leaving resumable DB state.
4. Restart normally:

```bash
cargo run -- --once --verbose
```

5. It resumes and finishes without duplicate output.

## Running Tests

CrashPipe includes both unit tests and integration tests for recovery behavior.

### Run all tests

```bash
cargo test
```

### Show test output

```bash
cargo test -- --nocapture
```

### Run the integration tests

```bash
cargo test --test pass2_integration
```

### Run one specific test

```bash
cargo test resume_after_failpoint_does_not_duplicate_output
```

Other useful examples:

```bash
cargo test idempotent_restart_reuses_existing_done_row
cargo test durable_claim_returns_distinct_jobs_for_workers
```

### Run tests in release mode

```bash
cargo test --release
```

Included tests cover:

- enum conversion and state roundtrips
- resume after failpoint without duplicate outputs
- idempotent restart reusing the same ingest record
- concurrent durable claims returning distinct jobs

## Why Rust fits this project

Rust is a strong fit for `crashpipe` because it encourages:

- explicit state handling
- safe resource cleanup through RAII
- robust error propagation
- reliable filesystem operations
- performance suitable for real file workloads

## License

MIT license
