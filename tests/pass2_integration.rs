use std::fs;
use std::thread;

use crashpipe::db::Database;
use crashpipe::pipeline::{Pipeline, PipelineConfig};
use crashpipe::state::{Failpoint, PipelineStatus, PipelineStep};
use tempfile::tempdir;

fn setup() -> (tempfile::TempDir, Database, PipelineConfig) {
    let temp = tempdir().expect("tempdir");
    let db_path = temp.path().join("state.db");
    let outbox = temp.path().join("outbox");
    fs::create_dir_all(&outbox).expect("create outbox");

    let db = Database::open(&db_path).expect("open db");
    db.run_migrations("migrations").expect("migrate");

    let config = PipelineConfig {
        outbox,
        lock_timeout_secs: 2,
        failpoint: None,
    };
    (temp, db, config)
}

#[test]
fn resume_after_failpoint_does_not_duplicate_output() {
    let (temp, db, mut config) = setup();
    let inbox = temp.path().join("inbox");
    fs::create_dir_all(&inbox).expect("create inbox");
    let src = inbox.join("sample.txt");
    fs::write(&src, "crashpipe-pass2").expect("write source");

    config.failpoint = Some(Failpoint::Moving);
    let failing = Pipeline::new(db.clone(), config.clone());
    failing.scan_and_enqueue(&inbox).expect("enqueue");
    failing
        .worker_loop("worker-fail".to_string(), true)
        .expect("run failpoint worker");

    let failed = db
        .get_by_src_path(&src.to_string_lossy())
        .expect("query failed row")
        .expect("row exists");
    assert_eq!(failed.status, PipelineStatus::Failed);

    config.failpoint = None;
    let resumed = Pipeline::new(db.clone(), config.clone());
    resumed.scan_and_enqueue(&inbox).expect("re-enqueue");
    resumed.reconcile_startup().expect("reconcile");
    resumed
        .worker_loop("worker-resume".to_string(), true)
        .expect("resume worker");

    let done = db
        .get_by_src_path(&src.to_string_lossy())
        .expect("query done row")
        .expect("done row exists");
    assert_eq!(done.status, PipelineStatus::Done);
    assert_eq!(done.current_step, PipelineStep::Done);
    let output = done.output_path.expect("output path");
    assert!(std::path::Path::new(&output).exists());

    let out_count = fs::read_dir(config.outbox)
        .expect("read outbox")
        .filter_map(Result::ok)
        .filter(|e| e.path().is_file())
        .count();
    assert_eq!(out_count, 1);
}

#[test]
fn idempotent_restart_reuses_existing_done_row() {
    let (temp, db, config) = setup();
    let inbox = temp.path().join("inbox");
    fs::create_dir_all(&inbox).expect("create inbox");
    let src = inbox.join("idempotent.txt");
    fs::write(&src, "same-file").expect("write source");

    let pipeline = Pipeline::new(db.clone(), config.clone());
    pipeline.scan_and_enqueue(&inbox).expect("enqueue first");
    pipeline
        .worker_loop("worker-1".to_string(), true)
        .expect("first run");

    let first = db
        .get_by_src_path(&src.to_string_lossy())
        .expect("query first")
        .expect("first row");
    let first_ingest = first.ingest_id.clone();

    pipeline.scan_and_enqueue(&inbox).expect("enqueue second");
    pipeline
        .worker_loop("worker-2".to_string(), true)
        .expect("second run");

    let second = db
        .get_by_src_path(&src.to_string_lossy())
        .expect("query second")
        .expect("second row");
    assert_eq!(second.ingest_id, first_ingest);
    assert_eq!(second.status, PipelineStatus::Done);
    assert_eq!(second.current_step, PipelineStep::Done);
}

#[test]
fn durable_claim_returns_distinct_jobs_for_workers() {
    let (temp, db, config) = setup();
    let inbox = temp.path().join("inbox");
    fs::create_dir_all(&inbox).expect("create inbox");
    for i in 0..4 {
        let src = inbox.join(format!("claim-{i}.txt"));
        fs::write(&src, format!("payload-{i}")).expect("write src");
        let meta = fs::metadata(&src).expect("meta");
        db.enqueue_file(&src.to_string_lossy(), meta.len() as i64)
            .expect("enqueue");
    }

    let db_a = db.clone();
    let db_b = db.clone();
    let a = thread::spawn(move || db_a.claim_next_job("worker-a", config.lock_timeout_secs));
    let b = thread::spawn(move || db_b.claim_next_job("worker-b", config.lock_timeout_secs));
    let job_a = a.join().expect("join a").expect("claim a").expect("job a");
    let job_b = b.join().expect("join b").expect("claim b").expect("job b");

    assert_ne!(job_a.ingest_id, job_b.ingest_id);
}
