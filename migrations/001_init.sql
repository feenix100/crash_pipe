CREATE TABLE IF NOT EXISTS files (
    ingest_id TEXT PRIMARY KEY,
    src_path TEXT NOT NULL,
    status TEXT NOT NULL,
    sha256 TEXT,
    output_path TEXT,
    file_size INTEGER,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    last_error TEXT
);

CREATE INDEX IF NOT EXISTS idx_files_src_path_status ON files(src_path, status);
