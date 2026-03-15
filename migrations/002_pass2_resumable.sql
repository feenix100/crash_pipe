ALTER TABLE files ADD COLUMN current_step TEXT NOT NULL DEFAULT 'Queued';
ALTER TABLE files ADD COLUMN attempt_count INTEGER NOT NULL DEFAULT 0;
ALTER TABLE files ADD COLUMN locked_by TEXT;
ALTER TABLE files ADD COLUMN locked_at TEXT;
ALTER TABLE files ADD COLUMN heartbeat_at TEXT;

UPDATE files
SET current_step = CASE
    WHEN status = 'Done' THEN 'Done'
    WHEN status = 'Failed' THEN 'Recording'
    ELSE 'Queued'
END
WHERE current_step IS NULL OR current_step = '';

CREATE UNIQUE INDEX IF NOT EXISTS idx_files_src_path_unique ON files(src_path);
CREATE INDEX IF NOT EXISTS idx_files_status_step_lock ON files(status, current_step, locked_at);
