// Migration runner for the SQLite database.
// SQL files are loaded from the migrations directory, sorted by version prefix,
// and recorded in `schema_version` after they are applied successfully.

use std::fs;
use std::path::{Path, PathBuf};

use chrono::Utc;
use rusqlite::{Connection, params};

use crate::db::DbError;

// Represents a discovered SQL migration file.
#[derive(Debug, Clone)]
struct Migration {
    version: i64,
    path: PathBuf,
}

// Apply any unapplied migrations in ascending version order.
pub fn run(conn: &mut Connection, migrations_dir: &Path) -> Result<(), DbError> {
    ensure_schema_version_table(conn)?;
    let mut pending = load_migrations(migrations_dir)?;
    pending.sort_by_key(|m| m.version);

    for migration in pending {
        if is_applied(conn, migration.version)? {
            continue;
        }

        let sql = fs::read_to_string(&migration.path)?;
        let tx = conn.transaction()?;
        tx.execute_batch(&sql)?;
        tx.execute(
            "INSERT INTO schema_version(version, applied_at) VALUES (?1, ?2)",
            params![migration.version, Utc::now().to_rfc3339()],
        )?;
        tx.commit()?;
    }

    Ok(())
}

// Create the bookkeeping table that records which migrations have already run.
fn ensure_schema_version_table(conn: &Connection) -> Result<(), DbError> {
    conn.execute_batch(
        "
        CREATE TABLE IF NOT EXISTS schema_version (
            version INTEGER PRIMARY KEY,
            applied_at TEXT NOT NULL
        );
        ",
    )?;
    Ok(())
}

// Check whether a version number is already present in `schema_version`.
fn is_applied(conn: &Connection, version: i64) -> Result<bool, DbError> {
    let exists = conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM schema_version WHERE version = ?1)",
        [version],
        |row| row.get::<_, i64>(0),
    )?;
    Ok(exists == 1)
}

// Read migration files from disk and extract their numeric version prefixes.
fn load_migrations(migrations_dir: &Path) -> Result<Vec<Migration>, DbError> {
    let mut migrations = Vec::new();
    for entry in fs::read_dir(migrations_dir)? {
        let entry = entry?;
        let path = entry.path();
        if !path.is_file() || path.extension().and_then(|x| x.to_str()) != Some("sql") {
            continue;
        }
        let file_name = path
            .file_name()
            .and_then(|n| n.to_str())
            .ok_or_else(|| DbError::InvalidMigrationName(path.display().to_string()))?;
        let version = parse_version(file_name)?;
        migrations.push(Migration { version, path });
    }
    Ok(migrations)
}

// Parse the leading numeric portion of a migration file name like `001_init.sql`.
fn parse_version(file_name: &str) -> Result<i64, DbError> {
    let version_str = file_name
        .split('_')
        .next()
        .ok_or_else(|| DbError::InvalidMigrationName(file_name.to_string()))?;
    version_str
        .parse::<i64>()
        .map_err(|_| DbError::InvalidMigrationName(file_name.to_string()))
}
