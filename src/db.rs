use camino::{Utf8Path, Utf8PathBuf};
use color_eyre::{eyre::WrapErr, Result};
use rusqlite::Connection;

pub fn open(data_path: &Utf8Path) -> Result<Connection> {
    let db_path = db_path(data_path);
    let conn =
        Connection::open(&db_path).wrap_err_with(|| format!("Failed to open db at {db_path}"))?;

    conn.execute(
        "
        CREATE TABLE IF NOT EXISTS files (
            path TEXT NOT NULL,
            hash TEXT NOT NULL PRIMARY KEY
        )",
        (),
    )
    .wrap_err("Failed creating table")?;
    Ok(conn)
}

pub fn db_path(data_path: &Utf8Path) -> Utf8PathBuf {
    data_path.join("cstfs.db")
}
