use camino::{Utf8Path, Utf8PathBuf};
use color_eyre::{eyre::{ensure, WrapErr}, Result};
use rusqlite::{Connection, Transaction};

pub fn open(data_path: &Utf8Path) -> Result<Connection> {
    let db_path = path(data_path);
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

pub fn path(data_path: &Utf8Path) -> Utf8PathBuf {
    data_path.join("cstfs.db")
}

pub fn insert_into(transaction: &Transaction<'_>, path: &Utf8Path, hash: &str) -> Result<()> {
    let rows = transaction
        .execute(
            "INSERT INTO files(path, hash) VALUES (?1, ?2)",
            [path.as_str(), hash],
        )
        .wrap_err_with(|| format!("Failed inserting path \"{path}\" into db"))?;
    ensure!(rows == 1, "More than one row was updated on insert: path={path}, hash={hash}");
    Ok(())
}
