use camino::{Utf8Path, Utf8PathBuf};
use color_eyre::eyre::eyre;
use rusqlite::{Connection, Transaction};

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("database could not be opened")]
    Open(rusqlite::Error),

    #[error("migrations could not be performed on database")]
    Migration(rusqlite::Error),

    #[error("{path}:{hash} could not be inserted into database")]
    InsertionFailure {
        path: Utf8PathBuf,
        hash: String,
        source: rusqlite::Error,
    },

    #[error("\"{path_new}\" is duplicate of \"{path_old}\" in database")]
    DuplicateInsertion {
        path_new: Utf8PathBuf,
        path_old: Utf8PathBuf,
    },

    #[error("fetch failure")]
    QueryFailure(rusqlite::Error),

    #[error("unknown db error")]
    Unknown(#[from] color_eyre::Report),
}

pub fn open(data_path: &Utf8Path) -> Result<Connection, Error> {
    let db_path = path(data_path);
    let conn = Connection::open(&db_path).map_err(Error::Open)?;

    conn.execute(
        "
        CREATE TABLE IF NOT EXISTS files (
            path TEXT NOT NULL,
            hash TEXT NOT NULL PRIMARY KEY
        )",
        (),
    )
    .map_err(Error::Migration)?;

    Ok(conn)
}

pub fn path(data_path: &Utf8Path) -> Utf8PathBuf {
    data_path.join("cstfs.db")
}

pub fn insert_into(
    transaction: &Transaction<'_>,
    path: &Utf8Path,
    hash: &str,
) -> Result<(), Error> {
    let select_result: Result<String, rusqlite::Error> = transaction.query_row(
        "SELECT path, hash FROM files WHERE hash = ?1",
        [hash],
        |row| row.get(0),
    );

    match select_result {
        Ok(path_old) => {
            return Err(Error::DuplicateInsertion {
                path_old: Utf8PathBuf::from(path_old),
                path_new: path.to_path_buf(),
            })
        }
        Err(rusqlite::Error::QueryReturnedNoRows) => {}
        Err(e) => return Err(Error::QueryFailure(e)),
    }

    let rows = transaction
        .execute(
            "INSERT INTO files(path, hash) VALUES (?1, ?2)",
            [path.as_str(), hash],
        )
        .map_err(|e| Error::InsertionFailure {
            path: path.to_path_buf(),
            hash: hash.to_owned(),
            source: e,
        })?;

    if rows != 1 {
        return Err(Error::Unknown(eyre!(
            "More than one row was updated on insert: path={path}, hash={hash}"
        )));
    }
    Ok(())
}
