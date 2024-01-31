use camino::{Utf8Path, Utf8PathBuf};
use color_eyre::eyre::eyre;
use rusqlite::{Connection, Transaction};

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("database could not be opened:\n{0}")]
    Open(rusqlite::Error),

    #[error("migrations could not be performed on database:\n{0}")]
    Migration(rusqlite::Error),

    #[error("{path}:{hash} could not be inserted into database:\n{source}")]
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

    #[error("fetch failure:\n{0}")]
    QueryFailure(rusqlite::Error),

    #[error("update failure:\n{0}")]
    UpdateFailure(rusqlite::Error),

    #[error("duplicate paths found on query: {0:?}")]
    DuplicatePaths(Vec<Utf8PathBuf>),

    #[error("cannot update path of hash {0} which does not exist")]
    HashDoesNotExist(String),

    #[error("query affected {count} rows, expected {min_rows}..{max_rows}: {msg}")]
    TooManyRowsAffected {
        count: usize,
        min_rows: usize,
        max_rows: usize,
        msg: String,
    },

    #[error("query affected {count} rows, expected {min_rows}..{max_rows}: {msg}")]
    TooFewRowsAffected {
        count: usize,
        min_rows: usize,
        max_rows: usize,
        msg: String,
    },

    #[error("unknown db error:\n{0}")]
    Unknown(#[from] color_eyre::Report),
}

pub fn open(data_path: &Utf8Path) -> Result<Connection, Error> {
    let db_path = path(data_path);
    let conn = Connection::open(db_path).map_err(Error::Open)?;

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
        "SELECT path FROM files as f WHERE f.hash = ?1",
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

pub fn update_path(
    transaction: &Transaction<'_>,
    path: &Utf8Path,
    hash: &str,
) -> Result<(), Error> {
    let mut query = transaction
        .prepare("SELECT path FROM files as f where f.hash = ?1")
        .map_err(Error::QueryFailure)?;
    let matched_paths: Vec<String> = query
        .query_map([hash], |row| row.get(0))
        .map_err(Error::QueryFailure)?
        .collect::<Result<_, _>>()
        .map_err(Error::QueryFailure)?;
    match matched_paths.len() {
        0 => return Err(Error::HashDoesNotExist(hash.to_owned())),
        1 => {}
        2.. => {
            return Err(Error::DuplicatePaths(
                matched_paths.iter().map(Utf8PathBuf::from).collect(),
            ))
        }
    };

    let rows = transaction
        .execute(
            "UPDATE files
             SET path = ?1
             WHERE hash = ?2",
            [path.as_str(), hash],
        )
        .map_err(Error::UpdateFailure)?;

    match rows {
        0 => {
            return Err(Error::TooFewRowsAffected {
                count: rows,
                min_rows: 1,
                max_rows: 1,
                msg: "updating a hash path should update a single row".to_owned(),
            })
        }
        1 => {}
        2.. => {
            return Err(Error::TooManyRowsAffected {
                count: rows,
                min_rows: 1,
                max_rows: 1,
                msg: "updating a hash path should update a single row".to_owned(),
            })
        }
    };

    Ok(())
}
