use camino::Utf8Path;
use color_eyre::{eyre::WrapErr, Result};
use std::time::Instant;

use crate::db;
use crate::utils::{hash_file, recursive_directory_read};

pub fn refresh(data_path: &Utf8Path) -> Result<()> {
    let mut conn = db::open(data_path).wrap_err("Failed to open db")?;
    println!("Starting refresh of \"{data_path}\"");
    let transaction = conn.transaction().wrap_err("Could not start transaction")?;
    let now = Instant::now();
    for p in recursive_directory_read(data_path).wrap_err("Failed reading directory contents")? {
        if p.file_name().expect("File has file name") == "cstfs.db" {
            continue;
        }
        let h = hash_file(&p).wrap_err_with(|| format!("Could not hash file {p}"))?;
        let p = p
            .strip_prefix(data_path)
            .wrap_err_with(|| format!("Path \"{p}\" was not a base of \"{data_path}\""))?;
        let res = transaction.query_row(
            "SELECT hash FROM files WHERE path=?1",
            [p.as_str()],
            |row| row.get(0),
        );

        let fetched_hash: Option<String> = match res {
            Ok(s) => Ok(Some(s)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e).wrap_err_with(|| format!("Failed querying hash for file {p}")),
        }?;

        if let Some(fetched_hash) = fetched_hash {
            assert_eq!(h, fetched_hash, "{p}");
        } else {
            println!("File {p} not found in cache");
        }
    }
    let elapsed = now.elapsed();
    println!("Done generating database at \"{data_path}\". Took {elapsed:.2?}");
    Ok(())
}
