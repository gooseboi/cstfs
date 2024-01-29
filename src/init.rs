use std::time::Instant;

use camino::Utf8Path;
use color_eyre::{eyre::WrapErr, Result};

use crate::db::open;
use crate::utils::{hash_file, recursive_directory_read};

pub fn init(data_path: &Utf8Path) -> Result<()> {
    let mut conn = open(data_path).wrap_err("Failed to open db")?;

    let transaction = conn
        .transaction()
        .wrap_err("Failed creating insert transaction")?;
    println!("Starting database generation at \"{data_path}\"");
    let now = Instant::now();
    for p in
        recursive_directory_read(data_path).wrap_err("Failed reading data directory contents")?
    {
        let h = hash_file(&p).wrap_err_with(|| format!("Could not hash file {p}"))?;
        let p = p
            .strip_prefix(data_path)
            .wrap_err_with(|| format!("Path \"{p}\" was not a base of \"{data_path}\""))?;
        transaction
            .execute(
                "INSERT INTO files(path, hash) VALUES (?1, ?2)",
                [p.as_str(), &h],
            )
            .wrap_err_with(|| format!("Failed inserting path \"{p}\" into db"))?;
    }
    transaction
        .commit()
        .wrap_err("Could not commit transaction")?;
    let elapsed = now.elapsed();
    println!("Done generating database at \"{data_path}\". Took {elapsed:.2?}");

    Ok(())
}
