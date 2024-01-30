use std::time::Instant;

use camino::Utf8Path;
use color_eyre::{eyre::WrapErr, Result};
use crossterm::{cursor::MoveToColumn, QueueableCommand};

use crate::db;
use crate::utils::{hash_file, recursive_directory_read};

pub fn init(data_path: &Utf8Path) -> Result<()> {
    let mut conn = db::open(data_path).wrap_err("Failed to open db")?;

    let transaction = conn
        .transaction()
        .wrap_err("Failed creating insert transaction")?;
    println!("Starting database generation at \"{data_path}\"");
    let now = Instant::now();
    let directory_contents =
        recursive_directory_read(data_path).wrap_err("Failed reading data directory contents")?;
    let total = directory_contents.len();
    for (i, p) in directory_contents
        .iter()
        .enumerate()
        .map(|(i, p)| (i + 1, p))
    {
        let mut stdout = std::io::stdout();
        stdout.queue(MoveToColumn(0)).wrap_err("Failed to move cursor to beginning of line")?;
        println!("Adding file {i}/{total}...");
        let h = hash_file(&p).wrap_err_with(|| format!("Could not hash file {p}"))?;
        let p = p
            .strip_prefix(data_path)
            .wrap_err_with(|| format!("Path \"{p}\" was not a base of \"{data_path}\""))?;
        db::insert_into(&transaction, p, &h).wrap_err("Failed inserting into table")?;
    }
    transaction
        .commit()
        .wrap_err("Could not commit transaction")?;
    let elapsed = now.elapsed();
    println!("Done generating database at \"{data_path}\". Took {elapsed:.2?}");

    Ok(())
}
