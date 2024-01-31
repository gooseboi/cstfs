use std::io::Write;
use std::time::Instant;

use camino::Utf8Path;
use color_eyre::{eyre::WrapErr, Result};
use crossterm::{
    cursor::{MoveToColumn, MoveUp},
    QueueableCommand,
};
use rusqlite::Transaction;

use crate::db;
use crate::utils::{self, hash_file, recursive_directory_read};

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
        stdout
            .queue(MoveToColumn(0))
            .wrap_err("Failed to move cursor to beginning of line")?;
        if i > 1 {
            stdout
                .queue(MoveUp(1))
                .wrap_err("Failed to move cursor up")?;
        }
        println!("Adding file {i}/{total}...");
        stdout.flush().wrap_err("Failed flushing")?;

        let h = hash_file(p).wrap_err_with(|| format!("Could not hash file {p}"))?;
        let p = p
            .strip_prefix(data_path)
            .wrap_err_with(|| format!("Path \"{p}\" was not a base of \"{data_path}\""))?;
        match db::insert_into(&transaction, p, &h) {
            Ok(()) => {}
            Err(db::Error::DuplicateInsertion { path_old, path_new }) => {
                handle_duplicate(&transaction, data_path, &path_old, &path_new, &h)
                    .wrap_err_with(|| format!("Could not handle duplicate file {p}"))?;
            }
            e @ Err(_) => e.wrap_err("Failed inserting into database")?,
        }
    }
    transaction
        .commit()
        .wrap_err("Could not commit transaction")?;
    let elapsed = now.elapsed();
    println!("Done generating database at \"{data_path}\". Took {elapsed:.2?}");

    Ok(())
}

fn handle_duplicate(
    transaction: &Transaction<'_>,
    data_path: &Utf8Path,
    path_old: &Utf8Path,
    path_new: &Utf8Path,
    hash: &str,
) -> Result<()> {
    const VALID_COMMANDS: &str = "Y/n/s/o/?";
    let flush = || -> Result<()> { std::io::stdout().flush().wrap_err("Failed flushing stdout") };

    print!("Found path \"{path_new}\", duplicate of \"{path_old}\", would you like to remove it? ({VALID_COMMANDS}): ");
    flush()?;

    let stdin = std::io::stdin();
    loop {
        let mut input = String::new();
        stdin
            .read_line(&mut input)
            .wrap_err("Failed reading line from stdin")?;
        println!();
        flush()?;
        match input.trim().to_lowercase().as_str() {
            "" | "y" => {
                let full_path = data_path.join(path_new);
                utils::remove_file(&full_path)
                    .wrap_err_with(|| format!("Could not remove path {path_new}"))?;
                println!("Removed file {path_new}");
                println!();
                flush()?;
                break;
            }
            "n" => {
                println!("Quitting...");
                std::process::exit(1);
            }
            "s" => todo!("Adding a file to the ignore list is not implemented"),
            "o" => {
                let full_path = data_path.join(path_old);
                utils::remove_file(&full_path)
                    .wrap_err_with(|| format!("Could not remove path {path_old}"))?;
                println!("Removed file {path_old}");
                db::update_path(transaction, path_new, hash)
                    .wrap_err_with(|| format!("Could not update path {path_new} at {hash}"))?;
                println!("Updated index with {path_new}");
                println!();
                flush()?;
                break;
            }
            "?" => {
                println!("y(Yes)  - Remove the new file");
                println!("n(No)   - Do not remove the file and quit the program");
                println!("s(Skip) - Skip the file and add it to the ignorelist");
                println!("o(Old)  - Remove the old file and keep the new one");
                println!("?(Help) - Print this message");
            }
            _ => println!("Invalid command, valid ones are ({VALID_COMMANDS})"),
        }
        flush()?;
    }
    Ok(())
}
