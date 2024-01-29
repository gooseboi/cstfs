use camino::Utf8Path;
use color_eyre::{eyre::WrapErr, Result};
use memmap2::Mmap;
use rusqlite::Connection;
use std::fs::OpenOptions;

fn hash_file(path: &Utf8Path) -> Result<String> {
    let file = OpenOptions::new()
        .read(true)
        .write(false)
        .create(false)
        .open(path)
        .wrap_err("Failed to open file")?;

    let mmap = unsafe { Mmap::map(&file).wrap_err("Failed mmaping file")? };

    let h = seahash::hash(&mmap);
    Ok(format!("{h:x}"))
}

fn main() -> Result<()> {
    let path = Utf8Path::new("cstfs.db");
    let mut conn =
        Connection::open(path).wrap_err_with(|| format!("Failed to open db at {path}"))?;

    conn.execute(
        "
        CREATE TABLE IF NOT EXISTS files (
            path TEXT NOT NULL,
            hash TEXT NOT NULL PRIMARY KEY
        )",
        (),
    )
    .wrap_err("Failed creating table")?;

    let transaction = conn
        .transaction()
        .wrap_err("Failed creating insert transaction")?;
    let data_path = Utf8Path::new("./data");
    for f in data_path
        .read_dir_utf8()
        .wrap_err("Failed reading directory")?
    {
        let f = f.wrap_err("Failed reading file metadata")?;
        if f.metadata()?.is_dir() {
            continue;
        }
        let p = f.path();
        let h = hash_file(&p).wrap_err_with(|| format!("Could not hash file {p}"))?;
        let p = p.strip_prefix(data_path).wrap_err_with(|| format!("Path {p} was not a base of {data_path}"))?;
        println!("hash({p}) = {h}");
        transaction
            .execute("INSERT INTO files(path, hash) VALUES (?1, ?2)", [p.as_str(), &h])
            .wrap_err_with(|| format!("Failed inserting path {p} into db"))?;
    }
    transaction.commit().wrap_err("Could not commit transaction")?;

    Ok(())
}
