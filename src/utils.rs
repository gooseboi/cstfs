use camino::Utf8Path;
use color_eyre::{eyre::WrapErr, Result};
use memmap2::Mmap;
use std::fs::OpenOptions;

pub fn hash_file(path: &Utf8Path) -> Result<String> {
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
