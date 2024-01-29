use camino::{Utf8Path, Utf8PathBuf};
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

pub fn recursive_directory_read(path: &Utf8Path) -> Result<Vec<Utf8PathBuf>> {
    let v: Result<Vec<_>> = path
        .read_dir_utf8()
        .wrap_err("Failed reading directory contents")?
        .map(|e| e.wrap_err("Failed reading file"))
        .collect();
    let entries = v?;
    let mut paths = vec![];
    for entry in entries {
        let p = entry.path();
        if entry
            .metadata()
            .wrap_err_with(|| format!("Failed reading metadata for {p}"))?
            .is_dir()
        {
            let v = recursive_directory_read(p)
                .wrap_err_with(|| format!("Failed reading directory contents of {p}"))?;
            paths.extend(v);
        } else {
            if p.file_name().expect("Path is a file") == "cstfs.db" {
                continue;
            }
            paths.push(p.into());
        }
    }

    Ok(paths)
}
