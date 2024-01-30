use camino::{Utf8Path, Utf8PathBuf};
use color_eyre::{eyre::WrapErr, Result};
use memmap2::Mmap;
use std::fs::OpenOptions;

pub fn is_image_extension(ext: &str) -> bool {
    matches!(ext, "png" | "jpg" | "jpeg" | "avif" | "webp" | "gif")
}

pub fn is_audio_extension(ext: &str) -> bool {
    matches!(ext, "mp3" | "opus" | "flac")
}

pub fn is_video_extension(ext: &str) -> bool {
    matches!(ext, "mkv" | "mp4" | "mov" | "avi" | "webm")
}

/// Check if ext is an extension corresponding to a media file (video, audio or image)
pub fn is_media_extension(ext: &str) -> bool {
    is_image_extension(ext) || is_audio_extension(ext) || is_video_extension(ext)
}

/// Hash the file at `path` using seahash
pub fn hash_file(path: &Utf8Path) -> Result<String> {
    let file = OpenOptions::new()
        .read(true)
        .write(false)
        .create(false)
        .open(path)
        .wrap_err("Failed to open file")?;

    let mmap = unsafe { Mmap::map(&file).wrap_err("Failed mmaping file")? };

    let h = seahash::hash(&mmap);
    Ok(format!("{h:016x}"))
}

/// Return an vector that contains the paths for all files within the directory, recursively, or an
/// error upon any io failure
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
            match p.extension().map(is_media_extension) {
                Some(true) => {}
                Some(false) => {
                    println!("Cowardly refusing to index file \"{p}\" which is not a media file");
                    continue;
                }
                None => {
                    println!("Cowardly refusing to index file \"{p}\" which has no extension");
                    continue;
                }
            }
            paths.push(p.into());
        }
    }

    Ok(paths)
}

/// Remove a file, ignoring the case where the file is not found (like rm -f <file>)
pub fn remove_file(path: &Utf8Path) -> Result<()> {
    match std::fs::remove_file(path) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        e @ Err(_) => e.wrap_err("Failed to remove file")?,
    }

    Ok(())
}
