use camino::{Utf8Path, Utf8PathBuf};
use color_eyre::{eyre::WrapErr, Result};
use std::time::Instant;

use crate::db;
use crate::utils::{hash_file, recursive_directory_read};

/// Represents a change in the filesystem, containing metadata for what exactly happened.
#[derive(Debug)]
struct Diff {
    /// Path to the file that this diff refers to
    path: Utf8PathBuf,
    /// Hash of the file that this diff refers to
    hash: String,
    /// Type of the diff
    ty: DiffType,
}

/// Represents exactly what operation a diff encodes, and some other information if necessary for
/// the specific operation
#[derive(Debug)]
enum DiffType {
    /// A new path was found, whose hash is not recorded in the db
    New,
    /// A new path was found, whose hash was already found in the db, while the original path still
    /// exists
    Duplicate {
        /// Path to the file that was in the index before
        orig_path: Utf8PathBuf
    },
    /// A path's hash changed
    Changed {
        /// Hash of the file that was previously recorded in the index
        prev_hash: String
    },
    /// The previous path was removed, and there is a new path with the same hash
    Moved {
        /// Original path of the file before it was moved
        orig_path: Utf8PathBuf
    },
    /// The path was removed, and there is no new path with the same hash
    Removed,
}

fn remove_indeces<T>(v: &mut Vec<T>, indices: &[usize]) {
    // Sort indices in descending order to avoid invalidating subsequent indices
    let mut sorted_indices = indices.to_vec();
    // If we remove indeces in decreasing order, then no index can shift
    sorted_indices.sort_by(|a, b| b.cmp(a));

    for index in sorted_indices {
        if index < v.len() {
            v.remove(index);
        }
    }
}

fn coalesce_diffs(diffs: &mut Vec<Diff>, db_paths_and_hashes: &[(String, String)]) {
    'outer: loop {
        // List of indexes to remove
        // If this is empty, then the loop can stop, because there is no coalescing to be done
        let mut indeces_to_remove = vec![];
        let mut to_push = vec![];
        'inner: for (i, diff) in diffs.iter().enumerate() {
            match diff.ty {
                DiffType::New => {
                    // If the file is not in the index, we can continue, because the diff can
                    // remain as a New, as there cannot exist a file it should be related to
                    let Some((db_path, _)) =
                        db_paths_and_hashes.iter().find(|(_, h)| *h == diff.hash)
                    else {
                        continue 'inner;
                    };
                    // Since we're going to replace this with some other diff, this should always
                    // be removed
                    indeces_to_remove.push(i);
                    // Can we find another diff where the file removed has the same hash as this
                    // one?
                    if let Some((
                        i,
                        Diff {
                            path: removed_path, ..
                        },
                    )) = diffs
                        .iter()
                        .enumerate()
                        .find(|(_, d)| matches!(d.ty, DiffType::Removed) && d.hash == diff.hash)
                    {
                        // If so, then it means that we moved the previous file to be this one, and
                        // we can remove the removed entry as well, and coalesce the new file and
                        // the removed file into a move operation
                        indeces_to_remove.push(i);
                        to_push.push(Diff {
                            path: diff.path.clone(),
                            hash: diff.hash.clone(),
                            ty: DiffType::Moved {
                                orig_path: removed_path.clone(),
                            },
                        });
                    } else {
                        // If not, then there is a file in the index with the same hash as the file
                        // that was added, which means there's a duplicate hash
                        to_push.push(Diff {
                            path: diff.path.clone(),
                            hash: diff.hash.clone(),
                            ty: DiffType::Duplicate {
                                orig_path: db_path.into(),
                            },
                        });
                    }
                    // We always break, so as to not duplicate the coalescing upon finding a
                    // Removed
                    break 'inner;
                }
                // Duplicate: There is no way to coalesce a duplicate into another operation, as
                // the only way that could happen is if two duplicate files were added, at the same
                // time there was a file in the index with the exact same hash, so three
                // files. However, it is easier to have this as two duplicate diffs, rather than a
                // single one
                //
                // Moved: There is also no way to coalesce a move, as two files cannot move to the
                // same location at the same time, nor can there be a file that is moved to two
                // locations, as one would be a copy, while the other is a move
                //
                // Removed: Removals can only be coalesced with a new, to make a moved, otherwise they
                // remain as removeds. Since this is already done above, there is no need to handle
                // a removed specifically
                //
                // Changed: There is no way to coalesce a removal
                DiffType::Duplicate { .. }
                | DiffType::Moved { .. }
                | DiffType::Removed
                | DiffType::Changed { .. } => {}
            }
        }
        if to_push.is_empty() && indeces_to_remove.is_empty() {
            break 'outer;
        }
        remove_indeces(diffs, &indeces_to_remove);
        diffs.extend(to_push);
    }
}

fn generate_diffs(data_path: &Utf8Path) -> Result<Vec<Diff>> {
    let conn = db::open(data_path).wrap_err("Failed to open db")?;
    let mut diffs = vec![];

    let mut query = conn
        .prepare("SELECT path, hash FROM files")
        .wrap_err("Failed preparing path and hash query")?;
    let db_paths_and_hashes: Result<Vec<(String, String)>> = query
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
        .wrap_err("Failed executing path and hash query")?
        .map(|v| v.wrap_err("Failed getting column from db"))
        .collect();
    let db_paths_and_hashes =
        db_paths_and_hashes.wrap_err("Failed fetching paths and hashes from db")?;

    let data_path_contents =
        recursive_directory_read(data_path).wrap_err("Failed reading directory contents")?;
    for path in &data_path_contents {
        if path.file_name().expect("File has file name") == "cstfs.db" {
            continue;
        }
        let hash = hash_file(path).wrap_err_with(|| format!("Could not hash file {path}"))?;
        let path = path
            .strip_prefix(data_path)
            .wrap_err_with(|| format!("Path \"{path}\" was not a base of \"{data_path}\""))?;

        // If the file is in the db...
        if let Some(db_hash_for_path) = db_paths_and_hashes
            .iter()
            .find(|(db_path, _)| *db_path == path)
            .map(|(_, h)| h)
        {
            // ...and the hash in the db is different, then the file changed.
            if *db_hash_for_path != hash {
                diffs.push(Diff {
                    path: path.to_path_buf(),
                    hash,
                    ty: DiffType::Changed {
                        prev_hash: db_hash_for_path.clone(),
                    },
                });
            }
        } else {
            // Otherwise, the path didn't exist in the db, and the file is new
            diffs.push(Diff {
                path: path.to_path_buf(),
                hash,
                ty: DiffType::New,
            });
        }
    }

    for (path, hash) in &db_paths_and_hashes {
        let path = Utf8Path::new(path);
        // If a path in the directory is not in the cache...
        if !data_path_contents
            .iter()
            .map(|p| {
                p.strip_prefix(data_path)
                    .expect("Path is subdir of base directory")
            })
            .any(|db_path| db_path == path)
        {
            // ...it was removed
            diffs.push(Diff {
                path: path.to_path_buf(),
                hash: hash.clone(),
                ty: DiffType::Removed,
            });
        }
    }
    coalesce_diffs(&mut diffs, &db_paths_and_hashes);

    Ok(diffs)
}

pub fn refresh(data_path: &Utf8Path) -> Result<()> {
    println!("Starting refresh of \"{data_path}\"");
    let now = Instant::now();

    println!("Generating diff from index db");
    let _diffs = generate_diffs(data_path).wrap_err("Failed generating diffs")?;
    println!("Cannot apply diffs on index yet");

    let elapsed = now.elapsed();
    println!("Done generating database at \"{data_path}\". Took {elapsed:.2?}");

    Ok(())
}
