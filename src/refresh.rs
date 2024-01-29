use camino::{Utf8Path, Utf8PathBuf};
use color_eyre::{eyre::WrapErr, Result};
use std::time::Instant;

use crate::db;
use crate::utils::{hash_file, recursive_directory_read};

#[derive(Debug)]
struct Diff {
    path: Utf8PathBuf,
    hash: String,
    ty: DiffType,
}

#[derive(Debug)]
enum DiffType {
    /// A new path was found, whose hash is not recorded in the db
    New,
    /// A new path was found, whose hash was already found in the db, while the original path still
    /// exists
    Duplicate { prev_path: Utf8PathBuf },
    /// A path's hash changed
    Changed { prev_hash: String },
    /// The previous path was removed, and there is a new path with the same hash
    Moved { prev_path: Utf8PathBuf },
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

fn coalesce_diffs(diffs: &mut Vec<Diff>, paths_and_hashes: &[(String, String)]) {
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
                    let Some((db_path, _)) = paths_and_hashes.iter().find(|(_, h)| *h == diff.hash)
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
                            path: prev_path, ..
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
                                prev_path: prev_path.clone(),
                            },
                        });
                    } else {
                        // If not, then there is a file in the index with the same hash as the file
                        // that was added, which means there's a duplicate hash
                        to_push.push(Diff {
                            path: diff.path.clone(),
                            hash: diff.hash.clone(),
                            ty: DiffType::Duplicate {
                                prev_path: db_path.into(),
                            },
                        });
                    }
                    // We always break, so as to not duplicate the coalescing upon finding a
                    // Removed
                    break 'inner;
                },
                // Duplicate: There is no way to coalesce a duplicate into another operation, as
                // the only way that could happen is if two duplicate files were added, at the same
                // time there was a file in the index with the exact same hash, so three
                // files. However, it is easier to have this as two duplicate diffs, rather than a
                // single one (TODO: Really?)
                //
                // Moved: There is also no way to coalesce a move, as two files cannot move to the
                // same location at the same time, nor can there be a file that is moved to two
                // locations, as one would be a copy, while the other is a move (FIXME: Maybe this
                // should be reflected?)
                DiffType::Duplicate { .. } | DiffType::Moved { .. } => {}
                _ => println!("Unimplemented coalescing of: {ty:#?}", ty = diff.ty),
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
    let paths_and_hashes: Result<Vec<(String, String)>> = query
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
        .wrap_err("Failed executing path and hash query")?
        .map(|v| v.wrap_err("Failed getting column from db"))
        .collect();
    let paths_and_hashes = paths_and_hashes.wrap_err("Failed fetching paths and hashes from db")?;

    let data_directory =
        recursive_directory_read(data_path).wrap_err("Failed reading directory contents")?;
    for p in &data_directory {
        if p.file_name().expect("File has file name") == "cstfs.db" {
            continue;
        }
        let h = hash_file(p).wrap_err_with(|| format!("Could not hash file {p}"))?;
        let p = p
            .strip_prefix(data_path)
            .wrap_err_with(|| format!("Path \"{p}\" was not a base of \"{data_path}\""))?;

        if let Some(fetched_hash) = paths_and_hashes
            .iter()
            .find(|(path, _)| *path == p)
            .map(|(_, h)| h)
        {
            if *fetched_hash != h {
                diffs.push(Diff {
                    path: p.to_path_buf(),
                    hash: h,
                    ty: DiffType::Changed {
                        prev_hash: fetched_hash.clone(),
                    },
                });
            }
        } else {
            diffs.push(Diff {
                path: p.to_path_buf(),
                hash: h,
                ty: DiffType::New,
            });
        }
    }

    for (p, h) in &paths_and_hashes {
        let p = Utf8Path::new(p);
        if !data_directory
            .iter()
            .map(|p| {
                p.strip_prefix(data_path)
                    .expect("Path is subdir of base directory")
            })
            .any(|path| path == p)
        {
            diffs.push(Diff {
                path: p.to_path_buf(),
                hash: h.clone(),
                ty: DiffType::Removed,
            });
        }
    }
    println!("Diffs before coalesce {diffs:#?}");
    coalesce_diffs(&mut diffs, &paths_and_hashes);
    println!("Diffs after coalesce {diffs:#?}");

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
