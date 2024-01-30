#![deny(
    clippy::enum_glob_use,
    clippy::pedantic,
    clippy::nursery,
    clippy::unwrap_used
)]

use camino::Utf8PathBuf;
use clap::{Parser, Subcommand};
use color_eyre::{
    eyre::{bail, WrapErr},
    Result,
};

mod db;
mod utils;

mod init;
mod refresh;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    /// Data store directory (where the pix are)
    #[arg(short, default_value_t = Utf8PathBuf::from("."))]
    data_dir: Utf8PathBuf,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Make an empty database in the directory
    Init {
        /// If true, will delete the existing database and make a new empty one
        #[arg(short, long)]
        force: bool,
    },
    /// Check the directory contents and compare against the database index,
    /// merging the new results
    Refresh,
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    let data_path = &cli.data_dir;

    let db_path = db::path(data_path);
    let db_exists = db_path
        .try_exists()
        .wrap_err("Could not check database existence")?;

    match cli.command {
        Command::Init { force } => {
            if db_exists && !force {
                bail!("Cannot initialize a database that already exists");
            }
            if force {
                println!("Regenerating database");
                std::fs::remove_file(db_path)
                    .wrap_err("Failed removing database to reinitialize")?;
            }
            init::init(data_path).wrap_err("Failed initializing db")?;
        }
        Command::Refresh => {
            refresh::refresh(data_path).wrap_err("Failed refreshing db contents")?;
        }
    };

    Ok(())
}
