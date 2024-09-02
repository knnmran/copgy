mod copgy_process;
mod pg;

use crate::copgy_process::process_run;
use chrono::{SecondsFormat, Utc};
use clap::{Parser, Subcommand};
use console::Emoji;
use serde::Deserialize;
use std::{fmt, time::Instant};

fn main() -> Result<(), CopgyError> {
    let args = Args::parse();

    println!("[{}] {} copgy started", get_time_now(), START);
    let now = Instant::now();

    if let Err(err) = process_run(args) {
        println!(
            "[{}] {} error: {:?}",
            get_time_now(),
            ERROR,
            err.to_string()
        );
    } else {
        let new_now = Instant::now();
        let duration = new_now.duration_since(now);

        println!(
            "[{}] {} copgy completed in {:?}",
            get_time_now(),
            END,
            &duration
        );
    }

    Ok(())
}

pub fn get_time_now() -> String {
    Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true)
}

#[derive(Debug)]
pub enum CopgyError {
    FileReadError(String),
    FileParseError(String),
    PostgresError(String),
    SqlParserError(String),
    UrlParserError(String),
    BufferReadError(String),
    BufferWriterError(String),
    BufferFinishError(String),
}

impl fmt::Display for CopgyError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
pub struct Args {
    #[arg(long)]
    source_db_url: String,

    #[arg(long)]
    dest_db_url: String,

    #[arg(long)]
    validate_sql: Option<bool>,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    Single {
        #[arg(long)]
        source_sql: Option<String>,

        #[arg(long)]
        dest_table: Option<String>,
    },
    Script {
        #[arg(long)]
        file_path: String,
    },
}

#[derive(Debug, Deserialize, Default, Clone)]
pub struct CopgyItem {
    pub copy: Option<CopyItem>,
    pub execute: Option<ExecuteItem>,
}

#[derive(Debug, Deserialize, Default, Clone)]
pub struct CopyItem {
    pub source_sql: String,
    pub dest_table: String,
}

#[derive(Debug, Deserialize, Default, Clone)]
pub struct ExecuteItem {
    pub source_sql: Option<String>,
    pub dest_sql: Option<String>,
}

pub static START: Emoji = Emoji("🔊", "");
pub static END: Emoji = Emoji("🏁", "");
pub static COPY: Emoji = Emoji("🟦", "");
pub static EXECUTE: Emoji = Emoji("🟨", "");
pub static SUCCESS: Emoji = Emoji("🟩", "");
pub static ERROR: Emoji = Emoji("🟥", "");
