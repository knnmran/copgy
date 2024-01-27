mod copgy_process;
mod pg;

use clap::{Parser, Subcommand};
use console::Emoji;
use serde::Deserialize;
use std::{fmt, fs::read_to_string, process};

use crate::copgy_process::process_run;

fn main() {
    let args = Args::parse();

    println!("{} copgy started", START);

    let copgy_items = match args.command {
        Commands::Single {
            source_sql,
            dest_table,
        } => {
            let mut copy_item: CopyItem = CopyItem::default();
            if let Some(source_sql) = source_sql {
                copy_item.source_sql = source_sql;
            }

            if let Some(dest_table) = dest_table {
                copy_item.dest_table = dest_table;
            }

            let mut copgy_item: CopgyItem = CopgyItem::default();
            copgy_item.copy = Some(copy_item);

            vec![copgy_item]
        }
        Commands::Script { file_path } => {
            let file_content = match read_to_string(file_path) {
                Ok(file_content) => file_content,
                Err(e) => {
                    println!("{} error: {:?}", ERROR, e.to_string());
                    process::exit(1);
                }
            };

            match serde_json::from_str::<Vec<CopgyItem>>(&file_content) {
                Ok(copgy_items) => copgy_items,
                Err(e) => {
                    println!("{} error: {:?}", ERROR, e.to_string());
                    process::exit(1);
                }
            }
        }
    };

    match process_run(&args.source_db_url, &args.dest_db_url, copgy_items) {
        Ok(_) => {
            println!("{} finished process", SUCCESS);
        }
        Err(e) => {
            println!("{} error: {:?}", ERROR, e.to_string());
            process::exit(1);
        }
    };

    println!("{} copgy ended", END);
}

#[derive(Debug)]
pub enum CopgyError {
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
