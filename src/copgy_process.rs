use crate::pg::{get_db_client, parse_sqls};
use crate::{
    get_time_now, Args, Commands, CopgyError, CopgyItem, CopyItem, ExecuteItem, COPY, EXECUTE,
    SUCCESS,
};
use postgres::Client;
use std::fs::read_to_string;
use std::io::{Read, Write};

pub fn process_run(args: Args) -> Result<(), CopgyError> {
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

            let copgy_item: CopgyItem = CopgyItem {
                copy: Some(copy_item),
                ..Default::default()
            };

            vec![copgy_item]
        }
        Commands::Script { file_path } => {
            let file_content = read_to_string(file_path)
                .map_err(|err| CopgyError::FileReadError(err.to_string()))?;

            serde_json::from_str::<Vec<CopgyItem>>(&file_content)
                .map_err(|err| CopgyError::FileParseError(err.to_string()))?
        }
    };

    if args.validate_sql.unwrap_or(false) {
        validate_process(&copgy_items)?;
    }

    let mut source_client = get_db_client(&args.source_db_url)?;
    let mut destination_client = get_db_client(&args.dest_db_url)?;

    for item in copgy_items {
        if let Some(copy_item) = item.copy {
            copy_process(&mut source_client, &mut destination_client, copy_item)?;
        }

        if let Some(execute_item) = item.execute {
            execute_process(&mut source_client, &mut destination_client, execute_item)?;
        }
    }

    Ok(())
}

fn validate_process(copgy_items: &[CopgyItem]) -> Result<(), CopgyError> {
    println!("[{}] {} validate sql", get_time_now(), SUCCESS);

    let mut sqls: Vec<String> = Vec::new();

    copgy_items.iter().for_each(|copgy_item| {
        if let Some(copy_item) = &copgy_item.copy {
            sqls.push(copy_item.source_sql.clone())
        } else if let Some(execute_item) = &copgy_item.execute {
            if let Some(source_sql) = &execute_item.source_sql {
                sqls.push(source_sql.to_string())
            }

            if let Some(dest_sql) = &execute_item.dest_sql {
                sqls.push(dest_sql.to_string())
            }
        }
    });

    let mut final_sql = sqls.join(";");
    final_sql.push(';');

    parse_sqls(&final_sql)?;

    Ok(())
}

fn copy_process(
    source_client: &mut Client,
    destination_client: &mut Client,
    copy_item: CopyItem,
) -> Result<(), CopgyError> {
    println!(
        r#"[{}] {} copy to "{}" using "{}""#,
        get_time_now(),
        COPY,
        &copy_item.dest_table,
        &copy_item.source_sql
    );
    let copy_sql = format!("COPY ({}) TO stdout", copy_item.source_sql);
    let reader = source_client
        .copy_out(&copy_sql)
        .map_err(|err| CopgyError::PostgresError(err.to_string()))?;

    let copy_to_sql = format!("COPY {} FROM stdin", copy_item.dest_table);
    let mut writer = destination_client
        .copy_in(&copy_to_sql)
        .map_err(|err| CopgyError::PostgresError(err.to_string()))?;

    for byte in reader.bytes() {
        let bytes = byte.map_err(|err| CopgyError::BufferReadError(err.to_string()))?;

        writer
            .write(&[bytes])
            .map_err(|err| CopgyError::BufferWriterError(err.to_string()))?;
    }

    writer
        .finish()
        .map_err(|err| CopgyError::BufferFinishError(err.to_string()))?;

    Ok(())
}

fn execute_process(
    source_client: &mut Client,
    destination_client: &mut Client,
    execute_item: ExecuteItem,
) -> Result<(), CopgyError> {
    if let Some(source_sql) = execute_item.source_sql {
        println!(
            r#"[{}] {} execute on source "{}""#,
            get_time_now(),
            EXECUTE,
            &source_sql
        );

        source_client
            .execute(&source_sql, &[])
            .map_err(|err| CopgyError::PostgresError(err.to_string()))?;
    };

    if let Some(dest_sql) = execute_item.dest_sql {
        println!(
            r#"[{}] {} execute on destination "{}""#,
            get_time_now(),
            EXECUTE,
            &dest_sql
        );

        destination_client
            .execute(&dest_sql, &[])
            .map_err(|err| CopgyError::PostgresError(err.to_string()))?;
    };

    Ok(())
}
