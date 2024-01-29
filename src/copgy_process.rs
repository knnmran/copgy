use crate::pg::{get_db_client, parse_sqls};
use crate::{
    get_time_now, CopgyError, CopgyItem, CopyItem, ExecuteItem, COPY, EXECUTE, SUCCESS,
};
use postgres::Client;
use std::io::{BufReader, Read, Write};

pub fn process_run(
    source_db_url: &str,
    dest_db_url: &str,
    copgy_items: Vec<CopgyItem>,
) -> Result<(), CopgyError> {
    validate_process(&copgy_items)?;

    let mut source_client = get_db_client(source_db_url)?;
    let mut destination_client = get_db_client(dest_db_url)?;

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
    let reader = match source_client.copy_out(&copy_sql) {
        Ok(client) => BufReader::new(client),
        Err(e) => return Err(CopgyError::PostgresError(e.to_string())),
    };

    let copy_to_sql = format!("COPY {} FROM stdin", copy_item.dest_table);
    let mut writer = match destination_client.copy_in(&copy_to_sql) {
        Ok(client) => client,
        Err(e) => return Err(CopgyError::PostgresError(e.to_string())),
    };

    for byte in reader.bytes() {
        let bytes = match byte {
            Ok(bytes) => bytes,
            Err(e) => return Err(CopgyError::BufferReadError(e.to_string())),
        };

        if let Err(e) = writer.write(&[bytes]) {
            return Err(CopgyError::BufferWriterError(e.to_string()));
        };
    }

    if let Err(e) = writer.finish() {
        return Err(CopgyError::BufferFinishError(e.to_string()));
    };

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

        if let Err(e) = source_client.execute(&source_sql, &[]) {
            return Err(CopgyError::PostgresError(e.to_string()));
        };
    };

    if let Some(dest_sql) = execute_item.dest_sql {
        println!(
            r#"[{}] {} execute on destination "{}""#,
            get_time_now(),
            EXECUTE,
            &dest_sql
        );

        if let Err(e) = destination_client.execute(&dest_sql, &[]) {
            return Err(CopgyError::PostgresError(e.to_string()));
        };
    };

    Ok(())
}
