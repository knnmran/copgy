use crate::pg::{get_db_client, parse_sqls};
use crate::{CopgyError, CopgyItem, CopyItem, ExecuteItem, COPY, EXECUTE, SUCCESS};
use postgres::Client;
use std::io::BufReader;
use std::io::{Read, Write};

pub fn process_run(
    source_db_url: &String,
    dest_db_url: &String,
    copgy_items: Vec<CopgyItem>,
) -> Result<(), CopgyError> {
    validate_process(&copgy_items)?;

    println!("{} obtaining source db connection", SUCCESS);
    let mut source_client = get_db_client(source_db_url)?;
    println!("{} obtaining destination db connection", SUCCESS);
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

fn validate_process(copgy_items: &Vec<CopgyItem>) -> Result<(), CopgyError> {
    println!("{} validating sql", SUCCESS);

    let mut sqls: Vec<String> = Vec::new();

    copgy_items.clone().into_iter().for_each(|copgy_item| {
        if let Some(copy_item) = copgy_item.copy {
            sqls.push(copy_item.source_sql)
        } else if let Some(execute_item) = copgy_item.execute {
            if let Some(source_sql) = execute_item.source_sql {
                sqls.push(source_sql)
            }

            if let Some(dest_sql) = execute_item.dest_sql {
                sqls.push(dest_sql)
            }
        }
    });

    let mut final_sql = sqls.join(";");
    final_sql.push_str(";");

    parse_sqls(&final_sql)?;

    Ok(())
}

fn copy_process(
    source_client: &mut Client,
    destination_client: &mut Client,
    copy_item: CopyItem,
) -> Result<(), CopgyError> {
    println!(
        r#"{} copying to "{}" using "{}""#,
        COPY, &copy_item.dest_table, &copy_item.source_sql
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
        println!(r#"{} executing on source db "{}""#, EXECUTE, &source_sql);

        match source_client.execute(&source_sql, &[]) {
            Ok(_) => {}
            Err(e) => return Err(CopgyError::PostgresError(e.to_string())),
        };
    };

    if let Some(dest_sql) = execute_item.dest_sql {
        println!(r#"{} executing on destination db "{}""#, EXECUTE, &dest_sql);
        match destination_client.execute(&dest_sql, &[]) {
            Ok(_) => {}
            Err(e) => return Err(CopgyError::PostgresError(e.to_string())),
        };
    };

    Ok(())
}
