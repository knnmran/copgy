use crate::{get_time_now, CopgyError, SUCCESS};
use native_tls::TlsConnector;
use postgres::{Client, Config};
use postgres_native_tls::MakeTlsConnector;
use sqlparser::{dialect::GenericDialect, parser::Parser};
use url::Url;

pub fn parse_db_url(url: &str) -> Result<PgParameters, CopgyError> {
    let url = Url::parse(url).map_err(|err| CopgyError::UrlParserError(err.to_string()))?;

    let host = url
        .host()
        .ok_or(CopgyError::UrlParserError("db url missing host".to_owned()))?;

    let port = if let Some(port) = url.port() {
        port
    } else {
        return Err(CopgyError::UrlParserError("db url missing port".to_owned()));
    };

    let db_name = if url.path().is_empty() {
        return Err(CopgyError::UrlParserError(
            "db url missing db name".to_owned(),
        ));
    } else {
        url.path().to_string().replace('/', "")
    };

    let mut pg_parameters = PgParameters::default();
    pg_parameters.host = host.to_string();
    pg_parameters.port = port;
    pg_parameters.dbname = db_name;

    if !url.username().is_empty() {
        pg_parameters.username = Some(url.username().to_owned());
    };

    if let Some(password) = url.password() {
        pg_parameters.password = Some(password.to_owned());
    };

    Ok(pg_parameters)
}

pub fn get_db_client(url: &str) -> Result<Client, CopgyError> {
    let pg_parameters = parse_db_url(url)?;

    let connector = TlsConnector::builder()
        .danger_accept_invalid_certs(true)
        .build()
        .map_err(|err| CopgyError::PostgresError(err.to_string()))?;

    let connector = MakeTlsConnector::new(connector);

    let mut config = Config::new();

    if let Some(username) = pg_parameters.username {
        config.user(&username);
    };

    if let Some(password) = pg_parameters.password {
        config.password(&password);
    };

    match config
        .host(pg_parameters.host.as_str())
        .port(pg_parameters.port)
        .dbname(&pg_parameters.dbname)
        .connect(connector)
    {
        Ok(client) => {
            println!(
                r#"[{}] {} obtain connection {} {} {}"#,
                get_time_now(),
                SUCCESS,
                &pg_parameters.host,
                &pg_parameters.port,
                &pg_parameters.dbname
            );
            Ok(client)
        }
        Err(e) => Err(CopgyError::PostgresError(e.to_string())),
    }
}

pub fn parse_sqls(sql: &str) -> Result<(), CopgyError> {
    let dialect = GenericDialect {};
    let parser = Parser::new(&dialect);

    match parser.try_with_sql(sql) {
        Ok(mut parser) => match parser.parse_statements() {
            Ok(_) => Ok(()),
            Err(e) => Err(CopgyError::SqlParserError(e.to_string())),
        },
        Err(e) => Err(CopgyError::SqlParserError(e.to_string())),
    }
}

#[derive(Default, Clone, Debug)]
pub struct PgParameters {
    pub host: String,
    pub port: u16,
    pub dbname: String,
    pub username: Option<String>,
    pub password: Option<String>,
}
