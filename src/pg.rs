use crate::CopgyError;
use native_tls::TlsConnector;
use postgres::{Client, Config};
use postgres_native_tls::MakeTlsConnector;
use sqlparser::{dialect::GenericDialect, parser::Parser};
use url::Url;

pub fn parse_db_url(url: &str) -> Result<PgParameters, CopgyError> {
    match Url::parse(url) {
        Ok(url) => {
            let mut pg_parameters = PgParameters::default();

            pg_parameters.host = match url.host() {
                Some(host) => host.to_string(),
                None => return Err(CopgyError::UrlParserError("db url missing host".to_owned())),
            };

            pg_parameters.port = match url.port() {
                Some(port) => port,
                None => return Err(CopgyError::UrlParserError("db url missing port".to_owned())),
            };

            pg_parameters.dbname = url.path().to_string().replace("/", "");
            if pg_parameters.dbname.is_empty() {
                return Err(CopgyError::UrlParserError(
                    "db url missing dbname".to_owned(),
                ));
            };

            let username = url.username();

            pg_parameters.username = if username.is_empty() {
                None
            } else {
                Some(username.to_string())
            };

            pg_parameters.password = if let Some(password) = url.password() {
                Some(password.to_string())
            } else {
                None
            };

            Ok(pg_parameters)
        }
        Err(e) => return Err(CopgyError::UrlParserError(e.to_string())),
    }
}

pub fn get_db_client(url: &str) -> Result<Client, CopgyError> {
    let pg_parameters = parse_db_url(url)?;

    let connector = match TlsConnector::builder()
        .danger_accept_invalid_certs(true)
        .build()
    {
        Ok(connector) => connector,
        Err(e) => return Err(CopgyError::PostgresError(e.to_string())),
    };

    let connector = MakeTlsConnector::new(connector);

    let mut config = Config::new();

    if let Some(username) = pg_parameters.username {
        config.user(&username);
    }

    if let Some(password) = pg_parameters.password {
        config.password(&password);
    }

    match config
        .host(pg_parameters.host.as_str())
        .port(pg_parameters.port)
        .dbname(&pg_parameters.dbname)
        .connect(connector)
    {
        Ok(client) => Ok(client),
        Err(e) => return Err(CopgyError::PostgresError(e.to_string())),
    }
}

pub fn parse_sqls(sql: &String) -> Result<(), CopgyError> {
    let dialect = GenericDialect {};
    let parser = Parser::new(&dialect);

    match parser.try_with_sql(sql) {
        Ok(mut parser) => match parser.parse_statements() {
            Ok(_) => Ok(()),
            Err(e) => return Err(CopgyError::SqlParserError(e.to_string())),
        },
        Err(e) => return Err(CopgyError::SqlParserError(e.to_string())),
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
