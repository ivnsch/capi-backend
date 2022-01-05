use algonaut::core::{Address, CompiledTeal, MicroAlgos};
use anyhow::{Error, Result};
use data_encoding::BASE64;
use tokio_postgres::{Client, NoTls, Row};

pub async fn create_db_client() -> Result<Client> {
    // Connect to the database.
    let (client, connection) =
        tokio_postgres::connect("host=localhost user=postgres password=postgres", NoTls).await?;

    // The connection object performs the actual communication with the database,
    // so spawn it off to run on its own.
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    Ok(client)
}

pub fn get_u64(row: &Row, index: usize) -> Result<u64> {
    row.get::<_, String>(index).parse().map_err(Error::msg)
}

pub fn get_microalgos(row: &Row, index: usize) -> Result<MicroAlgos> {
    Ok(MicroAlgos(
        row.get::<_, String>(index).parse().map_err(Error::msg)?,
    ))
}

pub fn get_address(row: &Row, index: usize) -> Result<Address> {
    row.get::<_, String>(index).parse().map_err(Error::msg)
}

pub fn get_bytes(row: &Row, index: usize) -> Result<CompiledTeal> {
    Ok(CompiledTeal(
        BASE64.decode(row.get::<_, String>(index).as_bytes())?,
    ))
}
