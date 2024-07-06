use anyhow::Context;
use tokio::net::TcpStream;

use crate::{
    db::{Db, Entry},
    message::Message,
};

pub(crate) async fn handle(
    db: &Db,
    key: String,
    value: String,
    expiration: Option<u128>,
    stream: &mut TcpStream,
) -> Result<(), anyhow::Error> {
    let mut db = db.lock().await;
    db.insert(key, Entry::new(value, expiration));

    let message = Message::SimpleString {
        data: String::from("OK"),
    };
    message
        .send(stream)
        .await
        .context("Failed to send SET reply")?;

    Ok(())
}
