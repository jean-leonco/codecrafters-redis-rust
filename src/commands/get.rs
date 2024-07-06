use anyhow::Context;
use tokio::net::TcpStream;

use crate::{
    db::Db,
    message::{BulkString, Message},
};

pub(crate) async fn handle(
    db: &Db,
    key: String,
    stream: &mut TcpStream,
) -> Result<(), anyhow::Error> {
    let mut db = db.lock().await;

    let message = match db.get(&key) {
        Some(value) if value.is_expired() => {
            db.remove(&key);
            Message::NullBulkString
        }
        Some(value) => Message::BulkString(BulkString {
            data: value.value.to_string(),
        }),
        None => Message::NullBulkString,
    };
    message
        .send(stream)
        .await
        .context("Failed to send GET reply")?;

    Ok(())
}
