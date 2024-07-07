use anyhow::Context;
use async_trait::async_trait;
use tokio::net::TcpStream;

use crate::{
    db::Db,
    message::{BulkString, Message},
    server_config::ServerConfig,
};

use super::Command;

#[derive(Debug)]
pub(crate) struct GetCommand {
    key: String,
}

#[async_trait]
impl Command for GetCommand {
    fn new(args: &[BulkString]) -> anyhow::Result<Self> {
        let key = args.first().expect("GET message should have key");

        Ok(Self {
            key: key.data.to_string(),
        })
    }

    async fn handle(
        &self,
        stream: &mut TcpStream,
        db: &Db,
        _: &ServerConfig,
    ) -> Result<(), anyhow::Error> {
        let mut db = db.lock().await;

        let message = match db.get(&self.key) {
            Some(value) if value.is_expired() => {
                db.remove(&self.key);
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
}
