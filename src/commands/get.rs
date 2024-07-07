use std::fmt;

use anyhow::Context;
use async_trait::async_trait;
use tokio::net::TcpStream;

use crate::{db::Db, message::Message, server_config::ServerConfig};

use super::{Command, CommandArgs};

#[derive(Debug)]
pub(crate) struct GetCommand {
    key: String,
}

impl fmt::Display for GetCommand {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "GET")
    }
}

#[async_trait]
impl Command for GetCommand {
    fn new(args: CommandArgs) -> anyhow::Result<Self> {
        let key = args
            .first()
            .expect("GET message should have key")
            .to_string();

        Ok(Self { key })
    }

    fn to_message(&self) -> Message {
        let elements = vec![
            Message::bulk_string(String::from("GET")),
            Message::bulk_string(self.key.to_string()),
        ];

        Message::array(elements)
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
            Some(value) => Message::bulk_string(value.value.to_string()),
            None => Message::NullBulkString,
        };
        message
            .send(stream)
            .await
            .context("Failed to send GET reply")?;

        Ok(())
    }
}
