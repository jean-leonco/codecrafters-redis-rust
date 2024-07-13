use std::fmt;

use anyhow::Context;
use async_trait::async_trait;
use tokio::{
    io::{BufWriter, WriteHalf},
    net::TcpStream,
};

use crate::{db::Db, message::Message};

use super::{Command, CommandArgs};

#[derive(Debug)]
pub(crate) struct GetCommand {
    key: String,
}

impl fmt::Display for GetCommand {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "GET {}", self.key)
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
        writer: &mut BufWriter<WriteHalf<TcpStream>>,
        db: &Db,
    ) -> anyhow::Result<()> {
        let message = match db.get_key(&self.key).await {
            Some(value) => Message::bulk_string(value.value.to_string()),
            None => Message::NullBulkString,
        };
        message
            .send(writer)
            .await
            .context("Failed to send GET reply")?;

        Ok(())
    }
}
