use std::fmt;

use anyhow::Context;
use async_trait::async_trait;
use tokio::net::TcpStream;

use crate::{db::Db, message::Message, server_config::ServerConfig};

use super::{Command, CommandArgs};

#[derive(Debug)]
pub(crate) struct PSyncCommand {
    replication_id: String,
    offset: isize,
}

impl PSyncCommand {
    pub(crate) fn new_command(replication_id: String, offset: isize) -> Self {
        Self {
            replication_id,
            offset,
        }
    }
}

impl fmt::Display for PSyncCommand {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "PSYNC")
    }
}

#[async_trait]
impl Command for PSyncCommand {
    fn new(args: CommandArgs) -> anyhow::Result<Self> {
        let replication_id = args
            .first()
            .expect("PSYNC message should have replication_id")
            .to_string();
        let offset: isize = args
            .get(1)
            .expect("PSYNC message should have offset")
            .data
            .parse()
            .expect("offset should be a integer");

        Ok(Self {
            replication_id,
            offset,
        })
    }

    fn to_message(&self) -> Message {
        let elements = vec![
            Message::bulk_string(String::from("PSYNC")),
            Message::bulk_string(self.replication_id.to_string()),
            Message::bulk_string(self.offset.to_string()),
        ];

        Message::array(elements)
    }

    async fn handle(&self, stream: &mut TcpStream, _: &Db, _: &ServerConfig) -> anyhow::Result<()> {
        let message = Message::simple_string(format!("FULLRESYNC {} 0", self.replication_id));
        message
            .send(stream)
            .await
            .context("Failed to send PSYNC reply")?;

        Ok(())
    }
}
