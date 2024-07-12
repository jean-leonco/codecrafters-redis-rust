use std::fmt;

use anyhow::Context;
use async_trait::async_trait;
use tokio::net::TcpStream;

use crate::{db::Db, message::Message};

use super::{Command, CommandArgs};

#[derive(Debug)]
pub(crate) struct EchoCommand {
    message: String,
}

impl fmt::Display for EchoCommand {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ECHO")
    }
}

#[async_trait]
impl Command for EchoCommand {
    fn new(args: CommandArgs) -> anyhow::Result<Self> {
        let message = args
            .first()
            .expect("ECHO message should have reply message")
            .to_string();

        Ok(Self { message })
    }

    fn to_message(&self) -> Message {
        let elements = vec![
            Message::bulk_string(String::from("ECHO")),
            Message::bulk_string(self.message.to_string()),
        ];

        Message::array(elements)
    }

    async fn handle(&self, stream: &mut TcpStream, _: &Db) -> anyhow::Result<()> {
        let message = Message::simple_string(self.message.to_string());
        message
            .send(stream)
            .await
            .context("Failed to send ECHO reply")?;

        Ok(())
    }
}
