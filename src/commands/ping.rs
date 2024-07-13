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
pub(crate) struct PingCommand {
    message: Option<String>,
}

impl PingCommand {
    pub(crate) fn new_command(message: Option<String>) -> Self {
        Self { message }
    }
}

impl fmt::Display for PingCommand {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "PING")
    }
}

#[async_trait]
impl Command for PingCommand {
    fn new(args: CommandArgs) -> anyhow::Result<Self> {
        Ok(Self {
            message: args.first().map(std::string::ToString::to_string),
        })
    }

    fn to_message(&self) -> Message {
        let mut elements = vec![Message::bulk_string(String::from("PING"))];

        if let Some(message) = &self.message {
            elements.push(Message::bulk_string(message.to_string()));
        }

        Message::array(elements)
    }

    async fn handle(
        &self,
        writer: &mut BufWriter<WriteHalf<TcpStream>>,
        _: &Db,
    ) -> anyhow::Result<()> {
        let message = Message::simple_string(match &self.message {
            Some(value) => value.to_string(),
            None => String::from("PONG"),
        });
        message
            .send(writer)
            .await
            .context("Failed to send PING reply")?;

        Ok(())
    }
}
