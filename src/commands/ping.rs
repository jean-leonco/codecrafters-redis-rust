use std::fmt;

use anyhow::Context;
use async_trait::async_trait;
use tokio::net::TcpStream;

use crate::{
    db::Db,
    message::{Array, BulkString, Message, SimpleString},
    server_config::ServerConfig,
};

use super::Command;

#[derive(Debug)]
pub(crate) struct PingCommand {
    message: Option<String>,
}

impl fmt::Display for PingCommand {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "PING")
    }
}

#[async_trait]
impl Command for PingCommand {
    fn new(args: &[BulkString]) -> anyhow::Result<Self> {
        Ok(Self {
            message: args.first().map(|value| value.data.to_string()),
        })
    }

    fn to_message(&self) -> Message {
        let mut elements = vec![Message::BulkString(BulkString {
            data: String::from("PING"),
        })];

        if let Some(message) = &self.message {
            elements.push(Message::BulkString(BulkString {
                data: message.to_string(),
            }))
        }

        Message::Array(Array { elements })
    }

    async fn handle(&self, stream: &mut TcpStream, _: &Db, _: &ServerConfig) -> anyhow::Result<()> {
        let message = Message::SimpleString(SimpleString {
            data: match &self.message {
                Some(value) => value.to_string(),
                None => String::from("PONG"),
            },
        });
        message
            .send(stream)
            .await
            .context("Failed to send PING reply")?;

        Ok(())
    }
}
