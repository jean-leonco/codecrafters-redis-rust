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
    fn new(args: &[BulkString]) -> anyhow::Result<Self> {
        let message = args
            .first()
            .expect("ECHO message should have reply message");

        Ok(Self {
            message: message.data.to_string(),
        })
    }

    fn to_message(&self) -> Message {
        let elements = vec![
            Message::BulkString(BulkString {
                data: String::from("ECHO"),
            }),
            Message::BulkString(BulkString {
                data: self.message.to_string(),
            }),
        ];

        Message::Array(Array { elements })
    }

    async fn handle(&self, stream: &mut TcpStream, _: &Db, _: &ServerConfig) -> anyhow::Result<()> {
        let message = Message::SimpleString(SimpleString {
            data: self.message.to_string(),
        });
        message
            .send(stream)
            .await
            .context("Failed to send ECHO reply")?;

        Ok(())
    }
}
