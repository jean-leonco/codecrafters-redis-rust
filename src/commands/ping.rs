use anyhow::Context;
use async_trait::async_trait;
use tokio::net::TcpStream;

use crate::{
    db::Db,
    message::{BulkString, Message, SimpleString},
    server_config::ServerConfig,
};

use super::Command;

#[derive(Debug)]
pub(crate) struct PingCommand {
    message: String,
}

#[async_trait]
impl Command for PingCommand {
    fn new(args: &[BulkString]) -> anyhow::Result<Self> {
        Ok(Self {
            message: match args.first() {
                Some(value) => value.data.to_string(),
                None => String::from("PONG"),
            },
        })
    }

    async fn handle(&self, stream: &mut TcpStream, _: &Db, _: &ServerConfig) -> anyhow::Result<()> {
        let message = Message::SimpleString(SimpleString {
            data: self.message.to_string(),
        });
        message
            .send(stream)
            .await
            .context("Failed to send PING reply")?;

        Ok(())
    }
}
