use anyhow::Context;
use async_trait::async_trait;
use tokio::net::TcpStream;

use crate::{
    db::Db,
    message::{BulkString, Message, SimpleString},
};

use super::Command;

#[derive(Debug)]
pub(crate) struct EchoCommand {
    message: String,
}

#[async_trait]
impl Command for EchoCommand {
    fn new(args: &[BulkString]) -> anyhow::Result<Self> {
        let message = args.first().expect("ECHO message should have reply string");

        Ok(Self {
            message: message.data.to_string(),
        })
    }

    async fn handle(&self, stream: &mut TcpStream, _: &Db) -> anyhow::Result<()> {
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
