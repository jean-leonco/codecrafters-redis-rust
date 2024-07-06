use std::io::Cursor;

use anyhow::{Context, Ok};
use async_trait::async_trait;
use tokio::net::TcpStream;

use crate::{
    db::Db,
    message::{Array, BulkString, Message},
};

pub(crate) mod echo;
pub(crate) mod get;
pub(crate) mod info;
pub(crate) mod ping;
pub(crate) mod set;

#[async_trait]
pub(crate) trait Command: Send {
    fn new(args: &[BulkString]) -> anyhow::Result<Self>
    where
        Self: Sized;

    async fn handle(&self, stream: &mut TcpStream, db: &Db) -> anyhow::Result<()>;
}

pub(crate) fn parse_command(buf: &mut [u8]) -> anyhow::Result<Box<dyn Command>> {
    let message: Array = Message::deserialize(&mut Cursor::new(buf))
        .context("Failed to parse message")?
        .try_into()?;

    let args: Vec<BulkString> = message
        .elements
        .iter()
        .map(|value| value.try_into().unwrap())
        .collect();

    let command = args.first().expect("Message should have command");
    let command_args = &args[1..];

    match command.data.to_lowercase().as_str() {
        "ping" => Ok(Box::new(ping::PingCommand::new(command_args)?)),
        "echo" => Ok(Box::new(echo::EchoCommand::new(command_args)?)),
        "set" => Ok(Box::new(set::SetCommand::new(command_args)?)),
        "get" => Ok(Box::new(get::GetCommand::new(command_args)?)),
        "info" => Ok(Box::new(info::InfoCommand::new(command_args)?)),
        command => anyhow::bail!("Command not implemented: {}", command),
    }
}
