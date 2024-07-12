use std::fmt;

use anyhow::Context;
use async_trait::async_trait;
use tokio::{
    io::{BufWriter, WriteHalf},
    net::TcpStream,
};

use crate::{
    db::{Db, State},
    message::Message,
};

use super::{Command, CommandArgs};

#[derive(Debug)]
enum Config {
    ListeningPort(u16),
    Capabilities(String),
}

#[derive(Debug)]
pub(crate) struct ReplConfCommand {
    config: Config,
}

impl ReplConfCommand {
    pub(crate) fn new_listening_port_command(port: u16) -> Self {
        Self {
            config: Config::ListeningPort(port),
        }
    }

    pub(crate) fn new_capabilities_command() -> Self {
        Self {
            config: Config::Capabilities(String::from("psync2")),
        }
    }
}

impl fmt::Display for ReplConfCommand {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "REPLCONF")
    }
}

#[async_trait]
impl Command for ReplConfCommand {
    fn new(args: CommandArgs) -> anyhow::Result<Self> {
        let config = args
            .first()
            .expect("REPLCONF message should have configuration");

        let config = match config.data.as_str() {
            "listening-port" => {
                let port: u16 = args
                    .get(1)
                    .expect("listening-port should have port number")
                    .data
                    .parse()
                    .expect("listening-port port should be a integer");

                Config::ListeningPort(port)
            }
            "capa" => {
                let capabilities = args
                    .get(1)
                    .expect("capa should have capabilities")
                    .to_string();

                Config::Capabilities(capabilities)
            }
            config => anyhow::bail!("Unsupported REPLCONF config {}", config),
        };

        Ok(Self { config })
    }

    fn to_message(&self) -> Message {
        let mut elements = vec![Message::bulk_string(String::from("REPLCONF"))];

        match &self.config {
            Config::ListeningPort(port) => {
                elements.push(Message::bulk_string(String::from("listening-port")));
                elements.push(Message::bulk_string(port.to_string()));
            }
            Config::Capabilities(capabilities) => {
                elements.push(Message::bulk_string(String::from("capa")));
                elements.push(Message::bulk_string(capabilities.to_string()));
            }
        }

        Message::array(elements)
    }

    async fn handle(
        &self,
        writer: &mut BufWriter<WriteHalf<TcpStream>>,
        db: &Db,
    ) -> anyhow::Result<()> {
        match &*db.state {
            State::Master { .. } => match self.config {
                Config::ListeningPort(_) => {
                    let message = Message::ok_message();
                    message
                        .send(writer)
                        .await
                        .context("Failed to send REPLCONF reply")?;

                    Ok(())
                }
                Config::Capabilities(_) => {
                    let message = Message::ok_message();
                    message
                        .send(writer)
                        .await
                        .context("Failed to send REPLCONF reply")?;

                    Ok(())
                }
            },
            State::Slave { .. } => anyhow::bail!("Slave can not handle REPLCONF command"),
        }
    }
}
