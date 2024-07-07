use std::{collections::HashSet, io::Write};

use anyhow::{Context, Ok};
use async_trait::async_trait;
use tokio::net::TcpStream;

use crate::{
    db::Db,
    message::{BulkString, Message},
    server_config::{ServerConfig, ServerRole},
};

use super::Command;

#[derive(Debug, Eq, PartialEq, Hash)]
pub(crate) enum InfoSection {
    Server,
    Replication,
    Default,
}

impl InfoSection {
    pub(crate) fn parse(value: String) -> anyhow::Result<Self> {
        match value.as_str() {
            "default" => Ok(Self::Default),
            "server" => Ok(Self::Server),
            "replication" => Ok(Self::Replication),
            value => anyhow::bail!("Unsupported option {value}"),
        }
    }
}

#[derive(Debug)]
pub(crate) struct InfoCommand {
    sections: HashSet<InfoSection>,
}

#[async_trait]
impl Command for InfoCommand {
    fn new(args: &[BulkString]) -> anyhow::Result<Self> {
        let mut sections = HashSet::new();
        for args in args.iter() {
            sections.insert(InfoSection::parse(args.data.to_string())?);
        }

        Ok(Self { sections })
    }

    async fn handle(
        &self,
        stream: &mut TcpStream,
        _: &Db,
        server_config: &ServerConfig,
    ) -> anyhow::Result<()> {
        let mut buf = Vec::new();

        if self.sections.is_empty() || self.sections.contains(&InfoSection::Default) {
            get_default_info(&mut buf, server_config)?
        } else {
            for section in &self.sections {
                match section {
                    InfoSection::Server => {
                        get_server_info(&mut buf, server_config)
                            .context("Failed to get server info")?;
                    }
                    InfoSection::Replication => {
                        get_replication_info(&mut buf, server_config)
                            .context("Failed to get replication info")?;
                    }
                    InfoSection::Default => unreachable!(),
                }
            }
        }

        let message = Message::BulkString(BulkString {
            data: String::from_utf8(buf)?,
        });
        message
            .send(stream)
            .await
            .context("Failed to send PING reply")?;

        Ok(())
    }
}

fn get_default_info(writer: &mut impl Write, server_config: &ServerConfig) -> anyhow::Result<()> {
    get_server_info(writer, server_config).context("Failed to get server info")?;
    writeln!(writer)?;
    get_replication_info(writer, server_config).context("Failed to get replication info")?;

    Ok(())
}

fn get_server_info(writer: &mut impl Write, server_config: &ServerConfig) -> anyhow::Result<()> {
    writeln!(writer, "# Server")?;
    writeln!(writer, "redis_version:{}", server_config.version)?;
    writeln!(writer, "redis_mode:{}", server_config.mode)?;
    writeln!(writer, "os:{}", server_config.os)?;
    writeln!(writer, "arch_bits:{}", server_config.arch_bits)?;

    Ok(())
}

fn get_replication_info(
    writer: &mut impl Write,
    server_config: &ServerConfig,
) -> anyhow::Result<()> {
    writeln!(writer, "# Replication")?;
    writeln!(writer, "role:{}", server_config.role)?;

    if server_config.role == ServerRole::Master {
        writeln!(writer, "master_replid:{}", server_config.replication_id)?;
        writeln!(
            writer,
            "master_repl_offset:{}",
            server_config.replication_offset
        )?;
    }

    Ok(())
}
