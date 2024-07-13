use std::{collections::HashSet, fmt, io::Write};

use anyhow::{Context, Ok};
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

#[derive(Debug, Eq, PartialEq, Hash)]
pub(crate) enum InfoSection {
    Server,
    Replication,
    Default,
}

impl fmt::Display for InfoSection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            InfoSection::Server => write!(f, "server"),
            InfoSection::Replication => write!(f, "replication"),
            InfoSection::Default => write!(f, "default"),
        }
    }
}

impl InfoSection {
    fn parse(value: &str) -> anyhow::Result<Self> {
        match value {
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

impl fmt::Display for InfoCommand {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "INFO")
    }
}

#[async_trait]
impl Command for InfoCommand {
    fn new(args: CommandArgs) -> anyhow::Result<Self> {
        let mut sections = HashSet::new();
        for args in args {
            sections.insert(InfoSection::parse(&args.data)?);
        }

        Ok(Self { sections })
    }

    fn to_message(&self) -> Message {
        let mut elements = vec![Message::bulk_string(String::from("INFO"))];

        for section in &self.sections {
            elements.push(Message::bulk_string(section.to_string()));
        }

        Message::array(elements)
    }

    async fn handle(
        &self,
        writer: &mut BufWriter<WriteHalf<TcpStream>>,
        db: &Db,
    ) -> anyhow::Result<()> {
        let mut buf = Vec::new();

        if self.sections.is_empty() || self.sections.contains(&InfoSection::Default) {
            get_default_info(&mut buf, &db.state)?;
        } else {
            for section in &self.sections {
                match section {
                    InfoSection::Server => {
                        get_server_info(&mut buf, &db.state)
                            .context("Failed to get server info")?;
                    }
                    InfoSection::Replication => {
                        get_replication_info(&mut buf, &db.state)
                            .context("Failed to get replication info")?;
                    }
                    InfoSection::Default => unreachable!(),
                }
            }
        }

        let message = Message::bulk_string(String::from_utf8(buf)?);
        message
            .send(writer)
            .await
            .context("Failed to send INFO reply")?;

        Ok(())
    }
}

fn get_default_info(writer: &mut impl Write, state: &State) -> anyhow::Result<()> {
    get_server_info(writer, state).context("Failed to get server info")?;
    writeln!(writer)?;
    get_replication_info(writer, state).context("Failed to get replication info")?;

    Ok(())
}

fn get_server_info(writer: &mut impl Write, state: &State) -> anyhow::Result<()> {
    let (version, mode, os, arch_bits) = match state {
        State::Master {
            version,
            mode,
            os,
            arch_bits,
            ..
        }
        | State::Slave {
            version,
            mode,
            os,
            arch_bits,
            ..
        } => (version, mode, os, arch_bits),
    };

    writeln!(writer, "# Server")?;
    writeln!(writer, "redis_version:{}", version)?;
    writeln!(writer, "redis_mode:{}", mode)?;
    writeln!(writer, "os:{}", os)?;
    writeln!(writer, "arch_bits:{}", arch_bits)?;

    Ok(())
}

fn get_replication_info(writer: &mut impl Write, state: &State) -> anyhow::Result<()> {
    writeln!(writer, "# Replication")?;

    match state {
        State::Master {
            replication_id,
            replication_offset,
            ..
        } => {
            writeln!(writer, "role:master")?;
            writeln!(writer, "master_replid:{}", replication_id)?;
            writeln!(writer, "master_repl_offset:{}", replication_offset)?;
        }
        State::Slave { .. } => {
            writeln!(writer, "role:slave")?;
        }
    }

    Ok(())
}
