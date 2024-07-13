use std::fmt;

use anyhow::Context;
use async_trait::async_trait;
use base64::{engine, Engine};
use tokio::{
    io::{AsyncWriteExt, BufWriter, WriteHalf},
    net::TcpStream,
};

use crate::{
    db::{Db, State},
    message::Message,
};

use super::{Command, CommandArgs};

#[derive(Debug)]
pub(crate) struct PSyncCommand {
    replication_id: String,
    offset: isize,
}

impl PSyncCommand {
    pub(crate) fn new_command(replication_id: String, offset: isize) -> Self {
        Self {
            replication_id,
            offset,
        }
    }
}

impl fmt::Display for PSyncCommand {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "PSYNC")
    }
}

#[async_trait]
impl Command for PSyncCommand {
    fn new(args: CommandArgs) -> anyhow::Result<Self> {
        let replication_id = args
            .first()
            .expect("PSYNC message should have replication_id")
            .to_string();
        let offset: isize = args
            .get(1)
            .expect("PSYNC message should have offset")
            .data
            .parse()
            .expect("offset should be a integer");

        Ok(Self {
            replication_id,
            offset,
        })
    }

    fn to_message(&self) -> Message {
        let elements = vec![
            Message::bulk_string(String::from("PSYNC")),
            Message::bulk_string(self.replication_id.to_string()),
            Message::bulk_string(self.offset.to_string()),
        ];

        Message::array(elements)
    }

    async fn handle(
        &self,
        writer: &mut BufWriter<WriteHalf<TcpStream>>,
        db: &Db,
    ) -> anyhow::Result<()> {
        match &*db.state {
            State::Master {
                replication_id,
                replication_offset,
                tx,
                ..
            } => {
                let message = Message::simple_string(format!(
                    "FULLRESYNC {} {}",
                    replication_id, replication_offset
                ));
                message
                    .send(writer)
                    .await
                    .context("Failed to send PSYNC FULLRESYNC reply")?;

                // https://github.com/codecrafters-io/redis-tester/blob/main/internal/assets/empty_rdb_hex.md
                let empty_rdb = engine::general_purpose::STANDARD.decode("UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog==")?;

                writer.write_all(b"$").await?;
                writer
                    .write_all(empty_rdb.len().to_string().as_bytes())
                    .await?;
                writer.write_all(b"\r\n").await?;
                writer.write_all(&empty_rdb).await?;
                writer.flush().await?;

                let mut rx = tx.subscribe();
                while let Ok(message) = rx.recv().await {
                    message
                        .send(writer)
                        .await
                        .context("Failed to broadcast message to replica")?;
                }

                Ok(())
            }
            State::Slave { .. } => anyhow::bail!("Slave can not handle PSYNC command"),
        }
    }
}
