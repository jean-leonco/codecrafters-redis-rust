use std::fmt;

use anyhow::Context;
use async_trait::async_trait;
use tokio::{
    io::{BufWriter, WriteHalf},
    net::TcpStream,
};

use crate::{
    db::{Db, Entry},
    message::Message,
};

use super::{Command, CommandArgs};

#[derive(Debug)]
pub(crate) struct SetCommand {
    key: String,
    value: String,
    expiration: Option<u128>,
}

impl SetCommand {
    pub(crate) fn new_command(key: String, value: String, expiration: Option<u128>) -> Self {
        Self {
            key,
            value,
            expiration,
        }
    }
}

impl fmt::Display for SetCommand {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SET")
    }
}

#[async_trait]
impl Command for SetCommand {
    fn new(args: CommandArgs) -> anyhow::Result<Self> {
        let key = args.first().expect("SET message should have key");
        let value = args.get(1).expect("SET message should have value");
        let mut expiration = None;

        for option in args[2..].windows(2) {
            let option_key = option.first().expect("SET message option should have key");
            let option_value = option.get(1).expect("SET message option should have value");

            if option_key.data.to_lowercase() == "px" {
                expiration = Some(option_value.data.parse::<u128>().with_context(|| {
                    format!("Failed to parse expiration time {}", option_value.data)
                })?);
            }
        }

        Ok(Self {
            key: key.to_string(),
            value: value.to_string(),
            expiration,
        })
    }

    fn to_message(&self) -> Message {
        let mut elements = vec![
            Message::bulk_string(String::from("SET")),
            Message::bulk_string(self.key.to_string()),
            Message::bulk_string(self.value.to_string()),
        ];

        if let Some(expiration) = &self.expiration {
            elements.push(Message::bulk_string(String::from("PX")));
            elements.push(Message::bulk_string(expiration.to_string()));
        }

        Message::array(elements)
    }

    async fn handle(
        &self,
        writer: &mut BufWriter<WriteHalf<TcpStream>>,
        db: &Db,
    ) -> Result<(), anyhow::Error> {
        db.insert_key(
            self.key.to_string(),
            Entry::new(self.value.to_string(), self.expiration),
        )
        .await;

        let message = Message::ok_message();
        message
            .send(writer)
            .await
            .context("Failed to send SET reply")?;

        Ok(())
    }
}
