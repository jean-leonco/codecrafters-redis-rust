use std::fmt;

use anyhow::Context;
use async_trait::async_trait;
use tokio::net::TcpStream;

use crate::{
    db::{Db, Entry},
    message::{Array, BulkString, Message, SimpleString},
    server_config::ServerConfig,
};

use super::Command;

#[derive(Debug)]
pub(crate) struct SetCommand {
    key: String,
    value: String,
    expiration: Option<u128>,
}

impl fmt::Display for SetCommand {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SET")
    }
}

#[async_trait]
impl Command for SetCommand {
    fn new(args: &[BulkString]) -> anyhow::Result<Self> {
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
            key: key.data.to_string(),
            value: value.data.to_string(),
            expiration,
        })
    }

    fn to_message(&self) -> Message {
        let mut elements = vec![
            Message::BulkString(BulkString {
                data: String::from("SET"),
            }),
            Message::BulkString(BulkString {
                data: self.key.to_string(),
            }),
            Message::BulkString(BulkString {
                data: self.value.to_string(),
            }),
        ];

        if let Some(expiration) = &self.expiration {
            elements.push(Message::BulkString(BulkString {
                data: String::from("PX"),
            }));
            elements.push(Message::BulkString(BulkString {
                data: expiration.to_string(),
            }));
        }

        Message::Array(Array { elements })
    }

    async fn handle(
        &self,
        stream: &mut TcpStream,
        db: &Db,
        _: &ServerConfig,
    ) -> Result<(), anyhow::Error> {
        let mut db = db.lock().await;
        db.insert(
            self.key.to_string(),
            Entry::new(self.value.to_string(), self.expiration),
        );

        let message = Message::SimpleString(SimpleString {
            data: String::from("OK"),
        });
        message
            .send(stream)
            .await
            .context("Failed to send SET reply")?;

        Ok(())
    }
}
