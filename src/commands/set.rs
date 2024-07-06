use anyhow::Context;
use async_trait::async_trait;
use tokio::net::TcpStream;

use crate::{
    db::{Db, Entry},
    message::{BulkString, Message, SimpleString},
};

use super::Command;

#[derive(Debug)]
pub(crate) struct SetCommand {
    key: String,
    value: String,
    expiration: Option<u128>,
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

    async fn handle(&self, stream: &mut TcpStream, db: &Db) -> Result<(), anyhow::Error> {
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
