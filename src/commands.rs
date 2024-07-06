use std::{collections::HashSet, io::Cursor};

use anyhow::{Context, Ok};
use info::InfoSection;

use crate::message::{Array, BulkString, Message};

pub(crate) mod echo;
pub(crate) mod get;
pub(crate) mod info;
pub(crate) mod ping;
pub(crate) mod set;

#[derive(Debug)]
pub(crate) enum Command {
    Ping {
        message: Option<String>,
    },
    Echo {
        message: String,
    },
    Set {
        key: String,
        value: String,
        expiration: Option<u128>,
    },
    Get {
        key: String,
    },
    Info {
        sections: HashSet<InfoSection>,
    },
}

impl Command {
    pub(crate) fn from_buf(buf: &mut [u8]) -> anyhow::Result<Command> {
        let message: Array = Message::deserialize(&mut Cursor::new(buf))
            .context("Failed to parse message")?
            .try_into()?;

        let elements: Vec<BulkString> = message
            .elements
            .iter()
            .map(|value| value.try_into().unwrap())
            .collect();

        let command = elements.first().expect("Message should have command");

        match command.data.to_lowercase().as_str() {
            "ping" => {
                let message = elements.get(1).map(|value| value.data.to_string());

                Ok(Command::Ping { message })
            }
            "echo" => {
                let message = elements
                    .get(1)
                    .expect("ECHO message should have reply string");

                Ok(Command::Echo {
                    message: message.data.to_string(),
                })
            }
            "set" => {
                let key = elements.get(1).expect("SET message should have key");
                let value = elements.get(2).expect("SET message should have value");
                let mut expiration = None;

                for option in elements[3..].windows(2) {
                    let option_key = option.first().expect("SET message option should have key");
                    let option_value = option.get(1).expect("SET message option should have value");

                    if option_key.data.to_lowercase() == "px" {
                        expiration =
                            Some(option_value.data.parse::<u128>().with_context(|| {
                                format!("Failed to parse expiration time {}", option_value.data)
                            })?);
                    }
                }

                Ok(Command::Set {
                    key: key.data.to_string(),
                    value: value.data.to_string(),
                    expiration,
                })
            }
            "get" => {
                let key = elements.get(1).expect("GET message should have key");

                Ok(Command::Get {
                    key: key.data.to_string(),
                })
            }
            "info" => {
                let mut sections = HashSet::new();
                for element in elements[1..elements.len()].iter() {
                    sections.insert(InfoSection::parse(element.data.to_string())?);
                }

                Ok(Command::Info { sections })
            }
            command => anyhow::bail!("Command not implemented: {}", command),
        }
    }
}
