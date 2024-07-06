use std::io::Cursor;

use anyhow::Context;

use crate::message::Message;

pub(crate) mod echo;
pub(crate) mod get;
pub(crate) mod ping;
pub(crate) mod set;

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
}

impl Command {
    pub(crate) fn from_buf(buf: &mut [u8]) -> anyhow::Result<Command> {
        let message =
            Message::deserialize(&mut Cursor::new(buf)).context("Failed to parse message")?;

        // TODO: improve this
        match message {
            Message::Array { elements } => {
                let element = elements.first().expect("Message should have command");

                match element {
                    Message::BulkString { data } => match data.to_lowercase().as_str() {
                        "ping" => {
                            let message = elements.get(1);
                            let message = match message {
                                Some(Message::BulkString { data }) => Some(data.to_string()),
                                Some(message) => {
                                    anyhow::bail!("Message type not support by PING {}", message)
                                }
                                None => None,
                            };

                            Ok(Command::Ping { message })
                        }
                        "echo" => {
                            let message = elements
                                .get(1)
                                .expect("ECHO message should have reply string");
                            let message = match message {
                                Message::BulkString { data } => data.to_string(),
                                _ => anyhow::bail!("Message type not support by ECHO {}", message),
                            };

                            Ok(Command::Echo { message })
                        }
                        "set" => {
                            let key = elements.get(1).expect("SET message should have key");
                            let value = elements.get(2).expect("SET message should have value");
                            let mut expiration = None;

                            for option in elements[3..].windows(2) {
                                let option_key =
                                    option.first().expect("SET message option should have key");
                                let option_value =
                                    option.get(1).expect("SET message option should have value");

                                match option_key {
                                    Message::BulkString { data } => {
                                        if data.to_lowercase() == "px" {
                                            expiration = match option_value {
                                                Message::BulkString { data } => Some(
                                                    data.parse::<u128>().with_context(|| {
                                                        format!(
                                                            "Failed to parse expiration time {}",
                                                            data
                                                        )
                                                    })?,
                                                ),
                                                _ => anyhow::bail!(
                                                    "Option key value not support by SET {}",
                                                    option_value
                                                ),
                                            };
                                        }
                                    }
                                    _ => {
                                        anyhow::bail!(
                                            "Option key type not support by SET {}",
                                            option_key
                                        )
                                    }
                                }
                            }

                            Ok(Command::Set {
                                key: match key {
                                    Message::BulkString { data } => data.to_string(),
                                    _ => anyhow::bail!("Message type not support by SET {}", key),
                                },
                                value: match value {
                                    Message::BulkString { data } => data.to_string(),
                                    _ => anyhow::bail!("Message type not support by SET {}", value),
                                },
                                expiration,
                            })
                        }
                        "get" => {
                            let key = elements.get(1).expect("GET message should have key");

                            Ok(Command::Get {
                                key: match key {
                                    Message::BulkString { data } => data.to_string(),
                                    _ => anyhow::bail!("Message type not support by GET {}", key),
                                },
                            })
                        }
                        _ => anyhow::bail!("Command not implemented"),
                    },
                    _ => anyhow::bail!("Command must be a BulkString"),
                }
            }
            _ => anyhow::bail!("Command must be a Array"),
        }
    }
}
