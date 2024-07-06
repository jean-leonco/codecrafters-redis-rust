use std::{collections::HashMap, io::Cursor, sync::Arc};

use anyhow::{Context, Ok};
use message::Message;
use tokio::{io::AsyncReadExt, net::TcpListener, sync::Mutex};

pub(crate) mod message;

enum Command {
    Ping,
    Echo { message: String },
    Set { key: String, value: String },
    Get { key: String },
}

impl Command {
    fn from_buf(buf: &mut [u8]) -> anyhow::Result<Command> {
        let message =
            Message::deserialize(&mut Cursor::new(buf)).context("Failed to parse message")?;

        // TODO: improve this
        match message {
            Message::Array { elements } => {
                match elements.get(0).expect("Message should have command") {
                    Message::BulkString { data } => match data.to_lowercase().as_str() {
                        "ping" => Ok(Command::Ping),
                        "echo" => {
                            let message = elements
                                .get(1)
                                .expect("ECHO message should have response string");

                            Ok(Command::Echo {
                                message: match message {
                                    Message::BulkString { data } => data.to_string(),
                                    _ => anyhow::bail!(
                                        "Message type not support by ECHO {}",
                                        message
                                    ),
                                },
                            })
                        }
                        "set" => {
                            let key = elements.get(1).expect("SET message should have key");
                            let value = elements.get(2).expect("SET message should have value");

                            Ok(Command::Set {
                                key: match key {
                                    Message::BulkString { data } => data.to_string(),
                                    _ => anyhow::bail!("Message type not support by SET {}", key),
                                },
                                value: match value {
                                    Message::BulkString { data } => data.to_string(),
                                    _ => anyhow::bail!("Message type not support by SET {}", value),
                                },
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

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    println!("Logs from your program will appear here!");

    let db = Arc::new(Mutex::new(HashMap::new()));

    let listener = TcpListener::bind("127.0.0.1:6379")
        .await
        .context("Failed to bind port")?;

    loop {
        let (mut stream, _) = listener.accept().await.context("Failed to get client")?;
        let db = db.clone();

        tokio::spawn(async move {
            let mut buf = [0; 1024];
            loop {
                let bytes_read = stream
                    .read(&mut buf)
                    .await
                    .context("Failed to read stream")?;
                if bytes_read == 0 {
                    break;
                }

                let command =
                    Command::from_buf(&mut buf[..bytes_read]).context("Failed to parse command")?;

                match command {
                    Command::Ping => {
                        let message = Message::SimpleString {
                            data: String::from("PONG"),
                        };

                        message
                            .send(&mut stream)
                            .await
                            .context("Failed to send PING reply")?;
                    }
                    Command::Echo { message } => {
                        let message = Message::SimpleString { data: message };

                        message
                            .send(&mut stream)
                            .await
                            .context("Failed to send ECHO reply")?;
                    }
                    Command::Set { key, value } => {
                        let mut db = db.lock().await;
                        db.insert(key, value);

                        let message = Message::SimpleString {
                            data: String::from("OK"),
                        };

                        message
                            .send(&mut stream)
                            .await
                            .context("Failed to send SET reply")?
                    }
                    Command::Get { key } => {
                        let db = db.lock().await;

                        let message = if let Some(value) = db.get(&key) {
                            Message::BulkString {
                                data: value.to_string(),
                            }
                        } else {
                            Message::Null
                        };

                        message
                            .send(&mut stream)
                            .await
                            .context("Failed to send GET reply")?;
                    }
                }
            }

            Ok(())
        });
    }
}
