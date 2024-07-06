use std::{io::Cursor, result::Result::Ok, time::Duration};

use anyhow::Context;
use db::{new_db, Db, Entry};
use message::Message;
use tokio::io::AsyncReadExt;
use tokio::net::{TcpListener, TcpStream};

pub(crate) mod db;
pub(crate) mod message;

enum Command {
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
    fn from_buf(buf: &mut [u8]) -> anyhow::Result<Command> {
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

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    println!("Logs from your program will appear here!");

    let db = new_db();

    let listener = TcpListener::bind("127.0.0.1:6379")
        .await
        .context("Failed to bind port")?;

    let cleanup_db = db.clone();
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(1)).await;
            let mut db = cleanup_db.lock().await;

            let entries_db = db.clone();
            let keys_to_delete = entries_db.iter().filter(|(_, value)| value.has_ttl());

            for (key, value) in keys_to_delete {
                if value.is_expired() {
                    db.remove(key);
                    println!("key {} deleted", key);
                }
            }
        }
    });

    loop {
        let (stream, addr) = listener.accept().await.context("Failed to get client")?;
        println!("Accepted connection from {}", addr);

        let db = db.clone();
        tokio::spawn(async move {
            handle_connection(stream, db)
                .await
                .context("Failed to handle connection")
        });
    }
}

async fn handle_connection(mut stream: TcpStream, db: Db) -> anyhow::Result<()> {
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
            Command::Ping { message } => {
                let data = match message {
                    Some(message) => message,
                    None => String::from("PONG"),
                };
                let message = Message::SimpleString { data };

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
            Command::Set {
                key,
                value,
                expiration,
            } => {
                let mut db = db.lock().await;
                db.insert(key, Entry::new(value, expiration));

                let message = Message::SimpleString {
                    data: String::from("OK"),
                };
                message
                    .send(&mut stream)
                    .await
                    .context("Failed to send SET reply")?;
            }
            Command::Get { key } => {
                let mut db = db.lock().await;

                let message = if let Some(value) = db.get(&key) {
                    if value.is_expired() {
                        db.remove(&key);
                        Message::NullBulkString
                    } else {
                        Message::BulkString {
                            data: value.value.to_string(),
                        }
                    }
                } else {
                    Message::NullBulkString
                };

                message
                    .send(&mut stream)
                    .await
                    .context("Failed to send GET reply")?;
            }
        }
    }

    Ok(())
}
