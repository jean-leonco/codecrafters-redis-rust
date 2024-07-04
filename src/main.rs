use std::{
    collections::HashMap,
    fmt,
    io::{BufRead, Cursor, Read},
    sync::Arc,
};

use anyhow::{Context, Ok};
use bytes::Buf;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    sync::Mutex,
};

#[derive(Debug, Clone)]
enum Message {
    Array { elements: Vec<Message> },
    BulkString { data: String },
    SimpleString { data: String },
    Null,
}

impl fmt::Display for Message {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Message::Array { .. } => write!(f, "Array"),
            Message::BulkString { .. } => write!(f, "BulkString"),
            Message::SimpleString { .. } => write!(f, "SimpleString"),
            Message::Null { .. } => write!(f, "Null"),
        }
    }
}

impl Message {
    fn from_buf(cursor: &mut Cursor<&[u8]>) -> anyhow::Result<Message> {
        let first_byte = cursor.get_u8();

        match first_byte {
            b'*' => {
                let mut size_buf = Vec::new();
                cursor
                    .read_until(b'\n', &mut size_buf)
                    .context("Failed to read Array size")?;
                let size: usize = std::str::from_utf8(&size_buf[..size_buf.len() - 2])
                    .context("Failed to convert Array size to string")?
                    .parse::<usize>()
                    .context("Failed to convert Array size to usize")?;
                let mut elements = Vec::with_capacity(size);
                for i in 0..size {
                    let message = Message::from_buf(cursor)
                        .with_context(|| format!("Failed to parse Array element {}", i))?;
                    elements.push(message);
                }

                Ok(Message::Array { elements })
            }
            b'$' => {
                let mut size_buf = Vec::new();
                cursor
                    .read_until(b'\n', &mut size_buf)
                    .context("Failed to read BulkString size")?;
                let size: usize = std::str::from_utf8(&size_buf[..size_buf.len() - 2])
                    .context("Failed to convert BulkString size to string")?
                    .parse::<usize>()
                    .context("Failed to convert BulkString size to usize")?;
                let mut data = vec![0; size + 2];
                Read::read_exact(cursor, &mut data).context("Failed to read BulkString")?;

                Ok(Message::BulkString {
                    data: std::str::from_utf8(&data[..data.len() - 2])
                        .context("Failed to parse BulkString data")?
                        .to_string(),
                })
            }
            b'+' => {
                let mut data_buf: Vec<u8> = Vec::new();
                cursor
                    .read_until(b'\n', &mut data_buf)
                    .context("Failed to SimpleString data")?;

                Ok(Message::SimpleString {
                    data: std::str::from_utf8(&&data_buf[..data_buf.len() - 2])
                        .context("Failed to parse SimpleString data")?
                        .to_string(),
                })
            }
            b'_' => {
                // advance \r\n
                cursor.advance(2);

                Ok(Message::Null)
            }
            _ => anyhow::bail!("Message type not supported {}", first_byte),
        }
    }

    fn serialize(self) -> Vec<u8> {
        match self {
            Message::Array { elements } => {
                let mut buf: Vec<u8> = Vec::new();
                buf.extend(format!("*{}\r\n", elements.len()).as_bytes());

                for element in elements {
                    buf.extend(element.serialize());
                }

                buf
            }
            Message::BulkString { data } => format!("${}\r\n{}\r\n", data.len(), data)
                .as_bytes()
                .to_vec(),
            Message::SimpleString { data } => format!("+{}\r\n", data).as_bytes().to_vec(),
            Message::Null => "_\r\n".as_bytes().to_vec(),
        }
    }
}

enum Command {
    Ping,
    Echo { message: String },
    Set { key: String, value: String },
    Get { key: String },
}

impl Command {
    fn from_buf(buf: &mut [u8]) -> anyhow::Result<Command> {
        let message =
            Message::from_buf(&mut Cursor::new(buf)).context("Failed to parse message")?;

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
                            let value = elements.get(1).expect("SET message should have value");

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
                        stream
                            .write_all(b"+PONG\r\n")
                            .await
                            .context("Failed to write PING response")?;
                    }
                    Command::Echo { message } => {
                        stream
                            .write_all(&Message::BulkString { data: message }.serialize())
                            .await
                            .context("Failed to write ECHO response")?;
                    }
                    Command::Set { key, value } => {
                        let mut db = db.lock().await;
                        db.insert(key, value);

                        stream
                            .write_all(
                                &Message::SimpleString {
                                    data: String::from("Ok"),
                                }
                                .serialize(),
                            )
                            .await
                            .context("Failed to write Set response")?
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

                        stream
                            .write_all(&message.serialize())
                            .await
                            .context("Failed to write Get response")?;
                    }
                }
            }

            Ok(())
        });
    }
}
