use std::{
    fmt,
    io::{BufRead, Cursor, Read},
};

use anyhow::{Context, Ok};
use bytes::Buf;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

#[derive(Debug, Clone)]
enum Message {
    Array { elements: Vec<Message> },
    BulkString { data: String },
}

impl fmt::Display for Message {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Message::Array { .. } => write!(f, "Array"),
            Message::BulkString { .. } => write!(f, "BulkString"),
        }
    }
}

impl Message {
    fn from_buf(cursor: &mut Cursor<&[u8]>) -> anyhow::Result<Message> {
        let first_byte = cursor.get_u8();

        let mut size_buf = Vec::new();
        cursor
            .read_until(b'\n', &mut size_buf)
            .context("Failed to read message size")?;

        let size: usize = std::str::from_utf8(&size_buf[..size_buf.len() - 2])
            .context("Failed to convert message size to string")?
            .parse::<usize>()
            .context("Failed to convert message size to usize")?;

        match first_byte {
            b'*' => {
                let mut elements = Vec::with_capacity(size);
                for i in 0..size {
                    let message = Message::from_buf(cursor)
                        .with_context(|| format!("Failed to parse element {}", i))?;
                    elements.push(message);
                }

                Ok(Message::Array { elements })
            }
            b'$' => {
                let mut data = vec![0; size + 2];
                Read::read_exact(cursor, &mut data).context("Failed to read Bulk String")?;

                Ok(Message::BulkString {
                    data: std::str::from_utf8(&data[..data.len() - 2])
                        .context("Failed to convert Bulk String to data")?
                        .to_string(),
                })
            }
            _ => anyhow::bail!("Message type not supported {}", first_byte),
        }
    }

    fn as_buf(self) -> Vec<u8> {
        match self {
            Message::Array { elements } => {
                let mut buf: Vec<u8> = Vec::new();
                buf.extend(format!("*{}\r\n", elements.len()).as_bytes());

                for element in elements {
                    buf.extend(element.as_buf());
                }

                buf
            }
            Message::BulkString { data } => format!("${}\r\n{}\r\n", data.len(), data)
                .as_bytes()
                .to_vec(),
        }
    }
}

enum Command {
    Ping,
    Echo { message: String },
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
                                        "Message type not support for ECHO {}",
                                        message
                                    ),
                                },
                            })
                        }
                        _ => anyhow::bail!("Command not implemented"),
                    },
                    _ => anyhow::bail!("Command must be a Bulk String"),
                }
            }
            _ => anyhow::bail!("Command must be a Array"),
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:6379")
        .await
        .context("Failed to bind port")?;

    loop {
        let (mut stream, _) = listener.accept().await.context("Failed to get client")?;

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
                            .context("Failed to write ping response")?;
                    }
                    Command::Echo { message } => {
                        stream
                            .write_all(&Message::BulkString { data: message }.as_buf())
                            .await
                            .context("Failed to write echo response")?;
                    }
                }
            }

            Ok(())
        });
    }
}
