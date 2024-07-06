use std::{
    fmt,
    io::{BufRead, Cursor, Read},
};

use anyhow::Context;
use bytes::Buf;
use tokio::io::AsyncWriteExt;

#[derive(Debug, Clone)]
pub(crate) enum Message {
    Array { elements: Vec<Message> },
    BulkString { data: String },
    NullBulkString,
    SimpleString { data: String },
}

impl fmt::Display for Message {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Message::Array { .. } => write!(f, "Array"),
            Message::BulkString { .. } => write!(f, "BulkString"),
            Message::NullBulkString { .. } => write!(f, "NullBulkString"),
            Message::SimpleString { .. } => write!(f, "SimpleString"),
        }
    }
}

const TERMINATOR_SIZE: usize = 2;

impl Message {
    pub(crate) fn deserialize(cursor: &mut Cursor<&[u8]>) -> anyhow::Result<Message> {
        let first_byte = cursor.get_u8();

        match first_byte {
            b'*' => {
                let size = parse_size(cursor).context("Failed to parse Array size")?;

                let mut elements = Vec::with_capacity(size);
                for i in 0..size {
                    let message = Message::deserialize(cursor)
                        .with_context(|| format!("Failed to parse Array element {}", i))?;
                    elements.push(message);
                }

                Ok(Message::Array { elements })
            }
            b'$' => {
                if cursor.chunk()[0] == b'-' {
                    Ok(Message::NullBulkString)
                } else {
                    let size = parse_size(cursor).context("Failed to parse BulkString size")?;

                    let mut data = vec![0; size + TERMINATOR_SIZE];
                    cursor
                        .read_exact(&mut data)
                        .context("Failed to read BulkString data")?;

                    Ok(Message::BulkString {
                        data: std::str::from_utf8(&data[..data.len() - TERMINATOR_SIZE])
                            .context("Failed to parse BulkString data")?
                            .to_string(),
                    })
                }
            }
            b'+' => {
                let mut data: Vec<u8> = Vec::new();
                cursor
                    .read_until(b'\n', &mut data)
                    .context("Failed to read SimpleString data")?;

                Ok(Message::SimpleString {
                    data: std::str::from_utf8(&&data[..data.len() - TERMINATOR_SIZE])
                        .context("Failed to parse SimpleString data")?
                        .to_string(),
                })
            }
            _ => anyhow::bail!("Unknown message type {}", first_byte),
        }
    }

    pub(crate) fn serialize(self) -> Vec<u8> {
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
            Message::NullBulkString => b"$-1\r\n".to_vec(),
            Message::SimpleString { data } => format!("+{}\r\n", data).as_bytes().to_vec(),
        }
    }

    pub(crate) async fn send(
        self,
        writer: &mut (impl AsyncWriteExt + std::marker::Unpin),
    ) -> anyhow::Result<()> {
        writer
            .write_all(&self.serialize())
            .await
            .context("Failed to send reply")?;

        Ok(())
    }
}

fn parse_size(cursor: &mut Cursor<&[u8]>) -> anyhow::Result<usize> {
    let mut size_buf = Vec::new();
    cursor
        .read_until(b'\n', &mut size_buf)
        .context("Failed to read size")?;

    let size = std::str::from_utf8(&size_buf[..size_buf.len() - 2])
        .context("Failed to convert size to string")?
        .parse::<usize>()
        .context("Failed to parse size to usize")?;

    Ok(size)
}
