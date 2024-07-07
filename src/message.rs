use std::{
    fmt,
    io::{BufRead, Cursor, Read},
};

use anyhow::Context;
use bytes::Buf;
use tokio::io::AsyncWriteExt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Array {
    pub(crate) elements: Vec<Message>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BulkString {
    pub(crate) data: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SimpleString {
    pub(crate) data: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Message {
    Array(Array),
    BulkString(BulkString),
    NullBulkString,
    SimpleString(SimpleString),
}

impl fmt::Display for Message {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Message::Array(value) => {
                let mut formatted = String::new();
                formatted.push('*');

                for element in value.elements.iter() {
                    formatted.push_str(&element.to_string());
                    formatted.push(' ');
                }

                write!(f, "{}", formatted)
            }
            Message::BulkString(value) => write!(f, "${}", value.data),
            Message::NullBulkString => write!(f, "$-1"),
            Message::SimpleString(value) => write!(f, "+{}", value.data),
        }
    }
}

impl TryFrom<Message> for Array {
    type Error = anyhow::Error;

    fn try_from(value: Message) -> anyhow::Result<Self> {
        match value {
            Message::Array(value) => Ok(value),
            value => anyhow::bail!("Failed to convert message to Array: {}", value),
        }
    }
}

impl TryFrom<&Message> for BulkString {
    type Error = anyhow::Error;

    fn try_from(value: &Message) -> anyhow::Result<Self> {
        match value {
            Message::BulkString(value) => Ok(value.clone()),
            value => anyhow::bail!("Failed to convert message to BulkString: {}", value),
        }
    }
}

impl fmt::Display for BulkString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.data)
    }
}

impl TryFrom<Message> for SimpleString {
    type Error = anyhow::Error;

    fn try_from(value: Message) -> anyhow::Result<Self> {
        match value {
            Message::SimpleString(value) => Ok(value.clone()),
            value => anyhow::bail!("Failed to convert message to SimpleString: {}", value),
        }
    }
}

impl fmt::Display for SimpleString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.data)
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

                Ok(Message::Array(Array { elements }))
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
                    anyhow::ensure!(data.len() >= 2, "BulkString data size should be at least 2");

                    Ok(Message::BulkString(BulkString {
                        data: std::str::from_utf8(&data[..data.len() - TERMINATOR_SIZE])
                            .context("Failed to parse BulkString data")?
                            .to_string(),
                    }))
                }
            }
            b'+' => {
                let mut data: Vec<u8> = Vec::new();
                cursor
                    .read_until(b'\n', &mut data)
                    .context("Failed to read SimpleString data")?;
                anyhow::ensure!(data.len() >= 2, "BulkString data size should be at least 2");

                Ok(Message::SimpleString(SimpleString {
                    data: std::str::from_utf8(&data[..data.len() - TERMINATOR_SIZE])
                        .context("Failed to parse SimpleString data")?
                        .to_string(),
                }))
            }
            _ => anyhow::bail!("Unknown message type: {}", first_byte),
        }
    }

    pub(crate) fn array(elements: Vec<Message>) -> Message {
        Message::Array(Array { elements })
    }

    pub(crate) fn bulk_string(data: String) -> Message {
        Message::BulkString(BulkString { data })
    }

    pub(crate) fn simple_string(data: String) -> Message {
        Message::SimpleString(SimpleString { data })
    }

    pub(crate) fn ok_message() -> Message {
        Message::simple_string(String::from("OK"))
    }

    async fn write_message(
        self,
        writer: &mut (impl AsyncWriteExt + std::marker::Unpin),
    ) -> anyhow::Result<()> {
        match self {
            Message::BulkString(value) => writer
                .write_all(format!("${}\r\n{}\r\n", value.data.len(), value.data).as_bytes())
                .await
                .context("Failed to write BulkString"),
            Message::NullBulkString => writer
                .write_all(b"$-1\r\n")
                .await
                .context("Failed to write NullBulkString"),
            Message::SimpleString(value) => writer
                .write_all(format!("+{}\r\n", value.data).as_bytes())
                .await
                .context("Failed to write SimpleString"),
            Message::Array { .. } => unreachable!(),
        }
    }

    pub(crate) async fn send(
        self,
        writer: &mut (impl AsyncWriteExt + std::marker::Unpin),
    ) -> anyhow::Result<()> {
        println!("Writing message {}", self);

        match self {
            Message::Array(value) => {
                writer
                    .write_all(format!("*{}\r\n", value.elements.len()).as_bytes())
                    .await
                    .context("Failed to write Array")?;

                for element in value.elements {
                    element.write_message(writer).await?;
                }
            }
            _ => self.write_message(writer).await?,
        }

        writer.flush().await?;

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
