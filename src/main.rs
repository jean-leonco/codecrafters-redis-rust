use anyhow::{Context, Ok};
use bytes::Buf;
use std::io::{BufRead, Cursor, Read};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

#[derive(Debug, Clone)]
enum Message {
    Array { elements: Vec<Message> },
    BulkString { data: String },
}

impl Message {
    fn from_buf(cursor: &mut Cursor<&[u8]>) -> anyhow::Result<Message> {
        // redis-cli PING -> "*1\r\n$4\r\nPING\r\n"
        // redis-cli ECHO heyyyyyyyyyyyyyyyyy -> "*2\r\n$4\r\nECHO\r\n$19\r\nheyyyyyyyyyyyyyyyyy\r\n"

        // 1. get first byte to identify type -> Array, BulkString, SimpleString
        // 2. if array, get the expected size and create Vec::with_capacity
        // 2.1 loop throw size
        // 2.1.3 pass the cursos to from_buf
        // 2.1.4 push result to elements
        // 2.2 return value
        // 3. if bulk string, get the expected size and read exact bytes
        // 3.1 return value

        // maybe consume should be used to avoid trim
        // or read until

        let first_byte = cursor.get_u8();

        let mut size_buf = Vec::new();
        cursor
            .read_until(b'\r', &mut size_buf)
            .context("Failed to read message size")?;

        // NOTE: is there an easier way to convert buf to usize?
        let size: usize = String::from_utf8(size_buf)
            .context("Failed to convert message size to string")?
            .parse::<usize>()
            .context("Failed to convert message size to usize")?;
        // skip \n
        cursor.consume(1);

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
                let mut data = Vec::with_capacity(size);
                Read::read_exact(cursor, &mut data).context("Failed to read Bulk String")?;
                // skip \r\n
                cursor.consume(2);

                Ok(Message::BulkString {
                    data: String::from_utf8(data)
                        .context("Failed to convert Bulk String to data")?,
                })
            }
            _ => anyhow::bail!("Message type not supported {}", first_byte),
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
            Message::Array { elements } => match elements.get(0) {
                Some(Message::BulkString { data }) => match data.to_lowercase().as_str() {
                    "ping" => Ok(Command::Ping),
                    "echo" => Ok(Command::Echo {
                        message: String::from("heey"),
                    }),
                    _ => anyhow::bail!("Command not implemented"),
                },
                _ => anyhow::bail!("Command must be a Bulk String"),
            },
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

        let result = tokio::spawn(async move {
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
                            .write_all(format!("${}\r\n{}\r\n", message.len(), message).as_bytes())
                            .await
                            .context("Failed to write echo response")?;
                    }
                }
            }

            Ok(())
        });
    }
}
