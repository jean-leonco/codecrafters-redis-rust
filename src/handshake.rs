use std::io::Cursor;

use tokio::{
    io::{AsyncReadExt, BufReader, BufWriter, ReadHalf, WriteHalf},
    net::TcpStream,
};

use crate::{
    commands::{ping, psync, replconf, Command},
    message::{Message, SimpleString},
};

pub(crate) struct Handshake<'a> {
    writer: &'a mut BufWriter<WriteHalf<TcpStream>>,
    reader: &'a mut BufReader<ReadHalf<TcpStream>>,
    buf: [u8; 1024],
    port: u16,
}

impl<'a> Handshake<'a> {
    pub(crate) fn new(
        writer: &'a mut BufWriter<WriteHalf<TcpStream>>,
        reader: &'a mut BufReader<ReadHalf<TcpStream>>,
        port: u16,
    ) -> Self {
        Self {
            writer,
            reader,
            buf: [0; 1024],
            port,
        }
    }

    pub(crate) async fn send_handshake(&mut self) -> anyhow::Result<()> {
        let ok_message = Message::ok_message();

        let command = ping::PingCommand::new_command(None);
        let response = self.send_command(command).await?;
        if response == Message::simple_string(String::from("PONG")) {
            println!("PING replied {}", response);
        } else {
            anyhow::bail!("Response is different than PONG: {}", response);
        }

        let command = replconf::ReplConfCommand::new_listening_port_command(self.port);
        let response = self.send_command(command).await?;
        if response == ok_message {
            println!("REPLCONF listening-port replied {}", response);
        } else {
            anyhow::bail!("Response is different than OK: {}", response);
        }

        let command = replconf::ReplConfCommand::new_capabilities_command();
        let response = self.send_command(command).await?;
        if response == ok_message {
            println!("REPLCONF capa replied {}", response);
        } else {
            anyhow::bail!("Response is different than OK: {}", response);
        }

        let command = psync::PSyncCommand::new_command(String::from("?"), -1);
        let response: SimpleString = self.send_command(command).await?.try_into()?;
        println!("PSYNC replied {}", response);
        if response.to_string().starts_with("FULLRESYNC") {
            println!("Full resync with master");
        }

        // consume RDB file
        let read = self.reader.read(&mut self.buf).await?;
        if read == 0 {
            println!("RDB file not sent");
        }

        Ok(())
    }

    async fn send_command(&mut self, command: impl Command) -> anyhow::Result<Message> {
        let message = command.to_message();
        message.send(self.writer).await?;

        let read = self.reader.read(&mut self.buf).await?;
        if read == 0 {
            anyhow::bail!("Failed to read response");
        }

        let message = Message::deserialize(&mut Cursor::new(&self.buf))?;
        self.buf.fill(0);
        Ok(message)
    }
}
