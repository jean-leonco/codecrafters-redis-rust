use std::io::Cursor;

use tokio::{
    io::{AsyncReadExt, BufReader, BufWriter, ReadHalf, WriteHalf},
    net::TcpStream,
};

use crate::{
    commands::{ping, psync, replconf, Command},
    message::{Message, SimpleString},
};

pub(crate) async fn send_handshake(
    writer: &mut BufWriter<WriteHalf<TcpStream>>,
    reader: &mut BufReader<ReadHalf<TcpStream>>,
    port: u16,
) -> anyhow::Result<()> {
    let mut buf = [0; 1024];
    let pong_message = Message::simple_string(String::from("PONG"));
    let ok_message = Message::ok_message();

    let ping_message = ping::PingCommand::new_command(None).to_message();
    ping_message.send(writer).await?;
    let read = reader.read(&mut buf).await?;
    if read == 0 {
        anyhow::bail!("Failed to read response");
    }

    let response = Message::deserialize(&mut Cursor::new(&buf))?;
    if response == pong_message {
        println!("PING replied {}", response);
    } else {
        anyhow::bail!("Response is different than PONG: {}", response);
    }

    buf.fill(0);

    let listening_message =
        replconf::ReplConfCommand::new_listening_port_command(port).to_message();
    listening_message.send(writer).await?;
    let read = reader.read(&mut buf).await?;
    if read == 0 {
        anyhow::bail!("Failed to read response");
    }

    let response = Message::deserialize(&mut Cursor::new(&buf))?;
    if response == ok_message {
        println!("REPLCONF listening-port replied {}", response);
    } else {
        anyhow::bail!("Response is different than OK: {}", response);
    }

    buf.fill(0);

    let capabilities_message = replconf::ReplConfCommand::new_capabilities_command().to_message();
    capabilities_message.send(writer).await?;
    let read = reader.read(&mut buf).await?;
    if read == 0 {
        anyhow::bail!("Failed to read response");
    }

    let response = Message::deserialize(&mut Cursor::new(&buf))?;
    if response == ok_message {
        println!("REPLCONF capa replied {}", response);
    } else {
        anyhow::bail!("Response is different than OK: {}", response);
    }

    buf.fill(0);

    let psync_message = psync::PSyncCommand::new_command(String::from("?"), -1).to_message();
    psync_message.send(writer).await?;
    let read = reader.read(&mut buf).await?;
    if read == 0 {
        anyhow::bail!("Failed to read response");
    }

    let response: SimpleString = Message::deserialize(&mut Cursor::new(&buf))?.try_into()?;
    println!("PSYNC replied {}", response);

    if response.to_string().starts_with("FULLRESYNC") {
        println!("Full resync with master");
    }

    buf.fill(0);

    // consume RDB file
    let read = reader.read(&mut buf).await?;
    if read == 0 {
        println!("RDB file not sent")
    }

    Ok(())
}
