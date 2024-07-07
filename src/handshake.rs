use std::io::Cursor;

use tokio::{
    io::{AsyncReadExt, BufReader, BufWriter},
    net::TcpStream,
};

use crate::{
    commands::{ping, psync, replconf, Command},
    message::Message,
};

pub(crate) async fn send_handshake(master_address: String, port: u16) -> anyhow::Result<()> {
    let socket = TcpStream::connect(master_address).await?;
    let (rd, wr) = tokio::io::split(socket);
    let mut writer = BufWriter::new(wr);
    let mut reader = BufReader::new(rd);

    let mut buf = [0; 1024];
    let pong_message = Message::simple_string(String::from("PONG"));
    let ok_message = Message::ok_message();

    let ping_message = ping::PingCommand::new_command(None).to_message();
    ping_message.send(&mut writer).await?;
    let read = reader.read(&mut buf).await?;
    if read == 0 {
        anyhow::bail!("Failed to read Master response");
    }

    let response = Message::deserialize(&mut Cursor::new(&buf))?;
    if response == pong_message {
        println!("Receiving PING response");
    } else {
        anyhow::bail!("Response is different than PONG: {}", response);
    }

    buf.fill(0);

    let listening_message =
        replconf::ReplConfCommand::new_listening_port_command(port).to_message();
    listening_message.send(&mut writer).await?;
    let read = reader.read(&mut buf).await?;
    if read == 0 {
        anyhow::bail!("Failed to read Master response");
    }

    let response = Message::deserialize(&mut Cursor::new(&buf))?;
    if response == ok_message {
        println!("Receiving REPLCONF listening-port response")
    } else {
        anyhow::bail!("Response is different than OK: {}", response);
    }

    buf.fill(0);

    let capabilities_message = replconf::ReplConfCommand::new_capabilities_command().to_message();
    capabilities_message.send(&mut writer).await?;
    let read = reader.read(&mut buf).await?;
    if read == 0 {
        anyhow::bail!("Failed to read Master response");
    }

    let response = Message::deserialize(&mut Cursor::new(&buf))?;
    if response == ok_message {
        println!("Receiving REPLCONF capa response");
    } else {
        anyhow::bail!("Response is different than OK: {}", response);
    }

    buf.fill(0);

    let psync_message = psync::PSyncCommand::new_command(String::from("?"), -1).to_message();
    psync_message.send(&mut writer).await?;
    let read = reader.read(&mut buf).await?;
    if read == 0 {
        anyhow::bail!("Failed to read Master response");
    }

    Message::deserialize(&mut Cursor::new(&buf))?;
    println!("Receiving PSYNC response");

    Ok(())
}
