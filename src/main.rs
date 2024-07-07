use std::io::Cursor;
use std::{result::Result::Ok, time::Duration};

use anyhow::Context;
use clap::Parser;
use commands::Command;
use tokio::io::{AsyncReadExt, BufReader, BufWriter};
use tokio::net::{TcpListener, TcpStream};

pub(crate) mod commands;
pub(crate) mod db;
pub(crate) mod message;
pub(crate) mod server_config;

#[derive(Parser, Debug)]
#[command()]
struct Args {
    #[arg(long, default_value_t = 6379)]
    // https://stackoverflow.com/a/113228
    port: u16,
    #[arg(long)]
    replicaof: Option<String>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    println!("Logs from your program will appear here!");

    let args = Args::parse();

    let listener = TcpListener::bind(format!("127.0.0.1:{}", args.port))
        .await
        .context("Failed to bind port")?;

    let server_config = server_config::ServerConfig::new(String::from("0.0.0"), args.replicaof);
    let db = db::new_db();

    match &server_config {
        server_config::ServerConfig::Master { .. } => {
            tokio::spawn(remove_expired_keys(db.clone()));
        }
        server_config::ServerConfig::Slave { master_address, .. } => {
            tokio::spawn(send_handshake(master_address.to_string(), args.port));
        }
    };

    loop {
        let (stream, addr) = listener.accept().await.context("Failed to get client")?;
        println!("Accepted connection from {}", addr);

        let db = db.clone();
        let server_config = server_config.clone();
        tokio::spawn(async move {
            if let Err(err) = handle_connection(stream, db, &server_config).await {
                eprintln!("ERROR: {}", err);
            }
        });
    }
}

async fn handle_connection(
    mut stream: TcpStream,
    db: db::Db,
    server_config: &server_config::ServerConfig,
) -> anyhow::Result<()> {
    let mut buf = [0; 1024];
    loop {
        let bytes_read = stream
            .read(&mut buf)
            .await
            .context("Failed to read stream")?;
        if bytes_read == 0 {
            break;
        }

        let command = commands::parse_command(&mut buf[..bytes_read])?;
        println!("Command received: {}", command);
        command.handle(&mut stream, &db, server_config).await?;
    }

    Ok(())
}

async fn remove_expired_keys(db: db::Db) {
    // TODO: improve how keys are expired.
    // https://redis.io/docs/latest/commands/expire/#how-redis-expires-keys
    // https://github.com/valkey-io/valkey/blob/unstable/src/expire.c

    loop {
        tokio::time::sleep(Duration::from_secs(1)).await;
        let mut db = db.lock().await;

        let entries_db = db.clone();
        let keys_to_delete = entries_db.iter().filter(|(_, value)| value.has_ttl());

        for (key, value) in keys_to_delete {
            if value.is_expired() {
                db.remove(key);
                println!("Entry {} deleted", key);
            }
        }
    }
}

async fn send_handshake(master_address: String, port: u16) -> anyhow::Result<()> {
    let socket = TcpStream::connect(master_address).await?;
    let (rd, wr) = tokio::io::split(socket);
    let mut writer = BufWriter::new(wr);
    let mut reader = BufReader::new(rd);

    let mut buf = [0; 1024];
    let pong_message = message::Message::simple_string_message(String::from("PONG"));
    let ok_message = message::Message::ok_message();

    let ping_message = commands::ping::PingCommand::new_command(None).to_message();
    ping_message.send(&mut writer).await?;
    reader.read(&mut buf).await?;

    let response = message::Message::deserialize(&mut Cursor::new(&buf))?;
    if response == pong_message {
        println!("Receiving PING response");
    } else {
        anyhow::bail!("Response is different than PONG: {}", response);
    }

    buf.fill(0);

    let listening_message =
        commands::replconf::ReplConfCommand::new_listening_port_command(port).to_message();
    listening_message.send(&mut writer).await?;
    reader.read(&mut buf).await?;

    let response = message::Message::deserialize(&mut Cursor::new(&buf))?;
    if response == ok_message {
        println!("Receiving REPLCONF listening-port response")
    } else {
        anyhow::bail!("Response is different than OK: {}", response);
    }

    buf.fill(0);

    let capabilities_message =
        commands::replconf::ReplConfCommand::new_capabilities_command().to_message();
    capabilities_message.send(&mut writer).await?;
    reader.read(&mut buf).await?;

    let response = message::Message::deserialize(&mut Cursor::new(&buf))?;
    if response == ok_message {
        println!("Receiving REPLCONF capa response");
    } else {
        anyhow::bail!("Response is different than OK: {}", response);
    }

    Ok(())
}
