use std::result::Result::Ok;

use anyhow::Context;
use clap::Parser;
use tokio::io::AsyncReadExt;
use tokio::net::{TcpListener, TcpStream};

pub(crate) mod commands;
pub(crate) mod db;
pub(crate) mod handshake;
pub(crate) mod message;

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

    let db = db::Db::new(args.replicaof);

    if let db::State::Slave { master_address, .. } = &*db.state {
        tokio::spawn(handshake::send_handshake(
            master_address.to_string(),
            args.port,
        ));
    };

    let expired_keys_db = db.clone();
    tokio::spawn(async move { expired_keys_db.remove_expired_keys().await });

    loop {
        let (stream, addr) = listener.accept().await.context("Failed to get client")?;
        println!("Accepted connection from {}", addr);

        let db = db.clone();
        tokio::spawn(async move {
            if let Err(err) = handle_connection(stream, db).await {
                eprintln!("ERROR: {}", err);
            }
        });
    }
}

async fn handle_connection(mut stream: TcpStream, db: db::Db) -> anyhow::Result<()> {
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
        command.handle(&mut stream, &db).await?;
    }

    Ok(())
}
