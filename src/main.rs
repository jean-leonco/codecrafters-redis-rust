use std::result::Result::Ok;

use anyhow::Context;
use clap::Parser;
use tokio::io::AsyncReadExt;
use tokio::net::{TcpListener, TcpStream};

pub(crate) mod commands;
pub(crate) mod db;
pub(crate) mod handshake;
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
            tokio::spawn(db::remove_expired_keys(db.clone()));
        }
        server_config::ServerConfig::Slave { master_address, .. } => {
            tokio::spawn(handshake::send_handshake(
                master_address.to_string(),
                args.port,
            ));
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
