use std::{result::Result::Ok, time::Duration};

use anyhow::Context;
use clap::Parser;
use commands::Command;
use tokio::io::AsyncReadExt;
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
            let mut master_stream = TcpStream::connect(master_address).await?;

            let ping_message = commands::ping::PingCommand::new(&[])?.to_message();
            ping_message.send(&mut master_stream).await?;
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
