use std::{result::Result::Ok, time::Duration};

use anyhow::Context;
use clap::Parser;
use commands::{echo, get, ping, set, Command};
use db::{new_db, Db};
use tokio::io::AsyncReadExt;
use tokio::net::{TcpListener, TcpStream};

pub(crate) mod commands;
pub(crate) mod db;
pub(crate) mod message;

#[derive(Parser, Debug)]
#[command()]
struct Args {
    #[arg(long, default_value_t = 6379)]
    // https://stackoverflow.com/a/113228
    port: u16,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    println!("Logs from your program will appear here!");

    let db = new_db();

    let args = Args::parse();
    let listener = TcpListener::bind(format!("127.0.0.1:{}", args.port))
        .await
        .context("Failed to bind port")?;

    tokio::spawn(remove_expired_keys(db.clone()));

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

async fn handle_connection(mut stream: TcpStream, db: Db) -> anyhow::Result<()> {
    let mut buf = [0; 1024];
    loop {
        let bytes_read = stream
            .read(&mut buf)
            .await
            .context("Failed to read stream")?;
        if bytes_read == 0 {
            break;
        }

        let command = Command::from_buf(&mut buf[..bytes_read])?;

        match command {
            Command::Ping { message } => ping::handle(message, &mut stream).await?,
            Command::Echo { message } => echo::handle(message, &mut stream).await?,
            Command::Set {
                key,
                value,
                expiration,
            } => set::handle(&db, key, value, expiration, &mut stream).await?,
            Command::Get { key } => get::handle(&db, key, &mut stream).await?,
        }
    }

    Ok(())
}

async fn remove_expired_keys(db: Db) {
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
                println!("entry {} deleted", key);
            }
        }
    }
}
