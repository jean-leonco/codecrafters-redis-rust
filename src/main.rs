use std::result::Result::Ok;

use anyhow::Context;
use clap::Parser;
use tokio::io::{AsyncReadExt, BufReader, BufWriter, ReadHalf, WriteHalf};
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

    let expired_keys_db = db.clone();
    tokio::spawn(async move { expired_keys_db.remove_expired_keys().await });

    if let db::State::Slave { master_address, .. } = &*db.state {
        let stream = TcpStream::connect(master_address)
            .await
            .context("Failed to connect to master")?;
        let (mut writer, mut reader) = split_stream(stream);

        handshake::send_handshake(&mut writer, &mut reader, args.port).await?;

        let db = db.clone();
        tokio::spawn(async move {
            if let Err(err) = handle_connection(&mut writer, &mut reader, db).await {
                eprintln!("ERROR: {}", err);
            };
        });
    }

    loop {
        let (stream, addr) = listener.accept().await.context("Failed to get client")?;
        let (mut writer, mut reader) = split_stream(stream);
        println!("Accepted connection from {}", addr);

        let db = db.clone();
        tokio::spawn(async move {
            if let Err(err) = handle_connection(&mut writer, &mut reader, db).await {
                eprintln!("ERROR: {}", err);
            }
        });
    }
}

fn split_stream(
    stream: TcpStream,
) -> (
    BufWriter<WriteHalf<TcpStream>>,
    BufReader<ReadHalf<TcpStream>>,
) {
    let (rd, wr) = tokio::io::split(stream);
    let writer = BufWriter::new(wr);
    let reader = BufReader::new(rd);
    (writer, reader)
}

async fn handle_connection(
    writer: &mut BufWriter<WriteHalf<TcpStream>>,
    reader: &mut BufReader<ReadHalf<TcpStream>>,
    db: db::Db,
) -> anyhow::Result<()> {
    let mut buf = [0; 1024];
    loop {
        let bytes_read = reader
            .read(&mut buf)
            .await
            .context("Failed to read stream")?;
        if bytes_read == 0 {
            break;
        }

        let command = commands::parse_command(&mut buf[..bytes_read])?;
        println!("Command received: {}", command);
        command.handle(writer, &db).await?;
    }

    Ok(())
}
