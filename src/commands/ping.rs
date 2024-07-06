use anyhow::Context;
use tokio::net::TcpStream;

use crate::message::{Message, SimpleString};

pub(crate) async fn handle(
    message: Option<String>,
    stream: &mut TcpStream,
) -> Result<(), anyhow::Error> {
    let data = match message {
        Some(message) => message,
        None => String::from("PONG"),
    };

    let message = Message::SimpleString(SimpleString { data });
    message
        .send(stream)
        .await
        .context("Failed to send PING reply")?;

    Ok(())
}
