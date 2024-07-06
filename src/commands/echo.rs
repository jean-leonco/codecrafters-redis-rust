use anyhow::Context;
use tokio::net::TcpStream;

use crate::message::{Message, SimpleString};

pub(crate) async fn handle(message: String, stream: &mut TcpStream) -> Result<(), anyhow::Error> {
    let message = Message::SimpleString(SimpleString { data: message });
    message
        .send(stream)
        .await
        .context("Failed to send ECHO reply")?;

    Ok(())
}
