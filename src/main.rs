use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

#[tokio::main]
async fn main() {
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();

    loop {
        let (mut stream, _) = listener.accept().await.unwrap();

        tokio::spawn(async move {
            let mut buf = [0; 1024];
            loop {
                let bytes_read = stream.read(&mut buf).await.unwrap();
                if bytes_read == 0 {
                    break;
                }

                stream.write_all(b"+PONG\r\n").await.unwrap();
            }
        });
    }
}
