use std::{
    io::{Read, Write},
    net::TcpListener,
};

fn main() {
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();
    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                let mut commands = String::new();
                stream.read_to_string(&mut commands).unwrap();

                commands.split("\n").into_iter().for_each(|_c| {
                    stream.write(b"+PONG\r\n").unwrap();
                });
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
