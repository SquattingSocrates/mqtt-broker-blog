use std::{
    io::{Read, Write},
    time::Duration,
};

use lunatic::{
    net::{TcpListener, TcpStream},
    spawn_link,
};

fn main() -> std::io::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:1883")?;
    while let Ok((stream, _)) = listener.accept() {
        println!("New client connected: {:?}", stream.peer_addr());
        // spawn a new process for every new connection
        spawn_link!(|stream| connection_handler(stream));
    }
    Ok(())
}

fn connection_handler(mut stream: TcpStream) {
    let mut buffer = [0; 1024];
    loop {
        match stream.read(&mut buffer) {
            Ok(n) if n > 0 => {
                let message_type = buffer[0] >> 4; // get the type of message
                let message_length = buffer[1];
                println!(
                    "Received message of type {} and length {}",
                    message_type, message_length
                );
                match message_type {
                    1 => {
                        // CONNECT
                        let connack_packet: Vec<u8> = vec![
                            // Fixed header
                            0x20, // CONNACK Packet type
                            0x02, // Remaining length
                            // Variable header
                            0x00, // Connection accepted
                            0x00, // Connection accepted
                        ];
                        stream
                            .write(&connack_packet)
                            .expect("Failed to write connack packet");
                        println!("Responded to CONNECT");
                    }
                    t => eprintln!("Unknown type of MQTT message: {t}"),
                }
            }
            Ok(_) => {
                eprintln!("Nothing to read...");
                // sleep for a while so that we don't spam the terminal with the eprintln message
                lunatic::sleep(Duration::from_secs(1));
            }
            Err(e) => eprintln!("Failed to read TcpStream {e:?}"),
        }
    }
}
