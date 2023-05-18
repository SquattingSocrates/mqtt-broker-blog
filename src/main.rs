use lunatic::{net::TcpListener, spawn_link, AbstractProcess};
use message_queue::MessageQueue;
use worker::worker_process;

use crate::client::Client;

mod client;
mod message_queue;
mod worker;

fn main() -> std::io::Result<()> {
    // first, we need to start the MessageQueue
    let message_queue = MessageQueue::start_as(&MessageQueue::default(), ())
        .expect("should have started MessageQueue");

    // we're starting the worker process with spawn_link because we want everything to crash if a worker goes down
    // as it indicates an issue with our code. This is appropriate in such an early version of our MQTT broker
    // but at a later point we will need some supervision and possibly a dynamic pool of workers
    let _worker = spawn_link!(|message_queue| worker_process(message_queue));

    let listener = TcpListener::bind("127.0.0.1:1883")?;
    while let Ok((stream, _)) = listener.accept() {
        println!("New client connected: {:?}", stream.peer_addr());
        // spawn a new process for every new connection
        spawn_link!(|stream| {
            // if the creation of a new client instance fails the other clients
            // will still keep running.
            // For now we won't use this struct, but later it
            // might come in handy for monitoring
            Client::start_new_client(stream);
        });
    }

    Ok(())
}
