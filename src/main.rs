use std::{
    collections::{HashMap, VecDeque},
    io::Write,
};

use lunatic::{abstract_process, ap::Config, net::TcpStream, AbstractProcess, Tag};
use mqtt_packet_3_5::*;
use serde::{Deserialize, Serialize};

fn main() -> std::io::Result<()> {
    // first, we need to start the MessageQueue
    let message_queue = MessageQueue::start(()).expect("should have started MessageQueue");

    // NOTE: this is only a dummy subscription as we have not implemented the client process yet
    message_queue.subscribe("mytopic".to_string(), ClientProcessRef { client_id: 1 });
    message_queue.publish(
        "mytopic".to_string(),
        PublishPacket {
            dup: false,
            qos: 0,
            retain: false,
            topic: "mytopic".to_string(),
            message_id: None,
            payload: "Hello, world!".into(),
            properties: None,
        },
    );

    Ok(())
}

#[derive(Debug, Serialize, Deserialize)]
// this process will store
struct ClientProcessRef {
    client_id: i32,
}

impl ClientProcessRef {
    // stub for publishing message to TcpStreamWriter
    pub fn publish(&self, _packet: PublishPacket) {}
}

#[derive(Debug, Serialize, Deserialize)]
enum WorkerJob {
    PublishJob {
        packet: PublishPacket,
        subscribers: Vec<ClientProcessRef>,
    },
}

#[derive(Debug, Serialize, Deserialize)]
struct MessageQueue {
    topic_subscriptions: HashMap<String, Vec<ClientProcessRef>>,
    jobs: VecDeque<WorkerJob>,
}

#[abstract_process]
impl MessageQueue {
    // setup upon creation of process
    #[init]
    fn init(_: Config<Self>, _: ()) -> Result<Self, ()> {
        Ok(Self {
            topic_subscriptions: HashMap::new(),
            jobs: VecDeque::new(),
        })
    }

    #[terminate]
    fn terminate(self) {
        println!("Shutdown process");
    }

    #[handle_link_death]
    fn handle_link_death(&self, _tag: Tag) {
        println!("Link trapped");
    }

    #[handle_request]
    // a client process will send a reference to themselve
    // in order to subscribe
    fn subscribe(&mut self, topic: String, client: ClientProcessRef) -> bool {
        let subscribers = self.topic_subscriptions.entry(topic).or_insert(vec![]);
        subscribers.push(client);
        true // let the Client know we registered him
    }

    #[handle_request]
    // a client process will send a reference to themselve
    fn publish(&mut self, topic: String, message: PublishPacket) -> bool {
        if let Some(subscribers) = self.topic_subscriptions.get(&topic) {
            // loop over subscribers to send message
            for subscriber in subscribers {
                // dummy code that won't work
                subscriber.publish(message.clone());
            }
        }
        false
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct TcpStreamWriter {
    tcp_stream: TcpStream,
}

#[abstract_process]
impl TcpStreamWriter {
    // setup upon creation of process
    #[init]
    fn init(_: Config<Self>, tcp_stream: TcpStream) -> Result<Self, ()> {
        Ok(Self { tcp_stream })
    }

    #[terminate]
    fn terminate(self) {
        println!("Shutdown process");
    }

    #[handle_link_death]
    fn handle_link_death(&self, _tag: Tag) {
        println!("Link trapped");
    }

    #[handle_request]
    // we're going to send already encoded messages to the writer
    // because the writer doesn't really need much information about
    // what it is writing at the moment
    fn write_packet(&mut self, encoded_packet: Vec<u8>) -> bool {
        match self.tcp_stream.write_all(&encoded_packet) {
            Ok(_) => true,
            Err(e) => {
                eprintln!("Writer failed to write packet {e:?}");
                false
            }
        }
    }
}
