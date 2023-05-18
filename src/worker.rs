use std::time::Duration;

use lunatic::{ap::ProcessRef, sleep};
use mqtt_packet_3_5::MqttPacket;

use crate::{
    client::tcp_stream_writer::TcpStreamWriterRequests,
    message_queue::{MessageQueue, MessageQueueRequests},
};

pub fn worker_process(message_queue_process: ProcessRef<MessageQueue>) {
    loop {
        match message_queue_process.get_next_job() {
            Some(job) => {
                match job.packet {
                    publish @ MqttPacket::Publish(_) => {
                        for subscriber in job.subscribers.into_iter() {
                            subscriber.writer.write_packet(publish.clone());
                        }
                    }
                    suback @ MqttPacket::Suback(_) => {
                        job.sender.writer.write_packet(suback);
                    }
                    packet => eprintln!("ERROR: no handling of packet type {packet:?}"),
                };
            }
            None => {
                // for now we will just wait for some time before sending another
                // get_next_job request. Later we can implement back-offs or
                // even switch to a push-based processing of worker jobs
                sleep(Duration::from_millis(100));
            }
        }
    }
}
