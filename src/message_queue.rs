use std::collections::{HashMap, VecDeque};

use lunatic::{abstract_process, ap::Config, ProcessName, Tag};
use mqtt_packet_3_5::{MqttPacket, PublishPacket, SubackPacket, SubscribePacket};
use serde::{Deserialize, Serialize};

use crate::client::Client;

#[derive(Debug, Serialize, Deserialize)]
pub struct WorkerJob {
    pub job_id: u32,
    pub packet: MqttPacket,
    // we will always have a list of subscribers
    // but not all types of messages require any subscribers
    pub subscribers: Vec<Client>,
    pub sender: Client,
}

#[derive(Debug, Serialize, Deserialize, ProcessName, Default)]
#[lunatic(process_name = "message_queue")]
pub struct MessageQueue {
    topic_subscriptions: HashMap<String, Vec<Client>>,
    jobs: VecDeque<WorkerJob>,
    job_counter: u32,
}

#[abstract_process(visibility=pub)]
impl MessageQueue {
    // setup upon creation of process
    #[init]
    fn init(_: Config<Self>, _: ()) -> Result<Self, ()> {
        Ok(Self::default())
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
    fn subscribe(&mut self, packet: SubscribePacket, sender: Client) -> bool {
        let mut granted = vec![];
        // add the client reference to each topic
        for subscription in packet.subscriptions.into_iter() {
            let subscribers = self
                .topic_subscriptions
                .entry(subscription.topic)
                .or_default();
            subscribers.push(sender.clone());

            // we will need to send a SUBACK to the subscriber
            // in which we will let the subscriber know which QoS level
            // was granted for each topic
            // NOTE: for now we will send QoS level 0 because that's the only
            // level the MQTT broker supports for now
            granted.push(mqtt_packet_3_5::Granted::QoS0);
        }

        // create a new job for sending the SUBACK
        let job_id = self.get_job_id();
        self.jobs.push_back(WorkerJob {
            job_id,
            // use helper method for creating a basic MQTTv3 SUBACK packet
            packet: MqttPacket::Suback(SubackPacket::new_v3(packet.message_id, granted)),
            subscribers: vec![],
            sender,
        });
        true // let the Client know we registered him
    }

    #[handle_request]
    // client forwarded the raw parsed packet
    fn publish(&mut self, packet: PublishPacket, sender: Client) -> bool {
        let job_id = self.get_job_id();
        if let Some(subscribers) = self.topic_subscriptions.get(&packet.topic) {
            self.jobs.push_back(WorkerJob {
                job_id,
                // use helper method for creating a basic MQTTv3 SUBACK packet
                packet: MqttPacket::Publish(packet),
                subscribers: subscribers.clone(),
                sender,
            });
        }
        false
    }

    #[handle_request]
    // client forwarded the raw parsed packet
    fn get_next_job(&mut self) -> Option<WorkerJob> {
        self.jobs.pop_front()
    }

    // helper method
    fn get_job_id(&mut self) -> u32 {
        let job_id = self.job_counter;
        self.job_counter += 1;
        job_id
    }
}
