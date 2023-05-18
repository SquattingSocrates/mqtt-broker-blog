use std::io::Write;

use lunatic::{abstract_process, ap::Config, net::TcpStream, Tag};
use mqtt_packet_3_5::{ConnectPacket, MqttPacket};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct TcpStreamWriter {
    tcp_stream: TcpStream,
    connect_packet: ConnectPacket,
}

#[abstract_process(visibility=pub)]
impl TcpStreamWriter {
    // setup upon creation of process
    #[init]
    fn init(
        _: Config<Self>,
        (tcp_stream, connect_packet): (TcpStream, ConnectPacket),
    ) -> Result<Self, ()> {
        Ok(Self {
            tcp_stream,
            connect_packet,
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
    // we're going to send the wrapper enum MqttPacket and let the writer
    // encode the message. This is not the most efficient way to do it,
    // especially since publish messages with QoS 0 or QoS 1 will
    // might be sent many times, but we will optimise this
    // in a later chapter of the series. For now it is better for
    // understanding which messages are sent where because we will
    // have a lot messaging going on
    pub fn write_packet(&mut self, packet: MqttPacket) -> bool {
        println!(
            "[writer {}] received packet to write {packet:?}",
            self.connect_packet.client_id
        );
        let encoded_packet = packet
            .encode(self.connect_packet.protocol_version)
            .expect("should encode packet");
        match self.tcp_stream.write_all(&encoded_packet) {
            Ok(_) => true,
            Err(e) => {
                eprintln!("Writer failed to write packet {e:?}");
                false
            }
        }
    }
}
