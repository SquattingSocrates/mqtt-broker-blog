use super::{tcp_stream_writer::TcpStreamWriterRequests, Client};
use crate::message_queue::MessageQueueRequests;
use mqtt_packet_3_5::{MqttPacket, PacketDecoder};

pub fn read_tcp_stream(client: Client) {
    let Client {
        connect_packet,
        writer,
        message_queue,
        tcp_stream,
        ..
    } = client.clone();
    let mut packet_decoder = PacketDecoder::from_stream(tcp_stream);
    let mqtt_version = connect_packet.protocol_version;

    while packet_decoder.has_more() {
        match packet_decoder.decode_packet(mqtt_version) {
            Ok(packet) => match packet {
                MqttPacket::Pingreq => {
                    writer.write_packet(MqttPacket::Pingresp);
                }
                MqttPacket::Publish(publish) => {
                    println!(
                        "[reader {}] received publish {publish:?}",
                        connect_packet.client_id
                    );
                    // later we will handle forwarding of publish messages
                    message_queue.publish(publish, client.clone());
                }
                MqttPacket::Subscribe(subscribe) => {
                    println!(
                        "[reader {}] received subscribe {subscribe:?}",
                        connect_packet.client_id
                    );
                    // subscribing in this context means sending a reference to this
                    // connection's writer process along with the subscribe packet
                    message_queue.subscribe(subscribe, client.clone());
                }
                // we will not handle other types of messages for now
                packet => eprintln!("No support for message type {packet:?}"),
            },
            Err(e) => eprintln!("Failed to read TcpStream {e:?}"),
        }
    }
    println!("[reader] stopped reader process");
}
