use std::net::SocketAddr;
use std::sync::Arc;

use bytes::Bytes;
use discord_voice_service::discord_voice::protection::ProtectionContext;
use discord_voice_service::discord_voice::udp::VoiceUdpTransport;
use tokio::net::UdpSocket;
use tokio::sync::Mutex;
use tokio::time::{Duration, Instant, sleep};

struct FakeUdpPeer {
    addr: SocketAddr,
    audio_packets: Arc<Mutex<Vec<Vec<u8>>>>,
}

impl FakeUdpPeer {
    async fn spawn() -> Self {
        let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let addr = socket.local_addr().unwrap();
        let audio_packets = Arc::new(Mutex::new(Vec::new()));
        let audio_packets_state = Arc::clone(&audio_packets);

        tokio::spawn(async move {
            let mut buf = [0u8; 512];
            loop {
                let Ok((len, from)) = socket.recv_from(&mut buf).await else {
                    break;
                };
                let packet = buf[..len].to_vec();

                if packet.len() == 74 && packet[..2] == 1u16.to_be_bytes() {
                    let mut response = [0u8; 74];
                    response[..2].copy_from_slice(&2u16.to_be_bytes());
                    response[2..4].copy_from_slice(&70u16.to_be_bytes());
                    response[4..8].copy_from_slice(&packet[4..8]);
                    response[8..17].copy_from_slice(b"127.0.0.1");
                    response[72..74].copy_from_slice(&from.port().to_be_bytes());
                    socket.send_to(&response, from).await.unwrap();
                    continue;
                }

                if packet.len() >= 12 {
                    audio_packets_state.lock().await.push(packet);
                }
            }
        });

        Self {
            addr,
            audio_packets,
        }
    }

    fn server_addr(&self) -> SocketAddr {
        self.addr
    }

    async fn next_audio_packet(&self) -> Vec<u8> {
        let deadline = Instant::now() + Duration::from_secs(1);
        loop {
            if let Some(packet) = self.audio_packets.lock().await.first().cloned() {
                return packet;
            }
            if Instant::now() >= deadline {
                panic!("timed out waiting for audio packet");
            }
            sleep(Duration::from_millis(10)).await;
        }
    }
}

#[tokio::test]
async fn voice_udp_transport_applies_selected_packet_protection_before_send() {
    let fake = FakeUdpPeer::spawn().await;
    let mut transport = VoiceUdpTransport::connect_protected(
        fake.server_addr(),
        7,
        ProtectionContext::test_xchacha(),
    )
    .await
    .unwrap();

    transport
        .send_audio_frame(Bytes::from_static(b"opus-frame"))
        .await
        .unwrap();

    let packet = fake.next_audio_packet().await;
    assert_ne!(&packet[12..], b"opus-frame");
}
