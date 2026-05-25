use std::net::SocketAddr;
use std::sync::Arc;

use bytes::Bytes;
use discord_voice_service_voice::crypto::EncryptionMode;
use discord_voice_service_voice::test_support::{ProtectionContext, VoiceUdpTransport};
use tokio::net::UdpSocket;
use tokio::sync::Mutex;
use tokio::time::{Duration, Instant, sleep};

const XCHACHA_TEST_KEY: [u8; 32] = [0x11; 32];
const AES_GCM_TEST_KEY: [u8; 32] = [0x22; 32];

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
async fn voice_udp_transport_round_trips_xchacha_packet_protection() {
    let fake = FakeUdpPeer::spawn().await;
    let protection = xchacha_test_context();
    let mut transport = VoiceUdpTransport::connect_protected(fake.server_addr(), 7, protection)
        .await
        .unwrap();

    let payload = Bytes::from_static(b"opus-frame");
    transport.send_audio_frame(payload.clone()).await.unwrap();

    let packet = fake.next_audio_packet().await;
    let (_, plaintext) = xchacha_test_context().unprotect_packet(&packet).unwrap();
    assert_eq!(plaintext, payload);
}

#[tokio::test]
async fn voice_udp_transport_round_trips_aes_gcm_packet_protection() {
    let fake = FakeUdpPeer::spawn().await;
    let protection = aes_gcm_test_context();
    let mut transport = VoiceUdpTransport::connect_protected(fake.server_addr(), 7, protection)
        .await
        .unwrap();

    let payload = Bytes::from_static(b"opus-frame");
    transport.send_audio_frame(payload.clone()).await.unwrap();

    let packet = fake.next_audio_packet().await;
    let (_, plaintext) = aes_gcm_test_context().unprotect_packet(&packet).unwrap();
    assert_eq!(plaintext, payload);
}

fn xchacha_test_context() -> ProtectionContext {
    ProtectionContext::new(
        EncryptionMode::AeadXChaCha20Poly1305Rtpsize,
        XCHACHA_TEST_KEY.to_vec(),
    )
    .unwrap()
}

fn aes_gcm_test_context() -> ProtectionContext {
    ProtectionContext::new(
        EncryptionMode::AeadAes256GcmRtpsize,
        AES_GCM_TEST_KEY.to_vec(),
    )
    .unwrap()
}
