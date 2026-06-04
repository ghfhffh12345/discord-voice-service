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
            let mut audio_packets = self.audio_packets.lock().await;
            if !audio_packets.is_empty() {
                let packet = audio_packets.remove(0);
                return packet;
            }
            drop(audio_packets);
            if Instant::now() >= deadline {
                panic!("timed out waiting for audio packet");
            }
            sleep(Duration::from_millis(10)).await;
        }
    }
}

#[tokio::test]
async fn prepared_packet_protection_matches_legacy_send() {
    let legacy_fake = FakeUdpPeer::spawn().await;
    let prepared_fake = FakeUdpPeer::spawn().await;
    let mut legacy =
        VoiceUdpTransport::connect_protected(legacy_fake.server_addr(), 7, xchacha_test_context())
            .await
            .unwrap();
    let mut prepared = VoiceUdpTransport::connect_protected(
        prepared_fake.server_addr(),
        7,
        xchacha_test_context(),
    )
    .await
    .unwrap();
    let payload = Bytes::from_static(b"opus-frame");

    legacy.send_audio_frame(payload.clone()).await.unwrap();
    let prepared_packet = prepared
        .prepare_audio_packet_with_duration_samples(payload.clone(), 20, 960, true)
        .unwrap();
    prepared
        .send_prepared_packet(&prepared_packet)
        .await
        .unwrap();

    let legacy_packet = legacy_fake.next_audio_packet().await;
    let sent_prepared_packet = prepared_fake.next_audio_packet().await;
    assert_eq!(
        prepared_packet.bytes.as_ref(),
        sent_prepared_packet.as_slice()
    );
    assert_eq!(legacy_packet, sent_prepared_packet);

    let (_, plaintext) = xchacha_test_context()
        .unprotect_packet(&sent_prepared_packet)
        .unwrap();
    assert_eq!(plaintext, payload);
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

#[tokio::test]
async fn voice_udp_transport_uses_frame_duration_for_rtp_timestamps() {
    let fake = FakeUdpPeer::spawn().await;
    let protection = xchacha_test_context();
    let mut transport = VoiceUdpTransport::connect_protected(fake.server_addr(), 7, protection)
        .await
        .unwrap();

    transport
        .send_audio_frame_with_duration_samples(Bytes::from_static(b"sixty-ms"), 2_880)
        .await
        .unwrap();
    transport
        .send_audio_frame_with_duration_samples(Bytes::from_static(b"ten-ms"), 480)
        .await
        .unwrap();
    transport
        .send_audio_frame_with_duration_samples(Bytes::from_static(b"twenty-ms"), 960)
        .await
        .unwrap();

    let first = fake.next_audio_packet().await;
    let second = fake.next_audio_packet().await;
    let third = fake.next_audio_packet().await;

    let (first_header, first_payload) = xchacha_test_context().unprotect_packet(&first).unwrap();
    let (second_header, second_payload) = xchacha_test_context().unprotect_packet(&second).unwrap();
    let (third_header, third_payload) = xchacha_test_context().unprotect_packet(&third).unwrap();

    assert_eq!(first_payload, Bytes::from_static(b"sixty-ms"));
    assert_eq!(second_payload, Bytes::from_static(b"ten-ms"));
    assert_eq!(third_payload, Bytes::from_static(b"twenty-ms"));
    assert_eq!(first_header.sequence, 0);
    assert_eq!(second_header.sequence, 1);
    assert_eq!(third_header.sequence, 2);
    assert_eq!(first_header.timestamp, 0);
    assert_eq!(second_header.timestamp, 2_880);
    assert_eq!(third_header.timestamp, 3_360);
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
