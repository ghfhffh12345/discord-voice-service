use std::net::SocketAddr;
use std::sync::Arc;

use aes_gcm::Aes256Gcm;
use aes_gcm::aead::{AeadInPlace, KeyInit};
use bytes::Bytes;
use chacha20poly1305::XChaCha20Poly1305;
use discord_voice_service_voice::crypto::EncryptionMode;
use discord_voice_service_voice::test_support::{ProtectionContext, VoiceUdpTransport};
use tokio::net::UdpSocket;
use tokio::sync::Mutex;
use tokio::time::{Duration, Instant, sleep};

const RTP_HEADER_LEN: usize = 12;
const TAG_LEN: usize = 16;
const NONCE_SUFFIX_LEN: usize = 4;
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
    let mut transport =
        VoiceUdpTransport::connect_protected(fake.server_addr(), 7, xchacha_test_context())
            .await
            .unwrap();

    let payload = Bytes::from_static(b"opus-frame");
    transport.send_audio_frame(payload.clone()).await.unwrap();

    let packet = fake.next_audio_packet().await;
    let plaintext = decrypt_xchacha_packet(&packet);
    assert_eq!(plaintext, payload);
}

#[tokio::test]
async fn voice_udp_transport_round_trips_aes_gcm_packet_protection() {
    let fake = FakeUdpPeer::spawn().await;
    let mut transport =
        VoiceUdpTransport::connect_protected(fake.server_addr(), 7, aes_gcm_test_context())
            .await
            .unwrap();

    let payload = Bytes::from_static(b"opus-frame");
    transport.send_audio_frame(payload.clone()).await.unwrap();

    let packet = fake.next_audio_packet().await;
    let plaintext = decrypt_aes_gcm_packet(&packet);
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

fn decrypt_xchacha_packet(packet: &[u8]) -> Bytes {
    let (header, ciphertext, tag, nonce_suffix) = split_protected_packet(packet);
    let cipher = XChaCha20Poly1305::new_from_slice(&XCHACHA_TEST_KEY).unwrap();
    let mut nonce = chacha20poly1305::XNonce::default();
    nonce[..NONCE_SUFFIX_LEN].copy_from_slice(nonce_suffix);
    let mut plaintext = ciphertext.to_vec();
    cipher
        .decrypt_in_place_detached(
            &nonce,
            header,
            &mut plaintext,
            chacha20poly1305::Tag::from_slice(tag),
        )
        .unwrap();
    Bytes::from(plaintext)
}

fn decrypt_aes_gcm_packet(packet: &[u8]) -> Bytes {
    let (header, ciphertext, tag, nonce_suffix) = split_protected_packet(packet);
    let cipher = Aes256Gcm::new_from_slice(&AES_GCM_TEST_KEY).unwrap();
    let mut nonce = aes_gcm::Nonce::default();
    nonce[..NONCE_SUFFIX_LEN].copy_from_slice(nonce_suffix);
    let mut plaintext = ciphertext.to_vec();
    cipher
        .decrypt_in_place_detached(
            &nonce,
            header,
            &mut plaintext,
            aes_gcm::Tag::from_slice(tag),
        )
        .unwrap();
    Bytes::from(plaintext)
}

fn split_protected_packet(packet: &[u8]) -> (&[u8], &[u8], &[u8], &[u8]) {
    assert!(packet.len() >= RTP_HEADER_LEN + TAG_LEN + NONCE_SUFFIX_LEN);

    let header = &packet[..RTP_HEADER_LEN];
    let body = &packet[RTP_HEADER_LEN..];
    let (ciphertext_and_tag, nonce_suffix) = body.split_at(body.len() - NONCE_SUFFIX_LEN);
    let (ciphertext, tag) = ciphertext_and_tag.split_at(ciphertext_and_tag.len() - TAG_LEN);

    (header, ciphertext, tag, nonce_suffix)
}
