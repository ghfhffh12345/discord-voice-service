use std::net::SocketAddr;
use std::sync::Arc;

use bytes::Bytes;
use discord_voice_service::discord_voice::crypto::{EncryptionMode, choose_mode};
use discord_voice_service::discord_voice::discovery::{
    build_ip_discovery_packet, discover_ip, parse_ip_discovery_response,
};
use discord_voice_service::discord_voice::speaking::ConnectedVoiceSession;
use discord_voice_service::discord_voice::speaking::OPUS_SILENCE_FRAME;
use discord_voice_service::discord_voice::udp::VoiceUdpTransport;
use futures::StreamExt;
use serde_json::Value;
use tokio::net::{TcpListener, UdpSocket};
use tokio::sync::Mutex;
use tokio::time::{Duration, Instant, sleep};
use tokio_tungstenite::accept_async;
use tokio_tungstenite::tungstenite::Message;

struct FakeUdpPeer {
    addr: SocketAddr,
    silence_frame_count: Arc<Mutex<usize>>,
}

impl FakeUdpPeer {
    async fn spawn() -> Self {
        let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let addr = socket.local_addr().unwrap();
        let silence_frame_count = Arc::new(Mutex::new(0usize));
        let silence_frame_count_state = Arc::clone(&silence_frame_count);

        tokio::spawn(async move {
            let mut buf = [0u8; 512];
            loop {
                let (len, from) = socket.recv_from(&mut buf).await.unwrap();
                let packet = &buf[..len];

                if packet.len() == 74 {
                    let mut response = [0u8; 74];
                    response[..2].copy_from_slice(&2u16.to_be_bytes());
                    response[2..4].copy_from_slice(&70u16.to_be_bytes());
                    response[4..8].copy_from_slice(&packet[4..8]);
                    response[8..17].copy_from_slice(b"127.0.0.1");
                    response[72..74].copy_from_slice(&from.port().to_be_bytes());
                    socket.send_to(&response, from).await.unwrap();
                    continue;
                }

                if packet.ends_with(&OPUS_SILENCE_FRAME) {
                    *silence_frame_count_state.lock().await += 1;
                }
            }
        });

        Self {
            addr,
            silence_frame_count,
        }
    }

    fn addr(&self) -> SocketAddr {
        self.addr
    }

    async fn silence_frame_count(&self) -> usize {
        wait_for_value(&self.silence_frame_count, |count| *count >= 5).await
    }
}

struct FakeVoiceGateway {
    url: String,
    speaking_index: Arc<Mutex<Option<usize>>>,
    audio_index: Arc<Mutex<Option<usize>>>,
}

impl FakeVoiceGateway {
    async fn spawn() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let udp_socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let ws_addr = listener.local_addr().unwrap();
        let udp_addr = udp_socket.local_addr().unwrap();
        let speaking_index = Arc::new(Mutex::new(None));
        let audio_index = Arc::new(Mutex::new(None));
        let next_index = Arc::new(Mutex::new(0usize));
        let speaking_state = Arc::clone(&speaking_index);
        let audio_state = Arc::clone(&audio_index);
        let udp_order_state = Arc::clone(&next_index);
        let ws_order_state = Arc::clone(&next_index);

        tokio::spawn(async move {
            let mut buf = [0u8; 512];
            loop {
                let (len, _) = udp_socket.recv_from(&mut buf).await.unwrap();
                if len >= 12 {
                    let mut index = udp_order_state.lock().await;
                    let current = *index;
                    *index += 1;
                    let mut audio = audio_state.lock().await;
                    if audio.is_none() {
                        *audio = Some(current);
                    }
                }
            }
        });

        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut ws = accept_async(stream).await.unwrap();

            while let Some(message) = ws.next().await {
                let message = message.unwrap();
                if let Message::Text(text) = message {
                    let payload: Value = serde_json::from_str(text.as_ref()).unwrap();
                    if payload.get("op").and_then(Value::as_u64) == Some(5) {
                        let mut index = ws_order_state.lock().await;
                        let current = *index;
                        *index += 1;
                        let mut speaking = speaking_state.lock().await;
                        if speaking.is_none() {
                            *speaking = Some(current);
                        }
                    }
                }
            }
        });

        Self {
            url: format!("ws://{ws_addr}/?udp={udp_addr}&ssrc=7"),
            speaking_index,
            audio_index,
        }
    }

    fn url(&self) -> &str {
        &self.url
    }

    async fn speaking_sent_before_audio(&self) -> bool {
        let deadline = Instant::now() + Duration::from_secs(1);
        loop {
            let speaking = *self.speaking_index.lock().await;
            let audio = *self.audio_index.lock().await;
            if let (Some(speaking), Some(audio)) = (speaking, audio) {
                return speaking < audio;
            }
            if Instant::now() >= deadline {
                return false;
            }
            sleep(Duration::from_millis(10)).await;
        }
    }
}

async fn wait_for_value<T, F>(slot: &Arc<Mutex<T>>, ready: F) -> T
where
    T: Clone,
    F: Fn(&T) -> bool,
{
    let deadline = Instant::now() + Duration::from_secs(1);
    loop {
        let value = slot.lock().await.clone();
        if ready(&value) || Instant::now() >= deadline {
            return value;
        }
        sleep(Duration::from_millis(10)).await;
    }
}

#[tokio::test]
async fn voice_udp_transport_stop_sends_five_opus_silence_frames() {
    let fake = FakeUdpPeer::spawn().await;
    let mut transport = VoiceUdpTransport::connect(fake.addr()).await.unwrap();

    transport.stop_audio().await.unwrap();

    assert_eq!(fake.silence_frame_count().await, 5);
}

#[tokio::test]
async fn voice_udp_transport_speaking_is_sent_before_first_audio_packet() {
    let fake = FakeVoiceGateway::spawn().await;
    let mut session = ConnectedVoiceSession::for_test(fake.url()).await.unwrap();

    session.start_speaking().await.unwrap();
    session
        .send_audio_frame(Bytes::from_static(b"opus"))
        .await
        .unwrap();

    assert!(fake.speaking_sent_before_audio().await);
}

#[tokio::test]
async fn voice_udp_transport_discover_ip_returns_local_ip_and_port_from_udp_handshake() {
    let fake = FakeUdpPeer::spawn().await;
    let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let local_addr = socket.local_addr().unwrap();

    let discovered = discover_ip(&socket, fake.addr(), 77).await.unwrap();

    assert_eq!(discovered.ip, "127.0.0.1");
    assert_eq!(discovered.port, local_addr.port());
}

#[test]
fn voice_udp_transport_discovery_packets_round_trip() {
    let packet = build_ip_discovery_packet(77);
    assert_eq!(packet.len(), 74);
    assert_eq!(&packet[..2], &1u16.to_be_bytes());
    assert_eq!(&packet[2..4], &70u16.to_be_bytes());
    assert_eq!(&packet[4..8], &77u32.to_be_bytes());

    let mut response = [0u8; 74];
    response[..2].copy_from_slice(&2u16.to_be_bytes());
    response[2..4].copy_from_slice(&70u16.to_be_bytes());
    response[4..8].copy_from_slice(&packet[4..8]);
    response[8..17].copy_from_slice(b"127.0.0.1");
    response[72..74].copy_from_slice(&4321u16.to_be_bytes());

    let discovered = parse_ip_discovery_response(&response).unwrap();
    assert_eq!(discovered.ip, "127.0.0.1");
    assert_eq!(discovered.port, 4321);
}

#[test]
fn voice_udp_transport_chooses_supported_encryption_modes_in_priority_order() {
    let mode = choose_mode(&[
        "aead_xchacha20_poly1305_rtpsize".to_owned(),
        "aead_aes256_gcm_rtpsize".to_owned(),
    ])
    .unwrap();
    assert_eq!(mode, EncryptionMode::AeadAes256GcmRtpsize);

    let fallback = choose_mode(&["aead_xchacha20_poly1305_rtpsize".to_owned()]).unwrap();
    assert_eq!(fallback, EncryptionMode::AeadXChaCha20Poly1305Rtpsize);
}
