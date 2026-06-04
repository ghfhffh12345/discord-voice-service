use std::net::SocketAddr;
use std::sync::Arc;

use bytes::Bytes;
use discord_voice_service_voice::VoiceError;
use discord_voice_service_voice::crypto::{EncryptionMode, choose_mode};
use discord_voice_service_voice::test_support::{
    OPUS_SILENCE_FRAME, VoiceGatewayClient, VoiceUdpTransport, build_ip_discovery_packet,
    discover_ip, parse_ip_discovery_response, parse_rtp_header, send_speaking,
};
use futures::StreamExt;
use serde_json::Value;
use tokio::net::{TcpListener, UdpSocket, lookup_host};
use tokio::sync::{Mutex, Notify};
use tokio::time::{Duration, Instant, sleep};
use tokio_tungstenite::accept_async;
use tokio_tungstenite::tungstenite::Message;

enum DiscoveryBehavior {
    Valid,
    ForeignSourceFirst,
    WrongSsrc,
}

struct FakeUdpPeer {
    addr: SocketAddr,
    advertised_ip: String,
    advertised_port: u16,
    discovery_count: Arc<Mutex<usize>>,
    silence_frame_count: Arc<Mutex<usize>>,
    audio_packets: Arc<Mutex<Vec<Vec<u8>>>>,
}

impl FakeUdpPeer {
    async fn spawn() -> Self {
        Self::spawn_with_behavior(DiscoveryBehavior::Valid).await
    }

    async fn spawn_with_behavior(behavior: DiscoveryBehavior) -> Self {
        let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let addr = socket.local_addr().unwrap();
        let advertised_ip = "127.0.0.1".to_owned();
        let advertised_port: u16 = 54_321;
        let discovery_count = Arc::new(Mutex::new(0usize));
        let silence_frame_count = Arc::new(Mutex::new(0usize));
        let audio_packets = Arc::new(Mutex::new(Vec::new()));
        let discovery_count_state = Arc::clone(&discovery_count);
        let silence_frame_count_state = Arc::clone(&silence_frame_count);
        let audio_packets_state = Arc::clone(&audio_packets);
        let advertised_ip_state = advertised_ip.clone();

        tokio::spawn(async move {
            let mut buf = [0u8; 512];
            loop {
                let (len, from) = socket.recv_from(&mut buf).await.unwrap();
                let packet = &buf[..len];

                if packet.len() == 74 && packet[..2] == 1u16.to_be_bytes() {
                    *discovery_count_state.lock().await += 1;
                    let mut response = [0u8; 74];
                    response[..2].copy_from_slice(&2u16.to_be_bytes());
                    response[2..4].copy_from_slice(&70u16.to_be_bytes());
                    let echoed_ssrc = match behavior {
                        DiscoveryBehavior::WrongSsrc => 999u32.to_be_bytes(),
                        _ => packet[4..8].try_into().unwrap(),
                    };
                    response[4..8].copy_from_slice(&echoed_ssrc);
                    response[8..8 + advertised_ip_state.len()]
                        .copy_from_slice(advertised_ip_state.as_bytes());
                    response[72..74].copy_from_slice(&advertised_port.to_be_bytes());
                    match behavior {
                        DiscoveryBehavior::ForeignSourceFirst => {
                            let foreign = UdpSocket::bind("127.0.0.1:0").await.unwrap();
                            foreign.send_to(&response, from).await.unwrap();
                        }
                        _ => {
                            socket.send_to(&response, from).await.unwrap();
                        }
                    }
                    continue;
                }

                if packet.ends_with(&OPUS_SILENCE_FRAME) {
                    *silence_frame_count_state.lock().await += 1;
                }
                if packet.len() >= 12 {
                    audio_packets_state.lock().await.push(packet.to_vec());
                }
            }
        });

        Self {
            addr,
            advertised_ip,
            advertised_port,
            discovery_count,
            silence_frame_count,
            audio_packets,
        }
    }

    fn addr(&self) -> SocketAddr {
        self.addr
    }

    async fn silence_frame_count(&self) -> usize {
        wait_for_value(&self.silence_frame_count, |count| *count >= 5).await
    }

    async fn discovery_count(&self) -> usize {
        wait_for_value(&self.discovery_count, |count| *count >= 1).await
    }

    async fn audio_packets(&self, minimum: usize) -> Vec<Vec<u8>> {
        wait_for_value(&self.audio_packets, |packets| packets.len() >= minimum).await
    }

    async fn audio_packet_count_now(&self) -> usize {
        self.audio_packets.lock().await.len()
    }

    fn advertised_ip(&self) -> &str {
        &self.advertised_ip
    }

    fn advertised_port(&self) -> u16 {
        self.advertised_port
    }
}

struct FakeVoiceGateway {
    url: String,
    speaking_index: Arc<Mutex<Option<usize>>>,
    audio_index: Arc<Mutex<Option<usize>>>,
    speaking_observed: Arc<Notify>,
}

impl FakeVoiceGateway {
    async fn spawn() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let udp_socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let ws_addr = listener.local_addr().unwrap();
        let udp_addr = udp_socket.local_addr().unwrap();
        let speaking_index = Arc::new(Mutex::new(None));
        let audio_index = Arc::new(Mutex::new(None));
        let speaking_observed = Arc::new(Notify::new());
        let next_index = Arc::new(Mutex::new(0usize));
        let speaking_state = Arc::clone(&speaking_index);
        let audio_state = Arc::clone(&audio_index);
        let speaking_notify_state = Arc::clone(&speaking_observed);
        let udp_order_state = Arc::clone(&next_index);
        let ws_order_state = Arc::clone(&next_index);

        tokio::spawn(async move {
            let mut buf = [0u8; 512];
            loop {
                let (len, from) = udp_socket.recv_from(&mut buf).await.unwrap();
                if len == 74 && buf[..2] == 1u16.to_be_bytes() {
                    let mut response = [0u8; 74];
                    response[..2].copy_from_slice(&2u16.to_be_bytes());
                    response[2..4].copy_from_slice(&70u16.to_be_bytes());
                    response[4..8].copy_from_slice(&buf[4..8]);
                    response[8..17].copy_from_slice(b"127.0.0.1");
                    response[72..74].copy_from_slice(&from.port().to_be_bytes());
                    udp_socket.send_to(&response, from).await.unwrap();
                    continue;
                }

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
                            speaking_notify_state.notify_waiters();
                        }
                    }
                }
            }
        });

        Self {
            url: format!("ws://{ws_addr}/?udp={udp_addr}&ssrc=7"),
            speaking_index,
            audio_index,
            speaking_observed,
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

    fn speaking_observed(&self) -> Arc<Notify> {
        Arc::clone(&self.speaking_observed)
    }
}

struct TestConnectedVoiceSession {
    gateway: VoiceGatewayClient,
    transport: VoiceUdpTransport,
    ssrc: u32,
    speaking_started: bool,
    speaking_observed: Arc<Notify>,
}

impl TestConnectedVoiceSession {
    async fn new(url: &str, speaking_observed: Arc<Notify>) -> Result<Self, VoiceError> {
        let uri: http::Uri = url.parse()?;
        let query = uri
            .path_and_query()
            .and_then(|path_and_query| path_and_query.query())
            .ok_or(VoiceError::InvalidState("voice session test query missing"))?;
        let udp = query_param(query, "udp")
            .ok_or(VoiceError::InvalidState("voice session test udp missing"))?;
        let ssrc = query_param(query, "ssrc")
            .ok_or(VoiceError::InvalidState("voice session test ssrc missing"))?
            .parse::<u32>()
            .map_err(|_| VoiceError::InvalidState("voice session test ssrc invalid"))?;
        let server = lookup_host(udp)
            .await?
            .next()
            .ok_or(VoiceError::InvalidState(
                "voice session test udp unresolved",
            ))?;

        Ok(Self {
            gateway: VoiceGatewayClient::connect(url).await?,
            transport: VoiceUdpTransport::connect(server, ssrc).await?,
            ssrc,
            speaking_started: false,
            speaking_observed,
        })
    }

    async fn send_audio_frame(&mut self, frame: Bytes) -> Result<(), VoiceError> {
        if !self.speaking_started {
            let observed = self.speaking_observed.notified();
            send_speaking(&self.gateway, self.ssrc).await?;
            observed.await;
            self.speaking_started = true;
        }
        self.transport.send_audio_frame(frame).await
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
async fn voice_udp_transport_prepares_packet_without_sending() {
    let fake = FakeUdpPeer::spawn().await;
    let mut transport = VoiceUdpTransport::connect(fake.addr(), 77).await.unwrap();

    let packet = transport
        .prepare_audio_packet_with_duration_samples(Bytes::from_static(b"opus-a"), 20, 960, true)
        .unwrap();

    assert_eq!(packet.duration_ms, 20);
    assert_eq!(packet.duration_samples, 960);
    assert!(packet.is_track);
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(fake.audio_packet_count_now().await, 0);
}

#[tokio::test]
async fn voice_udp_transport_sends_prepared_packet() {
    let fake = FakeUdpPeer::spawn().await;
    let mut transport = VoiceUdpTransport::connect(fake.addr(), 77).await.unwrap();
    let packet = transport
        .prepare_audio_packet_with_duration_samples(Bytes::from_static(b"opus-a"), 20, 960, true)
        .unwrap();

    transport.send_prepared_packet(&packet).await.unwrap();

    let packets = fake.audio_packets(1).await;
    assert_eq!(packets.len(), 1);
    assert_eq!(packets[0], packet.bytes.as_ref());
}

#[tokio::test]
async fn prepared_packets_preserve_rtp_duration_timestamps() {
    let fake = FakeUdpPeer::spawn().await;
    let mut transport = VoiceUdpTransport::connect(fake.addr(), 77).await.unwrap();

    let packets = [
        transport
            .prepare_audio_packet_with_duration_samples(
                Bytes::from_static(b"opus-a"),
                20,
                960,
                true,
            )
            .unwrap(),
        transport
            .prepare_audio_packet_with_duration_samples(
                Bytes::from_static(b"opus-b"),
                40,
                1_920,
                true,
            )
            .unwrap(),
        transport
            .prepare_audio_packet_with_duration_samples(
                Bytes::from_static(b"opus-c"),
                60,
                2_880,
                true,
            )
            .unwrap(),
    ];
    let headers = packets
        .iter()
        .map(|packet| parse_rtp_header(&packet.bytes).unwrap())
        .collect::<Vec<_>>();

    assert_eq!(
        headers
            .iter()
            .map(|header| header.sequence)
            .collect::<Vec<_>>(),
        vec![0, 1, 2]
    );
    assert_eq!(
        headers
            .iter()
            .map(|header| header.timestamp)
            .collect::<Vec<_>>(),
        vec![0, 960, 2_880]
    );
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(fake.audio_packet_count_now().await, 0);
}

#[tokio::test]
async fn voice_udp_transport_stop_sends_five_opus_silence_frames() {
    let fake = FakeUdpPeer::spawn().await;
    let mut transport = VoiceUdpTransport::connect(fake.addr(), 77).await.unwrap();

    transport.stop_audio().await.unwrap();

    assert_eq!(fake.silence_frame_count().await, 5);
}

#[tokio::test]
async fn voice_udp_transport_advances_rtp_timestamps_by_opus_duration_samples() {
    let fake = FakeUdpPeer::spawn().await;
    let mut transport = VoiceUdpTransport::connect(fake.addr(), 77).await.unwrap();

    transport
        .send_audio_frame_with_duration_samples(Bytes::from_static(b"opus-a"), 960)
        .await
        .unwrap();
    transport
        .send_audio_frame_with_duration_samples(Bytes::from_static(b"opus-b"), 1_920)
        .await
        .unwrap();
    transport
        .send_audio_frame_with_duration_samples(Bytes::from_static(b"opus-c"), 2_880)
        .await
        .unwrap();

    let packets = fake.audio_packets(3).await;
    let headers = packets
        .iter()
        .map(|packet| parse_rtp_header(packet).unwrap())
        .collect::<Vec<_>>();

    assert_eq!(
        headers
            .iter()
            .map(|header| header.sequence)
            .collect::<Vec<_>>(),
        vec![0, 1, 2]
    );
    assert_eq!(
        headers
            .iter()
            .map(|header| header.timestamp)
            .collect::<Vec<_>>(),
        vec![0, 960, 2_880]
    );
}

#[tokio::test]
async fn voice_udp_transport_speaking_is_sent_before_first_audio_packet() {
    let fake = FakeVoiceGateway::spawn().await;
    let mut session = TestConnectedVoiceSession::new(fake.url(), fake.speaking_observed())
        .await
        .unwrap();

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

    let discovered = discover_ip(&socket, fake.addr(), 77).await.unwrap();

    assert_eq!(discovered.ip, fake.advertised_ip());
    assert_eq!(discovered.port, fake.advertised_port());
}

#[tokio::test]
async fn voice_udp_transport_connect_performs_discovery_and_captures_discovered_addr() {
    let fake = FakeUdpPeer::spawn().await;

    let transport = VoiceUdpTransport::connect(fake.addr(), 77).await.unwrap();

    assert_eq!(fake.discovery_count().await, 1);
    assert_eq!(
        transport.local_addr().ip().to_string(),
        fake.advertised_ip()
    );
    assert_eq!(transport.local_addr().port(), fake.advertised_port());
}

#[tokio::test]
async fn voice_udp_transport_rejects_foreign_discovery_reply() {
    let fake = FakeUdpPeer::spawn_with_behavior(DiscoveryBehavior::ForeignSourceFirst).await;
    let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();

    let err = discover_ip(&socket, fake.addr(), 77).await.unwrap_err();

    assert!(err.to_string().contains("source"));
}

#[tokio::test]
async fn voice_udp_transport_rejects_discovery_reply_with_wrong_ssrc() {
    let fake = FakeUdpPeer::spawn_with_behavior(DiscoveryBehavior::WrongSsrc).await;
    let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();

    let err = discover_ip(&socket, fake.addr(), 77).await.unwrap_err();

    assert!(err.to_string().contains("ssrc"));
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

fn query_param<'a>(query: &'a str, key: &str) -> Option<&'a str> {
    query.split('&').find_map(|pair| {
        let (candidate, value) = pair.split_once('=')?;
        (candidate == key).then_some(value)
    })
}
