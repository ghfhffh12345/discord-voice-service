use bytes::Bytes;
use tokio::sync::oneshot;

use crate::dave::{DaveMediaType, DaveRuntimeContext};
use crate::error::VoiceError;
use crate::gateway::VoiceGatewayClient;
use crate::handshake;
use crate::protection::ProtectionContext;
use crate::protocol::SessionDescription;
use crate::rollover::VoiceSessionRollover;
use crate::speaking::{OPUS_SILENCE_FRAME, send_speaking};
use crate::udp::VoiceUdpTransport;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VoiceContext {
    pub guild_id: String,
    pub channel_id: String,
    pub user_id: String,
    pub session_id: String,
    pub endpoint: String,
    pub token: String,
}

pub struct ConnectedVoiceSession {
    voice: VoiceContext,
    rollover: VoiceSessionRollover,
    gateway: Option<VoiceGatewayClient>,
    transport: Option<VoiceUdpTransport>,
    ssrc: Option<u32>,
    session_description: Option<SessionDescription>,
    dave: Option<DaveRuntimeContext>,
    heartbeat_shutdown: Option<oneshot::Sender<()>>,
    speaking_started: bool,
}

impl ConnectedVoiceSession {
    pub(crate) fn new(voice: VoiceContext) -> Self {
        Self {
            voice,
            rollover: VoiceSessionRollover::default(),
            gateway: None,
            transport: None,
            ssrc: None,
            session_description: None,
            dave: None,
            heartbeat_shutdown: None,
            speaking_started: false,
        }
    }

    pub async fn connect(voice: VoiceContext) -> Result<Self, VoiceError> {
        let Some(result) = handshake::connect(&voice).await? else {
            return Ok(Self::new(voice));
        };
        let handshake::VoiceHandshakeResult {
            gateway,
            transport,
            ssrc,
            heartbeat_shutdown,
            session_description,
            dave,
        } = result;
        let transport =
            transport.with_protection(ProtectionContext::from_session(&session_description)?);

        Ok(Self {
            gateway: Some(gateway),
            transport: Some(transport),
            voice,
            rollover: VoiceSessionRollover::default(),
            ssrc: Some(ssrc),
            session_description: Some(session_description),
            dave,
            heartbeat_shutdown: Some(heartbeat_shutdown),
            speaking_started: false,
        })
    }

    pub fn voice_context(&self) -> &VoiceContext {
        &self.voice
    }

    pub fn is_connected(&self) -> bool {
        self.gateway.is_some()
            && self.transport.is_some()
            && self.ssrc.is_some()
            && self.session_description.is_some()
    }

    pub fn dave_enabled(&self) -> bool {
        self.dave.is_some()
    }

    pub fn media_started(&self) -> bool {
        self.speaking_started
    }

    pub fn recovering(&self) -> bool {
        self.rollover.recovering()
    }

    pub fn voice_reconnecting(&self) -> bool {
        self.rollover.voice_reconnecting()
    }

    pub async fn send_audio_frame(&mut self, frame: Bytes) -> Result<(), VoiceError> {
        let ssrc = self
            .ssrc
            .ok_or(VoiceError::InvalidState("voice ssrc unavailable"))?;
        if !self.speaking_started {
            let gateway = self
                .gateway
                .as_ref()
                .ok_or(VoiceError::InvalidState("voice gateway unavailable"))?;
            send_speaking(gateway, ssrc).await?;
            self.speaking_started = true;
        }

        let frame = if let Some(dave) = self.dave.as_mut() {
            Bytes::from(
                dave.encryptor
                    .encrypt(DaveMediaType::Audio, ssrc, frame.as_ref())
                    .map_err(|_| VoiceError::InvalidState("voice dave frame encryption failed"))?,
            )
        } else {
            frame
        };
        let transport = self
            .transport
            .as_mut()
            .ok_or(VoiceError::InvalidState("voice transport unavailable"))?;
        transport.send_audio_frame(frame).await?;
        self.speaking_started = true;
        Ok(())
    }

    pub async fn stop_audio(&mut self) -> Result<(), VoiceError> {
        for silence_frame_index in 0..5 {
            if let Err(err) = self
                .send_audio_frame(Bytes::copy_from_slice(&OPUS_SILENCE_FRAME))
                .await
            {
                tracing::debug!(
                    silence_frame_index,
                    error = ?err,
                    "voice stop_audio silence send failed"
                );
                return Err(err);
            }
        }
        self.speaking_started = false;
        Ok(())
    }
}

impl Drop for ConnectedVoiceSession {
    fn drop(&mut self) {
        if let Some(shutdown) = self.heartbeat_shutdown.take() {
            let _ = shutdown.send(());
        }
    }
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;
    use std::sync::Arc;

    use futures::StreamExt;
    use serde_json::Value;
    use tokio::net::{TcpListener, UdpSocket};
    use tokio::sync::{Mutex, Notify};
    use tokio::time::{Duration, Instant, sleep};
    use tokio_tungstenite::accept_async;
    use tokio_tungstenite::tungstenite::Message;

    use super::*;

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
                    let Ok((len, from)) = socket.recv_from(&mut buf).await else {
                        break;
                    };
                    let packet = &buf[..len];

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
        speaking_observed: Arc<Notify>,
    }

    impl FakeVoiceGateway {
        async fn spawn() -> Self {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let ws_addr = listener.local_addr().unwrap();
            let speaking_observed = Arc::new(Notify::new());
            let speaking_observed_state = Arc::clone(&speaking_observed);

            tokio::spawn(async move {
                let (stream, _) = listener.accept().await.unwrap();
                let mut ws = accept_async(stream).await.unwrap();

                while let Some(message) = ws.next().await {
                    let message = message.unwrap();
                    if let Message::Text(text) = message {
                        let payload: Value = serde_json::from_str(text.as_ref()).unwrap();
                        if payload.get("op").and_then(Value::as_u64) == Some(5) {
                            speaking_observed_state.notify_waiters();
                        }
                    }
                }
            });

            Self {
                url: format!("ws://{ws_addr}/"),
                speaking_observed,
            }
        }

        fn url(&self) -> &str {
            &self.url
        }

        fn speaking_observed(&self) -> Arc<Notify> {
            Arc::clone(&self.speaking_observed)
        }
    }

    #[tokio::test]
    async fn stop_audio_emits_speaking_and_silence_frames() {
        let gateway = FakeVoiceGateway::spawn().await;
        let udp = FakeUdpPeer::spawn().await;
        let mut session = test_connected_session(gateway.url(), udp.addr()).await;
        let speaking_observed = gateway.speaking_observed();
        let speaking_notified = speaking_observed.notified();

        session.stop_audio().await.unwrap();

        tokio::time::timeout(Duration::from_secs(1), speaking_notified)
            .await
            .expect("speaking should be observed");
        assert_eq!(udp.silence_frame_count().await, 5);
    }

    async fn test_connected_session(url: &str, udp_addr: SocketAddr) -> ConnectedVoiceSession {
        ConnectedVoiceSession {
            voice: VoiceContext {
                guild_id: "1".into(),
                channel_id: "2".into(),
                user_id: "user-1".into(),
                session_id: "session-1".into(),
                endpoint: url.into(),
                token: "token-1".into(),
            },
            rollover: VoiceSessionRollover::default(),
            gateway: Some(VoiceGatewayClient::connect(url).await.unwrap()),
            transport: Some(VoiceUdpTransport::connect(udp_addr, 7).await.unwrap()),
            ssrc: Some(7),
            session_description: Some(SessionDescription {
                mode: "xsalsa20_poly1305".into(),
                secret_key: vec![0; 32],
                dave_protocol_version: None,
            }),
            dave: None,
            heartbeat_shutdown: None,
            speaking_started: false,
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
}
