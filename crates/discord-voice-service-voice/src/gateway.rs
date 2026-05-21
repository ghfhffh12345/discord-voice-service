use std::sync::Arc;

use futures::StreamExt;
use serde_json::Value;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::Mutex;
use tokio_tungstenite::tungstenite::Message;

use crate::dave::DaveSession;
use crate::error::VoiceError;
use crate::protocol::{self, VoiceGatewayPayload};
use crate::resume::GatewayEvent;
use crate::session::VoiceContext;
use crate::udp::DiscoveredUdpAddress;
use crate::ws::{self, VoiceWebSocketReader, VoiceWebSocketWriter};

#[derive(Clone)]
pub struct VoiceGatewayClient {
    read: Arc<Mutex<VoiceWebSocketReader>>,
    write: Arc<Mutex<VoiceWebSocketWriter>>,
    seq_ack: Arc<Mutex<Option<u64>>>,
}

impl VoiceGatewayClient {
    pub async fn connect(url: &str) -> Result<Self, VoiceError> {
        let (write, read) = ws::connect(url).await?;
        Ok(Self {
            read: Arc::new(Mutex::new(read)),
            write: Arc::new(Mutex::new(write)),
            seq_ack: Arc::new(Mutex::new(None)),
        })
    }

    pub async fn record_seq_ack(&self, seq: u64) {
        *self.seq_ack.lock().await = Some(seq);
    }

    pub async fn apply_gateway_event(&self, event: &GatewayEvent) {
        if let Some(seq) = event.seq() {
            self.record_seq_ack(seq).await;
        }
    }

    pub async fn send_identify(&self, voice: &VoiceContext) -> Result<(), VoiceError> {
        let max_dave_protocol_version =
            Some(DaveSession::max_supported_protocol_version()).filter(|version| *version > 0);
        self.send_json(protocol::identify_payload(voice, max_dave_protocol_version))
            .await
    }

    pub async fn send_select_protocol(
        &self,
        address: &DiscoveredUdpAddress,
        mode: &str,
    ) -> Result<(), VoiceError> {
        self.send_json(protocol::select_protocol_payload(address, mode))
            .await
    }

    pub async fn send_heartbeat(&self) -> Result<(), VoiceError> {
        let seq_ack = *self.seq_ack.lock().await;
        tracing::debug!(seq_ack, "voice gateway sending heartbeat");
        self.send_json(protocol::heartbeat_payload(
            heartbeat_timestamp_millis()?,
            seq_ack,
        ))
        .await
    }

    pub async fn send_resume(
        &self,
        server_id: &str,
        session_id: &str,
        token: &str,
    ) -> Result<(), VoiceError> {
        let seq_ack = *self.seq_ack.lock().await;
        self.send_json(protocol::resume_payload(
            server_id, session_id, token, seq_ack,
        ))
        .await
    }

    pub async fn receive_event(&self) -> Result<VoiceGatewayPayload, VoiceError> {
        let mut reader = self.read.lock().await;
        while let Some(message) = reader.next().await {
            match message? {
                Message::Text(text) => {
                    let payload = protocol::parse_gateway_message(text.as_ref())?;
                    if let Some(seq) = payload.seq() {
                        *self.seq_ack.lock().await = Some(seq);
                    }
                    if let protocol::VoiceGatewayEvent::HeartbeatAck(ack) = payload.event() {
                        tracing::debug!(
                            seq = payload.seq(),
                            nonce = ack.nonce,
                            "voice gateway received heartbeat ack"
                        );
                    }
                    return Ok(payload);
                }
                Message::Binary(bytes) => {
                    let payload = protocol::parse_gateway_binary_message(bytes.as_ref())?;
                    if let Some(seq) = payload.seq() {
                        *self.seq_ack.lock().await = Some(seq);
                    }
                    return Ok(payload);
                }
                Message::Close(_) => {
                    return Err(VoiceError::InvalidState(
                        "voice gateway closed during receive",
                    ));
                }
                _ => {}
            }
        }

        Err(VoiceError::InvalidState(
            "voice gateway closed during receive",
        ))
    }

    pub(crate) async fn send_json(&self, payload: Value) -> Result<(), VoiceError> {
        let mut writer = self.write.lock().await;
        ws::send_json(&mut writer, payload).await
    }

    pub(crate) async fn send_binary(&self, payload: Vec<u8>) -> Result<(), VoiceError> {
        let mut writer = self.write.lock().await;
        ws::send_binary(&mut writer, payload).await
    }

    pub async fn send_dave_transition_ready(&self, transition_id: u16) -> Result<(), VoiceError> {
        self.send_json(protocol::dave_transition_ready_payload(transition_id))
            .await
    }

    pub async fn send_dave_mls_key_package(&self, key_package: &[u8]) -> Result<(), VoiceError> {
        self.send_binary(protocol::dave_mls_key_package_payload(key_package))
            .await
    }
}

fn heartbeat_timestamp_millis() -> Result<u64, VoiceError> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| VoiceError::InvalidState("system clock before unix epoch"))?;
    u64::try_from(duration.as_millis())
        .map_err(|_| VoiceError::InvalidState("heartbeat timestamp overflow"))
}
