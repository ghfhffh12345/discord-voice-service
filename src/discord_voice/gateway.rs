use futures::StreamExt;
use serde_json::Value;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio_tungstenite::tungstenite::Message;

use crate::discord_voice::protocol::{self, VoiceGatewayPayload};
use crate::discord_voice::resume::GatewayEvent;
use crate::discord_voice::udp::DiscoveredUdpAddress;
use crate::discord_voice::ws::{self, VoiceWebSocket};
use crate::error::AppError;
use crate::session::supervisor::VoiceContext;

pub struct VoiceGatewayClient {
    ws: VoiceWebSocket,
    seq_ack: Option<u64>,
}

impl VoiceGatewayClient {
    pub async fn connect(url: &str) -> Result<Self, AppError> {
        Ok(Self {
            ws: ws::connect(url).await?,
            seq_ack: None,
        })
    }

    pub fn record_seq_ack(&mut self, seq: u64) {
        self.seq_ack = Some(seq);
    }

    pub fn apply_gateway_event(&mut self, event: &GatewayEvent) {
        if let Some(seq) = event.seq() {
            self.seq_ack = Some(seq);
        }
    }

    pub async fn send_identify(&mut self, voice: &VoiceContext) -> Result<(), AppError> {
        self.send_json(protocol::identify_payload(voice)).await
    }

    pub async fn send_select_protocol(
        &mut self,
        address: &DiscoveredUdpAddress,
        mode: &str,
    ) -> Result<(), AppError> {
        self.send_json(protocol::select_protocol_payload(address, mode))
            .await
    }

    pub async fn send_heartbeat(&mut self) -> Result<(), AppError> {
        self.send_json(protocol::heartbeat_payload(
            heartbeat_timestamp_millis()?,
            self.seq_ack,
        ))
        .await
    }

    pub async fn send_resume(
        &mut self,
        server_id: &str,
        session_id: &str,
        token: &str,
    ) -> Result<(), AppError> {
        self.send_json(protocol::resume_payload(
            server_id,
            session_id,
            token,
            self.seq_ack,
        ))
        .await
    }

    pub async fn receive_event(&mut self) -> Result<VoiceGatewayPayload, AppError> {
        while let Some(message) = self.ws.next().await {
            match message? {
                Message::Text(text) => {
                    let payload = protocol::parse_gateway_message(text.as_ref())?;
                    if let Some(seq) = payload.seq() {
                        self.seq_ack = Some(seq);
                    }
                    return Ok(payload);
                }
                Message::Close(_) => {
                    return Err(AppError::InvalidState(
                        "voice gateway closed during receive",
                    ));
                }
                _ => {}
            }
        }

        Err(AppError::InvalidState(
            "voice gateway closed during receive",
        ))
    }

    pub(crate) async fn send_json(&mut self, payload: Value) -> Result<(), AppError> {
        ws::send_json(&mut self.ws, payload).await
    }
}

fn heartbeat_timestamp_millis() -> Result<u64, AppError> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| AppError::InvalidState("system clock before unix epoch"))?;
    u64::try_from(duration.as_millis())
        .map_err(|_| AppError::InvalidState("heartbeat timestamp overflow"))
}
