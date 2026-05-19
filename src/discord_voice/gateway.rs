use std::sync::Arc;

use futures::StreamExt;
use serde_json::Value;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::Mutex;
use tokio_tungstenite::tungstenite::Message;

use crate::discord_voice::protocol::{self, VoiceGatewayPayload};
use crate::discord_voice::resume::GatewayEvent;
use crate::discord_voice::udp::DiscoveredUdpAddress;
use crate::discord_voice::ws::{self, VoiceWebSocket};
use crate::error::AppError;
use crate::session::supervisor::VoiceContext;

struct VoiceGatewayInner {
    ws: VoiceWebSocket,
    seq_ack: Option<u64>,
}

#[derive(Clone)]
pub struct VoiceGatewayClient {
    inner: Arc<Mutex<VoiceGatewayInner>>,
}

impl VoiceGatewayClient {
    pub async fn connect(url: &str) -> Result<Self, AppError> {
        Ok(Self {
            inner: Arc::new(Mutex::new(VoiceGatewayInner {
                ws: ws::connect(url).await?,
                seq_ack: None,
            })),
        })
    }

    pub async fn record_seq_ack(&self, seq: u64) {
        self.inner.lock().await.seq_ack = Some(seq);
    }

    pub async fn apply_gateway_event(&self, event: &GatewayEvent) {
        if let Some(seq) = event.seq() {
            self.record_seq_ack(seq).await;
        }
    }

    pub async fn send_identify(&self, voice: &VoiceContext) -> Result<(), AppError> {
        self.send_json(protocol::identify_payload(voice)).await
    }

    pub async fn send_select_protocol(
        &self,
        address: &DiscoveredUdpAddress,
        mode: &str,
    ) -> Result<(), AppError> {
        self.send_json(protocol::select_protocol_payload(address, mode))
            .await
    }

    pub async fn send_heartbeat(&self) -> Result<(), AppError> {
        let seq_ack = self.inner.lock().await.seq_ack;
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
    ) -> Result<(), AppError> {
        let seq_ack = self.inner.lock().await.seq_ack;
        self.send_json(protocol::resume_payload(
            server_id, session_id, token, seq_ack,
        ))
        .await
    }

    pub async fn receive_event(&self) -> Result<VoiceGatewayPayload, AppError> {
        let mut inner = self.inner.lock().await;
        while let Some(message) = inner.ws.next().await {
            match message? {
                Message::Text(text) => {
                    let payload = protocol::parse_gateway_message(text.as_ref())?;
                    if let Some(seq) = payload.seq() {
                        inner.seq_ack = Some(seq);
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

    pub(crate) async fn send_json(&self, payload: Value) -> Result<(), AppError> {
        let mut inner = self.inner.lock().await;
        ws::send_json(&mut inner.ws, payload).await
    }
}

fn heartbeat_timestamp_millis() -> Result<u64, AppError> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| AppError::InvalidState("system clock before unix epoch"))?;
    u64::try_from(duration.as_millis())
        .map_err(|_| AppError::InvalidState("heartbeat timestamp overflow"))
}
