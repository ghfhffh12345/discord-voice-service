use serde_json::{Value, json};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::discord_voice::resume::GatewayEvent;
use crate::discord_voice::ws::{self, VoiceWebSocket};
use crate::error::AppError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VoiceContext {
    pub guild_id: String,
    pub channel_id: String,
    pub session_id: String,
    pub endpoint: String,
    pub token: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionDescription {
    pub mode: String,
    pub secret_key: Vec<u8>,
}

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

    pub async fn send_heartbeat(&mut self) -> Result<(), AppError> {
        self.send_json(json!({
            "op": 3,
            "d": {
                "t": heartbeat_timestamp_millis()?,
                "seq_ack": self.seq_ack,
            }
        }))
        .await
    }

    pub async fn send_resume(
        &mut self,
        server_id: &str,
        session_id: &str,
        token: &str,
    ) -> Result<(), AppError> {
        self.send_json(json!({
            "op": 7,
            "d": {
                "server_id": server_id,
                "session_id": session_id,
                "token": token,
                "seq_ack": self.seq_ack,
            }
        }))
        .await
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
