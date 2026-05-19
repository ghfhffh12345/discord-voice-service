use serde_json::{Value, json};

use crate::discord_voice::udp::DiscoveredUdpAddress;
use crate::error::AppError;
use crate::session::supervisor::VoiceContext;

pub const SUPPORTED_ENCRYPTION_MODE: &str = "xsalsa20_poly1305";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Hello {
    pub heartbeat_interval_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Ready {
    pub ssrc: u32,
    pub ip: String,
    pub port: u16,
    pub modes: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionDescription {
    pub mode: String,
    pub secret_key: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VoiceGatewayEvent {
    Hello(Hello),
    Ready(Ready),
    SessionDescription(SessionDescription),
    Resumed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VoiceGatewayPayload {
    event: VoiceGatewayEvent,
    seq: Option<u64>,
}

impl VoiceGatewayPayload {
    pub fn new(event: VoiceGatewayEvent, seq: Option<u64>) -> Self {
        Self { event, seq }
    }

    pub fn event(&self) -> &VoiceGatewayEvent {
        &self.event
    }

    pub fn into_event(self) -> VoiceGatewayEvent {
        self.event
    }

    pub fn seq(&self) -> Option<u64> {
        self.seq
    }
}

pub fn parse_gateway_message(text: &str) -> Result<VoiceGatewayPayload, AppError> {
    let payload: Value = serde_json::from_str(text)
        .map_err(|_| AppError::InvalidState("voice gateway payload invalid json"))?;
    let op = payload
        .get("op")
        .and_then(Value::as_u64)
        .ok_or(AppError::InvalidState("voice gateway op missing"))?;
    let seq = payload.get("s").and_then(Value::as_u64);
    let data = payload.get("d").cloned().unwrap_or(Value::Null);

    let event = match op {
        8 => VoiceGatewayEvent::Hello(Hello {
            heartbeat_interval_ms: data
                .get("heartbeat_interval")
                .and_then(Value::as_u64)
                .ok_or(AppError::InvalidState(
                    "voice hello heartbeat interval missing",
                ))?,
        }),
        2 => VoiceGatewayEvent::Ready(Ready {
            ssrc: data
                .get("ssrc")
                .and_then(Value::as_u64)
                .ok_or(AppError::InvalidState("voice ready ssrc missing"))?
                .try_into()
                .map_err(|_| AppError::InvalidState("voice ready ssrc invalid"))?,
            ip: data
                .get("ip")
                .and_then(Value::as_str)
                .ok_or(AppError::InvalidState("voice ready ip missing"))?
                .to_owned(),
            port: data
                .get("port")
                .and_then(Value::as_u64)
                .ok_or(AppError::InvalidState("voice ready port missing"))?
                .try_into()
                .map_err(|_| AppError::InvalidState("voice ready port invalid"))?,
            modes: data
                .get("modes")
                .and_then(Value::as_array)
                .ok_or(AppError::InvalidState("voice ready modes missing"))?
                .iter()
                .map(|mode| {
                    mode.as_str()
                        .map(str::to_owned)
                        .ok_or(AppError::InvalidState("voice ready mode invalid"))
                })
                .collect::<Result<Vec<_>, _>>()?,
        }),
        4 => VoiceGatewayEvent::SessionDescription(SessionDescription {
            mode: data
                .get("mode")
                .and_then(Value::as_str)
                .ok_or(AppError::InvalidState(
                    "voice session description mode missing",
                ))?
                .to_owned(),
            secret_key: data
                .get("secret_key")
                .and_then(Value::as_array)
                .ok_or(AppError::InvalidState(
                    "voice session description secret key missing",
                ))?
                .iter()
                .map(|octet| {
                    octet
                        .as_u64()
                        .and_then(|value| u8::try_from(value).ok())
                        .ok_or(AppError::InvalidState(
                            "voice session description secret key invalid",
                        ))
                })
                .collect::<Result<Vec<_>, _>>()?,
        }),
        9 => VoiceGatewayEvent::Resumed,
        _ => return Err(AppError::InvalidState("voice gateway op unsupported")),
    };

    Ok(VoiceGatewayPayload::new(event, seq))
}

pub fn identify_payload(voice: &VoiceContext) -> Value {
    json!({
        "op": 0,
        "d": {
            "server_id": voice.guild_id,
            "session_id": voice.session_id,
            "token": voice.token,
        }
    })
}

pub fn select_protocol_payload(address: &DiscoveredUdpAddress, mode: &str) -> Value {
    json!({
        "op": 1,
        "d": {
            "protocol": "udp",
            "data": {
                "address": address.ip,
                "port": address.port,
                "mode": mode,
            }
        }
    })
}

pub fn heartbeat_payload(timestamp_millis: u64, seq_ack: Option<u64>) -> Value {
    json!({
        "op": 3,
        "d": {
            "t": timestamp_millis,
            "seq_ack": seq_ack,
        }
    })
}

pub fn resume_payload(
    server_id: &str,
    session_id: &str,
    token: &str,
    seq_ack: Option<u64>,
) -> Value {
    json!({
        "op": 7,
        "d": {
            "server_id": server_id,
            "session_id": session_id,
            "token": token,
            "seq_ack": seq_ack,
        }
    })
}

pub fn choose_encryption_mode(ready: &Ready) -> Result<&str, AppError> {
    ready
        .modes
        .iter()
        .find(|mode| mode.as_str() == SUPPORTED_ENCRYPTION_MODE)
        .map(|mode| mode.as_str())
        .ok_or(AppError::UnsupportedEncryptionMode)
}
