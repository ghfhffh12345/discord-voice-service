use serde_json::{Value, json};

use crate::discord_voice::crypto;
use crate::discord_voice::udp::DiscoveredUdpAddress;
use crate::error::AppError;
use crate::session::supervisor::VoiceContext;

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
    let seq = payload
        .get("seq")
        .or_else(|| payload.get("s"))
        .and_then(Value::as_u64);
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
            "user_id": voice.user_id,
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
    let seq_ack = seq_ack_i64(seq_ack);
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
    let seq_ack = seq_ack_i64(seq_ack);
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

pub fn choose_encryption_mode(ready: &Ready) -> Result<&'static str, AppError> {
    crypto::pick_mode(&ready.modes).ok_or(AppError::UnsupportedEncryptionMode)
}

fn seq_ack_i64(seq_ack: Option<u64>) -> i64 {
    seq_ack
        .and_then(|seq| i64::try_from(seq).ok())
        .unwrap_or(-1)
}

#[cfg(test)]
mod tests {
    use super::{VoiceGatewayEvent, identify_payload, parse_gateway_message};
    use crate::session::supervisor::VoiceContext;

    #[test]
    fn parse_gateway_message_uses_real_seq_field_name() {
        let payload = parse_gateway_message(
            r#"{
                "op": 2,
                "seq": 42,
                "d": {
                    "ssrc": 7,
                    "ip": "127.0.0.1",
                    "port": 5000,
                    "modes": ["aead_xchacha20_poly1305_rtpsize"]
                }
            }"#,
        )
        .unwrap();

        assert_eq!(payload.seq(), Some(42));
        assert!(matches!(payload.event(), VoiceGatewayEvent::Ready(_)));
    }

    #[test]
    fn identify_payload_includes_required_user_id() {
        let payload = identify_payload(&VoiceContext {
            guild_id: "guild-1".into(),
            channel_id: "channel-1".into(),
            user_id: "user-1".into(),
            session_id: "session-1".into(),
            endpoint: "voice.example.discord.gg".into(),
            token: "token-1".into(),
        });

        assert_eq!(payload["d"]["server_id"], "guild-1");
        assert_eq!(payload["d"]["user_id"], "user-1");
        assert_eq!(payload["d"]["session_id"], "session-1");
        assert_eq!(payload["d"]["token"], "token-1");
    }
}
