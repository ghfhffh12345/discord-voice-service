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
    pub dave_protocol_version: Option<u16>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientsConnect {
    pub user_ids: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DaveExecuteTransition {
    pub transition_id: u16,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DavePrepareEpoch {
    pub transition_id: u16,
    pub epoch: String,
    pub protocol_version: u16,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DaveMlsExternalSenderPackage {
    pub external_sender: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DaveMlsWelcome {
    pub transition_id: u16,
    pub welcome: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VoiceGatewayEvent {
    Hello(Hello),
    Ready(Ready),
    SessionDescription(SessionDescription),
    ClientsConnect(ClientsConnect),
    DaveExecuteTransition(DaveExecuteTransition),
    DavePrepareEpoch(DavePrepareEpoch),
    DaveMlsExternalSenderPackage(DaveMlsExternalSenderPackage),
    DaveMlsWelcome(DaveMlsWelcome),
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
            secret_key: parse_byte_array(
                data.get("secret_key"),
                "voice session description secret key missing",
                "voice session description secret key invalid",
            )?,
            dave_protocol_version: parse_optional_u16(
                data.get("dave_protocol_version"),
                "voice session description dave protocol version invalid",
            )?,
        }),
        11 => VoiceGatewayEvent::ClientsConnect(ClientsConnect {
            user_ids: data
                .get("user_ids")
                .and_then(Value::as_array)
                .ok_or(AppError::InvalidState(
                    "voice clients connect user ids missing",
                ))?
                .iter()
                .map(|user_id| {
                    user_id
                        .as_str()
                        .map(str::to_owned)
                        .ok_or(AppError::InvalidState(
                            "voice clients connect user id invalid",
                        ))
                })
                .collect::<Result<Vec<_>, _>>()?,
        }),
        24 => VoiceGatewayEvent::DavePrepareEpoch(DavePrepareEpoch {
            transition_id: data
                .get("transition_id")
                .and_then(Value::as_u64)
                .and_then(|value| u16::try_from(value).ok())
                .ok_or(AppError::InvalidState(
                    "voice dave prepare epoch transition id invalid",
                ))?,
            epoch: data
                .get("epoch")
                .and_then(Value::as_str)
                .ok_or(AppError::InvalidState("voice dave prepare epoch missing"))?
                .to_owned(),
            protocol_version: data
                .get("protocol_version")
                .and_then(Value::as_u64)
                .and_then(|value| u16::try_from(value).ok())
                .ok_or(AppError::InvalidState(
                    "voice dave prepare epoch protocol version invalid",
                ))?,
        }),
        22 => VoiceGatewayEvent::DaveExecuteTransition(DaveExecuteTransition {
            transition_id: data
                .get("transition_id")
                .and_then(Value::as_u64)
                .and_then(|value| u16::try_from(value).ok())
                .ok_or(AppError::InvalidState(
                    "voice dave execute transition id invalid",
                ))?,
        }),
        9 => VoiceGatewayEvent::Resumed,
        _ => return Err(AppError::InvalidState("voice gateway op unsupported")),
    };

    Ok(VoiceGatewayPayload::new(event, seq))
}

pub fn identify_payload(voice: &VoiceContext, max_dave_protocol_version: Option<u16>) -> Value {
    let mut payload = json!({
        "op": 0,
        "d": {
            "server_id": voice.guild_id,
            "user_id": voice.user_id,
            "session_id": voice.session_id,
            "token": voice.token,
        }
    });
    if let Some(max_dave_protocol_version) = max_dave_protocol_version {
        payload["d"]["max_dave_protocol_version"] = json!(max_dave_protocol_version);
    }
    payload
}

pub fn dave_transition_ready_payload(transition_id: u16) -> Value {
    json!({
        "op": 23,
        "d": {
            "transition_id": transition_id,
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

pub fn parse_gateway_binary_message(bytes: &[u8]) -> Result<VoiceGatewayPayload, AppError> {
    if bytes.len() < 3 {
        return Err(AppError::InvalidState(
            "voice gateway binary payload too short",
        ));
    }

    let seq = u16::from_be_bytes([bytes[0], bytes[1]]) as u64;
    let event = match bytes[2] {
        25 => VoiceGatewayEvent::DaveMlsExternalSenderPackage(DaveMlsExternalSenderPackage {
            external_sender: bytes[3..].to_vec(),
        }),
        30 => {
            if bytes.len() < 5 {
                return Err(AppError::InvalidState(
                    "voice dave welcome payload too short",
                ));
            }
            VoiceGatewayEvent::DaveMlsWelcome(DaveMlsWelcome {
                transition_id: u16::from_be_bytes([bytes[3], bytes[4]]),
                welcome: bytes[5..].to_vec(),
            })
        }
        _ => {
            return Err(AppError::InvalidState(
                "voice gateway binary opcode unsupported",
            ));
        }
    };

    Ok(VoiceGatewayPayload::new(event, Some(seq)))
}

pub fn dave_mls_key_package_payload(key_package: &[u8]) -> Vec<u8> {
    let mut payload = Vec::with_capacity(1 + key_package.len());
    payload.push(26);
    payload.extend_from_slice(key_package);
    payload
}

fn seq_ack_i64(seq_ack: Option<u64>) -> i64 {
    seq_ack
        .and_then(|seq| i64::try_from(seq).ok())
        .unwrap_or(-1)
}

fn parse_optional_u16(
    value: Option<&Value>,
    invalid: &'static str,
) -> Result<Option<u16>, AppError> {
    value
        .filter(|value| !value.is_null())
        .map(|value| {
            value
                .as_u64()
                .and_then(|value| u16::try_from(value).ok())
                .ok_or(AppError::InvalidState(invalid))
        })
        .transpose()
}

fn parse_byte_array(
    value: Option<&Value>,
    missing: &'static str,
    invalid: &'static str,
) -> Result<Vec<u8>, AppError> {
    value
        .and_then(Value::as_array)
        .ok_or(AppError::InvalidState(missing))?
        .iter()
        .map(|octet| {
            octet
                .as_u64()
                .and_then(|value| u8::try_from(value).ok())
                .ok_or(AppError::InvalidState(invalid))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::{
        VoiceGatewayEvent, dave_mls_key_package_payload, dave_transition_ready_payload,
        identify_payload, parse_gateway_binary_message, parse_gateway_message,
    };
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
        let payload = identify_payload(
            &VoiceContext {
                guild_id: "guild-1".into(),
                channel_id: "channel-1".into(),
                user_id: "user-1".into(),
                session_id: "session-1".into(),
                endpoint: "voice.example.discord.gg".into(),
                token: "token-1".into(),
            },
            Some(1),
        );

        assert_eq!(payload["d"]["server_id"], "guild-1");
        assert_eq!(payload["d"]["user_id"], "user-1");
        assert_eq!(payload["d"]["session_id"], "session-1");
        assert_eq!(payload["d"]["token"], "token-1");
        assert_eq!(payload["d"]["max_dave_protocol_version"], 1);
    }

    #[test]
    fn parse_gateway_binary_message_supports_dave_external_sender_and_welcome() {
        let external_sender = parse_gateway_binary_message(&[0, 7, 25, 1, 2, 3]).unwrap();
        assert_eq!(external_sender.seq(), Some(7));
        assert!(matches!(
            external_sender.event(),
            VoiceGatewayEvent::DaveMlsExternalSenderPackage(_)
        ));

        let welcome = parse_gateway_binary_message(&[0, 8, 30, 0, 9, 4, 5, 6]).unwrap();
        assert_eq!(welcome.seq(), Some(8));
        match welcome.into_event() {
            VoiceGatewayEvent::DaveMlsWelcome(welcome) => {
                assert_eq!(welcome.transition_id, 9);
                assert_eq!(welcome.welcome, vec![4, 5, 6]);
            }
            other => panic!("expected dave welcome event, got {other:?}"),
        }
    }

    #[test]
    fn dave_payload_builders_encode_expected_shapes() {
        let ready = dave_transition_ready_payload(11);
        assert_eq!(ready["op"], 23);
        assert_eq!(ready["d"]["transition_id"], 11);

        assert_eq!(dave_mls_key_package_payload(&[1, 2, 3]), vec![26, 1, 2, 3]);
    }

    #[test]
    fn parse_gateway_message_supports_dave_prepare_epoch() {
        let payload = parse_gateway_message(
            r#"{
                "op": 24,
                "seq": 9,
                "d": {
                    "transition_id": 11,
                    "epoch": "1",
                    "protocol_version": 1
                }
            }"#,
        )
        .unwrap();

        assert_eq!(payload.seq(), Some(9));
        match payload.into_event() {
            VoiceGatewayEvent::DavePrepareEpoch(epoch) => {
                assert_eq!(epoch.transition_id, 11);
                assert_eq!(epoch.epoch, "1");
                assert_eq!(epoch.protocol_version, 1);
            }
            other => panic!("expected dave prepare epoch, got {other:?}"),
        }
    }
}
