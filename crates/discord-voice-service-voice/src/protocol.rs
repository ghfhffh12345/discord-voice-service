use openmls::prelude::{MlsMessageIn, tls_codec::DeserializeBytes};
use serde_json::{Value, json};

use crate::crypto;
use crate::dave::{DaveMlsProposalsOperation, unpack_commit_welcome};
use crate::error::VoiceError;
use crate::session::VoiceContext;
use crate::udp::DiscoveredUdpAddress;

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
pub struct Speaking {
    pub speaking: u64,
    pub delay: u64,
    pub ssrc: u32,
    pub user_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeartbeatAck {
    pub nonce: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientDisconnect {
    pub user_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Video {
    pub user_id: Option<String>,
    pub audio_ssrc: Option<u32>,
    pub video_ssrc: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientFlags {
    pub user_id: String,
    pub flags: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientPlatform {
    pub user_id: String,
    pub platform: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DavePrepareTransition {
    pub transition_id: u16,
    pub protocol_version: u16,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DaveExecuteTransition {
    pub transition_id: u16,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DavePrepareEpoch {
    pub transition_id: Option<u16>,
    pub epoch: String,
    pub protocol_version: u16,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DaveMlsExternalSenderPackage {
    pub external_sender: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DaveMlsProposals {
    pub(crate) operation: DaveMlsProposalsOperation,
    pub proposals: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DaveMlsPrepareCommitTransition {
    pub transition_id: u16,
    pub commit: Vec<u8>,
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
    Speaking(Speaking),
    HeartbeatAck(HeartbeatAck),
    ClientsConnect(ClientsConnect),
    Video(Video),
    ClientDisconnect(ClientDisconnect),
    MediaSinkWants,
    ClientFlags(ClientFlags),
    ClientPlatform(ClientPlatform),
    DavePrepareTransition(DavePrepareTransition),
    DaveExecuteTransition(DaveExecuteTransition),
    DavePrepareEpoch(DavePrepareEpoch),
    DaveMlsExternalSenderPackage(DaveMlsExternalSenderPackage),
    DaveMlsProposals(DaveMlsProposals),
    DaveMlsPrepareCommitTransition(DaveMlsPrepareCommitTransition),
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

pub fn parse_gateway_message(text: &str) -> Result<VoiceGatewayPayload, VoiceError> {
    let payload: Value = serde_json::from_str(text)
        .map_err(|_| VoiceError::InvalidState("voice gateway payload invalid json"))?;
    let op = payload
        .get("op")
        .and_then(Value::as_u64)
        .ok_or(VoiceError::InvalidState("voice gateway op missing"))?;
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
                .ok_or(VoiceError::InvalidState(
                    "voice hello heartbeat interval missing",
                ))?,
        }),
        2 => VoiceGatewayEvent::Ready(Ready {
            ssrc: data
                .get("ssrc")
                .and_then(Value::as_u64)
                .ok_or(VoiceError::InvalidState("voice ready ssrc missing"))?
                .try_into()
                .map_err(|_| VoiceError::InvalidState("voice ready ssrc invalid"))?,
            ip: data
                .get("ip")
                .and_then(Value::as_str)
                .ok_or(VoiceError::InvalidState("voice ready ip missing"))?
                .to_owned(),
            port: data
                .get("port")
                .and_then(Value::as_u64)
                .ok_or(VoiceError::InvalidState("voice ready port missing"))?
                .try_into()
                .map_err(|_| VoiceError::InvalidState("voice ready port invalid"))?,
            modes: data
                .get("modes")
                .and_then(Value::as_array)
                .ok_or(VoiceError::InvalidState("voice ready modes missing"))?
                .iter()
                .map(|mode| {
                    mode.as_str()
                        .map(str::to_owned)
                        .ok_or(VoiceError::InvalidState("voice ready mode invalid"))
                })
                .collect::<Result<Vec<_>, _>>()?,
        }),
        4 => VoiceGatewayEvent::SessionDescription(SessionDescription {
            mode: data
                .get("mode")
                .and_then(Value::as_str)
                .ok_or(VoiceError::InvalidState(
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
        5 => VoiceGatewayEvent::Speaking(Speaking {
            speaking: data
                .get("speaking")
                .and_then(Value::as_u64)
                .ok_or(VoiceError::InvalidState("voice speaking flags missing"))?,
            delay: data.get("delay").and_then(Value::as_u64).unwrap_or(0),
            ssrc: data
                .get("ssrc")
                .and_then(Value::as_u64)
                .ok_or(VoiceError::InvalidState("voice speaking ssrc missing"))?
                .try_into()
                .map_err(|_| VoiceError::InvalidState("voice speaking ssrc invalid"))?,
            user_id: data
                .get("user_id")
                .and_then(Value::as_str)
                .map(str::to_owned),
        }),
        6 => VoiceGatewayEvent::HeartbeatAck(HeartbeatAck {
            nonce: parse_heartbeat_ack_nonce(&data)?,
        }),
        11 => VoiceGatewayEvent::ClientsConnect(ClientsConnect {
            user_ids: data
                .get("user_ids")
                .and_then(Value::as_array)
                .ok_or(VoiceError::InvalidState(
                    "voice clients connect user ids missing",
                ))?
                .iter()
                .map(|user_id| {
                    user_id
                        .as_str()
                        .map(str::to_owned)
                        .ok_or(VoiceError::InvalidState(
                            "voice clients connect user id invalid",
                        ))
                })
                .collect::<Result<Vec<_>, _>>()?,
        }),
        12 => {
            require_object(&data, "voice video payload invalid")?;
            VoiceGatewayEvent::Video(Video {
                user_id: data
                    .get("user_id")
                    .and_then(Value::as_str)
                    .map(str::to_owned),
                audio_ssrc: parse_optional_u32(
                    data.get("audio_ssrc"),
                    "voice video audio ssrc invalid",
                )?,
                video_ssrc: parse_optional_u32(data.get("video_ssrc"), "voice video ssrc invalid")?,
            })
        }
        13 => VoiceGatewayEvent::ClientDisconnect(ClientDisconnect {
            user_id: data
                .get("user_id")
                .and_then(Value::as_str)
                .ok_or(VoiceError::InvalidState(
                    "voice client disconnect user id missing",
                ))?
                .to_owned(),
        }),
        15 => {
            require_object(&data, "voice media sink wants payload invalid")?;
            VoiceGatewayEvent::MediaSinkWants
        }
        18 => VoiceGatewayEvent::ClientFlags(ClientFlags {
            user_id: data
                .get("user_id")
                .and_then(Value::as_str)
                .ok_or(VoiceError::InvalidState(
                    "voice client flags user id missing",
                ))?
                .to_owned(),
            flags: parse_optional_u64(data.get("flags"), "voice client flags invalid")?,
        }),
        20 => VoiceGatewayEvent::ClientPlatform(ClientPlatform {
            user_id: data
                .get("user_id")
                .and_then(Value::as_str)
                .ok_or(VoiceError::InvalidState(
                    "voice client platform user id missing",
                ))?
                .to_owned(),
            platform: parse_optional_u64(data.get("platform"), "voice client platform invalid")?,
        }),
        21 => VoiceGatewayEvent::DavePrepareTransition(DavePrepareTransition {
            transition_id: data
                .get("transition_id")
                .and_then(Value::as_u64)
                .and_then(|value| u16::try_from(value).ok())
                .ok_or(VoiceError::InvalidState(
                    "voice dave prepare transition id invalid",
                ))?,
            protocol_version: data
                .get("protocol_version")
                .and_then(Value::as_u64)
                .and_then(|value| u16::try_from(value).ok())
                .ok_or(VoiceError::InvalidState(
                    "voice dave prepare transition protocol version invalid",
                ))?,
        }),
        24 => VoiceGatewayEvent::DavePrepareEpoch(DavePrepareEpoch {
            transition_id: parse_optional_u16(
                data.get("transition_id"),
                "voice dave prepare epoch transition id invalid",
            )?,
            epoch: data
                .get("epoch")
                .and_then(|value| match value {
                    Value::String(epoch) => Some(epoch.clone()),
                    Value::Number(epoch) => epoch.as_u64().map(|epoch| epoch.to_string()),
                    _ => None,
                })
                .ok_or(VoiceError::InvalidState("voice dave prepare epoch missing"))?
                .to_owned(),
            protocol_version: data
                .get("protocol_version")
                .and_then(Value::as_u64)
                .and_then(|value| u16::try_from(value).ok())
                .ok_or(VoiceError::InvalidState(
                    "voice dave prepare epoch protocol version invalid",
                ))?,
        }),
        22 => VoiceGatewayEvent::DaveExecuteTransition(DaveExecuteTransition {
            transition_id: data
                .get("transition_id")
                .and_then(Value::as_u64)
                .and_then(|value| u16::try_from(value).ok())
                .ok_or(VoiceError::InvalidState(
                    "voice dave execute transition id invalid",
                ))?,
        }),
        9 => VoiceGatewayEvent::Resumed,
        _ => return Err(unsupported_text_gateway_op_error(op)),
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

pub fn dave_mls_invalid_commit_welcome_payload(transition_id: u16) -> Value {
    json!({
        "op": 31,
        "d": {
            "transition_id": transition_id,
        }
    })
}

pub fn dave_mls_commit_welcome_payload(commit_welcome: &[u8]) -> Vec<u8> {
    let (commit, welcome) =
        unpack_commit_welcome(commit_welcome).expect("internal commit/welcome framing is valid");
    let mut payload = Vec::with_capacity(1 + commit.len() + welcome.len());
    payload.push(28);
    payload.extend_from_slice(&commit);
    payload.extend_from_slice(&welcome);
    payload
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

pub fn choose_encryption_mode(ready: &Ready) -> Result<&'static str, VoiceError> {
    crypto::pick_mode(&ready.modes).ok_or(VoiceError::UnsupportedEncryptionMode)
}

pub fn parse_gateway_binary_message(bytes: &[u8]) -> Result<VoiceGatewayPayload, VoiceError> {
    if bytes.len() < 3 {
        return Err(VoiceError::InvalidState(
            "voice gateway binary payload too short",
        ));
    }

    let seq = u16::from_be_bytes([bytes[0], bytes[1]]) as u64;
    let event = match bytes[2] {
        25 => VoiceGatewayEvent::DaveMlsExternalSenderPackage(DaveMlsExternalSenderPackage {
            external_sender: bytes[3..].to_vec(),
        }),
        27 => {
            if bytes.len() < 4 {
                return Err(VoiceError::InvalidState(
                    "voice dave proposals payload too short",
                ));
            }
            VoiceGatewayEvent::DaveMlsProposals(DaveMlsProposals {
                operation: parse_dave_proposals_operation(bytes[3])?,
                proposals: bytes[4..].to_vec(),
            })
        }
        29 => {
            if bytes.len() < 5 {
                return Err(VoiceError::InvalidState(
                    "voice dave prepare commit transition payload too short",
                ));
            }
            VoiceGatewayEvent::DaveMlsPrepareCommitTransition(DaveMlsPrepareCommitTransition {
                transition_id: u16::from_be_bytes([bytes[3], bytes[4]]),
                commit: bytes[5..].to_vec(),
            })
        }
        30 => {
            if bytes.len() < 5 {
                return Err(VoiceError::InvalidState(
                    "voice dave welcome payload too short",
                ));
            }
            VoiceGatewayEvent::DaveMlsWelcome(DaveMlsWelcome {
                transition_id: u16::from_be_bytes([bytes[3], bytes[4]]),
                welcome: bytes[5..].to_vec(),
            })
        }
        _ => {
            return Err(unsupported_binary_gateway_op_error(bytes[2]));
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

#[doc(hidden)]
pub fn split_dave_mls_commit_welcome_payload(
    commit_welcome: &[u8],
) -> Result<(Vec<u8>, Option<Vec<u8>>), VoiceError> {
    let (_commit_message, welcome) = MlsMessageIn::tls_deserialize_bytes(commit_welcome)
        .map_err(|_| VoiceError::InvalidState("voice dave commit welcome commit invalid"))?;
    let commit_len = commit_welcome.len() - welcome.len();
    let commit = commit_welcome[..commit_len].to_vec();
    let welcome = (!welcome.is_empty()).then(|| welcome.to_vec());
    Ok((commit, welcome))
}

fn seq_ack_i64(seq_ack: Option<u64>) -> i64 {
    seq_ack
        .and_then(|seq| i64::try_from(seq).ok())
        .unwrap_or(-1)
}

fn parse_dave_proposals_operation(value: u8) -> Result<DaveMlsProposalsOperation, VoiceError> {
    match value {
        0 => Ok(DaveMlsProposalsOperation::Append),
        1 => Ok(DaveMlsProposalsOperation::Revoke),
        _ => Err(VoiceError::InvalidState(
            "voice dave proposals operation invalid",
        )),
    }
}

fn unsupported_text_gateway_op_error(op: u64) -> VoiceError {
    VoiceError::UnsupportedGatewayOp(op)
}

fn unsupported_binary_gateway_op_error(op: u8) -> VoiceError {
    VoiceError::UnsupportedBinaryGatewayOp(op)
}

fn parse_heartbeat_ack_nonce(value: &Value) -> Result<Option<u64>, VoiceError> {
    match value {
        Value::Number(number) => number
            .as_u64()
            .map(Some)
            .ok_or(VoiceError::InvalidState("voice heartbeat ack invalid")),
        Value::Object(_) => value
            .get("t")
            .and_then(Value::as_u64)
            .map(Some)
            .ok_or(VoiceError::InvalidState("voice heartbeat ack missing")),
        _ => Err(VoiceError::InvalidState("voice heartbeat ack invalid")),
    }
}

fn require_object<'a>(value: &'a Value, invalid: &'static str) -> Result<&'a Value, VoiceError> {
    value.as_object().ok_or(VoiceError::InvalidState(invalid))?;
    Ok(value)
}

fn parse_optional_u16(
    value: Option<&Value>,
    invalid: &'static str,
) -> Result<Option<u16>, VoiceError> {
    value
        .filter(|value| !value.is_null())
        .map(|value| {
            value
                .as_u64()
                .and_then(|value| u16::try_from(value).ok())
                .ok_or(VoiceError::InvalidState(invalid))
        })
        .transpose()
}

fn parse_optional_u32(
    value: Option<&Value>,
    invalid: &'static str,
) -> Result<Option<u32>, VoiceError> {
    value
        .filter(|value| !value.is_null())
        .map(|value| {
            value
                .as_u64()
                .and_then(|value| u32::try_from(value).ok())
                .ok_or(VoiceError::InvalidState(invalid))
        })
        .transpose()
}

fn parse_optional_u64(
    value: Option<&Value>,
    invalid: &'static str,
) -> Result<Option<u64>, VoiceError> {
    value
        .filter(|value| !value.is_null())
        .map(|value| value.as_u64().ok_or(VoiceError::InvalidState(invalid)))
        .transpose()
}

fn parse_byte_array(
    value: Option<&Value>,
    missing: &'static str,
    invalid: &'static str,
) -> Result<Vec<u8>, VoiceError> {
    value
        .and_then(Value::as_array)
        .ok_or(VoiceError::InvalidState(missing))?
        .iter()
        .map(|octet| {
            octet
                .as_u64()
                .and_then(|value| u8::try_from(value).ok())
                .ok_or(VoiceError::InvalidState(invalid))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::{
        VoiceGatewayEvent, dave_mls_commit_welcome_payload, dave_mls_key_package_payload,
        dave_transition_ready_payload, identify_payload, parse_gateway_binary_message,
        parse_gateway_message,
    };
    use crate::dave::DaveMlsProposalsOperation;
    use crate::session::VoiceContext;

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
    fn parse_gateway_binary_message_supports_dave_group_creation_and_welcome_events() {
        let external_sender = parse_gateway_binary_message(&[0, 7, 25, 1, 2, 3]).unwrap();
        assert_eq!(external_sender.seq(), Some(7));
        assert!(matches!(
            external_sender.event(),
            VoiceGatewayEvent::DaveMlsExternalSenderPackage(_)
        ));

        let proposals = parse_gateway_binary_message(&[0, 8, 27, 0, 4, 5, 6]).unwrap();
        assert_eq!(proposals.seq(), Some(8));
        match proposals.into_event() {
            VoiceGatewayEvent::DaveMlsProposals(proposals) => {
                assert_eq!(proposals.operation, DaveMlsProposalsOperation::Append);
                assert_eq!(proposals.proposals, vec![4, 5, 6]);
            }
            other => panic!("expected dave proposals event, got {other:?}"),
        }

        let revoke = parse_gateway_binary_message(&[0, 8, 27, 1, 9, 10]).unwrap();
        match revoke.into_event() {
            VoiceGatewayEvent::DaveMlsProposals(proposals) => {
                assert_eq!(proposals.operation, DaveMlsProposalsOperation::Revoke);
                assert_eq!(proposals.proposals, vec![9, 10]);
            }
            other => panic!("expected dave proposals event, got {other:?}"),
        }

        let prepare_commit = parse_gateway_binary_message(&[0, 9, 29, 0, 12, 7, 8]).unwrap();
        assert_eq!(prepare_commit.seq(), Some(9));
        match prepare_commit.into_event() {
            VoiceGatewayEvent::DaveMlsPrepareCommitTransition(transition) => {
                assert_eq!(transition.transition_id, 12);
                assert_eq!(transition.commit, vec![7, 8]);
            }
            other => panic!("expected dave prepare commit transition, got {other:?}"),
        }

        let welcome = parse_gateway_binary_message(&[0, 10, 30, 0, 9, 4, 5, 6]).unwrap();
        assert_eq!(welcome.seq(), Some(10));
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
        assert_eq!(
            dave_mls_commit_welcome_payload(&[0, 0, 0, 2, 4, 5, 0, 0, 0, 3, 6, 7, 8]),
            vec![28, 4, 5, 6, 7, 8]
        );
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
                assert_eq!(epoch.transition_id, Some(11));
                assert_eq!(epoch.epoch, "1");
                assert_eq!(epoch.protocol_version, 1);
            }
            other => panic!("expected dave prepare epoch, got {other:?}"),
        }
    }

    #[test]
    fn parse_gateway_message_supports_dave_prepare_epoch_without_transition_id() {
        let payload = parse_gateway_message(
            r#"{
                "op": 24,
                "seq": 12,
                "d": {
                    "epoch": 1,
                    "protocol_version": 1
                }
            }"#,
        )
        .unwrap();

        assert_eq!(payload.seq(), Some(12));
        match payload.into_event() {
            VoiceGatewayEvent::DavePrepareEpoch(epoch) => {
                assert_eq!(epoch.transition_id, None);
                assert_eq!(epoch.epoch, "1");
                assert_eq!(epoch.protocol_version, 1);
            }
            other => panic!("expected dave prepare epoch, got {other:?}"),
        }
    }

    #[test]
    fn parse_gateway_message_supports_documented_server_text_opcodes() {
        let speaking = parse_gateway_message(
            r#"{
                "op": 5,
                "seq": 10,
                "d": {
                    "speaking": 1,
                    "delay": 0,
                    "ssrc": 42,
                    "user_id": "user-1"
                }
            }"#,
        )
        .unwrap();
        assert_eq!(speaking.seq(), Some(10));
        match speaking.into_event() {
            VoiceGatewayEvent::Speaking(speaking) => {
                assert_eq!(speaking.speaking, 1);
                assert_eq!(speaking.delay, 0);
                assert_eq!(speaking.ssrc, 42);
                assert_eq!(speaking.user_id.as_deref(), Some("user-1"));
            }
            other => panic!("expected speaking event, got {other:?}"),
        }

        let speaking_without_delay = parse_gateway_message(
            r#"{
                "op": 5,
                "seq": 11,
                "d": {
                    "speaking": 1,
                    "ssrc": 43,
                    "user_id": "user-2"
                }
            }"#,
        )
        .unwrap();
        assert_eq!(speaking_without_delay.seq(), Some(11));
        match speaking_without_delay.into_event() {
            VoiceGatewayEvent::Speaking(speaking) => {
                assert_eq!(speaking.speaking, 1);
                assert_eq!(speaking.delay, 0);
                assert_eq!(speaking.ssrc, 43);
                assert_eq!(speaking.user_id.as_deref(), Some("user-2"));
            }
            other => panic!("expected speaking event, got {other:?}"),
        }

        let heartbeat_ack = parse_gateway_message(
            r#"{
                "op": 6,
                "d": {
                    "t": 1501184119561
                }
            }"#,
        )
        .unwrap();
        match heartbeat_ack.into_event() {
            VoiceGatewayEvent::HeartbeatAck(ack) => {
                assert_eq!(ack.nonce, Some(1_501_184_119_561));
            }
            other => panic!("expected heartbeat ack event, got {other:?}"),
        }

        let legacy_heartbeat_ack = parse_gateway_message(
            r#"{
                "op": 6,
                "d": 1501184119561
            }"#,
        )
        .unwrap();
        match legacy_heartbeat_ack.into_event() {
            VoiceGatewayEvent::HeartbeatAck(ack) => {
                assert_eq!(ack.nonce, Some(1_501_184_119_561));
            }
            other => panic!("expected heartbeat ack event, got {other:?}"),
        }

        let disconnect = parse_gateway_message(
            r#"{
                "op": 13,
                "seq": 11,
                "d": {
                    "user_id": "user-2"
                }
            }"#,
        )
        .unwrap();
        assert_eq!(disconnect.seq(), Some(11));
        match disconnect.into_event() {
            VoiceGatewayEvent::ClientDisconnect(disconnect) => {
                assert_eq!(disconnect.user_id, "user-2");
            }
            other => panic!("expected client disconnect event, got {other:?}"),
        }

        let prepare_transition = parse_gateway_message(
            r#"{
                "op": 21,
                "d": {
                    "transition_id": 12,
                    "protocol_version": 1
                }
            }"#,
        )
        .unwrap();
        match prepare_transition.into_event() {
            VoiceGatewayEvent::DavePrepareTransition(transition) => {
                assert_eq!(transition.transition_id, 12);
                assert_eq!(transition.protocol_version, 1);
            }
            other => panic!("expected dave prepare transition event, got {other:?}"),
        }
    }

    #[test]
    fn parse_gateway_message_supports_known_but_undocumented_server_text_opcodes() {
        let video = parse_gateway_message(
            r#"{
                "op": 12,
                "seq": 12,
                "d": {
                    "user_id": "user-3",
                    "audio_ssrc": 13959,
                    "video_ssrc": 13960,
                    "streams": []
                }
            }"#,
        )
        .unwrap();
        assert_eq!(video.seq(), Some(12));
        match video.into_event() {
            VoiceGatewayEvent::Video(video) => {
                assert_eq!(video.user_id.as_deref(), Some("user-3"));
                assert_eq!(video.audio_ssrc, Some(13_959));
                assert_eq!(video.video_ssrc, Some(13_960));
            }
            other => panic!("expected video event, got {other:?}"),
        }

        let media_sink_wants = parse_gateway_message(
            r#"{
                "op": 15,
                "d": {
                    "8964": 100,
                    "pixelCounts": {
                        "8964": 1189844.5769597634
                    }
                }
            }"#,
        )
        .unwrap();
        assert!(matches!(
            media_sink_wants.into_event(),
            VoiceGatewayEvent::MediaSinkWants
        ));

        let client_flags = parse_gateway_message(
            r#"{
                "op": 18,
                "d": {
                    "user_id": "user-4",
                    "flags": 3
                }
            }"#,
        )
        .unwrap();
        match client_flags.into_event() {
            VoiceGatewayEvent::ClientFlags(flags) => {
                assert_eq!(flags.user_id, "user-4");
                assert_eq!(flags.flags, Some(3));
            }
            other => panic!("expected client flags event, got {other:?}"),
        }

        let client_platform = parse_gateway_message(
            r#"{
                "op": 20,
                "d": {
                    "user_id": "user-5",
                    "platform": 0
                }
            }"#,
        )
        .unwrap();
        match client_platform.into_event() {
            VoiceGatewayEvent::ClientPlatform(platform) => {
                assert_eq!(platform.user_id, "user-5");
                assert_eq!(platform.platform, Some(0));
            }
            other => panic!("expected client platform event, got {other:?}"),
        }
    }

    #[test]
    fn parse_gateway_message_keeps_unknown_opcodes_fail_closed() {
        let err = parse_gateway_message(
            r#"{
                "op": 10,
                "d": {}
            }"#,
        )
        .unwrap_err();

        assert!(matches!(
            err,
            crate::error::VoiceError::UnsupportedGatewayOp(10)
        ));
    }

    #[test]
    fn parse_gateway_binary_message_keeps_unknown_opcodes_fail_closed() {
        let err = parse_gateway_binary_message(&[0, 1, 31]).unwrap_err();

        assert!(matches!(
            err,
            crate::error::VoiceError::UnsupportedBinaryGatewayOp(31)
        ));
    }
}
