use std::collections::{BTreeMap, BTreeSet};
use std::net::{IpAddr, SocketAddr};

use tokio::net::lookup_host;
use tokio::time::{Duration, timeout};

use crate::discord_voice::dave::{DaveRuntimeContext, DaveSession};
use crate::discord_voice::gateway::VoiceGatewayClient;
use crate::discord_voice::protocol::{
    self, ClientsConnect, DaveExecuteTransition, DaveMlsExternalSenderPackage, DaveMlsWelcome,
    DavePrepareEpoch, Hello, Ready, SessionDescription, VoiceGatewayEvent,
};
use crate::discord_voice::udp::{DiscoveredUdpAddress, VoiceUdpTransport};
use crate::error::AppError;
use crate::session::supervisor::VoiceContext;

const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(2);

pub struct VoiceHandshakeResult {
    pub gateway: VoiceGatewayClient,
    pub transport: VoiceUdpTransport,
    pub ssrc: u32,
    pub heartbeat_interval_ms: u64,
    pub session_description: SessionDescription,
    pub dave: Option<DaveRuntimeContext>,
}

pub async fn connect(voice: &VoiceContext) -> Result<Option<VoiceHandshakeResult>, AppError> {
    let Some(gateway_url) = gateway_url(&voice.endpoint)? else {
        return Ok(None);
    };

    let gateway = timeout(HANDSHAKE_TIMEOUT, VoiceGatewayClient::connect(&gateway_url))
        .await
        .map_err(|_| AppError::InvalidState("voice gateway connect timed out"))??;
    let hello = expect_hello(&gateway).await?;
    gateway.send_identify(voice).await?;

    let ready = expect_ready(&gateway).await?;
    let mode = protocol::choose_encryption_mode(&ready)?.to_owned();
    let udp_target = resolve_udp_target(&ready).await?;
    let transport = VoiceUdpTransport::connect(udp_target, ready.ssrc).await?;
    let discovered = DiscoveredUdpAddress {
        ip: transport.local_addr().ip().to_string(),
        port: transport.local_addr().port(),
    };

    gateway.send_select_protocol(&discovered, &mode).await?;
    let session_description = expect_session_description(&gateway).await?;
    let dave = if let Some(protocol_version) = session_description.dave_protocol_version {
        Some(complete_initial_dave_transition(&gateway, voice, ready.ssrc, protocol_version).await?)
    } else {
        None
    };

    Ok(Some(VoiceHandshakeResult {
        gateway,
        transport,
        ssrc: ready.ssrc,
        heartbeat_interval_ms: hello.heartbeat_interval_ms,
        session_description,
        dave,
    }))
}

pub async fn resume(voice: &VoiceContext, seq_ack: Option<u64>) -> Result<(), AppError> {
    let Some(gateway_url) = gateway_url(&voice.endpoint)? else {
        return Err(AppError::InvalidState("voice endpoint invalid for resume"));
    };

    let gateway = timeout(HANDSHAKE_TIMEOUT, VoiceGatewayClient::connect(&gateway_url))
        .await
        .map_err(|_| AppError::InvalidState("voice gateway connect timed out"))??;
    expect_hello(&gateway).await?;

    if let Some(seq_ack) = seq_ack {
        gateway.record_seq_ack(seq_ack).await;
    }
    gateway
        .send_resume(&voice.guild_id, &voice.session_id, &voice.token)
        .await?;

    match next_event(&gateway).await?.into_event() {
        VoiceGatewayEvent::Resumed => Ok(()),
        _ => Err(AppError::InvalidState("voice handshake resume rejected")),
    }
}

fn gateway_url(endpoint: &str) -> Result<Option<String>, AppError> {
    let trimmed = endpoint.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }

    if let Ok(uri) = trimmed.parse::<http::Uri>() {
        if uri.scheme().is_some() && uri.authority().is_some() {
            return Ok(Some(normalize_absolute_gateway_url(&uri)?));
        }
    }

    if looks_like_local_endpoint(trimmed)? {
        return Ok(Some(format!("ws://{trimmed}")));
    }

    if looks_like_forwarded_endpoint(trimmed)? {
        return Ok(Some(format!("wss://{trimmed}")));
    }

    Ok(None)
}

fn normalize_absolute_gateway_url(uri: &http::Uri) -> Result<String, AppError> {
    let mut parts = uri.clone().into_parts();
    let Some(scheme) = parts.scheme.as_ref().map(http::uri::Scheme::as_str) else {
        return Err(AppError::InvalidState("voice endpoint scheme missing"));
    };

    parts.scheme = Some(
        match scheme {
            "ws" => "ws",
            "wss" => "wss",
            "http" => "ws",
            "https" => "wss",
            _ => return Err(AppError::InvalidState("voice endpoint scheme unsupported")),
        }
        .parse()
        .map_err(|_| AppError::InvalidState("voice endpoint scheme invalid"))?,
    );

    Ok(http::Uri::from_parts(parts)
        .map_err(|_| AppError::InvalidState("voice endpoint could not be normalized"))?
        .to_string())
}

fn looks_like_local_endpoint(endpoint: &str) -> Result<bool, AppError> {
    let Some(host) = endpoint_host(endpoint)? else {
        return Ok(false);
    };

    Ok(host == "localhost" || host.parse::<IpAddr>().map(is_local_ip).unwrap_or(false))
}

fn looks_like_forwarded_endpoint(endpoint: &str) -> Result<bool, AppError> {
    let Some(host) = endpoint_host(endpoint)? else {
        return Ok(false);
    };

    Ok(host.contains('.') && host != "localhost" && host.parse::<IpAddr>().is_err())
}

fn endpoint_host(endpoint: &str) -> Result<Option<String>, AppError> {
    let candidate = format!("https://{endpoint}");
    let uri = candidate
        .parse::<http::Uri>()
        .map_err(|_| AppError::InvalidState("voice endpoint invalid"))?;
    let Some(authority) = uri.authority() else {
        return Ok(None);
    };

    Ok(Some(authority.host().to_owned()))
}

fn is_local_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(addr) => addr.is_loopback() || addr.is_private(),
        IpAddr::V6(addr) => addr.is_loopback() || addr.is_unique_local(),
    }
}

async fn expect_hello(gateway: &VoiceGatewayClient) -> Result<Hello, AppError> {
    match next_event(gateway).await?.into_event() {
        VoiceGatewayEvent::Hello(hello) => Ok(hello),
        _ => Err(AppError::InvalidState("voice handshake hello missing")),
    }
}

async fn expect_ready(gateway: &VoiceGatewayClient) -> Result<Ready, AppError> {
    match next_event(gateway).await?.into_event() {
        VoiceGatewayEvent::Ready(ready) => Ok(ready),
        _ => Err(AppError::InvalidState("voice handshake ready missing")),
    }
}

async fn expect_session_description(
    gateway: &VoiceGatewayClient,
) -> Result<SessionDescription, AppError> {
    match next_event(gateway).await?.into_event() {
        VoiceGatewayEvent::SessionDescription(description) => Ok(description),
        _ => Err(AppError::InvalidState(
            "voice handshake session description missing",
        )),
    }
}

async fn complete_initial_dave_transition(
    gateway: &VoiceGatewayClient,
    voice: &VoiceContext,
    ssrc: u32,
    protocol_version: u16,
) -> Result<DaveRuntimeContext, AppError> {
    let group_id = voice
        .channel_id
        .parse::<u64>()
        .map_err(|_| AppError::InvalidState("voice dave group id invalid"))?;
    let mut recognized_user_ids = BTreeSet::from([voice.user_id.clone()]);
    let mut session = DaveSession::new(None)
        .map_err(|_| AppError::InvalidState("voice dave session create failed"))?;
    session
        .init(protocol_version, group_id, &voice.user_id)
        .map_err(|_| AppError::InvalidState("voice dave session init failed"))?;
    let mut pending_prepared_transitions = BTreeMap::<u16, u16>::new();
    let mut pending_transition = None::<(u16, DaveRuntimeContext)>;
    let mut pending_key_package = false;

    loop {
        match next_event(gateway).await?.into_event() {
            VoiceGatewayEvent::ClientsConnect(ClientsConnect { user_ids }) => {
                recognized_user_ids.extend(user_ids);
            }
            VoiceGatewayEvent::DaveMlsExternalSenderPackage(DaveMlsExternalSenderPackage {
                external_sender: sender,
            }) => {
                session
                    .set_external_sender(&sender)
                    .map_err(|_| AppError::InvalidState("voice dave external sender invalid"))?;
                send_pending_join_key_package(gateway, &mut session, &mut pending_key_package)
                    .await?;
            }
            VoiceGatewayEvent::DavePrepareEpoch(DavePrepareEpoch {
                transition_id,
                epoch,
                protocol_version: prepare_protocol_version,
            }) => {
                if prepare_protocol_version != protocol_version {
                    return Err(AppError::InvalidState(
                        "voice dave prepare epoch protocol version mismatch",
                    ));
                }
                if epoch.is_empty() {
                    return Err(AppError::InvalidState("voice dave prepare epoch missing"));
                }
                send_pending_join_key_package(gateway, &mut session, &mut pending_key_package)
                    .await?;
                pending_prepared_transitions.insert(transition_id, prepare_protocol_version);
            }
            VoiceGatewayEvent::DaveMlsWelcome(DaveMlsWelcome {
                transition_id,
                welcome,
            }) => {
                let runtime_protocol_version = if let Some(runtime_protocol_version) =
                    pending_prepared_transitions.remove(&transition_id)
                {
                    runtime_protocol_version
                } else if pending_key_package && pending_prepared_transitions.is_empty() {
                    protocol_version
                } else {
                    return Err(AppError::InvalidState(
                        "voice dave welcome transition missing pending join",
                    ));
                };
                let recognized = recognized_user_ids
                    .iter()
                    .map(String::as_str)
                    .collect::<Vec<_>>();
                session
                    .process_welcome(&welcome, &recognized)
                    .map_err(|_| AppError::InvalidState("voice dave welcome invalid"))?;
                pending_key_package = false;
                pending_prepared_transitions.clear();
                let runtime = DaveRuntimeContext::from_session(
                    &session,
                    runtime_protocol_version,
                    &voice.user_id,
                    ssrc,
                )?;
                gateway.send_dave_transition_ready(transition_id).await?;
                pending_transition = Some((transition_id, runtime));
            }
            VoiceGatewayEvent::DaveExecuteTransition(DaveExecuteTransition { transition_id }) => {
                if pending_transition
                    .as_ref()
                    .is_some_and(|(expected_transition_id, _)| {
                        transition_id == *expected_transition_id
                    })
                {
                    return Ok(pending_transition.take().expect("pending transition").1);
                }
            }
            _ => {}
        }
    }
}

async fn send_pending_join_key_package(
    gateway: &VoiceGatewayClient,
    session: &mut DaveSession,
    pending_key_package: &mut bool,
) -> Result<(), AppError> {
    if *pending_key_package {
        return Ok(());
    }

    let key_package = session
        .key_package()
        .map_err(|_| AppError::InvalidState("voice dave key package failed"))?;
    gateway.send_dave_mls_key_package(&key_package).await?;
    *pending_key_package = true;
    Ok(())
}

async fn next_event(
    gateway: &VoiceGatewayClient,
) -> Result<protocol::VoiceGatewayPayload, AppError> {
    timeout(HANDSHAKE_TIMEOUT, gateway.receive_event())
        .await
        .map_err(|_| AppError::InvalidState("voice handshake timed out"))?
}

async fn resolve_udp_target(ready: &Ready) -> Result<SocketAddr, AppError> {
    if let Ok(ip) = ready.ip.parse::<IpAddr>() {
        return Ok(SocketAddr::new(ip, ready.port));
    }

    lookup_host((ready.ip.as_str(), ready.port))
        .await?
        .next()
        .ok_or(AppError::InvalidState("voice ready udp target unresolved"))
}

#[cfg(test)]
mod tests {
    use super::gateway_url;

    #[test]
    fn gateway_url_normalizes_forwarded_voice_hosts() {
        assert_eq!(
            gateway_url("voice.example.discord.gg").unwrap(),
            Some("wss://voice.example.discord.gg".to_owned())
        );
    }

    #[test]
    fn gateway_url_keeps_loopback_endpoints_on_ws() {
        assert_eq!(
            gateway_url("127.0.0.1:9000").unwrap(),
            Some("ws://127.0.0.1:9000".to_owned())
        );
    }
}
