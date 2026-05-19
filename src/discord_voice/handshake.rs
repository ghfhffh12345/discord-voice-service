use std::net::{IpAddr, SocketAddr};

use tokio::net::lookup_host;
use tokio::time::{Duration, timeout};

use crate::discord_voice::gateway::VoiceGatewayClient;
use crate::discord_voice::protocol::{self, Ready, SessionDescription, VoiceGatewayEvent};
use crate::discord_voice::udp::{DiscoveredUdpAddress, VoiceUdpTransport};
use crate::error::AppError;
use crate::session::supervisor::VoiceContext;

const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(2);

pub struct VoiceHandshakeResult {
    pub gateway: VoiceGatewayClient,
    pub transport: VoiceUdpTransport,
    pub ssrc: u32,
    pub session_description: SessionDescription,
}

pub async fn connect(voice: &VoiceContext) -> Result<Option<VoiceHandshakeResult>, AppError> {
    let Some(gateway_url) = gateway_url(&voice.endpoint)? else {
        return Ok(None);
    };

    let mut gateway = timeout(HANDSHAKE_TIMEOUT, VoiceGatewayClient::connect(&gateway_url))
        .await
        .map_err(|_| AppError::InvalidState("voice gateway connect timed out"))??;
    expect_hello(&mut gateway).await?;

    gateway.send_identify(voice).await?;

    let ready = expect_ready(&mut gateway).await?;
    let mode = protocol::choose_encryption_mode(&ready)?.to_owned();
    let udp_target = resolve_udp_target(&ready).await?;
    let transport = VoiceUdpTransport::connect(udp_target, ready.ssrc).await?;
    let discovered = DiscoveredUdpAddress {
        ip: transport.local_addr().ip().to_string(),
        port: transport.local_addr().port(),
    };

    gateway.send_select_protocol(&discovered, &mode).await?;
    let session_description = expect_session_description(&mut gateway).await?;

    Ok(Some(VoiceHandshakeResult {
        gateway,
        transport,
        ssrc: ready.ssrc,
        session_description,
    }))
}

fn gateway_url(endpoint: &str) -> Result<Option<String>, AppError> {
    let trimmed = endpoint.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }

    if let Ok(uri) = trimmed.parse::<http::Uri>() {
        if uri.scheme().is_some() && uri.authority().is_some() {
            return Ok(Some(trimmed.to_owned()));
        }
    }

    if looks_like_host_with_port(trimmed) {
        return Ok(Some(format!("ws://{trimmed}")));
    }

    Ok(None)
}

fn looks_like_host_with_port(endpoint: &str) -> bool {
    endpoint.contains(':')
        && !endpoint.contains("://")
        && !endpoint.contains('/')
        && !endpoint.contains('?')
}

async fn expect_hello(gateway: &mut VoiceGatewayClient) -> Result<(), AppError> {
    match next_event(gateway).await?.into_event() {
        VoiceGatewayEvent::Hello(_) => Ok(()),
        _ => Err(AppError::InvalidState("voice handshake hello missing")),
    }
}

async fn expect_ready(gateway: &mut VoiceGatewayClient) -> Result<Ready, AppError> {
    match next_event(gateway).await?.into_event() {
        VoiceGatewayEvent::Ready(ready) => Ok(ready),
        _ => Err(AppError::InvalidState("voice handshake ready missing")),
    }
}

async fn expect_session_description(
    gateway: &mut VoiceGatewayClient,
) -> Result<SessionDescription, AppError> {
    match next_event(gateway).await?.into_event() {
        VoiceGatewayEvent::SessionDescription(description) => Ok(description),
        _ => Err(AppError::InvalidState(
            "voice handshake session description missing",
        )),
    }
}

async fn next_event(
    gateway: &mut VoiceGatewayClient,
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
