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

pub async fn resume(voice: &VoiceContext, seq_ack: Option<u64>) -> Result<(), AppError> {
    let Some(gateway_url) = gateway_url(&voice.endpoint)? else {
        return Err(AppError::InvalidState("voice endpoint invalid for resume"));
    };

    let mut gateway = timeout(HANDSHAKE_TIMEOUT, VoiceGatewayClient::connect(&gateway_url))
        .await
        .map_err(|_| AppError::InvalidState("voice gateway connect timed out"))??;
    expect_hello(&mut gateway).await?;

    if let Some(seq_ack) = seq_ack {
        gateway.record_seq_ack(seq_ack);
    }
    gateway
        .send_resume(&voice.guild_id, &voice.session_id, &voice.token)
        .await?;

    match next_event(&mut gateway).await?.into_event() {
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
