use tokio::net::lookup_host;

use crate::discord_voice::gateway::VoiceGatewayClient;
use crate::discord_voice::rollover::VoiceSessionRollover;
use crate::discord_voice::udp::VoiceUdpTransport;
use crate::error::AppError;
use crate::session::supervisor::VoiceContext;

pub(crate) struct ConnectedVoiceSession {
    voice: VoiceContext,
    rollover: VoiceSessionRollover,
    gateway: Option<VoiceGatewayClient>,
    transport: Option<VoiceUdpTransport>,
}

impl ConnectedVoiceSession {
    pub(crate) fn new(voice: VoiceContext) -> Self {
        Self {
            voice,
            rollover: VoiceSessionRollover::default(),
            gateway: None,
            transport: None,
        }
    }

    pub(crate) async fn connect(voice: VoiceContext) -> Result<Self, AppError> {
        let (udp_target, ssrc) = connection_params(&voice.endpoint).await?;

        Ok(Self {
            gateway: Some(VoiceGatewayClient::connect(&voice.endpoint).await?),
            transport: Some(VoiceUdpTransport::connect(udp_target, ssrc).await?),
            voice,
            rollover: VoiceSessionRollover::default(),
        })
    }

    pub(crate) fn voice_context(&self) -> &VoiceContext {
        &self.voice
    }

    pub(crate) fn rollover(&self) -> &VoiceSessionRollover {
        &self.rollover
    }

    pub(crate) fn rollover_mut(&mut self) -> &mut VoiceSessionRollover {
        &mut self.rollover
    }

    pub(crate) fn is_connected(&self) -> bool {
        self.gateway.is_some() && self.transport.is_some()
    }
}

async fn connection_params(endpoint: &str) -> Result<(std::net::SocketAddr, u32), AppError> {
    let uri: http::Uri = endpoint.parse()?;
    let query = uri
        .path_and_query()
        .and_then(|path_and_query| path_and_query.query())
        .ok_or(AppError::InvalidState("voice endpoint query missing"))?;
    let udp = query_param(query, "udp")
        .ok_or(AppError::InvalidState("voice endpoint udp target missing"))?;
    let ssrc = query_param(query, "ssrc")
        .ok_or(AppError::InvalidState("voice endpoint ssrc missing"))?
        .parse::<u32>()
        .map_err(|_| AppError::InvalidState("voice endpoint ssrc invalid"))?;
    let udp_target = lookup_host(udp)
        .await?
        .next()
        .ok_or(AppError::InvalidState(
            "voice endpoint udp target unresolved",
        ))?;

    Ok((udp_target, ssrc))
}

fn query_param<'a>(query: &'a str, key: &str) -> Option<&'a str> {
    query.split('&').find_map(|pair| {
        let (candidate, value) = pair.split_once('=')?;
        (candidate == key).then_some(value)
    })
}
