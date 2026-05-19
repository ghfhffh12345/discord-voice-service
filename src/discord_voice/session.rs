use bytes::Bytes;
use tokio::net::lookup_host;

use crate::discord_voice::gateway::VoiceGatewayClient;
use crate::discord_voice::rollover::VoiceSessionRollover;
use crate::discord_voice::speaking::send_speaking;
use crate::discord_voice::udp::VoiceUdpTransport;
use crate::error::AppError;
use crate::session::supervisor::VoiceContext;

pub(crate) struct ConnectedVoiceSession {
    voice: VoiceContext,
    rollover: VoiceSessionRollover,
    gateway: Option<VoiceGatewayClient>,
    transport: Option<VoiceUdpTransport>,
    ssrc: Option<u32>,
    speaking_started: bool,
}

impl ConnectedVoiceSession {
    pub(crate) fn new(voice: VoiceContext) -> Self {
        Self {
            voice,
            rollover: VoiceSessionRollover::default(),
            gateway: None,
            transport: None,
            ssrc: None,
            speaking_started: false,
        }
    }

    pub(crate) async fn connect(voice: VoiceContext) -> Result<Self, AppError> {
        let Some((udp_target, ssrc)) = connection_params(&voice.endpoint).await? else {
            return Ok(Self::new(voice));
        };

        Ok(Self {
            gateway: Some(VoiceGatewayClient::connect(&voice.endpoint).await?),
            transport: Some(VoiceUdpTransport::connect(udp_target, ssrc).await?),
            voice,
            rollover: VoiceSessionRollover::default(),
            ssrc: Some(ssrc),
            speaking_started: false,
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
        self.gateway.is_some() && self.transport.is_some() && self.ssrc.is_some()
    }

    pub(crate) async fn send_audio_frame(&mut self, frame: Bytes) -> Result<(), AppError> {
        if !self.speaking_started {
            let gateway = self
                .gateway
                .as_mut()
                .ok_or(AppError::InvalidState("voice gateway unavailable"))?;
            let ssrc = self
                .ssrc
                .ok_or(AppError::InvalidState("voice ssrc unavailable"))?;
            send_speaking(gateway, ssrc).await?;
            self.speaking_started = true;
        }

        let transport = self
            .transport
            .as_mut()
            .ok_or(AppError::InvalidState("voice transport unavailable"))?;
        transport.send_audio_frame(frame).await
    }
}

async fn connection_params(
    endpoint: &str,
) -> Result<Option<(std::net::SocketAddr, u32)>, AppError> {
    let Ok(uri) = endpoint.parse::<http::Uri>() else {
        return Ok(None);
    };
    if uri.scheme().is_none() || uri.authority().is_none() {
        return Ok(None);
    }
    let query = uri
        .path_and_query()
        .and_then(|path_and_query| path_and_query.query())
        .ok_or(AppError::InvalidState("voice endpoint query missing"))?;
    let Some(udp) = query_param(query, "udp") else {
        return Ok(None);
    };
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

    Ok(Some((udp_target, ssrc)))
}

fn query_param<'a>(query: &'a str, key: &str) -> Option<&'a str> {
    query.split('&').find_map(|pair| {
        let (candidate, value) = pair.split_once('=')?;
        (candidate == key).then_some(value)
    })
}
