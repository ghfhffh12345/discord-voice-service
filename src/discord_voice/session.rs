use bytes::Bytes;

use crate::discord_voice::gateway::VoiceGatewayClient;
use crate::discord_voice::handshake;
use crate::discord_voice::protocol::SessionDescription;
use crate::discord_voice::rollover::VoiceSessionRollover;
use crate::discord_voice::speaking::send_speaking;
use crate::discord_voice::udp::VoiceUdpTransport;
use crate::error::AppError;
use crate::session::supervisor::VoiceContext;

pub struct ConnectedVoiceSession {
    voice: VoiceContext,
    rollover: VoiceSessionRollover,
    gateway: Option<VoiceGatewayClient>,
    transport: Option<VoiceUdpTransport>,
    ssrc: Option<u32>,
    session_description: Option<SessionDescription>,
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
            session_description: None,
            speaking_started: false,
        }
    }

    pub async fn connect(voice: VoiceContext) -> Result<Self, AppError> {
        let Some(result) = handshake::connect(&voice).await? else {
            return Ok(Self::new(voice));
        };

        Ok(Self {
            gateway: Some(result.gateway),
            transport: Some(result.transport),
            voice,
            rollover: VoiceSessionRollover::default(),
            ssrc: Some(result.ssrc),
            session_description: Some(result.session_description),
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

    pub fn is_connected(&self) -> bool {
        self.gateway.is_some()
            && self.transport.is_some()
            && self.ssrc.is_some()
            && self.session_description.is_some()
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
