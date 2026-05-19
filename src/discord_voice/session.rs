use bytes::Bytes;
use tokio::sync::oneshot;
use tokio::time::{Duration, sleep};

use crate::discord_voice::dave::{DaveMediaType, DaveRuntimeContext};
use crate::discord_voice::gateway::VoiceGatewayClient;
use crate::discord_voice::handshake;
use crate::discord_voice::protection::ProtectionContext;
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
    dave: Option<DaveRuntimeContext>,
    heartbeat_shutdown: Option<oneshot::Sender<()>>,
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
            dave: None,
            heartbeat_shutdown: None,
            speaking_started: false,
        }
    }

    pub async fn connect(voice: VoiceContext) -> Result<Self, AppError> {
        let Some(result) = handshake::connect(&voice).await? else {
            return Ok(Self::new(voice));
        };
        let handshake::VoiceHandshakeResult {
            gateway,
            transport,
            ssrc,
            heartbeat_interval_ms,
            session_description,
            dave,
        } = result;
        let transport =
            transport.with_protection(ProtectionContext::from_session(&session_description)?);

        let heartbeat_shutdown = spawn_heartbeat_task(gateway.clone(), heartbeat_interval_ms);

        Ok(Self {
            gateway: Some(gateway),
            transport: Some(transport),
            voice,
            rollover: VoiceSessionRollover::default(),
            ssrc: Some(ssrc),
            session_description: Some(session_description),
            dave,
            heartbeat_shutdown: Some(heartbeat_shutdown),
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

    pub fn dave_enabled(&self) -> bool {
        self.dave.is_some()
    }

    pub(crate) async fn send_audio_frame(&mut self, frame: Bytes) -> Result<(), AppError> {
        let ssrc = self
            .ssrc
            .ok_or(AppError::InvalidState("voice ssrc unavailable"))?;
        if !self.speaking_started {
            let gateway = self
                .gateway
                .as_ref()
                .ok_or(AppError::InvalidState("voice gateway unavailable"))?;
            send_speaking(gateway, ssrc).await?;
            self.speaking_started = true;
        }

        let frame = if let Some(dave) = self.dave.as_mut() {
            Bytes::from(
                dave.encryptor
                    .encrypt(DaveMediaType::Audio, ssrc, frame.as_ref())
                    .map_err(|_| AppError::InvalidState("voice dave frame encryption failed"))?,
            )
        } else {
            frame
        };
        let transport = self
            .transport
            .as_mut()
            .ok_or(AppError::InvalidState("voice transport unavailable"))?;
        transport.send_audio_frame(frame).await
    }
}

impl Drop for ConnectedVoiceSession {
    fn drop(&mut self) {
        if let Some(shutdown) = self.heartbeat_shutdown.take() {
            let _ = shutdown.send(());
        }
    }
}

fn spawn_heartbeat_task(
    gateway: VoiceGatewayClient,
    heartbeat_interval_ms: u64,
) -> oneshot::Sender<()> {
    let (shutdown_tx, mut shutdown_rx) = oneshot::channel();
    let interval = Duration::from_millis(heartbeat_interval_ms.max(1));
    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = &mut shutdown_rx => break,
                _ = sleep(interval) => {
                    if gateway.send_heartbeat().await.is_err() {
                        break;
                    }
                }
            }
        }
    });

    shutdown_tx
}
