use crate::discord_voice::rollover::VoiceSessionRollover;
use crate::session::supervisor::VoiceContext;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ConnectedVoiceSession {
    voice: VoiceContext,
    rollover: VoiceSessionRollover,
}

impl ConnectedVoiceSession {
    pub(crate) fn new(voice: VoiceContext) -> Self {
        Self {
            voice,
            rollover: VoiceSessionRollover::default(),
        }
    }

    pub(crate) async fn connect(voice: VoiceContext) -> Result<Self, crate::error::AppError> {
        Ok(Self::new(voice))
    }

    pub(crate) fn voice_context(&self) -> &VoiceContext {
        &self.voice
    }

    pub(crate) fn update_voice_context(&mut self, voice: VoiceContext) {
        self.voice = voice;
    }

    pub(crate) fn rollover(&self) -> &VoiceSessionRollover {
        &self.rollover
    }

    pub(crate) fn rollover_mut(&mut self) -> &mut VoiceSessionRollover {
        &mut self.rollover
    }
}
