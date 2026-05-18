use crate::discord_voice::rollover::VoiceSessionRollover;
use crate::session::supervisor::VoiceContext;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnectedVoiceSession {
    voice: VoiceContext,
    rollover: VoiceSessionRollover,
}

impl ConnectedVoiceSession {
    pub fn new(voice: VoiceContext) -> Self {
        Self {
            voice,
            rollover: VoiceSessionRollover::default(),
        }
    }

    pub fn voice_context(&self) -> &VoiceContext {
        &self.voice
    }

    pub fn update_voice_context(&mut self, voice: VoiceContext) {
        self.voice = voice;
    }

    pub fn rollover(&self) -> &VoiceSessionRollover {
        &self.rollover
    }
}
