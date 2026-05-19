#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct VoiceSessionRollover {
    recovering: bool,
    voice_reconnecting: bool,
}

impl VoiceSessionRollover {
    pub(crate) fn recovering(&self) -> bool {
        self.recovering
    }

    pub(crate) fn voice_reconnecting(&self) -> bool {
        self.voice_reconnecting
    }

    pub(crate) fn set_voice_reconnecting(&mut self, voice_reconnecting: bool) {
        self.voice_reconnecting = voice_reconnecting;
    }
}
