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
}
