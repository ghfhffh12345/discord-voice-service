#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct VoiceSessionRollover {
    recovering: bool,
    voice_reconnecting: bool,
}

impl VoiceSessionRollover {
    pub fn recovering(&self) -> bool {
        self.recovering
    }

    pub fn voice_reconnecting(&self) -> bool {
        self.voice_reconnecting
    }
}
