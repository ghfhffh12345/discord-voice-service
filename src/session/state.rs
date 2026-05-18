#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SessionState {
    Idle,
    ConnectingVoice,
    VoiceReady,
    ResolvingTrack,
    Buffering,
    Playing,
    Paused,
    Stopping,
    Error,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Snapshot {
    pub state: SessionState,
    pub guild_id: Option<String>,
    pub channel_id: Option<String>,
    pub current_video_id: Option<String>,
}

impl Default for Snapshot {
    fn default() -> Self {
        Self {
            state: SessionState::Idle,
            guild_id: None,
            channel_id: None,
            current_video_id: None,
        }
    }
}
