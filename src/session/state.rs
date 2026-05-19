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
    pub selected_itag: Option<u32>,
    pub queue_depth: usize,
    pub position_ms: u64,
    pub recovering: bool,
    pub voice_reconnecting: bool,
    pub last_reason: Option<String>,
}

impl Default for Snapshot {
    fn default() -> Self {
        Self {
            state: SessionState::Idle,
            guild_id: None,
            channel_id: None,
            current_video_id: None,
            selected_itag: None,
            queue_depth: 0,
            position_ms: 0,
            recovering: false,
            voice_reconnecting: false,
            last_reason: None,
        }
    }
}
