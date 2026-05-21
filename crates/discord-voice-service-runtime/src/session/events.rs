use tokio::sync::broadcast;

use super::state::Snapshot;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SessionEventKind {
    VoiceConnecting,
    VoiceReady,
    TrackResolving,
    Buffering,
    Playing,
    Paused,
    Stopped,
    TrackEnded,
    PlaybackInterrupted,
    RecoverableWarning,
    FatalError,
    VoiceReconnecting,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionEventRecord {
    pub kind: SessionEventKind,
    pub guild_id: Option<String>,
    pub channel_id: Option<String>,
    pub current_video_id: Option<String>,
    pub selected_itag: Option<u32>,
    pub message: Option<String>,
}

impl SessionEventRecord {
    pub fn new(kind: SessionEventKind) -> Self {
        Self {
            kind,
            guild_id: None,
            channel_id: None,
            current_video_id: None,
            selected_itag: None,
            message: None,
        }
    }

    pub fn from_snapshot(kind: SessionEventKind, snapshot: &Snapshot) -> Self {
        Self {
            kind,
            guild_id: snapshot.guild_id.clone(),
            channel_id: snapshot.channel_id.clone(),
            current_video_id: snapshot.current_video_id.clone(),
            selected_itag: snapshot.selected_itag,
            message: snapshot.last_reason.clone(),
        }
    }
}

#[derive(Clone)]
pub(crate) struct EventBus {
    tx: broadcast::Sender<SessionEventRecord>,
}

impl EventBus {
    pub fn new(capacity: usize) -> Self {
        let (tx, _) = broadcast::channel(capacity);
        Self { tx }
    }

    pub fn subscribe(&self) -> broadcast::Receiver<SessionEventRecord> {
        self.tx.subscribe()
    }

    pub fn emit(&self, event: SessionEventRecord) {
        let _ = self.tx.send(event);
    }
}
