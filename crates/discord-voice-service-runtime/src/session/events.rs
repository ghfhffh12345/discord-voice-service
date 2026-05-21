use tokio::sync::broadcast;

use crate::session::state::Snapshot;
use discord_voice_service_proto::discordvoice::v1::{
    SessionEvent, SessionEventKind as ProtoSessionEventKind, SessionEventReason,
};

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

    pub fn into_proto(self) -> SessionEvent {
        SessionEvent {
            kind: map_session_event_kind(self.kind) as i32,
            guild_id: self.guild_id.unwrap_or_default(),
            channel_id: self.channel_id.unwrap_or_default(),
            current_video_id: self.current_video_id.unwrap_or_default(),
            selected_itag: self.selected_itag.unwrap_or_default(),
            message: self.message.unwrap_or_default(),
            reason: SessionEventReason::Unspecified as i32,
            ..Default::default()
        }
    }
}

#[derive(Clone)]
pub struct EventBus {
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

fn map_session_event_kind(kind: SessionEventKind) -> ProtoSessionEventKind {
    match kind {
        SessionEventKind::VoiceConnecting => ProtoSessionEventKind::VoiceConnecting,
        SessionEventKind::VoiceReady => ProtoSessionEventKind::VoiceReady,
        SessionEventKind::TrackResolving => ProtoSessionEventKind::TrackResolving,
        SessionEventKind::Buffering => ProtoSessionEventKind::Buffering,
        SessionEventKind::Playing => ProtoSessionEventKind::Playing,
        SessionEventKind::Paused => ProtoSessionEventKind::Paused,
        SessionEventKind::Stopped => ProtoSessionEventKind::Stopped,
        SessionEventKind::TrackEnded => ProtoSessionEventKind::TrackEnded,
        SessionEventKind::PlaybackInterrupted => ProtoSessionEventKind::PlaybackInterrupted,
        SessionEventKind::RecoverableWarning => ProtoSessionEventKind::RecoverableWarning,
        SessionEventKind::FatalError => ProtoSessionEventKind::FatalError,
        SessionEventKind::VoiceReconnecting => ProtoSessionEventKind::VoiceReconnecting,
    }
}
