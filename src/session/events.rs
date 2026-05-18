use tokio::sync::broadcast;

use crate::proto::discordvoice::v1::{
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
    pub reason: Option<String>,
}

impl SessionEventRecord {
    pub fn new(kind: SessionEventKind) -> Self {
        Self { kind, reason: None }
    }

    pub fn into_proto(self) -> SessionEvent {
        SessionEvent {
            kind: map_session_event_kind(self.kind) as i32,
            message: self.reason.unwrap_or_default(),
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
