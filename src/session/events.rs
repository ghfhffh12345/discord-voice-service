use tokio::sync::broadcast;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EventKind {
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
}

pub type EventBus = broadcast::Sender<EventKind>;

pub fn event_bus() -> (EventBus, broadcast::Receiver<EventKind>) {
    broadcast::channel(64)
}
