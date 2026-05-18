use tokio::sync::broadcast;

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
