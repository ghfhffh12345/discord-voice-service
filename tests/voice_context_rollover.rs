use discord_voice_service::session::events::{SessionEventKind, SessionEventRecord};
use discord_voice_service::session::state::Snapshot;
use discord_voice_service::session::supervisor::{Command, Supervisor, VoiceContext};
use tokio::sync::Mutex;
use tokio::sync::broadcast;

#[tokio::test]
async fn rollover_rebuilds_transport_and_preserves_track_identity() {
    let harness = VoiceRolloverHarness::spawn().await;
    harness.start_playing("video-1").await.unwrap();

    harness
        .update_voice_context(test_voice_context_rotated())
        .await
        .unwrap();

    let snapshot = harness.snapshot().await;
    assert_eq!(snapshot.current_video_id.as_deref(), Some("video-1"));
    assert!(harness.seen_event("voice-reconnected").await);
}

struct VoiceRolloverHarness {
    supervisor: Supervisor,
    events: Mutex<broadcast::Receiver<SessionEventRecord>>,
}

impl VoiceRolloverHarness {
    async fn spawn() -> Self {
        let supervisor = Supervisor::new();
        let events = supervisor.subscribe_events();
        Self {
            supervisor,
            events: Mutex::new(events),
        }
    }

    async fn start_playing(
        &self,
        video_id: &str,
    ) -> Result<(), discord_voice_service::error::AppError> {
        self.supervisor
            .send(Command::JoinVoice {
                voice: test_voice_context(),
            })
            .await?;
        self.supervisor
            .send(Command::Play {
                video_id: video_id.into(),
            })
            .await
    }

    async fn update_voice_context(
        &self,
        voice: VoiceContext,
    ) -> Result<(), discord_voice_service::error::AppError> {
        self.supervisor
            .send(Command::UpdateVoiceContext { voice })
            .await
    }

    async fn snapshot(&self) -> Snapshot {
        self.supervisor.snapshot().await
    }

    async fn seen_event(&self, name: &str) -> bool {
        let mut events = self.events.lock().await;
        loop {
            match events.try_recv() {
                Ok(event) if event_name(&event) == name => return true,
                Ok(_) => continue,
                Err(broadcast::error::TryRecvError::Empty) => return false,
                Err(broadcast::error::TryRecvError::Lagged(_)) => continue,
                Err(broadcast::error::TryRecvError::Closed) => return false,
            }
        }
    }
}

fn event_name(event: &SessionEventRecord) -> &'static str {
    match event.kind {
        SessionEventKind::VoiceConnecting => "voice-connecting",
        SessionEventKind::VoiceReady => "voice-reconnected",
        SessionEventKind::TrackResolving => "track-resolving",
        SessionEventKind::Buffering => "buffering",
        SessionEventKind::Playing => "playing",
        SessionEventKind::Paused => "paused",
        SessionEventKind::Stopped => "stopped",
        SessionEventKind::TrackEnded => "track-ended",
        SessionEventKind::PlaybackInterrupted => "playback-interrupted",
        SessionEventKind::RecoverableWarning => "recoverable-warning",
        SessionEventKind::FatalError => "fatal-error",
        SessionEventKind::VoiceReconnecting => "voice-reconnecting",
    }
}

fn test_voice_context() -> VoiceContext {
    VoiceContext {
        guild_id: "1".into(),
        channel_id: "2".into(),
        session_id: "3".into(),
        endpoint: "voice.example".into(),
        token: "token".into(),
    }
}

fn test_voice_context_rotated() -> VoiceContext {
    VoiceContext {
        guild_id: "1".into(),
        channel_id: "9".into(),
        session_id: "rotated-session".into(),
        endpoint: "rotated.voice.example".into(),
        token: "rotated-token".into(),
    }
}
