#[path = "support/fake_voice.rs"]
mod fake_voice;

use discord_voice_service::session::events::{SessionEventKind, SessionEventRecord};
use discord_voice_service::session::state::Snapshot;
use discord_voice_service::session::supervisor::{Command, Supervisor, VoiceContext};
use tokio::sync::Mutex;
use tokio::sync::broadcast;
use tokio::time::{Duration, Instant, sleep};

use self::fake_voice::FakeVoiceEndpoint;

#[tokio::test]
async fn rollover_rebuilds_transport_and_preserves_track_identity() {
    let initial_voice = FakeVoiceEndpoint::spawn().await;
    let replacement_voice = FakeVoiceEndpoint::spawn().await;
    let harness = VoiceRolloverHarness::spawn().await;
    harness
        .start_playing(
            initial_voice.voice_context("1", "2", "user-1", "3", "token"),
            "video-1",
        )
        .await
        .unwrap();

    harness
        .update_voice_context(replacement_voice.voice_context(
            "1",
            "9",
            "user-1",
            "rotated-session",
            "rotated-token",
        ))
        .await
        .unwrap();

    let snapshot = harness.snapshot().await;
    assert_eq!(snapshot.current_video_id.as_deref(), Some("video-1"));
    assert!(harness.seen_event("voice-reconnected").await);
    assert_eq!(replacement_voice.discovery_count().await, 1);
    assert!(replacement_voice.gateway_connected().await);
    assert!(
        replacement_voice
            .gateway_path()
            .await
            .unwrap()
            .contains("v=8")
    );
}

#[tokio::test]
async fn rollover_keeps_snapshot_reconnecting_until_replacement_voice_is_ready() {
    let initial_voice = FakeVoiceEndpoint::spawn().await;
    let replacement_voice =
        FakeVoiceEndpoint::spawn_with_gateway_delay(Duration::from_millis(250)).await;
    let harness = VoiceRolloverHarness::spawn().await;
    harness
        .start_playing(
            initial_voice.voice_context("1", "2", "user-1", "3", "token"),
            "video-1",
        )
        .await
        .unwrap();

    let supervisor = harness.supervisor.clone();
    let replacement =
        replacement_voice.voice_context("1", "9", "user-1", "rotated-session", "rotated-token");
    let rollover = tokio::spawn(async move {
        supervisor
            .send(Command::UpdateVoiceContext { voice: replacement })
            .await
    });

    let reconnecting_snapshot = harness.wait_for_voice_reconnecting(true).await;
    assert!(reconnecting_snapshot.voice_reconnecting);
    assert!(harness.seen_event("voice-reconnecting").await);

    rollover.await.unwrap().unwrap();

    let ready_snapshot = harness.wait_for_voice_reconnecting(false).await;
    assert!(!ready_snapshot.voice_reconnecting);
    assert!(harness.seen_event("voice-reconnected").await);
    assert!(replacement_voice.gateway_connected().await);
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
        voice: VoiceContext,
        video_id: &str,
    ) -> Result<(), discord_voice_service::error::AppError> {
        self.supervisor.send(Command::JoinVoice { voice }).await?;
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

    async fn wait_for_voice_reconnecting(&self, expected: bool) -> Snapshot {
        let deadline = Instant::now() + Duration::from_secs(1);
        loop {
            let snapshot = self.snapshot().await;
            if snapshot.voice_reconnecting == expected || Instant::now() >= deadline {
                return snapshot;
            }
            sleep(Duration::from_millis(10)).await;
        }
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
