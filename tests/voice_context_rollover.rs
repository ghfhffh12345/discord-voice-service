#[path = "support/fake_discord.rs"]
mod fake_discord;
#[path = "support/fake_voice.rs"]
mod fake_voice;
#[path = "support/fake_ytmusic.rs"]
mod fake_ytmusic;
#[path = "support/fixtures.rs"]
mod fixtures;

use discord_voice_service::session::events::{SessionEventKind, SessionEventRecord};
use discord_voice_service::session::state::{SessionState, Snapshot};
use discord_voice_service::session::supervisor::{Command, Supervisor, VoiceContext};
use tokio::sync::Mutex;
use tokio::sync::broadcast;
use tokio::task::JoinHandle;
use tokio::time::{Duration, Instant, sleep, timeout};

use self::fake_discord::FakeDiscordPeer;
use self::fake_voice::FakeVoiceEndpoint;
use self::fake_ytmusic::FakeYtMusic;
use self::fixtures::{spawn_stream_server, spawn_stream_server_with_status_after_requests};

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

#[tokio::test]
async fn update_voice_context_failure_while_not_playing_emits_terminal_warning() {
    let harness = VoiceRolloverHarness::spawn().await;
    let joined = FakeVoiceEndpoint::spawn().await;
    let invalid = VoiceContext {
        guild_id: "1".into(),
        channel_id: "9".into(),
        user_id: "user-1".into(),
        session_id: "broken-session".into(),
        endpoint: "ws://127.0.0.1:1".into(),
        token: "broken-token".into(),
    };

    harness
        .supervisor
        .send(Command::JoinVoice {
            voice: joined.voice_context("1", "2", "user-1", "3", "token"),
        })
        .await
        .unwrap();

    let err = harness.update_voice_context(invalid).await.unwrap_err();
    assert!(err.to_string().contains("voice"));

    let snapshot = harness.wait_for_event_snapshot("recoverable-warning").await;
    assert_eq!(snapshot.state, SessionState::VoiceReady);
    assert_eq!(snapshot.current_video_id, None);
    assert!(!snapshot.voice_reconnecting);
    assert!(
        snapshot
            .last_reason
            .as_deref()
            .is_some_and(|reason| !reason.is_empty())
    );
}

#[tokio::test]
async fn update_voice_context_reconnects_and_resumes_after_last_emitted_position() {
    let harness = RolloverHarness::spawn().await;

    harness.play_until_position_ms(2_000).await;
    harness.update_voice_context().await;

    assert!(harness.resumed_after_position_ms(2_000).await);
}

#[tokio::test]
async fn update_voice_context_does_not_resume_stopped_track_after_stop_during_reconnect() {
    let harness = RolloverHarness::spawn().await;

    harness.play_until_position_ms(2_000).await;
    let rollover = harness.start_rollover().await;
    let reconnecting_snapshot = harness.wait_for_voice_reconnecting(true).await;
    assert!(reconnecting_snapshot.voice_reconnecting);

    harness.stop().await;
    rollover.await.unwrap().unwrap();

    let snapshot = harness.snapshot().await;
    assert_eq!(snapshot.state, SessionState::VoiceReady);
    assert_eq!(snapshot.current_video_id, None);
    assert!(!snapshot.voice_reconnecting);
    assert!(
        timeout(
            Duration::from_millis(400),
            harness.replacement_voice.speaking_observed().notified()
        )
        .await
        .is_err(),
        "replacement voice should stay silent after stop"
    );
}

#[tokio::test]
async fn update_voice_context_does_not_reattach_after_leave_voice_during_reconnect() {
    let harness = RolloverHarness::spawn().await;

    harness.play_until_position_ms(2_000).await;
    let rollover = harness.start_rollover().await;
    let reconnecting_snapshot = harness.wait_for_voice_reconnecting(true).await;
    assert!(reconnecting_snapshot.voice_reconnecting);

    harness.leave_voice().await;
    rollover.await.unwrap().unwrap();

    let snapshot = harness.snapshot().await;
    assert_eq!(snapshot, Snapshot::default());
    assert_eq!(harness.current_voice_context().await, None);
    assert!(
        timeout(
            Duration::from_millis(400),
            harness.replacement_voice.speaking_observed().notified()
        )
        .await
        .is_err(),
        "replacement voice should not resume after leave_voice"
    );
}

#[tokio::test]
async fn update_voice_context_failure_interrupts_active_playback_state() {
    let harness = RolloverHarness::spawn().await;

    harness.play_until_position_ms(2_000).await;
    let invalid_voice = VoiceContext {
        guild_id: "1".into(),
        channel_id: "9".into(),
        user_id: "user-1".into(),
        session_id: "broken-session".into(),
        endpoint: "ws://127.0.0.1:1".into(),
        token: "broken-token".into(),
    };

    let err = harness
        .supervisor
        .send(Command::UpdateVoiceContext {
            voice: invalid_voice,
        })
        .await
        .unwrap_err();

    assert!(err.to_string().contains("voice"));
    let snapshot = harness
        .wait_for_event_snapshot("playback-interrupted")
        .await;
    assert_eq!(snapshot.state, SessionState::VoiceReady);
    assert_eq!(snapshot.current_video_id, None);
    assert_eq!(snapshot.selected_itag, None);
    assert_eq!(snapshot.queue_depth, 0);
    assert!(!snapshot.voice_reconnecting);
    assert!(
        snapshot
            .last_reason
            .as_deref()
            .is_some_and(|reason| !reason.is_empty())
    );
}

#[tokio::test]
async fn update_voice_context_resume_failure_surfaces_interrupted_state() {
    let stream = spawn_stream_server_with_status_after_requests(
        "tests/fixtures/audio-long.webm",
        1,
        "HTTP/1.1 500 Internal Server Error",
    )
    .await;
    let harness = RolloverHarness::spawn_with_stream_url(stream.url()).await;

    harness.play_until_position_ms(2_000).await;
    harness.update_voice_context().await;

    let snapshot = harness
        .wait_for_event_snapshot("playback-interrupted")
        .await;
    assert_eq!(snapshot.state, SessionState::VoiceReady);
    assert_eq!(snapshot.current_video_id, None);
    assert_eq!(snapshot.selected_itag, None);
    assert_eq!(snapshot.queue_depth, 0);
    assert!(!snapshot.voice_reconnecting);
    assert!(
        snapshot
            .last_reason
            .as_deref()
            .is_some_and(|reason| reason.contains("500"))
    );
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

    async fn snapshot(&self) -> Snapshot {
        self.supervisor.snapshot().await
    }

    async fn update_voice_context(
        &self,
        voice: VoiceContext,
    ) -> Result<(), discord_voice_service::error::AppError> {
        self.supervisor
            .send(Command::UpdateVoiceContext { voice })
            .await
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

    async fn wait_for_event_snapshot(&self, expected: &str) -> Snapshot {
        let deadline = Instant::now() + Duration::from_secs(2);
        let mut events = self.events.lock().await;
        loop {
            match timeout(Duration::from_millis(100), events.recv()).await {
                Ok(Ok(event)) if event_name(&event) == expected => return self.snapshot().await,
                Ok(Ok(_)) => {}
                Ok(Err(_)) => {}
                Err(_) if Instant::now() >= deadline => {
                    panic!("timed out waiting for event {expected}")
                }
                Err(_) => {}
            }
        }
    }
}

struct RolloverHarness {
    supervisor: Supervisor,
    events: Mutex<broadcast::Receiver<SessionEventRecord>>,
    initial_voice: FakeDiscordPeer,
    replacement_voice: FakeDiscordPeer,
    initial_frame_count_at_rollover: Mutex<Option<usize>>,
}

impl RolloverHarness {
    async fn spawn() -> Self {
        let stream = spawn_stream_server("tests/fixtures/audio-long.webm").await;
        Self::spawn_with_stream_url(stream.url()).await
    }

    async fn spawn_with_stream_url(stream_url: String) -> Self {
        let fake_yt = FakeYtMusic::spawn().await;
        fake_yt.set_playable_url(stream_url).await;

        let initial_voice = FakeDiscordPeer::spawn().await;
        let replacement_voice =
            FakeDiscordPeer::spawn_with_gateway_delay(Duration::from_millis(250)).await;
        let supervisor = Supervisor::with_ytmusic_endpoint(fake_yt.endpoint())
            .await
            .unwrap();
        let events = supervisor.subscribe_events();

        supervisor
            .send(Command::JoinVoice {
                voice: initial_voice.voice_context("1", "2", "user-1", "3", "token"),
            })
            .await
            .unwrap();

        let play_supervisor = supervisor.clone();
        tokio::spawn(async move {
            let _ = play_supervisor
                .send(Command::Play {
                    video_id: "video-1".into(),
                })
                .await;
        });

        let harness = Self {
            supervisor,
            events: Mutex::new(events),
            initial_voice,
            replacement_voice,
            initial_frame_count_at_rollover: Mutex::new(None),
        };
        harness.wait_for_playback_start().await;
        harness
    }

    async fn play_until_position_ms(&self, target_position_ms: u64) {
        let deadline = Instant::now() + Duration::from_secs(4);
        loop {
            let snapshot = self.snapshot().await;
            if snapshot.position_ms >= target_position_ms {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "expected playback to reach {target_position_ms}ms, got {}ms",
                snapshot.position_ms
            );
            sleep(Duration::from_millis(10)).await;
        }
    }

    async fn update_voice_context(&self) {
        self.start_rollover()
            .await
            .await
            .expect("rollover task should complete")
            .unwrap();
    }

    async fn start_rollover(
        &self,
    ) -> JoinHandle<Result<(), discord_voice_service::error::AppError>> {
        let frame_count = self.initial_voice.audio_frame_count_at_least(0).await;
        *self.initial_frame_count_at_rollover.lock().await = Some(frame_count);
        let supervisor = self.supervisor.clone();
        let voice = self.replacement_voice.voice_context(
            "1",
            "9",
            "user-1",
            "rotated-session",
            "rotated-token",
        );
        tokio::spawn(async move {
            timeout(
                Duration::from_secs(2),
                supervisor.send(Command::UpdateVoiceContext { voice }),
            )
            .await
            .expect("voice rollover should not hang")
        })
    }

    async fn stop(&self) {
        self.supervisor.send(Command::Stop).await.unwrap();
    }

    async fn leave_voice(&self) {
        self.supervisor.send(Command::LeaveVoice).await.unwrap();
    }

    async fn current_voice_context(&self) -> Option<VoiceContext> {
        self.supervisor.current_voice_context().await
    }

    async fn snapshot(&self) -> Snapshot {
        self.supervisor.snapshot().await
    }

    async fn wait_for_voice_reconnecting(&self, expected: bool) -> Snapshot {
        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            let snapshot = self.snapshot().await;
            if snapshot.voice_reconnecting == expected || Instant::now() >= deadline {
                return snapshot;
            }
            sleep(Duration::from_millis(10)).await;
        }
    }

    async fn wait_for_event_snapshot(&self, expected: &str) -> Snapshot {
        let deadline = Instant::now() + Duration::from_secs(2);
        let mut events = self.events.lock().await;
        loop {
            match timeout(Duration::from_millis(100), events.recv()).await {
                Ok(Ok(event)) if event_name(&event) == expected => return self.snapshot().await,
                Ok(Ok(_)) => {}
                Ok(Err(_)) => {}
                Err(_) if Instant::now() >= deadline => {
                    panic!("timed out waiting for event {expected}")
                }
                Err(_) => {}
            }
        }
    }

    async fn resumed_after_position_ms(&self, target_position_ms: u64) -> bool {
        let frame_count_before_rollover = self
            .initial_frame_count_at_rollover
            .lock()
            .await
            .unwrap_or(0);

        if timeout(
            Duration::from_secs(2),
            self.replacement_voice.speaking_observed().notified(),
        )
        .await
        .is_err()
        {
            return false;
        }

        let initial_frame_count_after_rollover =
            self.initial_voice.audio_frame_count_at_least(0).await;
        if initial_frame_count_after_rollover > frame_count_before_rollover + 1 {
            return false;
        }

        let resumed_snapshot = self.wait_for_event_snapshot("track-resolving").await;
        if resumed_snapshot.position_ms < target_position_ms {
            return false;
        }

        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            let snapshot = self.snapshot().await;
            if snapshot.position_ms > target_position_ms {
                return true;
            }
            if Instant::now() >= deadline {
                return false;
            }
            sleep(Duration::from_millis(10)).await;
        }
    }

    async fn wait_for_playback_start(&self) {
        timeout(
            Duration::from_secs(2),
            self.initial_voice.speaking_observed().notified(),
        )
        .await
        .expect("playback should start speaking");
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
