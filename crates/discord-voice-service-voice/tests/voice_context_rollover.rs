use std::pin::Pin;
use std::sync::Arc;

use discord_voice_service_proto::discordvoice::v1::discord_voice_control_server::DiscordVoiceControl;
use discord_voice_service_proto::discordvoice::v1::{
    SessionEvent, SessionEventKind as ProtoSessionEventKind, SubscribeEventsRequest,
};
use discord_voice_service_runtime::{
    Command, ControlService, Readiness, RuntimeError, SessionState, Snapshot, Supervisor,
    VoiceContext,
};
use discord_voice_service_test_support::fake_discord::FakeDiscordPeer;
use discord_voice_service_test_support::fake_voice::FakeVoiceEndpoint;
use discord_voice_service_test_support::fake_ytmusic::FakeYtMusic;
use discord_voice_service_test_support::fixtures::{
    spawn_stream_server, spawn_stream_server_with_status_after_requests,
};
use futures::StreamExt;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio::time::{Duration, Instant, sleep, timeout};
use tonic::Request;

const SERVICE_USER_ID: &str = "1111111111111111";

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
    assert!(matches!(err, RuntimeError::Voice(_)));

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
async fn update_voice_context_matching_current_context_resumes_active_playback() {
    let harness = RolloverHarness::spawn().await;

    harness.play_until_position_ms(2_000).await;
    harness
        .supervisor
        .send(Command::UpdateVoiceContext {
            voice: harness
                .initial_voice
                .voice_context("1", "2", SERVICE_USER_ID, "3", "token"),
        })
        .await
        .unwrap();

    harness.wait_for_event_snapshot("voice-reconnecting").await;
    let ready_snapshot = harness.wait_for_event_snapshot("voice-reconnected").await;
    assert_eq!(ready_snapshot.current_video_id.as_deref(), Some("video-1"));
    let playing_snapshot = harness.wait_for_event_snapshot("playing").await;
    assert_eq!(
        playing_snapshot.current_video_id.as_deref(),
        Some("video-1")
    );
    harness.play_until_position_ms(2_200).await;
}

#[tokio::test]
async fn update_voice_context_settles_replacement_initial_dave_before_resuming() {
    let stream = spawn_stream_server("audio-long.webm").await;
    let replacement_voice = FakeDiscordPeer::spawn_with_dave().await;
    let harness = RolloverHarness::spawn_with_stream_url_and_replacement_voice(
        stream.url(),
        replacement_voice,
    )
    .await;

    harness.play_until_position_ms(2_000).await;
    harness.update_voice_context().await;

    assert!(harness.replacement_voice.saw_dave_prepare_epoch().await);
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

    assert!(matches!(err, RuntimeError::Voice(_)));
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
        "audio-long.webm",
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
    events: Mutex<EventStream>,
}

impl VoiceRolloverHarness {
    async fn spawn() -> Self {
        let supervisor = Supervisor::new();
        let events = subscribe_events(supervisor.clone()).await;
        Self {
            supervisor,
            events: Mutex::new(events),
        }
    }

    async fn start_playing(&self, voice: VoiceContext, video_id: &str) -> Result<(), RuntimeError> {
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

    async fn update_voice_context(&self, voice: VoiceContext) -> Result<(), RuntimeError> {
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
            match timeout(Duration::from_millis(10), events.next()).await {
                Ok(Some(Ok(event))) if event_name(&event) == name => return true,
                Ok(Some(Ok(_))) => continue,
                Ok(Some(Err(_))) => continue,
                Ok(None) => return false,
                Err(_) => return false,
            }
        }
    }

    async fn wait_for_event_snapshot(&self, expected: &str) -> Snapshot {
        let deadline = Instant::now() + Duration::from_secs(2);
        let mut events = self.events.lock().await;
        loop {
            match timeout(Duration::from_millis(100), events.next()).await {
                Ok(Some(Ok(event))) if event_name(&event) == expected => {
                    return self.snapshot().await;
                }
                Ok(Some(Ok(_))) => {}
                Ok(Some(Err(_))) => {}
                Ok(None) => {}
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
    events: Mutex<EventStream>,
    initial_voice: FakeDiscordPeer,
    replacement_voice: FakeDiscordPeer,
    initial_frame_count_at_rollover: Mutex<Option<usize>>,
}

impl RolloverHarness {
    async fn spawn() -> Self {
        let stream = spawn_stream_server("audio-long.webm").await;
        Self::spawn_with_stream_url(stream.url()).await
    }

    async fn spawn_with_stream_url(stream_url: String) -> Self {
        let replacement_voice =
            FakeDiscordPeer::spawn_with_gateway_delay(Duration::from_millis(250)).await;
        Self::spawn_with_stream_url_and_replacement_voice(stream_url, replacement_voice).await
    }

    async fn spawn_with_stream_url_and_replacement_voice(
        stream_url: String,
        replacement_voice: FakeDiscordPeer,
    ) -> Self {
        let fake_yt = FakeYtMusic::spawn().await;
        fake_yt.set_playable_url(stream_url).await;

        let initial_voice = FakeDiscordPeer::spawn().await;
        let supervisor = Supervisor::with_ytmusic_endpoint(fake_yt.endpoint())
            .await
            .unwrap();
        let events = subscribe_events(supervisor.clone()).await;

        supervisor
            .send(Command::JoinVoice {
                voice: initial_voice.voice_context("1", "2", SERVICE_USER_ID, "3", "token"),
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

    async fn start_rollover(&self) -> JoinHandle<Result<(), RuntimeError>> {
        let frame_count = self.initial_voice.audio_frame_count_at_least(0).await;
        *self.initial_frame_count_at_rollover.lock().await = Some(frame_count);
        let supervisor = self.supervisor.clone();
        let voice = self.replacement_voice.voice_context(
            "1",
            "9",
            SERVICE_USER_ID,
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
            match timeout(Duration::from_millis(100), events.next()).await {
                Ok(Some(Ok(event))) if event_name(&event) == expected => {
                    return self.snapshot().await;
                }
                Ok(Some(Ok(_))) => {}
                Ok(Some(Err(_))) => {}
                Ok(None) => {}
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

type EventStream =
    Pin<Box<dyn futures::Stream<Item = Result<SessionEvent, tonic::Status>> + Send + 'static>>;

async fn subscribe_events(supervisor: Supervisor) -> EventStream {
    ControlService {
        supervisor,
        readiness: Arc::new(Readiness::default()),
    }
    .subscribe_events(Request::new(SubscribeEventsRequest {}))
    .await
    .unwrap()
    .into_inner()
}

fn event_name(event: &SessionEvent) -> &'static str {
    match event.kind {
        kind if kind == ProtoSessionEventKind::VoiceConnecting as i32 => "voice-connecting",
        kind if kind == ProtoSessionEventKind::VoiceReady as i32 => "voice-reconnected",
        kind if kind == ProtoSessionEventKind::TrackResolving as i32 => "track-resolving",
        kind if kind == ProtoSessionEventKind::Buffering as i32 => "buffering",
        kind if kind == ProtoSessionEventKind::Playing as i32 => "playing",
        kind if kind == ProtoSessionEventKind::Paused as i32 => "paused",
        kind if kind == ProtoSessionEventKind::Stopped as i32 => "stopped",
        kind if kind == ProtoSessionEventKind::TrackEnded as i32 => "track-ended",
        kind if kind == ProtoSessionEventKind::PlaybackInterrupted as i32 => "playback-interrupted",
        kind if kind == ProtoSessionEventKind::RecoverableWarning as i32 => "recoverable-warning",
        kind if kind == ProtoSessionEventKind::FatalError as i32 => "fatal-error",
        kind if kind == ProtoSessionEventKind::VoiceReconnecting as i32 => "voice-reconnecting",
        _ => "unknown",
    }
}
