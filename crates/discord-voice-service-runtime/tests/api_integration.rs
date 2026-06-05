use std::sync::Arc;

use discord_voice_service_proto::discordvoice::v1::discord_voice_control_server::DiscordVoiceControl;
use discord_voice_service_proto::discordvoice::v1::{
    GetPlaybackMetricsRequest, GetStateRequest, JoinVoiceRequest, PauseRequest, PlayRequest,
    ResumeRequest, SessionEvent, SessionEventKind, SessionStateSnapshot, SubscribeEventsRequest,
    UpdateVoiceContextRequest, join_voice_request,
};
use discord_voice_service_runtime::{ControlService, Readiness, Supervisor};
use discord_voice_service_test_support::fake_discord::FakeDiscordPeer;
use discord_voice_service_test_support::fake_ytmusic::FakeYtMusic;
use discord_voice_service_test_support::fixtures::spawn_stream_server;
use futures::StreamExt;
use tokio::time::{Duration, Instant};
use tonic::{Code, Request};

#[tokio::test]
async fn play_before_join_voice_returns_failed_precondition() {
    let service = test_service();

    let error = service
        .play(Request::new(PlayRequest {
            video_id: "video123".into(),
        }))
        .await
        .unwrap_err();

    assert_eq!(error.code(), Code::FailedPrecondition);
}

#[tokio::test]
async fn pause_before_join_voice_returns_failed_precondition() {
    let service = test_service();

    let error = service
        .pause(Request::new(PauseRequest {}))
        .await
        .unwrap_err();

    assert_eq!(error.code(), Code::FailedPrecondition);
}

#[tokio::test]
async fn pause_while_resolving_track_returns_failed_precondition() {
    let service = test_service();

    service
        .join_voice(Request::new(JoinVoiceRequest {
            voice: Some(join_voice_request::VoiceContext {
                guild_id: "1".into(),
                channel_id: "2".into(),
                user_id: "user-1".into(),
                session_id: "3".into(),
                endpoint: "voice-placeholder".into(),
                token: "token".into(),
            }),
        }))
        .await
        .unwrap();

    service
        .play(Request::new(PlayRequest {
            video_id: "video123".into(),
        }))
        .await
        .unwrap();

    let error = service
        .pause(Request::new(PauseRequest {}))
        .await
        .unwrap_err();

    assert_eq!(error.code(), Code::FailedPrecondition);
}

#[tokio::test]
async fn join_voice_rejects_empty_required_fields() {
    let service = test_service();

    let cases = [
        join_voice_request::VoiceContext {
            guild_id: String::new(),
            channel_id: "2".into(),
            user_id: "user-1".into(),
            session_id: "3".into(),
            endpoint: "voice-placeholder".into(),
            token: "token".into(),
        },
        join_voice_request::VoiceContext {
            guild_id: "1".into(),
            channel_id: String::new(),
            user_id: "user-1".into(),
            session_id: "3".into(),
            endpoint: "voice-placeholder".into(),
            token: "token".into(),
        },
        join_voice_request::VoiceContext {
            guild_id: "1".into(),
            channel_id: "2".into(),
            user_id: String::new(),
            session_id: "3".into(),
            endpoint: "voice-placeholder".into(),
            token: "token".into(),
        },
        join_voice_request::VoiceContext {
            guild_id: "1".into(),
            channel_id: "2".into(),
            user_id: "user-1".into(),
            session_id: String::new(),
            endpoint: "voice-placeholder".into(),
            token: "token".into(),
        },
        join_voice_request::VoiceContext {
            guild_id: "1".into(),
            channel_id: "2".into(),
            user_id: "user-1".into(),
            session_id: "3".into(),
            endpoint: String::new(),
            token: "token".into(),
        },
        join_voice_request::VoiceContext {
            guild_id: "1".into(),
            channel_id: "2".into(),
            user_id: "user-1".into(),
            session_id: "3".into(),
            endpoint: "voice-placeholder".into(),
            token: String::new(),
        },
    ];

    for voice in cases {
        let error = service
            .join_voice(Request::new(JoinVoiceRequest { voice: Some(voice) }))
            .await
            .unwrap_err();

        assert_eq!(error.code(), Code::InvalidArgument);
    }
}

#[tokio::test]
async fn play_rejects_empty_video_id() {
    let service = test_service();

    let error = service
        .play(Request::new(PlayRequest {
            video_id: String::new(),
        }))
        .await
        .unwrap_err();

    assert_eq!(error.code(), Code::InvalidArgument);
}

#[tokio::test]
async fn resume_after_join_voice_before_play_returns_failed_precondition() {
    let service = test_service();

    service
        .join_voice(Request::new(JoinVoiceRequest {
            voice: Some(join_voice_request::VoiceContext {
                guild_id: "1".into(),
                channel_id: "2".into(),
                user_id: "user-1".into(),
                session_id: "3".into(),
                endpoint: "voice-placeholder".into(),
                token: "token".into(),
            }),
        }))
        .await
        .unwrap();

    let error = service
        .resume(Request::new(ResumeRequest {}))
        .await
        .unwrap_err();

    assert_eq!(error.code(), Code::FailedPrecondition);
}

#[tokio::test]
async fn duplicate_join_voice_returns_failed_precondition_without_clobbering_session() {
    let service = test_service();

    service
        .join_voice(Request::new(JoinVoiceRequest {
            voice: Some(join_voice_request::VoiceContext {
                guild_id: "1".into(),
                channel_id: "2".into(),
                user_id: "user-1".into(),
                session_id: "3".into(),
                endpoint: "voice-placeholder".into(),
                token: "token".into(),
            }),
        }))
        .await
        .unwrap();

    service
        .play(Request::new(PlayRequest {
            video_id: "video123".into(),
        }))
        .await
        .unwrap();

    let error = service
        .join_voice(Request::new(JoinVoiceRequest {
            voice: Some(join_voice_request::VoiceContext {
                guild_id: "9".into(),
                channel_id: "8".into(),
                user_id: "user-9".into(),
                session_id: "7".into(),
                endpoint: "voice.other".into(),
                token: "other-token".into(),
            }),
        }))
        .await
        .unwrap_err();

    assert_eq!(error.code(), Code::FailedPrecondition);

    let snapshot = service.supervisor.snapshot().await;
    assert_eq!(snapshot.guild_id.as_deref(), Some("1"));
    assert_eq!(snapshot.channel_id.as_deref(), Some("2"));
    assert_eq!(snapshot.current_video_id.as_deref(), Some("video123"));
}

#[tokio::test]
async fn subscribe_events_streams_runtime_events() {
    let harness = ApiHarness::spawn().await;
    let response = harness
        .service
        .subscribe_events(Request::new(SubscribeEventsRequest {}))
        .await
        .unwrap();
    let mut stream = response.into_inner();

    harness.join_voice().await.unwrap();
    harness.play("video-1").await.unwrap();

    let first = stream.next().await.transpose().unwrap().unwrap();
    let second = stream.next().await.transpose().unwrap().unwrap();

    assert_eq!(first.kind, SessionEventKind::VoiceConnecting as i32);
    assert_eq!(first.guild_id, "1");
    assert_eq!(first.channel_id, "2");
    assert_eq!(second.kind, SessionEventKind::TrackResolving as i32);
    assert_eq!(second.current_video_id, "video-1");
}

#[tokio::test]
async fn join_voice_then_play_streams_end_to_end_playback_events_and_audio() {
    let fake_yt = FakeYtMusic::spawn().await;
    let http = spawn_stream_server("audio-itag250.webm").await;
    fake_yt.set_playable_url(http.url()).await;
    let fake_discord = FakeDiscordPeer::spawn().await;
    let speaking_observed = fake_discord.speaking_observed();
    let service = test_service_with_ytmusic_endpoint(fake_yt.endpoint()).await;
    let response = service
        .subscribe_events(Request::new(SubscribeEventsRequest {}))
        .await
        .unwrap();
    let mut stream = response.into_inner();

    service
        .join_voice(Request::new(JoinVoiceRequest {
            voice: Some(join_voice_request::VoiceContext {
                guild_id: "1".into(),
                channel_id: "2".into(),
                user_id: "user-1".into(),
                session_id: "session-1".into(),
                endpoint: fake_discord.endpoint(),
                token: "token-1".into(),
            }),
        }))
        .await
        .unwrap();

    service
        .play(Request::new(PlayRequest {
            video_id: "video-1".into(),
        }))
        .await
        .unwrap();

    let events = collect_proto_events(&mut stream, 6).await;
    assert_eq!(events[0].kind, SessionEventKind::VoiceConnecting as i32);
    assert_eq!(events[1].kind, SessionEventKind::VoiceReady as i32);
    assert_eq!(events[2].kind, SessionEventKind::TrackResolving as i32);
    assert_eq!(events[3].kind, SessionEventKind::Buffering as i32);
    assert_eq!(events[3].current_video_id, "video-1");
    assert_eq!(events[3].selected_itag, 250);
    assert_eq!(events[4].kind, SessionEventKind::Playing as i32);
    assert_eq!(events[4].current_video_id, "video-1");
    assert_eq!(events[4].selected_itag, 250);
    assert_eq!(events[5].kind, SessionEventKind::TrackEnded as i32);
    assert_eq!(events[5].current_video_id, "video-1");
    assert_eq!(events[5].selected_itag, 250);

    let state = service
        .get_state(Request::new(GetStateRequest {}))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        state.state,
        discord_voice_service_proto::discordvoice::v1::SessionState::VoiceReadyState as i32
    );
    assert!(state.current_video_id.is_empty());
    assert_eq!(state.selected_itag, 0);
    assert_eq!(state.queue_depth, 0);
    assert!(state.message.is_empty());

    let metrics = wait_for_playback_metrics(&service).await;
    assert!(metrics.available);
    assert!(metrics.ended);
    assert_eq!(metrics.video_id, "video-1");
    assert_eq!(metrics.selected_itag, 250);
    assert!(metrics.track_packet_count >= 5);
    assert_eq!(metrics.buffer_underrun_count, 0);
    assert_eq!(metrics.continuity_silence_packet_count, 0);
    assert_eq!(metrics.inserted_silence_duration_ms, 0);
    assert_eq!(metrics.source_buffer_target_ms, 5_000);
    assert!(metrics.max_source_buffer_depth.unwrap().duration_ms <= 5_000);
    assert_eq!(metrics.adaptive_buffer_target_ms, 5_000);
    assert_eq!(metrics.max_adaptive_buffer_target_ms, 5_000);
    assert_eq!(metrics.rebuffer_count, 0);
    assert!(metrics.sender_lateness.unwrap().samples >= 5);
    assert_eq!(metrics.source_underrun_count, 0);
    assert_eq!(metrics.sender_forbidden_work_count, 0);
    let max_egress_depth = metrics
        .max_egress_buffer_depth
        .as_ref()
        .expect("egress max depth should be published");
    assert_eq!(metrics.egress_buffer_target_ms, 400);
    assert!(max_egress_depth.duration_ms > 0);
    assert!(max_egress_depth.duration_ms <= 500);
    assert_eq!(metrics.prepared_rtp_queue_depth_ms, 0);
    assert_eq!(metrics.playout_builder_prepare_duration.unwrap().samples, 0);
    assert!(metrics.sender_send_duration.unwrap().samples >= 5);
    assert!(metrics.sender_loop_non_send_work_duration.unwrap().max_ms <= 2);
    assert!(metrics.gateway_event_drain_duration.unwrap().samples >= 5);
    assert_eq!(metrics.gateway_event_drain_count, 0);
    assert_eq!(metrics.dave_transition_count, 0);
    assert_eq!(metrics.dave_transition_count_during_playback, 0);
    assert_eq!(metrics.stale_dave_send_prevented_count, 0);
    assert_eq!(metrics.controlled_media_interruption_count, 0);

    let gateway_path = fake_discord.gateway_path().await.unwrap();
    assert_eq!(gateway_path, "/?v=8&encoding=json");
    assert_eq!(fake_discord.discovery_count().await, 1);
    tokio::time::timeout(Duration::from_secs(2), speaking_observed.notified())
        .await
        .expect("speaking should be observed");
    assert!(fake_discord.non_silence_audio_frame_count_at_least(5).await >= 5);
    let first_five_span = fake_discord.non_silence_audio_frame_span_for_first(5).await;
    assert!(
        first_five_span >= Duration::from_millis(70),
        "the first five audio frames should span at least four 20ms pacing intervals: {first_five_span:?}"
    );
}

#[tokio::test]
async fn update_voice_context_is_accepted_during_playback() {
    let harness = ApiHarness::spawn().await;
    harness.join_voice().await.unwrap();
    harness.play("video-1").await.unwrap();
    let rotated = test_voice_context_rotated();

    let result = harness.update_voice_context(rotated.clone()).await;

    assert!(result.is_ok());

    let state = harness.get_state().await;
    assert_eq!(state.guild_id, rotated.guild_id);
    assert_eq!(state.channel_id, rotated.channel_id);
    assert_eq!(state.current_video_id, "video-1");
}

#[tokio::test]
async fn get_state_keeps_message_empty_in_healthy_steady_state() {
    let harness = ApiHarness::spawn().await;
    harness.join_voice().await.unwrap();

    let state = harness.get_state().await;

    assert!(state.message.is_empty());
}

#[tokio::test]
async fn join_voice_then_play_updates_supervisor_snapshot_through_service() {
    let service = test_service();

    service
        .join_voice(Request::new(JoinVoiceRequest {
            voice: Some(join_voice_request::VoiceContext {
                guild_id: "1".into(),
                channel_id: "2".into(),
                user_id: "user-1".into(),
                session_id: "3".into(),
                endpoint: "voice-placeholder".into(),
                token: "token".into(),
            }),
        }))
        .await
        .unwrap();

    service
        .play(Request::new(PlayRequest {
            video_id: "video123".into(),
        }))
        .await
        .unwrap();

    assert_eq!(
        service
            .supervisor
            .snapshot()
            .await
            .current_video_id
            .as_deref(),
        Some("video123")
    );
}

struct ApiHarness {
    service: ControlService,
}

impl ApiHarness {
    async fn spawn() -> Self {
        Self {
            service: test_service(),
        }
    }

    async fn join_voice(&self) -> Result<(), tonic::Status> {
        self.service
            .join_voice(Request::new(JoinVoiceRequest {
                voice: Some(test_voice_context()),
            }))
            .await
            .map(|_| ())
    }

    async fn play(&self, video_id: &str) -> Result<(), tonic::Status> {
        self.service
            .play(Request::new(PlayRequest {
                video_id: video_id.into(),
            }))
            .await
            .map(|_| ())
    }

    async fn update_voice_context(
        &self,
        voice: join_voice_request::VoiceContext,
    ) -> Result<(), tonic::Status> {
        self.service
            .update_voice_context(Request::new(UpdateVoiceContextRequest {
                voice: Some(voice),
            }))
            .await
            .map(|_| ())
    }

    async fn get_state(&self) -> SessionStateSnapshot {
        self.service
            .get_state(Request::new(GetStateRequest {}))
            .await
            .unwrap()
            .into_inner()
    }
}

async fn wait_for_playback_metrics(
    service: &ControlService,
) -> discord_voice_service_proto::discordvoice::v1::PlaybackStabilitySnapshot {
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        let metrics = service
            .get_playback_metrics(Request::new(GetPlaybackMetricsRequest {}))
            .await
            .unwrap()
            .into_inner();
        if metrics.available && metrics.ended {
            return metrics;
        }
        assert!(
            Instant::now() < deadline,
            "playback metrics should become available after TrackEnded"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

fn test_voice_context() -> join_voice_request::VoiceContext {
    join_voice_request::VoiceContext {
        guild_id: "1".into(),
        channel_id: "2".into(),
        user_id: "user-1".into(),
        session_id: "3".into(),
        endpoint: "voice-placeholder".into(),
        token: "token".into(),
    }
}

fn test_service() -> ControlService {
    ControlService {
        supervisor: Supervisor::new(),
        readiness: Arc::new(Readiness::default()),
    }
}

async fn test_service_with_ytmusic_endpoint(endpoint: String) -> ControlService {
    ControlService {
        supervisor: Supervisor::with_ytmusic_endpoint(endpoint).await.unwrap(),
        readiness: Arc::new(Readiness::default()),
    }
}

async fn collect_proto_events<S>(stream: &mut S, count: usize) -> Vec<SessionEvent>
where
    S: futures::Stream<Item = Result<SessionEvent, tonic::Status>> + Unpin,
{
    let deadline = Instant::now() + Duration::from_secs(2);
    let mut events = Vec::with_capacity(count);
    while events.len() < count && Instant::now() < deadline {
        if let Ok(Some(event)) =
            tokio::time::timeout(Duration::from_millis(200), stream.next()).await
        {
            events.push(event.unwrap());
        }
    }
    events
}

fn test_voice_context_rotated() -> join_voice_request::VoiceContext {
    join_voice_request::VoiceContext {
        guild_id: "1".into(),
        channel_id: "9".into(),
        user_id: "user-1".into(),
        session_id: "rotated-session".into(),
        endpoint: "rotated-voice-placeholder".into(),
        token: "rotated-token".into(),
    }
}
