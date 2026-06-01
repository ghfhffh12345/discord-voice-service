use std::sync::Arc;

use discord_voice_service_proto::discordvoice::v1::discord_voice_control_server::DiscordVoiceControl;
use discord_voice_service_proto::discordvoice::v1::{
    SessionEvent, SessionEventKind, SubscribeEventsRequest,
};
use discord_voice_service_runtime::{Command, ControlService, Readiness, SessionState, Supervisor};
use discord_voice_service_test_support::fake_discord::FakeDiscordPeer;
use discord_voice_service_test_support::fake_ytmusic::FakeYtMusic;
use discord_voice_service_test_support::fixtures::spawn_stream_server;
use futures::StreamExt;
use tokio::time::{Duration, Instant};
use tonic::Request;

const SERVICE_USER_ID: &str = "1111111111111111";
#[tokio::test]
async fn join_voice_then_play_reaches_connected_runtime_playback_path() {
    let fake_yt = FakeYtMusic::spawn().await;
    let http = spawn_stream_server("audio-itag250.webm").await;
    fake_yt.set_playable_url(http.url()).await;
    let fake_voice = FakeDiscordPeer::spawn().await;
    let speaking_observed = fake_voice.speaking_observed();
    let supervisor = Supervisor::with_ytmusic_endpoint(fake_yt.endpoint())
        .await
        .unwrap();
    let mut stream = subscribe_events(supervisor.clone()).await;

    supervisor
        .send(Command::JoinVoice {
            voice: fake_voice.voice_context("1", "2", "user-1", "session-1", "token-1"),
        })
        .await
        .unwrap();

    supervisor
        .send(Command::Play {
            video_id: "video-1".into(),
        })
        .await
        .unwrap();

    let startup_events = collect_events(&mut stream, 4).await;
    assert_eq!(startup_events[0].kind, SessionEventKind::VoiceReady as i32);
    assert_eq!(
        startup_events[1].kind,
        SessionEventKind::TrackResolving as i32
    );
    assert_eq!(startup_events[2].kind, SessionEventKind::Buffering as i32);
    assert_eq!(startup_events[2].current_video_id, "video-1");
    assert_eq!(startup_events[2].selected_itag, 250);
    assert_eq!(startup_events[3].kind, SessionEventKind::Playing as i32);
    assert_eq!(startup_events[3].current_video_id, "video-1");
    assert_eq!(startup_events[3].selected_itag, 250);

    tokio::time::timeout(Duration::from_secs(2), speaking_observed.notified())
        .await
        .expect("speaking should be observed");

    let ending_events = collect_events(&mut stream, 1).await;
    assert_eq!(ending_events[0].kind, SessionEventKind::TrackEnded as i32);
    assert_eq!(ending_events[0].current_video_id, "video-1");
    assert_eq!(ending_events[0].selected_itag, 250);

    assert!(fake_voice.audio_frame_count_at_least(5).await >= 5);
    assert!(
        fake_voice.audio_frame_span_for_first(5).await >= Duration::from_millis(70),
        "the first five audio frames should span at least four 20ms pacing intervals"
    );
    assert_eq!(fake_voice.discovery_count().await, 1);

    let snapshot = supervisor.snapshot().await;
    assert_eq!(snapshot.state, SessionState::VoiceReady);
    assert_eq!(snapshot.current_video_id, None);
    assert_eq!(snapshot.selected_itag, None);
    assert_eq!(snapshot.queue_depth, 0);
    assert_eq!(snapshot.position_ms, 0);
}

#[tokio::test]
async fn join_voice_accepts_self_only_pending_initial_dave_session() {
    let fake_voice = FakeDiscordPeer::spawn_with_dave_self_only_no_proposals().await;
    let supervisor = Supervisor::new();
    let mut stream = subscribe_events(supervisor.clone()).await;

    supervisor
        .send(Command::JoinVoice {
            voice: fake_voice.voice_context("1", "2", SERVICE_USER_ID, "session-1", "token-1"),
        })
        .await
        .unwrap();

    let startup_events = collect_events(&mut stream, 1).await;
    assert_eq!(startup_events[0].kind, SessionEventKind::VoiceReady as i32);
    assert_eq!(supervisor.snapshot().await.state, SessionState::VoiceReady);
    assert!(fake_voice.saw_dave_key_package_before_prepare_epoch().await);
    assert!(
        !fake_voice
            .saw_dave_commit_welcome_within(Duration::from_millis(100))
            .await
    );
}

#[tokio::test]
async fn play_handles_queued_replayed_established_join_welcome_before_first_audio_frame() {
    let fake_yt = FakeYtMusic::spawn().await;
    let http = spawn_stream_server("audio-itag250.webm").await;
    fake_yt.set_playable_url(http.url()).await;
    let fake_voice = FakeDiscordPeer::spawn_with_established_dave_group().await;
    let speaking_observed = fake_voice.speaking_observed();
    let supervisor = Supervisor::with_ytmusic_endpoint(fake_yt.endpoint())
        .await
        .unwrap();
    let mut stream = subscribe_events(supervisor.clone()).await;

    supervisor
        .send(Command::JoinVoice {
            voice: fake_voice.voice_context("1", "2", SERVICE_USER_ID, "session-1", "token-1"),
        })
        .await
        .unwrap();

    let startup_events = collect_events(&mut stream, 1).await;
    assert_eq!(startup_events[0].kind, SessionEventKind::VoiceReady as i32);

    fake_voice
        .replay_established_join_welcome_transition()
        .await
        .unwrap();

    supervisor
        .send(Command::Play {
            video_id: "video-1".into(),
        })
        .await
        .unwrap();

    let playback_events = collect_events(&mut stream, 3).await;
    assert_eq!(
        playback_events[0].kind,
        SessionEventKind::TrackResolving as i32
    );
    assert_eq!(playback_events[1].kind, SessionEventKind::Buffering as i32);
    assert_eq!(playback_events[2].kind, SessionEventKind::Playing as i32);

    tokio::time::timeout(Duration::from_secs(2), speaking_observed.notified())
        .await
        .expect("speaking should be observed");
    assert!(fake_voice.audio_frame_count_at_least(2).await >= 2);

    let ending_events = collect_events(&mut stream, 1).await;
    assert_eq!(ending_events[0].kind, SessionEventKind::TrackEnded as i32);
}

#[tokio::test]
async fn play_handles_queued_replayed_local_init_creator_commit_before_first_audio_frame() {
    let fake_yt = FakeYtMusic::spawn().await;
    let http = spawn_stream_server("audio-itag250.webm").await;
    fake_yt.set_playable_url(http.url()).await;
    let fake_voice =
        FakeDiscordPeer::spawn_with_dave_queued_init_prepare_commit_until_control().await;
    let speaking_observed = fake_voice.speaking_observed();
    let supervisor = Supervisor::with_ytmusic_endpoint(fake_yt.endpoint())
        .await
        .unwrap();
    let mut stream = subscribe_events(supervisor.clone()).await;

    supervisor
        .send(Command::JoinVoice {
            voice: fake_voice.voice_context("1", "2", SERVICE_USER_ID, "session-1", "token-1"),
        })
        .await
        .unwrap();

    let startup_events = collect_events(&mut stream, 1).await;
    assert_eq!(startup_events[0].kind, SessionEventKind::VoiceReady as i32);

    fake_voice
        .replay_local_init_creator_commit_transition()
        .await
        .unwrap();

    supervisor
        .send(Command::Play {
            video_id: "video-1".into(),
        })
        .await
        .unwrap();

    let playback_events = collect_events(&mut stream, 3).await;
    assert_eq!(
        playback_events[0].kind,
        SessionEventKind::TrackResolving as i32
    );
    assert_eq!(playback_events[1].kind, SessionEventKind::Buffering as i32);
    assert_eq!(playback_events[2].kind, SessionEventKind::Playing as i32);

    tokio::time::timeout(Duration::from_secs(2), speaking_observed.notified())
        .await
        .expect("speaking should be observed");
    assert!(fake_voice.audio_frame_count_at_least(2).await >= 2);

    let ending_events = collect_events(&mut stream, 1).await;
    assert_eq!(ending_events[0].kind, SessionEventKind::TrackEnded as i32);
}

#[tokio::test]
async fn stop_interrupts_in_flight_playback_without_track_ended() {
    let fake_yt = FakeYtMusic::spawn().await;
    let http = spawn_stream_server("audio-itag250.webm").await;
    fake_yt.set_playable_url(http.url()).await;
    let fake_voice = FakeDiscordPeer::spawn().await;
    let speaking_observed = fake_voice.speaking_observed();
    let supervisor = Supervisor::with_ytmusic_endpoint(fake_yt.endpoint())
        .await
        .unwrap();
    let mut stream = subscribe_events(supervisor.clone()).await;

    supervisor
        .send(Command::JoinVoice {
            voice: fake_voice.voice_context("1", "2", "user-1", "session-1", "token-1"),
        })
        .await
        .unwrap();

    let play_supervisor = supervisor.clone();
    let play_task = tokio::spawn(async move {
        play_supervisor
            .send(Command::Play {
                video_id: "video-1".into(),
            })
            .await
    });

    let startup_events = collect_events(&mut stream, 4).await;
    assert_eq!(startup_events[0].kind, SessionEventKind::VoiceReady as i32);
    assert_eq!(
        startup_events[1].kind,
        SessionEventKind::TrackResolving as i32
    );
    assert_eq!(startup_events[2].kind, SessionEventKind::Buffering as i32);
    assert_eq!(startup_events[3].kind, SessionEventKind::Playing as i32);

    tokio::time::timeout(Duration::from_secs(2), speaking_observed.notified())
        .await
        .expect("speaking should be observed");
    assert!(fake_voice.audio_frame_count_at_least(1).await >= 1);

    supervisor.send(Command::Stop).await.unwrap();
    play_task.await.unwrap().unwrap();

    let follow_up_events = collect_events(&mut stream, 2).await;
    assert_eq!(follow_up_events.len(), 1);
    assert_eq!(follow_up_events[0].kind, SessionEventKind::Stopped as i32);
    assert!(
        follow_up_events
            .iter()
            .all(|event| event.kind != SessionEventKind::TrackEnded as i32)
    );

    let snapshot = supervisor.snapshot().await;
    assert_eq!(snapshot.state, SessionState::VoiceReady);
    assert_eq!(snapshot.current_video_id, None);
    assert_eq!(snapshot.selected_itag, None);
    assert_eq!(snapshot.position_ms, 0);
}

#[tokio::test]
async fn stop_during_resolution_prevents_canceled_track_state_from_reappearing() {
    let fake_yt = FakeYtMusic::spawn().await;
    fake_yt.set_decipher_delay(Duration::from_millis(300)).await;
    let http = spawn_stream_server("audio-itag250.webm").await;
    fake_yt.set_playable_url(http.url()).await;
    let fake_voice = FakeDiscordPeer::spawn().await;
    let speaking_observed = fake_voice.speaking_observed();
    let speaking_notified = speaking_observed.notified();
    let supervisor = Supervisor::with_ytmusic_endpoint(fake_yt.endpoint())
        .await
        .unwrap();
    let mut stream = subscribe_events(supervisor.clone()).await;

    supervisor
        .send(Command::JoinVoice {
            voice: fake_voice.voice_context("1", "2", "user-1", "session-1", "token-1"),
        })
        .await
        .unwrap();

    let play_supervisor = supervisor.clone();
    let play_task = tokio::spawn(async move {
        play_supervisor
            .send(Command::Play {
                video_id: "video-1".into(),
            })
            .await
    });

    let startup_events = collect_events(&mut stream, 2).await;
    assert_eq!(startup_events[0].kind, SessionEventKind::VoiceReady as i32);
    assert_eq!(
        startup_events[1].kind,
        SessionEventKind::TrackResolving as i32
    );
    assert_eq!(startup_events[1].current_video_id, "video-1");

    let stop_started = Instant::now();
    supervisor.send(Command::Stop).await.unwrap();
    let stop_elapsed = stop_started.elapsed();
    play_task.await.unwrap().unwrap();

    assert!(
        stop_elapsed < Duration::from_millis(150),
        "stop should not wait on slow playback preparation I/O: {stop_elapsed:?}"
    );

    let follow_up_events = collect_events(&mut stream, 4).await;
    assert_eq!(follow_up_events.len(), 1);
    assert_eq!(follow_up_events[0].kind, SessionEventKind::Stopped as i32);
    assert!(follow_up_events.iter().all(|event| {
        event.kind != SessionEventKind::TrackResolving as i32
            && event.kind != SessionEventKind::Buffering as i32
            && event.kind != SessionEventKind::Playing as i32
            && event.kind != SessionEventKind::TrackEnded as i32
    }));
    assert!(
        tokio::time::timeout(Duration::from_millis(100), speaking_notified)
            .await
            .is_err(),
        "pre-start stop should not emit a speaking update"
    );
    assert_eq!(fake_voice.audio_frame_count_at_least(0).await, 0);

    let snapshot = supervisor.snapshot().await;
    assert_eq!(snapshot.state, SessionState::VoiceReady);
    assert_eq!(snapshot.current_video_id, None);
    assert_eq!(snapshot.selected_itag, None);
    assert_eq!(snapshot.queue_depth, 0);
    assert_eq!(snapshot.position_ms, 0);
}

#[tokio::test]
async fn stop_then_replay_same_video_reaches_track_ended_again() {
    let fake_yt = FakeYtMusic::spawn().await;
    let http = spawn_stream_server("audio-itag250.webm").await;
    fake_yt.set_playable_url(http.url()).await;
    let fake_voice = FakeDiscordPeer::spawn().await;
    let speaking_observed = fake_voice.speaking_observed();
    let supervisor = Supervisor::with_ytmusic_endpoint(fake_yt.endpoint())
        .await
        .unwrap();
    let mut stream = subscribe_events(supervisor.clone()).await;

    supervisor
        .send(Command::JoinVoice {
            voice: fake_voice.voice_context("1", "2", "user-1", "session-1", "token-1"),
        })
        .await
        .unwrap();

    let interrupted_play = supervisor.clone();
    let play_task = tokio::spawn(async move {
        interrupted_play
            .send(Command::Play {
                video_id: "video-1".into(),
            })
            .await
    });

    let startup_events = collect_events(&mut stream, 4).await;
    assert_eq!(startup_events[0].kind, SessionEventKind::VoiceReady as i32);
    assert_eq!(
        startup_events[1].kind,
        SessionEventKind::TrackResolving as i32
    );
    assert_eq!(startup_events[2].kind, SessionEventKind::Buffering as i32);
    assert_eq!(startup_events[3].kind, SessionEventKind::Playing as i32);

    tokio::time::timeout(Duration::from_secs(2), speaking_observed.notified())
        .await
        .expect("speaking should be observed");
    assert!(fake_voice.audio_frame_count_at_least(1).await >= 1);

    supervisor.send(Command::Stop).await.unwrap();
    play_task.await.unwrap().unwrap();

    let stop_events = collect_events(&mut stream, 2).await;
    assert_eq!(stop_events.len(), 1);
    assert_eq!(stop_events[0].kind, SessionEventKind::Stopped as i32);

    supervisor
        .send(Command::Play {
            video_id: "video-1".into(),
        })
        .await
        .unwrap();

    let replay_events = collect_events(&mut stream, 4).await;
    assert_eq!(
        replay_events[0].kind,
        SessionEventKind::TrackResolving as i32
    );
    assert_eq!(replay_events[1].kind, SessionEventKind::Buffering as i32);
    assert_eq!(replay_events[2].kind, SessionEventKind::Playing as i32);
    assert_eq!(replay_events[3].kind, SessionEventKind::TrackEnded as i32);
}

async fn subscribe_events(
    supervisor: Supervisor,
) -> <ControlService as DiscordVoiceControl>::SubscribeEventsStream {
    ControlService {
        supervisor,
        readiness: Arc::new(Readiness::default()),
    }
    .subscribe_events(Request::new(SubscribeEventsRequest {}))
    .await
    .unwrap()
    .into_inner()
}

async fn collect_events<S>(stream: &mut S, count: usize) -> Vec<SessionEvent>
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
