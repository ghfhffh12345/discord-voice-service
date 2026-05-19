#[path = "support/fake_discord.rs"]
mod fake_discord;
#[path = "support/fake_ytmusic.rs"]
mod fake_ytmusic;
#[path = "support/fixtures.rs"]
mod fixtures;

use discord_voice_service::session::events::SessionEventKind;
use discord_voice_service::session::state::SessionState;
use discord_voice_service::session::supervisor::{Command, Supervisor};
use tokio::time::{Duration, Instant};

use self::fake_discord::FakeDiscordPeer;
use self::fake_ytmusic::FakeYtMusic;
use self::fixtures::spawn_stream_server;

#[tokio::test]
async fn join_voice_then_play_reaches_connected_runtime_playback_path() {
    let fake_yt = FakeYtMusic::spawn().await;
    let http = spawn_stream_server("tests/fixtures/audio-itag250.webm").await;
    fake_yt.set_playable_url(http.url()).await;
    let fake_voice = FakeDiscordPeer::spawn().await;
    let speaking_observed = fake_voice.speaking_observed();
    let supervisor = Supervisor::with_ytmusic_endpoint(fake_yt.endpoint())
        .await
        .unwrap();
    let mut rx = supervisor.subscribe_events();

    supervisor
        .send(Command::JoinVoice {
            voice: fake_voice.voice_context("1", "2", "session-1", "token-1"),
        })
        .await
        .unwrap();

    supervisor
        .send(Command::Play {
            video_id: "video-1".into(),
        })
        .await
        .unwrap();

    let events = collect_events(&mut rx, 4).await;
    assert_eq!(events[0].kind, SessionEventKind::VoiceReady);
    assert_eq!(events[1].kind, SessionEventKind::TrackResolving);
    assert_eq!(events[2].kind, SessionEventKind::Playing);
    assert_eq!(events[2].current_video_id.as_deref(), Some("video-1"));
    assert_eq!(events[2].selected_itag, Some(250));
    assert_eq!(events[3].kind, SessionEventKind::TrackEnded);
    assert_eq!(events[3].current_video_id.as_deref(), Some("video-1"));
    assert_eq!(events[3].selected_itag, Some(250));

    let snapshot = supervisor.snapshot().await;
    assert_eq!(snapshot.state, SessionState::VoiceReady);
    assert_eq!(snapshot.current_video_id, None);
    assert_eq!(snapshot.selected_itag, None);
    assert_eq!(snapshot.position_ms, 0);

    assert_eq!(fake_voice.discovery_count().await, 1);
    tokio::time::timeout(Duration::from_secs(2), speaking_observed.notified())
        .await
        .expect("speaking should be observed");
    assert!(fake_voice.audio_frame_count_at_least(5).await >= 5);
}

async fn collect_events(
    rx: &mut tokio::sync::broadcast::Receiver<
        discord_voice_service::session::events::SessionEventRecord,
    >,
    count: usize,
) -> Vec<discord_voice_service::session::events::SessionEventRecord> {
    let deadline = Instant::now() + Duration::from_secs(2);
    let mut events = Vec::with_capacity(count);
    while events.len() < count && Instant::now() < deadline {
        if let Ok(event) = tokio::time::timeout(Duration::from_millis(200), rx.recv()).await {
            events.push(event.unwrap());
        }
    }
    events
}
