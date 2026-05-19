#[path = "support/fake_voice.rs"]
mod fake_voice;

use discord_voice_service::session::state::SessionState;
use discord_voice_service::session::supervisor::{Command, Supervisor, VoiceContext};

use self::fake_voice::FakeVoiceEndpoint;

#[tokio::test]
async fn join_then_play_advances_state_machine() {
    let supervisor = Supervisor::new();

    supervisor
        .send(Command::JoinVoice {
            voice: test_voice_context(),
        })
        .await
        .unwrap();

    assert_eq!(
        supervisor.snapshot().await.state,
        SessionState::ConnectingVoice
    );

    supervisor
        .send(Command::Play {
            video_id: "abc123".into(),
        })
        .await
        .unwrap();

    assert_eq!(
        supervisor.snapshot().await.state,
        SessionState::ResolvingTrack
    );
}

#[tokio::test]
async fn snapshot_exposes_runtime_position_queue_depth_and_rollover_flags() {
    let supervisor = Supervisor::new();
    let snapshot = supervisor.snapshot().await;

    assert_eq!(snapshot.queue_depth, 0);
    assert_eq!(snapshot.position_ms, 0);
    assert!(!snapshot.recovering);
    assert!(!snapshot.voice_reconnecting);
}

#[tokio::test]
async fn update_voice_context_replaces_full_runtime_voice_context() {
    let supervisor = Supervisor::new();
    let fake = FakeVoiceEndpoint::spawn().await;
    let joined = test_voice_context();
    let updated = fake.voice_context("1", "9", "updated-session", "updated-token");

    supervisor
        .send(Command::JoinVoice {
            voice: joined.clone(),
        })
        .await
        .unwrap();
    supervisor
        .send(Command::UpdateVoiceContext {
            voice: updated.clone(),
        })
        .await
        .unwrap();

    assert_eq!(supervisor.current_voice_context().await, Some(updated));
    assert_eq!(supervisor.snapshot().await.channel_id, Some("9".into()));
    assert_ne!(supervisor.current_voice_context().await, Some(joined));
    assert_eq!(fake.discovery_count().await, 1);
    assert!(fake.gateway_connected().await);
    assert!(fake.gateway_path().await.unwrap().contains("v=8"));
}

#[tokio::test]
async fn update_voice_context_failure_keeps_snapshot_and_runtime_on_previous_voice() {
    let supervisor = Supervisor::new();
    let fake = FakeVoiceEndpoint::spawn().await;
    let joined = fake.voice_context("1", "2", "3", "token");
    let invalid = VoiceContext {
        guild_id: "1".into(),
        channel_id: "9".into(),
        session_id: "broken-session".into(),
        endpoint: "ws://127.0.0.1:1".into(),
        token: "broken-token".into(),
    };

    supervisor
        .send(Command::JoinVoice {
            voice: joined.clone(),
        })
        .await
        .unwrap();

    let err = supervisor
        .send(Command::UpdateVoiceContext {
            voice: invalid.clone(),
        })
        .await
        .unwrap_err();

    assert!(err.to_string().contains("voice"));
    assert_eq!(
        supervisor.current_voice_context().await,
        Some(joined.clone())
    );

    let snapshot = supervisor.snapshot().await;
    assert_eq!(snapshot.guild_id, Some(joined.guild_id.clone()));
    assert_eq!(snapshot.channel_id, Some(joined.channel_id.clone()));
    assert!(!snapshot.voice_reconnecting);
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
