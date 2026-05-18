use discord_voice_service::session::state::SessionState;
use discord_voice_service::session::supervisor::{Command, Supervisor, VoiceContext};

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
    let joined = test_voice_context();
    let updated = VoiceContext {
        guild_id: "1".into(),
        channel_id: "9".into(),
        session_id: "updated-session".into(),
        endpoint: "updated.voice.example".into(),
        token: "updated-token".into(),
    };

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
