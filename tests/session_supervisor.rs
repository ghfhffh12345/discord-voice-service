use discord_voice_service::session::state::SessionState;
use discord_voice_service::session::supervisor::{Command, Supervisor};

#[tokio::test]
async fn join_then_play_advances_state_machine() {
    let supervisor = Supervisor::new();

    supervisor
        .send(Command::JoinVoice {
            guild_id: "1".into(),
            channel_id: "2".into(),
            session_id: "3".into(),
            endpoint: "voice.example".into(),
            token: "token".into(),
        })
        .await
        .unwrap();

    assert_eq!(supervisor.snapshot().await.state, SessionState::VoiceReady);

    supervisor
        .send(Command::Play {
            video_id: "abc123".into(),
        })
        .await
        .unwrap();

    assert_eq!(supervisor.snapshot().await.state, SessionState::ResolvingTrack);
}
