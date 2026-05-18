use discord_voice_service::session::events::SessionEventKind;
use discord_voice_service::session::supervisor::{Command, Supervisor, VoiceContext};

#[tokio::test]
async fn subscribe_receives_join_and_play_events() {
    let supervisor = Supervisor::new();
    let mut rx = supervisor.subscribe_events();

    supervisor
        .send(Command::JoinVoice {
            voice: test_voice_context(),
        })
        .await
        .unwrap();
    assert_eq!(
        supervisor.snapshot().await.state,
        discord_voice_service::session::state::SessionState::VoiceReady
    );
    supervisor
        .send(Command::Play {
            video_id: "abc123".into(),
        })
        .await
        .unwrap();

    let first = rx.recv().await.unwrap();
    let second = rx.recv().await.unwrap();
    assert_eq!(first.kind, SessionEventKind::VoiceReady);
    assert_eq!(second.kind, SessionEventKind::TrackResolving);
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
