use discord_voice_service::api::service::map_play_request;
use discord_voice_service::proto::discordvoice::v1::PlayRequest;
use discord_voice_service::session::supervisor::{Command, Supervisor};

#[test]
fn maps_proto_play_request_into_internal_video_id() {
    let request = PlayRequest {
        video_id: "video123".into(),
    };

    assert_eq!(map_play_request(request), "video123");
}

#[tokio::test]
async fn play_request_moves_supervisor_into_resolving_track() {
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

    supervisor
        .send(Command::Play {
            video_id: "video123".into(),
        })
        .await
        .unwrap();

    assert_eq!(
        supervisor.snapshot().await.current_video_id.as_deref(),
        Some("video123")
    );
}
