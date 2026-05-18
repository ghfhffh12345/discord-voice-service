use discord_voice_service::api::service::{ControlService, map_play_request};
use discord_voice_service::proto::discordvoice::v1::discord_voice_control_server::DiscordVoiceControl;
use discord_voice_service::proto::discordvoice::v1::{
    JoinVoiceRequest, PauseRequest, PlayRequest, join_voice_request,
};
use discord_voice_service::session::supervisor::Supervisor;
use tonic::{Code, Request};

#[test]
fn maps_proto_play_request_into_internal_video_id() {
    let request = PlayRequest {
        video_id: "video123".into(),
    };

    assert_eq!(map_play_request(request), "video123");
}

#[tokio::test]
async fn play_before_join_voice_returns_failed_precondition() {
    let service = ControlService {
        supervisor: Supervisor::new(),
    };

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
    let service = ControlService {
        supervisor: Supervisor::new(),
    };

    let error = service
        .pause(Request::new(PauseRequest {}))
        .await
        .unwrap_err();

    assert_eq!(error.code(), Code::FailedPrecondition);
}

#[tokio::test]
async fn join_voice_then_play_updates_supervisor_snapshot_through_service() {
    let service = ControlService {
        supervisor: Supervisor::new(),
    };

    service
        .join_voice(Request::new(JoinVoiceRequest {
            voice: Some(join_voice_request::VoiceContext {
                guild_id: "1".into(),
                channel_id: "2".into(),
                session_id: "3".into(),
                endpoint: "voice.example".into(),
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
