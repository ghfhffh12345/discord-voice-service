use std::sync::Arc;

use discord_voice_service_proto::discordvoice::v1::discord_voice_control_server::DiscordVoiceControl;
use discord_voice_service_proto::discordvoice::v1::{SessionEventKind, SubscribeEventsRequest};
use discord_voice_service_runtime::{Command, ControlService, Readiness, Supervisor, VoiceContext};
use futures::StreamExt;
use tonic::Request;

#[tokio::test]
async fn subscribe_receives_join_and_play_events() {
    let supervisor = Supervisor::new();
    let service = ControlService {
        supervisor: supervisor.clone(),
        readiness: Arc::new(Readiness::default()),
    };
    let response = service
        .subscribe_events(Request::new(SubscribeEventsRequest {}))
        .await
        .unwrap();
    let mut stream = response.into_inner();

    supervisor
        .send(Command::JoinVoice {
            voice: test_voice_context(),
        })
        .await
        .unwrap();
    assert_eq!(
        supervisor.snapshot().await.state,
        discord_voice_service_runtime::SessionState::ConnectingVoice
    );
    supervisor
        .send(Command::Play {
            video_id: "abc123".into(),
        })
        .await
        .unwrap();

    let first = stream.next().await.transpose().unwrap().unwrap();
    let second = stream.next().await.transpose().unwrap().unwrap();
    assert_eq!(first.kind, SessionEventKind::VoiceConnecting as i32);
    assert_eq!(second.kind, SessionEventKind::TrackResolving as i32);
}

fn test_voice_context() -> VoiceContext {
    VoiceContext {
        guild_id: "1".into(),
        channel_id: "2".into(),
        user_id: "user-1".into(),
        session_id: "3".into(),
        endpoint: "voice-placeholder".into(),
        token: "token".into(),
    }
}
