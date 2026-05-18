use discord_voice_service::proto::discordvoice::v1::join_voice_request::VoiceContext;
use discord_voice_service::proto::discordvoice::v1::{PlayRequest, SessionEvent, SessionEventKind};

#[test]
fn generated_control_messages_expose_expected_fields() {
    let request = PlayRequest {
        video_id: "dQw4w9WgXcQ".into(),
    };
    assert_eq!(request.video_id, "dQw4w9WgXcQ");

    let context = VoiceContext {
        guild_id: "1".into(),
        channel_id: "2".into(),
        session_id: "abc".into(),
        endpoint: "voice.example.discord.gg".into(),
        token: "token".into(),
    };
    assert_eq!(context.guild_id, "1");

    let event = SessionEvent {
        kind: SessionEventKind::VoiceReady as i32,
        message: "ready".into(),
        ..Default::default()
    };
    assert_eq!(event.kind, SessionEventKind::VoiceReady as i32);
}
