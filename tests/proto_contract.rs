use discord_voice_service::proto::discordvoice::v1::join_voice_request::VoiceContext;
use discord_voice_service::proto::discordvoice::v1::{
    PlayRequest, SessionEvent, SessionEventKind, SessionEventReason, UpdateVoiceContextRequest,
};

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
        reason: SessionEventReason::JoinTimeout as i32,
        message: "ready".into(),
        ..Default::default()
    };
    assert_eq!(event.kind, SessionEventKind::VoiceReady as i32);
    assert_eq!(event.reason, SessionEventReason::JoinTimeout as i32);

    let update = UpdateVoiceContextRequest {
        voice: Some(context),
    };
    assert_eq!(update.voice.as_ref().unwrap().guild_id, "1");
}

#[test]
fn control_proto_exposes_update_voice_context_and_reason_codes() {
    let proto = std::fs::read_to_string("proto/discordvoice/v1/control.proto").unwrap();
    assert!(proto.contains("rpc UpdateVoiceContext"));
    assert!(proto.contains("enum SessionEventReason"));
    assert!(proto.contains("VOICE_RECONNECTING"));
    assert!(proto.contains("JOIN_TIMEOUT"));
}
