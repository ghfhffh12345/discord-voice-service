use discord_voice_service_twilight::{
    SessionEvent, SessionEventKind, SessionEventReason, SessionState, StateSnapshot, VoiceContext,
    VoiceContextTracker, join_voice_channel, leave_voice_channel, proto,
};
use twilight_gateway::Event;
use twilight_model::{
    gateway::payload::incoming::{VoiceServerUpdate, VoiceStateUpdate},
    id::{
        Id,
        marker::{ChannelMarker, GuildMarker, UserMarker},
    },
    voice::VoiceState,
};

#[test]
fn bundled_proto_matches_workspace_contract_when_available() {
    let manifest_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let workspace_proto = manifest_dir
        .parent()
        .expect("crate should be inside crates/")
        .join("discord-voice-service-proto/proto/discordvoice/v1/control.proto");

    if workspace_proto.exists() {
        let bundled =
            std::fs::read_to_string(manifest_dir.join("proto/discordvoice/v1/control.proto"))
                .unwrap();
        let workspace = std::fs::read_to_string(workspace_proto).unwrap();

        assert_eq!(bundled, workspace);
    }
}

#[test]
fn voice_context_uses_twilight_ids_and_proto_strings() {
    let context = VoiceContext::new(
        Id::<GuildMarker>::new(1),
        Id::<ChannelMarker>::new(2),
        Id::<UserMarker>::new(3),
        "session-1",
        "voice.example.discord.gg",
        "token-1",
    );

    let proto = context.to_proto();

    assert_eq!(proto.guild_id, "1");
    assert_eq!(proto.channel_id, "2");
    assert_eq!(proto.user_id, "3");
    assert_eq!(proto.session_id, "session-1");
    assert_eq!(proto.endpoint, "voice.example.discord.gg");
    assert_eq!(proto.token, "token-1");
}

#[test]
fn gateway_voice_state_commands_are_twilight_native() {
    let guild_id = Id::<GuildMarker>::new(10);
    let channel_id = Id::<ChannelMarker>::new(20);

    let join = join_voice_channel(guild_id, channel_id, true, false);
    assert_eq!(join.d.guild_id, guild_id);
    assert_eq!(join.d.channel_id, Some(channel_id));
    assert!(join.d.self_deaf);
    assert!(!join.d.self_mute);

    let leave = leave_voice_channel(guild_id);
    assert_eq!(leave.d.guild_id, guild_id);
    assert_eq!(leave.d.channel_id, None);
    assert!(!leave.d.self_deaf);
    assert!(!leave.d.self_mute);
}

#[test]
fn tracker_returns_context_after_matching_twilight_voice_events() {
    let guild_id = Id::<GuildMarker>::new(100);
    let channel_id = Id::<ChannelMarker>::new(200);
    let user_id = Id::<UserMarker>::new(300);
    let mut tracker = VoiceContextTracker::new(guild_id, channel_id, user_id);

    assert!(
        tracker
            .observe(&voice_server_event(
                guild_id,
                "voice.example.discord.gg",
                "token-1"
            ))
            .is_none()
    );

    let context = tracker
        .observe(&voice_state_event(
            guild_id,
            channel_id,
            user_id,
            "session-1",
        ))
        .expect("voice state should complete the context");

    assert_eq!(context.guild_id, guild_id);
    assert_eq!(context.channel_id, channel_id);
    assert_eq!(context.user_id, user_id);
    assert_eq!(context.session_id, "session-1");
    assert_eq!(context.endpoint, "voice.example.discord.gg");
    assert_eq!(context.token, "token-1");
    assert!(tracker.current().is_some());

    assert!(
        tracker
            .observe(&voice_state_event(
                guild_id,
                channel_id,
                user_id,
                "session-1"
            ))
            .is_none()
    );

    let refreshed = tracker
        .observe(&voice_server_event(
            guild_id,
            "voice-2.example.discord.gg",
            "token-2",
        ))
        .expect("rotated Discord voice server data should refresh the context");
    assert_eq!(refreshed.session_id, "session-1");
    assert_eq!(refreshed.endpoint, "voice-2.example.discord.gg");
    assert_eq!(refreshed.token, "token-2");
}

#[test]
fn tracker_ignores_unrelated_twilight_voice_events() {
    let guild_id = Id::<GuildMarker>::new(100);
    let channel_id = Id::<ChannelMarker>::new(200);
    let user_id = Id::<UserMarker>::new(300);
    let mut tracker = VoiceContextTracker::new(guild_id, channel_id, user_id);

    assert!(
        tracker
            .observe(&voice_state_event(
                guild_id,
                channel_id,
                Id::<UserMarker>::new(301),
                "session-other-user",
            ))
            .is_none()
    );
    assert!(
        tracker
            .observe(&voice_state_event(
                guild_id,
                Id::<ChannelMarker>::new(201),
                user_id,
                "session-other-channel",
            ))
            .is_none()
    );
    assert!(
        tracker
            .observe(&voice_server_event(
                Id::<GuildMarker>::new(101),
                "voice.example.discord.gg",
                "token-1",
            ))
            .is_none()
    );
    assert!(tracker.current().is_none());
}

#[test]
fn proto_state_snapshot_converts_to_typed_state() {
    let snapshot = StateSnapshot::try_from(proto::SessionStateSnapshot {
        state: proto::SessionState::PlayingState as i32,
        guild_id: "42".into(),
        channel_id: "43".into(),
        current_video_id: "video-1".into(),
        queue_depth: 7,
        selected_itag: 251,
        message: "steady".into(),
    })
    .unwrap();

    assert_eq!(snapshot.state, SessionState::Playing);
    assert_eq!(snapshot.guild_id, Some(Id::<GuildMarker>::new(42)));
    assert_eq!(snapshot.channel_id, Some(Id::<ChannelMarker>::new(43)));
    assert_eq!(snapshot.current_video_id.as_deref(), Some("video-1"));
    assert_eq!(snapshot.queue_depth, 7);
    assert_eq!(snapshot.selected_itag, Some(251));
    assert_eq!(snapshot.message.as_deref(), Some("steady"));
}

#[test]
fn proto_session_event_converts_to_typed_event() {
    let event = SessionEvent::try_from(proto::SessionEvent {
        kind: proto::SessionEventKind::VoiceReconnecting as i32,
        guild_id: "42".into(),
        channel_id: "43".into(),
        current_video_id: "video-1".into(),
        selected_itag: 251,
        message: "rotating voice server".into(),
        error_code: "voice_resume_failed".into(),
        reason: proto::SessionEventReason::VoiceResumeFailed as i32,
    })
    .unwrap();

    assert_eq!(event.kind, SessionEventKind::VoiceReconnecting);
    assert_eq!(event.guild_id, Some(Id::<GuildMarker>::new(42)));
    assert_eq!(event.channel_id, Some(Id::<ChannelMarker>::new(43)));
    assert_eq!(event.current_video_id.as_deref(), Some("video-1"));
    assert_eq!(event.selected_itag, Some(251));
    assert_eq!(event.message.as_deref(), Some("rotating voice server"));
    assert_eq!(event.error_code.as_deref(), Some("voice_resume_failed"));
    assert_eq!(event.reason, SessionEventReason::VoiceResumeFailed);
}

#[test]
fn invalid_proto_ids_are_rejected_instead_of_panicking() {
    let error = StateSnapshot::try_from(proto::SessionStateSnapshot {
        state: proto::SessionState::Idle as i32,
        guild_id: "0".into(),
        ..Default::default()
    })
    .unwrap_err();

    assert_eq!(error.field(), "guild_id");
    assert_eq!(error.value(), "0");
}

fn voice_server_event(guild_id: Id<GuildMarker>, endpoint: &str, token: &str) -> Event {
    Event::VoiceServerUpdate(VoiceServerUpdate {
        endpoint: Some(endpoint.into()),
        guild_id,
        token: token.into(),
    })
}

fn voice_state_event(
    guild_id: Id<GuildMarker>,
    channel_id: Id<ChannelMarker>,
    user_id: Id<UserMarker>,
    session_id: &str,
) -> Event {
    Event::VoiceStateUpdate(Box::new(VoiceStateUpdate(VoiceState {
        channel_id: Some(channel_id),
        deaf: false,
        guild_id: Some(guild_id),
        member: None,
        mute: false,
        self_deaf: false,
        self_mute: false,
        self_stream: false,
        self_video: false,
        session_id: session_id.into(),
        suppress: false,
        user_id,
        request_to_speak_timestamp: None,
    })))
}
