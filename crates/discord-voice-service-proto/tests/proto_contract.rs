use discord_voice_service_proto::discordvoice::v1::join_voice_request::VoiceContext;
use discord_voice_service_proto::discordvoice::v1::{
    DurationStatsSnapshot, GetPlaybackMetricsRequest, PlayRequest, PlaybackBufferDepthSnapshot,
    PlaybackStabilitySnapshot, SessionEvent, SessionEventKind, SessionEventReason,
    UpdateVoiceContextRequest,
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
        user_id: "user-1".into(),
        session_id: "abc".into(),
        endpoint: "voice.example.discord.gg".into(),
        token: "token".into(),
    };
    assert_eq!(context.guild_id, "1");
    assert_eq!(context.user_id, "user-1");

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

    let metrics = PlaybackStabilitySnapshot {
        available: true,
        playback_epoch: 42,
        video_id: "video-1".into(),
        selected_itag: 250,
        track_packet_count: 50,
        track_interval: Some(DurationStatsSnapshot {
            samples: 49,
            p50_ms: 20,
            p95_ms: 22,
            p99_ms: 25,
            min_ms: 19,
            max_ms: 28,
        }),
        track_media_duration_sent_ms: 1_000,
        track_wall_clock_elapsed_ms: 1_000,
        track_media_to_wall_clock_ratio_ppm: 1_000_000,
        track_fast_interval_count: 0,
        track_fast_interval_min_ms: 0,
        track_fast_interval_min_us: 0,
        track_tempo_window_count: 1,
        track_tempo_window_post_source_buffer_count: 0,
        track_tempo_window_min_ratio_ppm: 1_000_000,
        track_tempo_window_max_ratio_ppm: 1_000_000,
        track_tempo_window_fast_count: 0,
        track_tempo_window_fastest_ratio_ppm: 0,
        track_tempo_window_fastest_media_ms: 0,
        track_tempo_window_fastest_wall_clock_us: 0,
        track_tempo_window_slow_count: 0,
        track_tempo_window_slowest_ratio_ppm: 0,
        track_tempo_window_slowest_media_ms: 0,
        track_tempo_window_slowest_wall_clock_us: 0,
        skipped_source_frame_count: 0,
        skipped_source_duration_ms: 0,
        tempo_rebase_count: 0,
        current_buffer_depth: Some(PlaybackBufferDepthSnapshot {
            packets: 10,
            bytes: 4096,
            duration_ms: 200,
            duration_samples: 9600,
        }),
        current_source_buffer_depth: Some(PlaybackBufferDepthSnapshot {
            packets: 250,
            bytes: 128_000,
            duration_ms: 5_000,
            duration_samples: 240_000,
        }),
        current_playout_buffer_depth: Some(PlaybackBufferDepthSnapshot {
            packets: 20,
            bytes: 10_240,
            duration_ms: 400,
            duration_samples: 19_200,
        }),
        playout_underrun_count: 0,
        source_underrun_count: 0,
        sender_forbidden_work_count: 0,
        prepared_rtp_queue_depth_ms: 0,
        gateway_event_drain_duration: Some(DurationStatsSnapshot {
            samples: 50,
            p50_ms: 0,
            p95_ms: 1,
            p99_ms: 1,
            min_ms: 0,
            max_ms: 1,
        }),
        gateway_event_drain_count: 3,
        dave_transition_count: 2,
        dave_transition_count_during_playback: 1,
        stale_dave_send_prevented_count: 0,
        controlled_media_interruption_count: 0,
        media_clock_reset_count: 0,
        scheduler_late_reset_count: 0,
        source_underrun_reset_count: 0,
        pause_resume_reset_count: 0,
        dave_transition_recovery_reset_count: 0,
        source_buffer_target_ms: 5_000,
        adaptive_buffer_target_ms: 5_000,
        max_adaptive_buffer_target_ms: 5_000,
        ended: true,
        ..Default::default()
    };
    assert!(metrics.available);
    assert_eq!(metrics.track_interval.as_ref().unwrap().p95_ms, 22);
    assert_eq!(metrics.track_media_duration_sent_ms, 1_000);
    assert_eq!(metrics.track_wall_clock_elapsed_ms, 1_000);
    assert_eq!(metrics.track_media_to_wall_clock_ratio_ppm, 1_000_000);
    assert_eq!(metrics.track_fast_interval_count, 0);
    assert_eq!(metrics.track_fast_interval_min_us, 0);
    assert_eq!(metrics.track_tempo_window_count, 1);
    assert_eq!(metrics.track_tempo_window_min_ratio_ppm, 1_000_000);
    assert_eq!(metrics.track_tempo_window_max_ratio_ppm, 1_000_000);
    assert_eq!(metrics.track_tempo_window_fast_count, 0);
    assert_eq!(metrics.track_tempo_window_slow_count, 0);
    assert_eq!(
        metrics
            .current_buffer_depth
            .as_ref()
            .unwrap()
            .duration_samples,
        9600
    );
    assert_eq!(
        metrics
            .current_source_buffer_depth
            .as_ref()
            .unwrap()
            .duration_ms,
        5_000
    );
    assert_eq!(metrics.source_buffer_target_ms, 5_000);
    assert_eq!(
        metrics
            .current_playout_buffer_depth
            .as_ref()
            .unwrap()
            .duration_ms,
        400
    );
    assert_eq!(metrics.playout_underrun_count, 0);
    assert_eq!(metrics.source_underrun_count, 0);
    assert_eq!(metrics.sender_forbidden_work_count, 0);
    assert_eq!(metrics.prepared_rtp_queue_depth_ms, 0);
    assert_eq!(
        metrics
            .gateway_event_drain_duration
            .as_ref()
            .unwrap()
            .samples,
        50
    );
    assert_eq!(metrics.gateway_event_drain_count, 3);
    assert_eq!(metrics.dave_transition_count, 2);
    assert_eq!(metrics.dave_transition_count_during_playback, 1);
    assert_eq!(metrics.stale_dave_send_prevented_count, 0);
    assert_eq!(metrics.controlled_media_interruption_count, 0);
    assert_eq!(metrics.media_clock_reset_count, 0);
    assert_eq!(metrics.adaptive_buffer_target_ms, 5_000);
    assert_eq!(metrics.max_adaptive_buffer_target_ms, 5_000);
    let _request = GetPlaybackMetricsRequest {};
}

#[test]
fn control_proto_exposes_update_voice_context_and_reason_codes() {
    let proto = std::fs::read_to_string(format!(
        "{}/proto/discordvoice/v1/control.proto",
        env!("CARGO_MANIFEST_DIR")
    ))
    .unwrap();
    assert!(proto.contains("rpc UpdateVoiceContext"));
    assert!(proto.contains("rpc GetPlaybackMetrics"));
    assert!(proto.contains("string session_id = 3;"));
    assert!(proto.contains("string endpoint = 4;"));
    assert!(proto.contains("string token = 5;"));
    assert!(proto.contains("string user_id = 6;"));
    assert!(proto.contains("enum SessionEventReason"));
    assert!(proto.contains("SessionEventReason reason = 8;"));
    assert!(proto.contains("message PlaybackStabilitySnapshot"));
    assert!(proto.contains("DurationStatsSnapshot track_interval = 8;"));
    assert!(proto.contains("PlaybackBufferDepthSnapshot current_buffer_depth = 13;"));
    assert!(proto.contains("uint64 adaptive_buffer_target_ms = 32;"));
    assert!(proto.contains("uint64 max_adaptive_buffer_target_ms = 33;"));
    assert!(proto.contains("PlaybackBufferDepthSnapshot current_source_buffer_depth = 34;"));
    assert!(proto.contains("uint64 source_buffer_target_ms = 37;"));
    assert!(proto.contains("uint64 rebuffer_count = 39;"));
    assert!(proto.contains("PlaybackBufferDepthSnapshot current_playout_buffer_depth = 40;"));
    assert!(proto.contains("uint64 playout_underrun_count = 44;"));
    assert!(proto.contains("DurationStatsSnapshot playout_sender_lateness = 45;"));
    assert!(proto.contains("uint64 source_underrun_count = 49;"));
    assert!(proto.contains("DurationStatsSnapshot source_producer_fill_duration = 50;"));
    assert!(proto.contains("DurationStatsSnapshot playout_builder_prepare_duration = 51;"));
    assert!(proto.contains("DurationStatsSnapshot sender_send_duration = 52;"));
    assert!(proto.contains("DurationStatsSnapshot sender_loop_non_send_work_duration = 53;"));
    assert!(proto.contains("uint64 sender_forbidden_work_count = 54;"));
    assert!(proto.contains("uint64 prepared_rtp_queue_depth_ms = 55;"));
    assert!(proto.contains("DurationStatsSnapshot gateway_event_drain_duration = 56;"));
    assert!(proto.contains("uint64 gateway_event_drain_count = 57;"));
    assert!(proto.contains("uint64 dave_transition_count = 58;"));
    assert!(proto.contains("uint64 dave_transition_count_during_playback = 59;"));
    assert!(proto.contains("uint64 stale_dave_send_prevented_count = 60;"));
    assert!(proto.contains("uint64 controlled_media_interruption_count = 61;"));
    assert!(proto.contains("uint64 media_clock_reset_count = 62;"));
    assert!(proto.contains("uint64 scheduler_late_reset_count = 63;"));
    assert!(proto.contains("uint64 source_underrun_reset_count = 64;"));
    assert!(proto.contains("uint64 pause_resume_reset_count = 65;"));
    assert!(proto.contains("uint64 dave_transition_recovery_reset_count = 66;"));
    assert!(proto.contains("uint64 track_media_duration_sent_ms = 67;"));
    assert!(proto.contains("uint64 track_wall_clock_elapsed_ms = 68;"));
    assert!(proto.contains("uint64 track_media_to_wall_clock_ratio_ppm = 69;"));
    assert!(proto.contains("uint64 track_fast_interval_count = 70;"));
    assert!(proto.contains("uint64 track_fast_interval_min_ms = 71;"));
    assert!(proto.contains("uint64 skipped_source_frame_count = 72;"));
    assert!(proto.contains("uint64 skipped_source_duration_ms = 73;"));
    assert!(proto.contains("uint64 tempo_rebase_count = 74;"));
    assert!(proto.contains("uint64 track_fast_interval_min_us = 75;"));
    assert!(proto.contains("uint64 track_tempo_window_count = 76;"));
    assert!(proto.contains("uint64 track_tempo_window_post_source_buffer_count = 77;"));
    assert!(proto.contains("uint64 track_tempo_window_fast_count = 78;"));
    assert!(proto.contains("uint64 track_tempo_window_fastest_ratio_ppm = 79;"));
    assert!(proto.contains("uint64 track_tempo_window_fastest_media_ms = 80;"));
    assert!(proto.contains("uint64 track_tempo_window_fastest_wall_clock_us = 81;"));
    assert!(proto.contains("uint64 track_tempo_window_min_ratio_ppm = 82;"));
    assert!(proto.contains("uint64 track_tempo_window_max_ratio_ppm = 83;"));
    assert!(proto.contains("uint64 track_tempo_window_slow_count = 84;"));
    assert!(proto.contains("uint64 track_tempo_window_slowest_ratio_ppm = 85;"));
    assert!(proto.contains("uint64 track_tempo_window_slowest_media_ms = 86;"));
    assert!(proto.contains("uint64 track_tempo_window_slowest_wall_clock_us = 87;"));
    assert!(proto.contains("VOICE_RECONNECTING"));
    for reason in [
        "SESSION_EVENT_REASON_UNSPECIFIED",
        "JOIN_TIMEOUT",
        "JOIN_FAILED",
        "INVALID_VOICE_TOKEN",
        "VOICE_RESUME_FAILED",
        "DAVE_TRANSITION_FAILED",
        "UNSUPPORTED_ENCRYPTION_MODE",
        "UDP_DISCOVERY_FAILED",
        "UPSTREAM_URL_STALE",
        "PLAYBACK_SOURCE_UNSUPPORTED",
    ] {
        assert!(proto.contains(reason), "missing reason variant: {reason}");
    }
}
