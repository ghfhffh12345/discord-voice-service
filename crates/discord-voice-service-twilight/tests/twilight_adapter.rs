use discord_voice_service_twilight::{
    PlaybackSendCommandKind, PlaybackStabilitySnapshot, PreparedPlayoutQueueEventKind,
    PreparedPlayoutQueueEventReason, PreparedTrackQueueSamplePhase, SessionEvent, SessionEventKind,
    SessionEventReason, SessionState, StateSnapshot, VoiceContext, VoiceContextTracker,
    join_voice_channel, leave_voice_channel, proto,
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
fn proto_playback_metrics_convert_to_typed_snapshot() {
    let prepared_track_queue_depth = || proto::PlaybackQueueDepthStatsSnapshot {
        sample_count: 50,
        empty_count: 0,
        current_depth: Some(proto::PlaybackBufferDepthSnapshot {
            packets: 20,
            bytes: 10_240,
            duration_ms: 400,
            duration_samples: 19_200,
        }),
        min_depth: Some(proto::PlaybackBufferDepthSnapshot {
            packets: 16,
            bytes: 8_192,
            duration_ms: 320,
            duration_samples: 15_360,
        }),
        p5_depth: Some(proto::PlaybackBufferDepthSnapshot {
            packets: 16,
            bytes: 8_192,
            duration_ms: 320,
            duration_samples: 15_360,
        }),
        p50_depth: Some(proto::PlaybackBufferDepthSnapshot {
            packets: 20,
            bytes: 10_240,
            duration_ms: 400,
            duration_samples: 19_200,
        }),
        p95_depth: Some(proto::PlaybackBufferDepthSnapshot {
            packets: 24,
            bytes: 12_288,
            duration_ms: 480,
            duration_samples: 23_040,
        }),
        max_depth: Some(proto::PlaybackBufferDepthSnapshot {
            packets: 25,
            bytes: 12_800,
            duration_ms: 500,
            duration_samples: 24_000,
        }),
    };

    let snapshot = PlaybackStabilitySnapshot::from(proto::PlaybackStabilitySnapshot {
        available: true,
        playback_epoch: 9,
        video_id: "video-1".into(),
        selected_itag: 250,
        track_packet_count: 60,
        continuity_silence_packet_count: 1,
        inserted_silence_duration_ms: 20,
        track_interval: Some(proto::DurationStatsSnapshot {
            samples: 59,
            p50_ms: 20,
            p95_ms: 24,
            p99_ms: 31,
            min_ms: 18,
            max_ms: 40,
        }),
        track_media_duration_sent_ms: 1200,
        track_wall_clock_elapsed_ms: 1200,
        track_media_to_wall_clock_ratio_ppm: 1_000_000,
        track_fast_interval_count: 0,
        track_fast_interval_min_ms: 0,
        track_fast_interval_min_us: 0,
        track_tempo_window_count: 12,
        track_tempo_window_post_source_buffer_count: 3,
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
        tempo_rebase_count: 2,
        expected_track_frame_count: 63,
        sent_track_frame_count: 60,
        silence_frame_count: 2,
        frame_deficit_count: 1,
        dropped_frame_count: 2,
        late_frame_count: 3,
        all_packet_interval: Some(proto::DurationStatsSnapshot {
            samples: 60,
            p50_ms: 20,
            p95_ms: 23,
            p99_ms: 30,
            min_ms: 18,
            max_ms: 40,
        }),
        sender_lateness: Some(proto::DurationStatsSnapshot {
            samples: 60,
            p50_ms: 0,
            p95_ms: 4,
            p99_ms: 8,
            min_ms: 0,
            max_ms: 10,
        }),
        current_buffer_depth: Some(proto::PlaybackBufferDepthSnapshot {
            packets: 8,
            bytes: 4096,
            duration_ms: 160,
            duration_samples: 7680,
        }),
        min_buffer_depth: Some(proto::PlaybackBufferDepthSnapshot {
            packets: 4,
            bytes: 2048,
            duration_ms: 80,
            duration_samples: 3840,
        }),
        max_buffer_depth: Some(proto::PlaybackBufferDepthSnapshot {
            packets: 100,
            bytes: 51200,
            duration_ms: 2000,
            duration_samples: 96000,
        }),
        current_source_buffer_depth: Some(proto::PlaybackBufferDepthSnapshot {
            packets: 240,
            bytes: 122880,
            duration_ms: 4800,
            duration_samples: 230400,
        }),
        min_source_buffer_depth: Some(proto::PlaybackBufferDepthSnapshot {
            packets: 50,
            bytes: 25600,
            duration_ms: 1000,
            duration_samples: 48000,
        }),
        max_source_buffer_depth: Some(proto::PlaybackBufferDepthSnapshot {
            packets: 250,
            bytes: 128000,
            duration_ms: 5000,
            duration_samples: 240000,
        }),
        source_buffer_depth: Some(proto::PlaybackQueueDepthStatsSnapshot {
            sample_count: 50,
            empty_count: 0,
            current_depth: Some(proto::PlaybackBufferDepthSnapshot {
                packets: 240,
                bytes: 122880,
                duration_ms: 4800,
                duration_samples: 230400,
            }),
            min_depth: Some(proto::PlaybackBufferDepthSnapshot {
                packets: 50,
                bytes: 25600,
                duration_ms: 1000,
                duration_samples: 48000,
            }),
            p5_depth: Some(proto::PlaybackBufferDepthSnapshot {
                packets: 200,
                bytes: 102400,
                duration_ms: 4000,
                duration_samples: 192000,
            }),
            p50_depth: Some(proto::PlaybackBufferDepthSnapshot {
                packets: 240,
                bytes: 122880,
                duration_ms: 4800,
                duration_samples: 230400,
            }),
            p95_depth: Some(proto::PlaybackBufferDepthSnapshot {
                packets: 250,
                bytes: 128000,
                duration_ms: 5000,
                duration_samples: 240000,
            }),
            max_depth: Some(proto::PlaybackBufferDepthSnapshot {
                packets: 250,
                bytes: 128000,
                duration_ms: 5000,
                duration_samples: 240000,
            }),
        }),
        current_playout_buffer_depth: Some(proto::PlaybackBufferDepthSnapshot {
            packets: 12,
            bytes: 6144,
            duration_ms: 240,
            duration_samples: 11520,
        }),
        min_playout_buffer_depth: Some(proto::PlaybackBufferDepthSnapshot {
            packets: 6,
            bytes: 3072,
            duration_ms: 120,
            duration_samples: 5760,
        }),
        max_playout_buffer_depth: Some(proto::PlaybackBufferDepthSnapshot {
            packets: 20,
            bytes: 10240,
            duration_ms: 400,
            duration_samples: 19200,
        }),
        source_buffer_target_ms: 5000,
        source_buffer_low_watermark_count: 2,
        playout_buffer_low_watermark_count: 3,
        playout_underrun_count: 4,
        source_underrun_count: 5,
        playout_sender_lateness: Some(proto::DurationStatsSnapshot {
            samples: 60,
            p50_ms: 0,
            p95_ms: 3,
            p99_ms: 7,
            min_ms: 0,
            max_ms: 9,
        }),
        source_producer_fill_duration: Some(proto::DurationStatsSnapshot {
            samples: 2,
            p50_ms: 4,
            p95_ms: 6,
            p99_ms: 7,
            min_ms: 3,
            max_ms: 7,
        }),
        playout_builder_prepare_duration: Some(proto::DurationStatsSnapshot {
            samples: 20,
            p50_ms: 1,
            p95_ms: 2,
            p99_ms: 3,
            min_ms: 0,
            max_ms: 3,
        }),
        sender_send_duration: Some(proto::DurationStatsSnapshot {
            samples: 60,
            p50_ms: 0,
            p95_ms: 1,
            p99_ms: 2,
            min_ms: 0,
            max_ms: 2,
        }),
        sender_loop_non_send_work_duration: Some(proto::DurationStatsSnapshot {
            samples: 60,
            p50_ms: 0,
            p95_ms: 1,
            p99_ms: 1,
            min_ms: 0,
            max_ms: 1,
        }),
        sender_forbidden_work_count: 0,
        prepared_rtp_queue_depth_ms: 380,
        prepared_track_queue_target_ms: 400,
        prepared_track_queue_low_watermark_ms: 300,
        prepared_track_queue_high_watermark_ms: 500,
        active_pre_pause_prepared_track_queue_depth: Some(prepared_track_queue_depth()),
        active_post_resume_prepared_track_queue_depth: Some(prepared_track_queue_depth()),
        prepared_track_queue_depth_sample_count: 100,
        prepared_track_queue_empty_count: 0,
        raw_send_events: vec![proto::PlaybackSendEventSnapshot {
            packet_index: 0,
            command_kind: proto::PlaybackSendCommandKind::Track as i32,
            expected_deadline_offset_us: 0,
            send_started_offset_us: 1,
            sent_offset_us: 2,
            media_duration_ms: 20,
            media_duration_samples: 960,
            rtp_sequence: 7,
            rtp_timestamp: 13_440,
            protection_nonce: Some(42),
            source_frame_epoch: Some(1),
            source_media_position_ms: Some(5_000),
            source_media_position_samples: Some(240_000),
            source_media_byte_position: Some(123_456),
            committed_heard_media: true,
        }],
        raw_prepared_track_queue_samples: vec![proto::PreparedTrackQueueDepthSampleSnapshot {
            sample_index: 0,
            phase: proto::PreparedTrackQueueSamplePhase::ActivePostResume as i32,
            depth: Some(proto::PlaybackBufferDepthSnapshot {
                packets: 20,
                bytes: 10_240,
                duration_ms: 400,
                duration_samples: 19_200,
            }),
        }],
        raw_prepared_playout_queue_events: vec![proto::PreparedPlayoutQueueEventSnapshot {
            event_index: 0,
            event_kind: proto::PreparedPlayoutQueueEventKind::Rebuilt as i32,
            reason: proto::PreparedPlayoutQueueEventReason::Pause as i32,
            command_kind: proto::PlaybackSendCommandKind::Track as i32,
            media_duration_ms: 20,
            media_duration_samples: 960,
            rtp_sequence: 8,
            rtp_timestamp: 14_400,
            protection_nonce: Some(43),
            source_frame_epoch: Some(1),
            source_media_position_ms: Some(5_020),
            source_media_position_samples: Some(240_960),
            source_media_byte_position: Some(124_000),
            queue_depth_after: Some(proto::PlaybackBufferDepthSnapshot {
                packets: 20,
                bytes: 10_240,
                duration_ms: 400,
                duration_samples: 19_200,
            }),
        }],
        current_scheduled_silence_queue_depth: Some(proto::PlaybackBufferDepthSnapshot::default()),
        max_scheduled_silence_queue_depth: Some(proto::PlaybackBufferDepthSnapshot::default()),
        current_boundary_queue_depth: Some(proto::PlaybackBufferDepthSnapshot::default()),
        max_boundary_queue_depth: Some(proto::PlaybackBufferDepthSnapshot {
            packets: 1,
            bytes: 3,
            duration_ms: 20,
            duration_samples: 960,
        }),
        prepared_track_packet_drop_count: 2,
        prepared_silence_packet_drop_count: 0,
        prepared_packet_rebuild_count: 1,
        scheduled_silence_packet_count: 0,
        pause_media_boundary_count: 1,
        stop_media_boundary_count: 0,
        recovery_media_boundary_count: 0,
        natural_end_media_boundary_count: 0,
        dave_transition_recovery_reached_builder_count: 1,
        dave_transition_recovery_reached_deadline_sender_count: 0,
        source_underrun_reached_builder_count: 0,
        source_underrun_reached_deadline_sender_count: 0,
        discarded_source_frame_count: 0,
        discarded_source_duration_ms: 0,
        discarded_source_duration_samples: 0,
        stop_discarded_source_frame_count: 0,
        stop_discarded_source_duration_ms: 0,
        stop_discarded_source_duration_samples: 0,
        interruption_discarded_source_frame_count: 0,
        interruption_discarded_source_duration_ms: 0,
        interruption_discarded_source_duration_samples: 0,
        restored_source_frame_count: 2,
        restored_source_duration_ms: 40,
        restored_source_duration_samples: 1_920,
        gateway_event_drain_duration: Some(proto::DurationStatsSnapshot {
            samples: 60,
            p50_ms: 0,
            p95_ms: 1,
            p99_ms: 2,
            min_ms: 0,
            max_ms: 2,
        }),
        gateway_event_drain_count: 4,
        dave_transition_count: 3,
        dave_transition_count_during_playback: 2,
        stale_dave_send_prevented_count: 1,
        controlled_media_interruption_count: 1,
        media_clock_reset_count: 5,
        scheduler_late_reset_count: 1,
        source_underrun_reset_count: 2,
        pause_resume_reset_count: 3,
        dave_transition_recovery_reset_count: 4,
        max_consecutive_playout_late_packets: 2,
        speaking_prepare_duration: Some(proto::DurationStatsSnapshot {
            samples: 1,
            p50_ms: 100,
            p95_ms: 100,
            p99_ms: 100,
            min_ms: 100,
            max_ms: 100,
        }),
        rebuffer_count: 1,
        pause_resume_first_intervals_ms: vec![20, 21],
        post_stall_first_intervals_ms: vec![22],
        post_rebuffer_first_intervals_ms: vec![23],
        adaptive_buffer_target_ms: 5000,
        max_adaptive_buffer_target_ms: 5000,
        ended: true,
        ..Default::default()
    });

    assert!(snapshot.available);
    assert_eq!(snapshot.video_id.as_deref(), Some("video-1"));
    assert_eq!(snapshot.selected_itag, Some(250));
    assert_eq!(snapshot.track_packet_count, 60);
    assert_eq!(snapshot.continuity_silence_packet_count, 1);
    assert_eq!(snapshot.inserted_silence_duration_ms, 20);
    assert_eq!(snapshot.track_interval.p95_ms, 24);
    assert_eq!(snapshot.track_media_duration_sent_ms, 1200);
    assert_eq!(snapshot.track_wall_clock_elapsed_ms, 1200);
    assert_eq!(snapshot.track_media_to_wall_clock_ratio_ppm, 1_000_000);
    assert_eq!(snapshot.track_fast_interval_count, 0);
    assert_eq!(snapshot.track_fast_interval_min_ms, 0);
    assert_eq!(snapshot.track_fast_interval_min_us, 0);
    assert_eq!(snapshot.track_tempo_window_count, 12);
    assert_eq!(snapshot.track_tempo_window_post_source_buffer_count, 3);
    assert_eq!(snapshot.track_tempo_window_min_ratio_ppm, 1_000_000);
    assert_eq!(snapshot.track_tempo_window_max_ratio_ppm, 1_000_000);
    assert_eq!(snapshot.track_tempo_window_fast_count, 0);
    assert_eq!(snapshot.track_tempo_window_slow_count, 0);
    assert_eq!(snapshot.skipped_source_frame_count, 0);
    assert_eq!(snapshot.skipped_source_duration_ms, 0);
    assert_eq!(snapshot.tempo_rebase_count, 2);
    assert_eq!(snapshot.expected_track_frame_count, 63);
    assert_eq!(snapshot.sent_track_frame_count, 60);
    assert_eq!(snapshot.silence_frame_count, 2);
    assert_eq!(snapshot.frame_deficit_count, 1);
    assert_eq!(snapshot.dropped_frame_count, 2);
    assert_eq!(snapshot.late_frame_count, 3);
    assert_eq!(snapshot.all_packet_interval.samples, 60);
    assert_eq!(snapshot.sender_lateness.p99_ms, 8);
    assert_eq!(snapshot.current_buffer_depth.duration_samples, 7680);
    assert_eq!(snapshot.min_buffer_depth.duration_ms, 80);
    assert_eq!(snapshot.max_buffer_depth.packets, 100);
    assert_eq!(snapshot.current_source_buffer_depth.duration_ms, 4800);
    assert_eq!(snapshot.min_source_buffer_depth.duration_ms, 1000);
    assert_eq!(snapshot.max_source_buffer_depth.duration_ms, 5000);
    assert_eq!(
        snapshot
            .source_buffer_depth
            .as_ref()
            .expect("source reservoir depth percentiles should be preserved")
            .p50_depth
            .duration_ms,
        4800
    );
    assert_eq!(snapshot.current_playout_buffer_depth.duration_ms, 240);
    assert_eq!(snapshot.min_playout_buffer_depth.duration_ms, 120);
    assert_eq!(snapshot.max_playout_buffer_depth.duration_ms, 400);
    assert_eq!(snapshot.source_buffer_target_ms, 5000);
    assert_eq!(snapshot.source_buffer_low_watermark_count, 2);
    assert_eq!(snapshot.playout_buffer_low_watermark_count, 3);
    assert_eq!(snapshot.playout_underrun_count, 4);
    assert_eq!(snapshot.source_underrun_count, 5);
    assert_eq!(snapshot.playout_sender_lateness.p99_ms, 7);
    assert_eq!(snapshot.source_producer_fill_duration.samples, 2);
    assert_eq!(snapshot.playout_builder_prepare_duration.samples, 20);
    assert_eq!(snapshot.sender_send_duration.max_ms, 2);
    assert_eq!(snapshot.sender_loop_non_send_work_duration.max_ms, 1);
    assert_eq!(snapshot.sender_forbidden_work_count, 0);
    assert_eq!(snapshot.prepared_rtp_queue_depth_ms, 380);
    assert_eq!(snapshot.prepared_track_queue_target_ms, 400);
    assert_eq!(snapshot.prepared_track_queue_low_watermark_ms, 300);
    assert_eq!(snapshot.prepared_track_queue_high_watermark_ms, 500);
    let pre_pause_queue = snapshot
        .active_pre_pause_prepared_track_queue_depth
        .as_ref()
        .expect("pre-pause prepared track queue depth should be preserved");
    assert_eq!(pre_pause_queue.sample_count, 50);
    assert_eq!(pre_pause_queue.empty_count, 0);
    assert_eq!(pre_pause_queue.p50_depth.duration_ms, 400);
    assert_eq!(pre_pause_queue.p95_depth.duration_ms, 480);
    let post_resume_queue = snapshot
        .active_post_resume_prepared_track_queue_depth
        .as_ref()
        .expect("post-resume prepared track queue depth should be preserved");
    assert_eq!(post_resume_queue.sample_count, 50);
    assert_eq!(snapshot.prepared_track_queue_depth_sample_count, 100);
    assert_eq!(snapshot.prepared_track_queue_empty_count, 0);
    assert_eq!(snapshot.raw_send_events.len(), 1);
    let raw_send = &snapshot.raw_send_events[0];
    assert_eq!(raw_send.command_kind, PlaybackSendCommandKind::Track);
    assert_eq!(raw_send.protection_nonce, Some(42));
    assert_eq!(raw_send.source_media_position_ms, Some(5_000));
    assert_eq!(raw_send.source_media_position_samples, Some(240_000));
    assert!(raw_send.committed_heard_media);
    assert_eq!(snapshot.raw_prepared_track_queue_samples.len(), 1);
    let raw_queue_sample = &snapshot.raw_prepared_track_queue_samples[0];
    assert_eq!(
        raw_queue_sample.phase,
        PreparedTrackQueueSamplePhase::ActivePostResume
    );
    assert_eq!(raw_queue_sample.depth.duration_ms, 400);
    assert_eq!(snapshot.raw_prepared_playout_queue_events.len(), 1);
    let raw_playout_event = &snapshot.raw_prepared_playout_queue_events[0];
    assert_eq!(
        raw_playout_event.event_kind,
        PreparedPlayoutQueueEventKind::Rebuilt
    );
    assert_eq!(
        raw_playout_event.reason,
        PreparedPlayoutQueueEventReason::Pause
    );
    assert_eq!(
        raw_playout_event.command_kind,
        PlaybackSendCommandKind::Track
    );
    assert_eq!(raw_playout_event.protection_nonce, Some(43));
    assert_eq!(
        raw_playout_event.source_media_position_samples,
        Some(240_960)
    );
    assert_eq!(raw_playout_event.queue_depth_after.duration_ms, 400);
    assert_eq!(snapshot.prepared_track_packet_drop_count, 2);
    assert_eq!(snapshot.prepared_packet_rebuild_count, 1);
    assert_eq!(snapshot.pause_media_boundary_count, 1);
    assert_eq!(snapshot.restored_source_frame_count, 2);
    assert_eq!(snapshot.restored_source_duration_ms, 40);
    assert_eq!(snapshot.restored_source_duration_samples, 1_920);
    assert_eq!(snapshot.gateway_event_drain_duration.max_ms, 2);
    assert_eq!(snapshot.gateway_event_drain_count, 4);
    assert_eq!(snapshot.dave_transition_count, 3);
    assert_eq!(snapshot.dave_transition_count_during_playback, 2);
    assert_eq!(snapshot.stale_dave_send_prevented_count, 1);
    assert_eq!(snapshot.controlled_media_interruption_count, 1);
    assert_eq!(snapshot.media_clock_reset_count, 5);
    assert_eq!(snapshot.scheduler_late_reset_count, 1);
    assert_eq!(snapshot.source_underrun_reset_count, 2);
    assert_eq!(snapshot.pause_resume_reset_count, 3);
    assert_eq!(snapshot.dave_transition_recovery_reset_count, 4);
    assert_eq!(snapshot.max_consecutive_playout_late_packets, 2);
    assert_eq!(snapshot.speaking_prepare_duration.max_ms, 100);
    assert_eq!(snapshot.rebuffer_count, 1);
    assert_eq!(snapshot.adaptive_buffer_target_ms, 5000);
    assert_eq!(snapshot.max_adaptive_buffer_target_ms, 5000);
    assert_eq!(snapshot.pause_resume_first_intervals_ms, vec![20, 21]);
    assert_eq!(snapshot.post_stall_first_intervals_ms, vec![22]);
    assert_eq!(snapshot.post_rebuffer_first_intervals_ms, vec![23]);
    assert!(snapshot.ended);
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
