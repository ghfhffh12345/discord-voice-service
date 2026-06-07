use anyhow::{Context, Result};
use serde::Serialize;
use std::fs;

use discord_voice_service_twilight::{
    DurationStatsSnapshot as TwilightDurationStatsSnapshot,
    PlaybackBufferDepthSnapshot as TwilightPlaybackBufferDepthSnapshot,
    PlaybackQueueDepthStatsSnapshot as TwilightPlaybackQueueDepthStatsSnapshot,
    PlaybackSendCommandKind as TwilightPlaybackSendCommandKind,
    PlaybackSendEventSnapshot as TwilightPlaybackSendEventSnapshot,
    PlaybackStabilitySnapshot as TwilightPlaybackStabilitySnapshot,
    PreparedPlayoutQueueEventKind as TwilightPreparedPlayoutQueueEventKind,
    PreparedPlayoutQueueEventReason as TwilightPreparedPlayoutQueueEventReason,
    PreparedPlayoutQueueEventSnapshot as TwilightPreparedPlayoutQueueEventSnapshot,
    PreparedTrackQueueDepthSampleSnapshot as TwilightPreparedTrackQueueDepthSampleSnapshot,
    PreparedTrackQueueSamplePhase as TwilightPreparedTrackQueueSamplePhase, SessionEvent,
    SessionEventKind,
};

use crate::audio::AudioIntervalStats;

#[derive(Debug, Clone, Default)]
pub struct LiveContractState {
    pub validated_join_voice: bool,
    pub validated_update_voice_context: bool,
    pub validated_play: bool,
    pub validated_pause: bool,
    pub validated_resume: bool,
    pub validated_invalid_resume_ignored: bool,
    pub validated_redundant_pause_ignored: bool,
    pub observer_proved_pause: bool,
    pub observer_proved_resume: bool,
    pub validated_reconnect_rollover_during_playback: bool,
    pub validated_stop: bool,
    pub validated_stop_during_playback: bool,
    pub validated_leave_voice: bool,
    pub validated_leave_voice_during_playback: bool,
    pub validated_get_state: bool,
    pub validated_get_playback_metrics: bool,
    pub validated_subscribe_events: bool,
    pub saw_voice_connecting: bool,
    pub saw_voice_ready: bool,
    pub saw_track_resolving: bool,
    pub saw_playing: bool,
    pub saw_track_ended: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct LiveValidationEvidence {
    pub outcome: String,
    pub service_uri: String,
    pub ytmusic_addr: String,
    pub test_video_id: String,
    pub expected_track_duration_ms: u64,
    pub active_validation_duration_after_resume_ms: u64,
    pub pause_silence_packet_count: u64,
    pub pause_silence_spacing_ms: Vec<u64>,
    pub live_staging_profile: String,
    pub live_staging_service_cpus: String,
    pub live_staging_cpu_contention_workers: u64,
    pub live_staging_http_read_delay_ms: u64,
    pub live_staging_http_read_jitter_ms: u64,
    pub validated_join_voice: bool,
    pub validated_update_voice_context: bool,
    pub validated_play: bool,
    pub validated_pause: bool,
    pub validated_resume: bool,
    pub validated_invalid_resume_ignored: bool,
    pub validated_redundant_pause_ignored: bool,
    pub observer_proved_pause: bool,
    pub observer_proved_resume: bool,
    pub observer_pause_self_mute_observed: bool,
    pub observer_pause_speaking_stopped: bool,
    pub observer_pause_rtp_silence_observed: bool,
    pub observer_resume_speaking_started: bool,
    pub observer_pause_silence_ms: u64,
    pub observer_resume_packet_count: u64,
    pub validated_reconnect_rollover_during_playback: bool,
    pub validated_stop: bool,
    pub validated_stop_during_playback: bool,
    pub validated_leave_voice: bool,
    pub validated_leave_voice_during_playback: bool,
    pub validated_get_state: bool,
    pub validated_get_playback_metrics: bool,
    pub validated_subscribe_events: bool,
    pub saw_voice_connecting: bool,
    pub saw_voice_ready: bool,
    pub saw_track_resolving: bool,
    pub saw_playing: bool,
    pub saw_track_ended: bool,
    pub observed_packet_count: u64,
    pub decoded_audio_ms: u64,
    pub observer_wall_clock_elapsed_ms: u64,
    pub observer_decoded_audio_to_wall_clock_ratio_ppm: u64,
    pub non_silent_audio_ms: u64,
    pub observer_rtp_inter_arrival: PlaybackDurationStatsEvidence,
    pub observer_rtp_gap_count_gte_100ms: u64,
    pub observer_rtp_fast_interval_count: u64,
    pub observer_rtp_fast_interval_min_ms: u64,
    pub observer_rtp_fast_interval_min_us: u64,
    pub observer_decoded_audio_tempo_window_count: u64,
    pub observer_decoded_audio_tempo_window_post_source_buffer_count: u64,
    pub observer_decoded_audio_tempo_window_min_ratio_ppm: u64,
    pub observer_decoded_audio_tempo_window_max_ratio_ppm: u64,
    pub observer_decoded_audio_tempo_window_fast_count: u64,
    pub observer_decoded_audio_tempo_window_fastest_ratio_ppm: u64,
    pub observer_decoded_audio_tempo_window_fastest_media_ms: u64,
    pub observer_decoded_audio_tempo_window_fastest_wall_clock_us: u64,
    pub observer_decoded_audio_tempo_window_slow_count: u64,
    pub observer_decoded_audio_tempo_window_slowest_ratio_ppm: u64,
    pub observer_decoded_audio_tempo_window_slowest_media_ms: u64,
    pub observer_decoded_audio_tempo_window_slowest_wall_clock_us: u64,
    pub dave_transition_count_during_playback: u64,
    pub playback_metrics: Option<PlaybackStabilityEvidence>,
    pub reconnect_probe_metrics: Option<PlaybackStabilityEvidence>,
    pub validated_constrained_profile: bool,
    pub validated_slow_jittery_http: bool,
    pub failure_reason: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct PlaybackDurationStatsEvidence {
    pub samples: u64,
    pub p50_ms: u64,
    pub p95_ms: u64,
    pub p99_ms: u64,
    pub min_ms: u64,
    pub max_ms: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct PlaybackBufferDepthEvidence {
    pub packets: u64,
    pub bytes: u64,
    pub duration_ms: u64,
    pub duration_samples: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct PlaybackQueueDepthStatsEvidence {
    pub sample_count: u64,
    pub empty_count: u64,
    pub current_depth: PlaybackBufferDepthEvidence,
    pub min_depth: PlaybackBufferDepthEvidence,
    pub p5_depth: PlaybackBufferDepthEvidence,
    pub p50_depth: PlaybackBufferDepthEvidence,
    pub p95_depth: PlaybackBufferDepthEvidence,
    pub max_depth: PlaybackBufferDepthEvidence,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct PlaybackSendEventEvidence {
    pub packet_index: u64,
    pub command_kind: String,
    pub expected_deadline_offset_us: u64,
    pub send_started_offset_us: u64,
    pub sent_offset_us: u64,
    pub media_duration_ms: u64,
    pub media_duration_samples: u32,
    pub rtp_sequence: u32,
    pub rtp_timestamp: u32,
    pub protection_nonce: Option<u32>,
    pub source_frame_epoch: Option<u64>,
    pub source_media_position_ms: Option<u64>,
    pub source_media_byte_position: Option<u64>,
    pub committed_heard_media: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct PreparedTrackQueueDepthSampleEvidence {
    pub sample_index: u64,
    pub phase: String,
    pub depth: PlaybackBufferDepthEvidence,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct PreparedPlayoutQueueEventEvidence {
    pub event_index: u64,
    pub event_kind: String,
    pub reason: String,
    pub command_kind: String,
    pub media_duration_ms: u64,
    pub media_duration_samples: u32,
    pub rtp_sequence: u32,
    pub rtp_timestamp: u32,
    pub protection_nonce: Option<u32>,
    pub source_frame_epoch: Option<u64>,
    pub source_media_position_ms: Option<u64>,
    pub source_media_byte_position: Option<u64>,
    pub queue_depth_after: PlaybackBufferDepthEvidence,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct PlaybackStabilityEvidence {
    pub playback_epoch: u64,
    pub video_id: Option<String>,
    pub selected_itag: Option<u32>,
    pub track_packet_count: u64,
    pub continuity_silence_packet_count: u64,
    pub inserted_silence_duration_ms: u64,
    pub track_interval: PlaybackDurationStatsEvidence,
    pub track_media_duration_sent_ms: u64,
    pub track_wall_clock_elapsed_ms: u64,
    pub track_media_to_wall_clock_ratio_ppm: u64,
    pub track_fast_interval_count: u64,
    pub track_fast_interval_min_ms: u64,
    pub track_fast_interval_min_us: u64,
    pub track_tempo_window_count: u64,
    pub track_tempo_window_post_source_buffer_count: u64,
    pub track_tempo_window_min_ratio_ppm: u64,
    pub track_tempo_window_max_ratio_ppm: u64,
    pub track_tempo_window_fast_count: u64,
    pub track_tempo_window_fastest_ratio_ppm: u64,
    pub track_tempo_window_fastest_media_ms: u64,
    pub track_tempo_window_fastest_wall_clock_us: u64,
    pub track_tempo_window_slow_count: u64,
    pub track_tempo_window_slowest_ratio_ppm: u64,
    pub track_tempo_window_slowest_media_ms: u64,
    pub track_tempo_window_slowest_wall_clock_us: u64,
    pub skipped_source_frame_count: u64,
    pub skipped_source_duration_ms: u64,
    pub tempo_rebase_count: u64,
    pub all_packet_interval: PlaybackDurationStatsEvidence,
    pub sender_lateness: PlaybackDurationStatsEvidence,
    pub max_consecutive_late_packets: u64,
    pub current_consecutive_late_packets: u64,
    pub current_buffer_depth: PlaybackBufferDepthEvidence,
    pub min_buffer_depth: PlaybackBufferDepthEvidence,
    pub max_buffer_depth: PlaybackBufferDepthEvidence,
    pub current_source_buffer_depth: PlaybackBufferDepthEvidence,
    pub min_source_buffer_depth: PlaybackBufferDepthEvidence,
    pub max_source_buffer_depth: PlaybackBufferDepthEvidence,
    pub source_buffer_depth: Option<PlaybackQueueDepthStatsEvidence>,
    pub current_playout_buffer_depth: PlaybackBufferDepthEvidence,
    pub min_playout_buffer_depth: PlaybackBufferDepthEvidence,
    pub max_playout_buffer_depth: PlaybackBufferDepthEvidence,
    pub egress_buffer_target_ms: u64,
    pub current_egress_buffer_depth: PlaybackBufferDepthEvidence,
    pub min_egress_buffer_depth: PlaybackBufferDepthEvidence,
    pub max_egress_buffer_depth: PlaybackBufferDepthEvidence,
    pub prepared_rtp_queue_depth_ms: u64,
    pub prepared_track_queue_target_ms: u64,
    pub prepared_track_queue_low_watermark_ms: u64,
    pub prepared_track_queue_high_watermark_ms: u64,
    pub active_pre_pause_prepared_track_queue_depth: Option<PlaybackQueueDepthStatsEvidence>,
    pub active_post_resume_prepared_track_queue_depth: Option<PlaybackQueueDepthStatsEvidence>,
    pub prepared_track_queue_depth_sample_count: u64,
    pub prepared_track_queue_empty_count: u64,
    pub raw_send_event_count: u64,
    pub raw_prepared_track_queue_sample_count: u64,
    pub raw_prepared_playout_queue_event_count: u64,
    pub raw_send_events: Vec<PlaybackSendEventEvidence>,
    pub raw_prepared_track_queue_samples: Vec<PreparedTrackQueueDepthSampleEvidence>,
    pub raw_prepared_playout_queue_events: Vec<PreparedPlayoutQueueEventEvidence>,
    pub current_scheduled_silence_queue_depth: PlaybackBufferDepthEvidence,
    pub max_scheduled_silence_queue_depth: PlaybackBufferDepthEvidence,
    pub current_boundary_queue_depth: PlaybackBufferDepthEvidence,
    pub max_boundary_queue_depth: PlaybackBufferDepthEvidence,
    pub prepared_track_packet_drop_count: u64,
    pub prepared_silence_packet_drop_count: u64,
    pub prepared_packet_rebuild_count: u64,
    pub scheduled_silence_packet_count: u64,
    pub pause_media_boundary_count: u64,
    pub stop_media_boundary_count: u64,
    pub recovery_media_boundary_count: u64,
    pub natural_end_media_boundary_count: u64,
    pub dave_transition_recovery_reached_builder_count: u64,
    pub dave_transition_recovery_reached_deadline_sender_count: u64,
    pub source_underrun_reached_builder_count: u64,
    pub source_underrun_reached_deadline_sender_count: u64,
    pub discarded_source_frame_count: u64,
    pub discarded_source_duration_ms: u64,
    pub stop_discarded_source_frame_count: u64,
    pub stop_discarded_source_duration_ms: u64,
    pub interruption_discarded_source_frame_count: u64,
    pub interruption_discarded_source_duration_ms: u64,
    pub restored_source_frame_count: u64,
    pub restored_source_duration_ms: u64,
    pub source_buffer_target_ms: u64,
    pub adaptive_buffer_target_ms: u64,
    pub max_adaptive_buffer_target_ms: u64,
    pub buffer_low_watermark_count: u64,
    pub source_buffer_low_watermark_count: u64,
    pub playout_buffer_low_watermark_count: u64,
    pub buffer_underrun_count: u64,
    pub playout_underrun_count: u64,
    pub egress_underrun_count: u64,
    pub egress_inserted_silence_duration_ms: u64,
    pub egress_dropped_music_frame_count: u64,
    pub egress_dropped_music_duration_ms: u64,
    pub source_underrun_count: u64,
    pub rebuffer_count: u64,
    pub refill_duration: PlaybackDurationStatsEvidence,
    pub source_producer_fill_duration: PlaybackDurationStatsEvidence,
    pub producer_stall_duration: PlaybackDurationStatsEvidence,
    pub max_producer_lag_ms: u64,
    pub http_retry_count: u64,
    pub response_open_count: u64,
    pub range_reopen_count: u64,
    pub read_error_reopen_count: u64,
    pub url_reresolve_count: u64,
    pub pause_resume_first_intervals_ms: Vec<u64>,
    pub post_stall_first_intervals_ms: Vec<u64>,
    pub post_rebuffer_first_intervals_ms: Vec<u64>,
    pub playout_sender_lateness: PlaybackDurationStatsEvidence,
    pub playout_builder_prepare_duration: PlaybackDurationStatsEvidence,
    pub sender_send_duration: PlaybackDurationStatsEvidence,
    pub sender_loop_non_send_work_duration: PlaybackDurationStatsEvidence,
    pub max_consecutive_playout_late_packets: u64,
    pub max_consecutive_late_egress_ticks: u64,
    pub speaking_prepare_duration: PlaybackDurationStatsEvidence,
    pub sender_forbidden_work_count: u64,
    pub gateway_event_drain_duration: PlaybackDurationStatsEvidence,
    pub gateway_event_drain_count: u64,
    pub dave_transition_count: u64,
    pub dave_transition_count_during_playback: u64,
    pub stale_dave_send_prevented_count: u64,
    pub controlled_media_interruption_count: u64,
    pub media_clock_reset_count: u64,
    pub egress_clock_reset_count: u64,
    pub scheduler_late_reset_count: u64,
    pub source_underrun_reset_count: u64,
    pub pause_resume_reset_count: u64,
    pub dave_transition_recovery_reset_count: u64,
    pub gateway_interruptions: u64,
    pub dave_interruptions: u64,
    pub reconnect_interruptions: u64,
    pub ended: bool,
}

impl From<&TwilightDurationStatsSnapshot> for PlaybackDurationStatsEvidence {
    fn from(value: &TwilightDurationStatsSnapshot) -> Self {
        Self {
            samples: value.samples,
            p50_ms: value.p50_ms,
            p95_ms: value.p95_ms,
            p99_ms: value.p99_ms,
            min_ms: value.min_ms,
            max_ms: value.max_ms,
        }
    }
}

impl From<&TwilightPlaybackBufferDepthSnapshot> for PlaybackBufferDepthEvidence {
    fn from(value: &TwilightPlaybackBufferDepthSnapshot) -> Self {
        Self {
            packets: value.packets,
            bytes: value.bytes,
            duration_ms: value.duration_ms,
            duration_samples: value.duration_samples,
        }
    }
}

impl From<&TwilightPlaybackQueueDepthStatsSnapshot> for PlaybackQueueDepthStatsEvidence {
    fn from(value: &TwilightPlaybackQueueDepthStatsSnapshot) -> Self {
        Self {
            sample_count: value.sample_count,
            empty_count: value.empty_count,
            current_depth: (&value.current_depth).into(),
            min_depth: (&value.min_depth).into(),
            p5_depth: (&value.p5_depth).into(),
            p50_depth: (&value.p50_depth).into(),
            p95_depth: (&value.p95_depth).into(),
            max_depth: (&value.max_depth).into(),
        }
    }
}

impl From<&TwilightPlaybackSendEventSnapshot> for PlaybackSendEventEvidence {
    fn from(value: &TwilightPlaybackSendEventSnapshot) -> Self {
        Self {
            packet_index: value.packet_index,
            command_kind: playback_send_command_kind_label(value.command_kind).to_owned(),
            expected_deadline_offset_us: value.expected_deadline_offset_us,
            send_started_offset_us: value.send_started_offset_us,
            sent_offset_us: value.sent_offset_us,
            media_duration_ms: value.media_duration_ms,
            media_duration_samples: value.media_duration_samples,
            rtp_sequence: value.rtp_sequence,
            rtp_timestamp: value.rtp_timestamp,
            protection_nonce: value.protection_nonce,
            source_frame_epoch: value.source_frame_epoch,
            source_media_position_ms: value.source_media_position_ms,
            source_media_byte_position: value.source_media_byte_position,
            committed_heard_media: value.committed_heard_media,
        }
    }
}

impl From<&TwilightPreparedTrackQueueDepthSampleSnapshot>
    for PreparedTrackQueueDepthSampleEvidence
{
    fn from(value: &TwilightPreparedTrackQueueDepthSampleSnapshot) -> Self {
        Self {
            sample_index: value.sample_index,
            phase: prepared_track_queue_sample_phase_label(value.phase).to_owned(),
            depth: (&value.depth).into(),
        }
    }
}

impl From<&TwilightPreparedPlayoutQueueEventSnapshot> for PreparedPlayoutQueueEventEvidence {
    fn from(value: &TwilightPreparedPlayoutQueueEventSnapshot) -> Self {
        Self {
            event_index: value.event_index,
            event_kind: prepared_playout_queue_event_kind_label(value.event_kind).to_owned(),
            reason: prepared_playout_queue_event_reason_label(value.reason).to_owned(),
            command_kind: playback_send_command_kind_label(value.command_kind).to_owned(),
            media_duration_ms: value.media_duration_ms,
            media_duration_samples: value.media_duration_samples,
            rtp_sequence: value.rtp_sequence,
            rtp_timestamp: value.rtp_timestamp,
            protection_nonce: value.protection_nonce,
            source_frame_epoch: value.source_frame_epoch,
            source_media_position_ms: value.source_media_position_ms,
            source_media_byte_position: value.source_media_byte_position,
            queue_depth_after: (&value.queue_depth_after).into(),
        }
    }
}

impl From<&AudioIntervalStats> for PlaybackDurationStatsEvidence {
    fn from(value: &AudioIntervalStats) -> Self {
        Self {
            samples: value.samples,
            p50_ms: value.p50_ms,
            p95_ms: value.p95_ms,
            p99_ms: value.p99_ms,
            min_ms: value.min_ms,
            max_ms: value.max_ms,
        }
    }
}

impl From<&TwilightPlaybackStabilitySnapshot> for PlaybackStabilityEvidence {
    fn from(value: &TwilightPlaybackStabilitySnapshot) -> Self {
        Self {
            playback_epoch: value.playback_epoch,
            video_id: value.video_id.clone(),
            selected_itag: value.selected_itag,
            track_packet_count: value.track_packet_count,
            continuity_silence_packet_count: value.continuity_silence_packet_count,
            inserted_silence_duration_ms: value.inserted_silence_duration_ms,
            track_interval: (&value.track_interval).into(),
            track_media_duration_sent_ms: value.track_media_duration_sent_ms,
            track_wall_clock_elapsed_ms: value.track_wall_clock_elapsed_ms,
            track_media_to_wall_clock_ratio_ppm: value.track_media_to_wall_clock_ratio_ppm,
            track_fast_interval_count: value.track_fast_interval_count,
            track_fast_interval_min_ms: value.track_fast_interval_min_ms,
            track_fast_interval_min_us: value.track_fast_interval_min_us,
            track_tempo_window_count: value.track_tempo_window_count,
            track_tempo_window_post_source_buffer_count: value
                .track_tempo_window_post_source_buffer_count,
            track_tempo_window_min_ratio_ppm: value.track_tempo_window_min_ratio_ppm,
            track_tempo_window_max_ratio_ppm: value.track_tempo_window_max_ratio_ppm,
            track_tempo_window_fast_count: value.track_tempo_window_fast_count,
            track_tempo_window_fastest_ratio_ppm: value.track_tempo_window_fastest_ratio_ppm,
            track_tempo_window_fastest_media_ms: value.track_tempo_window_fastest_media_ms,
            track_tempo_window_fastest_wall_clock_us: value
                .track_tempo_window_fastest_wall_clock_us,
            track_tempo_window_slow_count: value.track_tempo_window_slow_count,
            track_tempo_window_slowest_ratio_ppm: value.track_tempo_window_slowest_ratio_ppm,
            track_tempo_window_slowest_media_ms: value.track_tempo_window_slowest_media_ms,
            track_tempo_window_slowest_wall_clock_us: value
                .track_tempo_window_slowest_wall_clock_us,
            skipped_source_frame_count: value.skipped_source_frame_count,
            skipped_source_duration_ms: value.skipped_source_duration_ms,
            tempo_rebase_count: value.tempo_rebase_count,
            all_packet_interval: (&value.all_packet_interval).into(),
            sender_lateness: (&value.sender_lateness).into(),
            max_consecutive_late_packets: value.max_consecutive_late_packets,
            current_consecutive_late_packets: value.current_consecutive_late_packets,
            current_buffer_depth: (&value.current_buffer_depth).into(),
            min_buffer_depth: (&value.min_buffer_depth).into(),
            max_buffer_depth: (&value.max_buffer_depth).into(),
            current_source_buffer_depth: (&value.current_source_buffer_depth).into(),
            min_source_buffer_depth: (&value.min_source_buffer_depth).into(),
            max_source_buffer_depth: (&value.max_source_buffer_depth).into(),
            source_buffer_depth: value.source_buffer_depth.as_ref().map(Into::into),
            current_playout_buffer_depth: (&value.current_playout_buffer_depth).into(),
            min_playout_buffer_depth: (&value.min_playout_buffer_depth).into(),
            max_playout_buffer_depth: (&value.max_playout_buffer_depth).into(),
            egress_buffer_target_ms: value.egress_buffer_target_ms,
            current_egress_buffer_depth: (&value.current_egress_buffer_depth).into(),
            min_egress_buffer_depth: (&value.min_egress_buffer_depth).into(),
            max_egress_buffer_depth: (&value.max_egress_buffer_depth).into(),
            prepared_rtp_queue_depth_ms: value.prepared_rtp_queue_depth_ms,
            prepared_track_queue_target_ms: value.prepared_track_queue_target_ms,
            prepared_track_queue_low_watermark_ms: value.prepared_track_queue_low_watermark_ms,
            prepared_track_queue_high_watermark_ms: value.prepared_track_queue_high_watermark_ms,
            active_pre_pause_prepared_track_queue_depth: value
                .active_pre_pause_prepared_track_queue_depth
                .as_ref()
                .map(Into::into),
            active_post_resume_prepared_track_queue_depth: value
                .active_post_resume_prepared_track_queue_depth
                .as_ref()
                .map(Into::into),
            prepared_track_queue_depth_sample_count: value.prepared_track_queue_depth_sample_count,
            prepared_track_queue_empty_count: value.prepared_track_queue_empty_count,
            raw_send_event_count: value.raw_send_events.len() as u64,
            raw_prepared_track_queue_sample_count: value.raw_prepared_track_queue_samples.len()
                as u64,
            raw_prepared_playout_queue_event_count: value.raw_prepared_playout_queue_events.len()
                as u64,
            raw_send_events: value.raw_send_events.iter().map(Into::into).collect(),
            raw_prepared_track_queue_samples: value
                .raw_prepared_track_queue_samples
                .iter()
                .map(Into::into)
                .collect(),
            raw_prepared_playout_queue_events: value
                .raw_prepared_playout_queue_events
                .iter()
                .map(Into::into)
                .collect(),
            current_scheduled_silence_queue_depth: (&value.current_scheduled_silence_queue_depth)
                .into(),
            max_scheduled_silence_queue_depth: (&value.max_scheduled_silence_queue_depth).into(),
            current_boundary_queue_depth: (&value.current_boundary_queue_depth).into(),
            max_boundary_queue_depth: (&value.max_boundary_queue_depth).into(),
            prepared_track_packet_drop_count: value.prepared_track_packet_drop_count,
            prepared_silence_packet_drop_count: value.prepared_silence_packet_drop_count,
            prepared_packet_rebuild_count: value.prepared_packet_rebuild_count,
            scheduled_silence_packet_count: value.scheduled_silence_packet_count,
            pause_media_boundary_count: value.pause_media_boundary_count,
            stop_media_boundary_count: value.stop_media_boundary_count,
            recovery_media_boundary_count: value.recovery_media_boundary_count,
            natural_end_media_boundary_count: value.natural_end_media_boundary_count,
            dave_transition_recovery_reached_builder_count: value
                .dave_transition_recovery_reached_builder_count,
            dave_transition_recovery_reached_deadline_sender_count: value
                .dave_transition_recovery_reached_deadline_sender_count,
            source_underrun_reached_builder_count: value.source_underrun_reached_builder_count,
            source_underrun_reached_deadline_sender_count: value
                .source_underrun_reached_deadline_sender_count,
            discarded_source_frame_count: value.discarded_source_frame_count,
            discarded_source_duration_ms: value.discarded_source_duration_ms,
            stop_discarded_source_frame_count: value.stop_discarded_source_frame_count,
            stop_discarded_source_duration_ms: value.stop_discarded_source_duration_ms,
            interruption_discarded_source_frame_count: value
                .interruption_discarded_source_frame_count,
            interruption_discarded_source_duration_ms: value
                .interruption_discarded_source_duration_ms,
            restored_source_frame_count: value.restored_source_frame_count,
            restored_source_duration_ms: value.restored_source_duration_ms,
            source_buffer_target_ms: value.source_buffer_target_ms,
            adaptive_buffer_target_ms: value.adaptive_buffer_target_ms,
            max_adaptive_buffer_target_ms: value.max_adaptive_buffer_target_ms,
            buffer_low_watermark_count: value.buffer_low_watermark_count,
            source_buffer_low_watermark_count: value.source_buffer_low_watermark_count,
            playout_buffer_low_watermark_count: value.playout_buffer_low_watermark_count,
            buffer_underrun_count: value.buffer_underrun_count,
            playout_underrun_count: value.playout_underrun_count,
            egress_underrun_count: value.egress_underrun_count,
            egress_inserted_silence_duration_ms: value.egress_inserted_silence_duration_ms,
            egress_dropped_music_frame_count: value.egress_dropped_music_frame_count,
            egress_dropped_music_duration_ms: value.egress_dropped_music_duration_ms,
            source_underrun_count: value.source_underrun_count,
            rebuffer_count: value.rebuffer_count,
            refill_duration: (&value.refill_duration).into(),
            source_producer_fill_duration: (&value.source_producer_fill_duration).into(),
            producer_stall_duration: (&value.producer_stall_duration).into(),
            max_producer_lag_ms: value.max_producer_lag_ms,
            http_retry_count: value.http_retry_count,
            response_open_count: value.response_open_count,
            range_reopen_count: value.range_reopen_count,
            read_error_reopen_count: value.read_error_reopen_count,
            url_reresolve_count: value.url_reresolve_count,
            pause_resume_first_intervals_ms: value.pause_resume_first_intervals_ms.clone(),
            post_stall_first_intervals_ms: value.post_stall_first_intervals_ms.clone(),
            post_rebuffer_first_intervals_ms: value.post_rebuffer_first_intervals_ms.clone(),
            playout_sender_lateness: (&value.playout_sender_lateness).into(),
            playout_builder_prepare_duration: (&value.playout_builder_prepare_duration).into(),
            sender_send_duration: (&value.sender_send_duration).into(),
            sender_loop_non_send_work_duration: (&value.sender_loop_non_send_work_duration).into(),
            max_consecutive_playout_late_packets: value.max_consecutive_playout_late_packets,
            max_consecutive_late_egress_ticks: value.max_consecutive_late_egress_ticks,
            speaking_prepare_duration: (&value.speaking_prepare_duration).into(),
            sender_forbidden_work_count: value.sender_forbidden_work_count,
            gateway_event_drain_duration: (&value.gateway_event_drain_duration).into(),
            gateway_event_drain_count: value.gateway_event_drain_count,
            dave_transition_count: value.dave_transition_count,
            dave_transition_count_during_playback: value.dave_transition_count_during_playback,
            stale_dave_send_prevented_count: value.stale_dave_send_prevented_count,
            controlled_media_interruption_count: value.controlled_media_interruption_count,
            media_clock_reset_count: value.media_clock_reset_count,
            egress_clock_reset_count: value.egress_clock_reset_count,
            scheduler_late_reset_count: value.scheduler_late_reset_count,
            source_underrun_reset_count: value.source_underrun_reset_count,
            pause_resume_reset_count: value.pause_resume_reset_count,
            dave_transition_recovery_reset_count: value.dave_transition_recovery_reset_count,
            gateway_interruptions: value.gateway_interruptions,
            dave_interruptions: value.dave_interruptions,
            reconnect_interruptions: value.reconnect_interruptions,
            ended: value.ended,
        }
    }
}

pub fn emit_validation_evidence(evidence: &LiveValidationEvidence) -> Result<()> {
    let json = serde_json::to_string(evidence).context("serialize live validation evidence")?;
    if let Some(path) = std::env::var_os("LIVE_VALIDATION_EVIDENCE_PATH")
        && !path.is_empty()
    {
        fs::write(path, format!("{json}\n")).context("write live validation evidence file")?;
        return Ok(());
    }

    println!("{json}");
    Ok(())
}

pub fn finalize_success_evidence<State, BuildEvidence, EmitEvidence>(
    flow_result: Result<State>,
    cleanup: Result<()>,
    build_evidence: BuildEvidence,
    emit_evidence: EmitEvidence,
) -> Result<()>
where
    BuildEvidence: FnOnce(State) -> LiveValidationEvidence,
    EmitEvidence: FnOnce(&LiveValidationEvidence) -> Result<()>,
{
    match (flow_result, cleanup) {
        (Ok(state), Ok(())) => {
            let evidence = build_evidence(state);
            emit_evidence(&evidence)
        }
        (Ok(state), Err(cleanup_error)) => {
            tracing::warn!(
                error = %cleanup_error,
                "live validation succeeded but cleanup confirmation failed"
            );
            let evidence = build_evidence(state);
            emit_evidence(&evidence)
        }
        (Err(primary), Ok(())) => Err(primary),
        (Err(primary), Err(cleanup_error)) => {
            Err(primary.context(format!("cleanup also failed: {cleanup_error}")))
        }
    }
}

impl LiveContractState {
    pub fn mark_join_voice(&mut self) {
        self.validated_join_voice = true;
    }

    pub fn mark_update_voice_context(&mut self) {
        self.validated_update_voice_context = true;
    }

    pub fn mark_play(&mut self) {
        self.validated_play = true;
    }

    pub fn mark_pause(&mut self) {
        self.validated_pause = true;
        self.observer_proved_pause = true;
    }

    pub fn mark_resume(&mut self) {
        self.validated_resume = true;
        self.observer_proved_resume = true;
    }

    pub fn mark_invalid_resume_ignored(&mut self) {
        self.validated_invalid_resume_ignored = true;
    }

    pub fn mark_redundant_pause_ignored(&mut self) {
        self.validated_redundant_pause_ignored = true;
    }

    pub fn mark_reconnect_rollover_during_playback(&mut self) {
        self.validated_reconnect_rollover_during_playback = true;
    }

    pub fn mark_stop(&mut self) {
        self.validated_stop = true;
    }

    pub fn mark_stop_during_playback(&mut self) {
        self.validated_stop = true;
        self.validated_stop_during_playback = true;
    }

    pub fn mark_leave_voice(&mut self) {
        self.validated_leave_voice = true;
    }

    pub fn mark_leave_voice_during_playback(&mut self) {
        self.validated_leave_voice = true;
        self.validated_leave_voice_during_playback = true;
    }

    pub fn mark_get_state(&mut self) {
        self.validated_get_state = true;
    }

    pub fn mark_get_playback_metrics(&mut self) {
        self.validated_get_playback_metrics = true;
    }

    pub fn mark_subscribe_events(&mut self) {
        self.validated_subscribe_events = true;
    }

    pub fn ensure_complete(&self) -> Result<()> {
        let missing = self.missing_required_coverage();
        if missing.is_empty() {
            return Ok(());
        }

        anyhow::bail!(
            "live validation missing required coverage: {}",
            missing.join(", ")
        );
    }

    pub fn observe_event(&mut self, event: SessionEvent, expected_video_id: &str) -> Result<bool> {
        let kind = event.kind;

        match kind {
            SessionEventKind::VoiceConnecting
            | SessionEventKind::TrackResolving
            | SessionEventKind::Buffering
            | SessionEventKind::Stopped
                if self.saw_playing && !self.saw_track_ended =>
            {
                anyhow::bail!(
                    "playback left steady Playing state after start: {}",
                    kind.as_str_name()
                );
            }
            SessionEventKind::VoiceConnecting => {
                self.saw_voice_connecting = true;
            }
            SessionEventKind::VoiceReady => {
                self.saw_voice_ready = true;
            }
            SessionEventKind::TrackResolving => {
                validate_expected_video_id(&event, expected_video_id, kind)?;
                self.saw_track_resolving = true;
            }
            SessionEventKind::Playing if !self.saw_playing => {
                validate_expected_video_id(&event, expected_video_id, kind)?;
                self.saw_playing = true;
            }
            SessionEventKind::Playing => {
                validate_expected_video_id(&event, expected_video_id, kind)?;
            }
            SessionEventKind::TrackEnded => {
                if !self.saw_voice_ready {
                    anyhow::bail!("TrackEnded observed before VoiceReady");
                }
                if !self.saw_playing {
                    anyhow::bail!("TrackEnded observed before Playing");
                }
                if !self.saw_track_resolving {
                    anyhow::bail!("TrackEnded observed before TrackResolving");
                }
                validate_expected_video_id(&event, expected_video_id, kind)?;
                self.saw_track_ended = true;

                return Ok(true);
            }
            SessionEventKind::FatalError => {
                anyhow::bail!("FatalError observed: {}", display_event_message(&event));
            }
            SessionEventKind::PlaybackInterrupted => {
                anyhow::bail!(
                    "PlaybackInterrupted observed: {}",
                    display_event_message(&event)
                );
            }
            SessionEventKind::VoiceReconnecting if self.saw_playing && !self.saw_track_ended => {
                anyhow::bail!(
                    "VoiceReconnecting observed: {}",
                    display_event_message(&event)
                );
            }
            _ => {}
        }

        Ok(false)
    }

    fn missing_required_coverage(&self) -> Vec<&'static str> {
        let mut missing = Vec::new();
        if !self.validated_join_voice {
            missing.push("JoinVoice");
        }
        if !self.validated_update_voice_context {
            missing.push("UpdateVoiceContext");
        }
        if !self.validated_play {
            missing.push("Play");
        }
        if !self.validated_pause {
            missing.push("Pause");
        }
        if !self.validated_resume {
            missing.push("Resume");
        }
        if !self.validated_invalid_resume_ignored {
            missing.push("ignored invalid Resume");
        }
        if !self.validated_redundant_pause_ignored {
            missing.push("ignored redundant Pause");
        }
        if !self.observer_proved_pause {
            missing.push("Pause observer proof");
        }
        if !self.observer_proved_resume {
            missing.push("Resume observer proof");
        }
        if !self.validated_reconnect_rollover_during_playback {
            missing.push("reconnect rollover during playback");
        }
        if !self.validated_stop {
            missing.push("Stop");
        }
        if !self.validated_stop_during_playback {
            missing.push("Stop during playback");
        }
        if !self.validated_leave_voice {
            missing.push("LeaveVoice");
        }
        if !self.validated_leave_voice_during_playback {
            missing.push("LeaveVoice during playback");
        }
        if !self.validated_get_state {
            missing.push("GetState");
        }
        if !self.validated_get_playback_metrics {
            missing.push("GetPlaybackMetrics");
        }
        if !self.validated_subscribe_events {
            missing.push("SubscribeEvents");
        }
        if !self.saw_voice_connecting {
            missing.push("VoiceConnecting");
        }
        if !self.saw_voice_ready {
            missing.push("VoiceReady");
        }
        if !self.saw_track_resolving {
            missing.push("TrackResolving");
        }
        if !self.saw_playing {
            missing.push("Playing");
        }
        if !self.saw_track_ended {
            missing.push("TrackEnded");
        }
        missing
    }
}

fn validate_expected_video_id(
    event: &SessionEvent,
    expected_video_id: &str,
    kind: SessionEventKind,
) -> Result<()> {
    let current_video_id = event.current_video_id.as_deref().unwrap_or("").trim();
    if current_video_id == expected_video_id {
        return Ok(());
    }

    let observed = if current_video_id.is_empty() {
        "none".to_owned()
    } else {
        current_video_id.to_owned()
    };
    anyhow::bail!(
        "{} observed wrong current_video_id: expected video `{expected_video_id}`, got `{observed}`",
        kind.as_str_name()
    );
}

fn display_event_message(event: &SessionEvent) -> String {
    if event.message.as_deref().unwrap_or("").trim().is_empty() {
        "no message".to_owned()
    } else {
        event.message.as_ref().unwrap().clone()
    }
}

fn playback_send_command_kind_label(kind: TwilightPlaybackSendCommandKind) -> &'static str {
    match kind {
        TwilightPlaybackSendCommandKind::Track => "track",
        TwilightPlaybackSendCommandKind::ScheduledSilence => "scheduled_silence",
        TwilightPlaybackSendCommandKind::BoundarySilence => "boundary_silence",
        TwilightPlaybackSendCommandKind::OtherBoundary => "other_boundary",
        TwilightPlaybackSendCommandKind::Unspecified => "unspecified",
    }
}

fn prepared_track_queue_sample_phase_label(
    phase: TwilightPreparedTrackQueueSamplePhase,
) -> &'static str {
    match phase {
        TwilightPreparedTrackQueueSamplePhase::ActivePrePause => "active_pre_pause",
        TwilightPreparedTrackQueueSamplePhase::ActivePostResume => "active_post_resume",
        TwilightPreparedTrackQueueSamplePhase::Unspecified => "unspecified",
    }
}

fn prepared_playout_queue_event_kind_label(
    kind: TwilightPreparedPlayoutQueueEventKind,
) -> &'static str {
    match kind {
        TwilightPreparedPlayoutQueueEventKind::Enqueued => "enqueued",
        TwilightPreparedPlayoutQueueEventKind::DequeuedToDeadlineSender => {
            "dequeued_to_deadline_sender"
        }
        TwilightPreparedPlayoutQueueEventKind::DroppedBeforeSend => "dropped_before_send",
        TwilightPreparedPlayoutQueueEventKind::Rebuilt => "rebuilt",
        TwilightPreparedPlayoutQueueEventKind::Unspecified => "unspecified",
    }
}

fn prepared_playout_queue_event_reason_label(
    reason: TwilightPreparedPlayoutQueueEventReason,
) -> &'static str {
    match reason {
        TwilightPreparedPlayoutQueueEventReason::SteadyPlayback => "steady_playback",
        TwilightPreparedPlayoutQueueEventReason::Pause => "pause",
        TwilightPreparedPlayoutQueueEventReason::Stop => "stop",
        TwilightPreparedPlayoutQueueEventReason::DaveTransitionRecovery => {
            "dave_transition_recovery"
        }
        TwilightPreparedPlayoutQueueEventReason::Reconnect => "reconnect",
        TwilightPreparedPlayoutQueueEventReason::SourceUnderrun => "source_underrun",
        TwilightPreparedPlayoutQueueEventReason::NaturalEnd => "natural_end",
        TwilightPreparedPlayoutQueueEventReason::Interruption => "interruption",
        TwilightPreparedPlayoutQueueEventReason::Unspecified => "unspecified",
    }
}
