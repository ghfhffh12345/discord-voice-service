use std::time::Duration;

use discord_voice_service_playback::media::http_stream::HttpOpusStreamMetrics;
use discord_voice_service_playback::media::opus_queue::{
    OpusBufferDepth, duration_from_samples, duration_ms_from_samples, samples_from_duration_ms_u64,
};
use discord_voice_service_playback::recovery::PlaybackRecoveryMetrics;
use discord_voice_service_voice::VoiceGatewayDrainReport;
use tokio::time::Instant;

const FIRST_INTERVAL_SAMPLE_LIMIT: usize = 10;
const LATE_PACKET_THRESHOLD: Duration = Duration::from_millis(5);
const TRACK_TEMPO_WINDOW_PACKETS: usize = 50;
const TRACK_POST_SOURCE_BUFFER_MEDIA_MS: u64 = 5_000;
const MIN_MEDIA_TO_WALL_CLOCK_RATIO_PPM: u64 = 980_000;
const MAX_MEDIA_TO_WALL_CLOCK_RATIO_PPM: u64 = 1_020_000;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DurationStatsSnapshot {
    pub samples: usize,
    pub p50_ms: u64,
    pub p95_ms: u64,
    pub p99_ms: u64,
    pub min_ms: u64,
    pub max_ms: u64,
}

impl Default for DurationStatsSnapshot {
    fn default() -> Self {
        Self::empty()
    }
}

impl DurationStatsSnapshot {
    pub fn empty() -> Self {
        Self {
            samples: 0,
            p50_ms: 0,
            p95_ms: 0,
            p99_ms: 0,
            min_ms: 0,
            max_ms: 0,
        }
    }

    fn from_samples(samples: &[Duration]) -> Self {
        if samples.is_empty() {
            return Self::empty();
        }

        let mut sorted = samples.to_vec();
        sorted.sort_unstable();
        Self {
            samples: sorted.len(),
            p50_ms: duration_millis(percentile_duration(&sorted, 50)),
            p95_ms: duration_millis(percentile_duration(&sorted, 95)),
            p99_ms: duration_millis(percentile_duration(&sorted, 99)),
            min_ms: duration_millis(sorted[0]),
            max_ms: duration_millis(sorted[sorted.len() - 1]),
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PlaybackBufferDepthSnapshot {
    pub packets: usize,
    pub bytes: usize,
    pub duration_ms: u64,
    pub duration_samples: u64,
}

impl From<OpusBufferDepth> for PlaybackBufferDepthSnapshot {
    fn from(depth: OpusBufferDepth) -> Self {
        Self {
            packets: depth.packets,
            bytes: depth.bytes,
            duration_ms: depth.duration_ms,
            duration_samples: depth.duration_samples,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PlaybackQueueDepthStatsSnapshot {
    pub sample_count: usize,
    pub empty_count: u64,
    pub current_depth: PlaybackBufferDepthSnapshot,
    pub min_depth: PlaybackBufferDepthSnapshot,
    pub p5_depth: PlaybackBufferDepthSnapshot,
    pub p50_depth: PlaybackBufferDepthSnapshot,
    pub p95_depth: PlaybackBufferDepthSnapshot,
    pub max_depth: PlaybackBufferDepthSnapshot,
}

impl PlaybackQueueDepthStatsSnapshot {
    fn from_samples(samples: &[PlaybackBufferDepthSnapshot], empty_count: u64) -> Self {
        if samples.is_empty() {
            return Self::default();
        }

        let mut sorted = samples.to_vec();
        sorted.sort_unstable_by_key(|depth| depth.duration_samples);
        Self {
            sample_count: samples.len(),
            empty_count,
            current_depth: samples[samples.len() - 1],
            min_depth: sorted[0],
            p5_depth: percentile_depth(&sorted, 5),
            p50_depth: percentile_depth(&sorted, 50),
            p95_depth: percentile_depth(&sorted, 95),
            max_depth: sorted[sorted.len() - 1],
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum PlaybackSendCommandKind {
    Track,
    ScheduledSilence,
    BoundarySilence,
    #[default]
    OtherBoundary,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum PreparedTrackQueueSamplePhase {
    #[default]
    ActivePrePause,
    ActivePostResume,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PlaybackSendEventSnapshot {
    pub packet_index: u64,
    pub command_kind: PlaybackSendCommandKind,
    pub expected_deadline_offset_us: u64,
    pub send_started_offset_us: u64,
    pub sent_offset_us: u64,
    pub media_duration_ms: u64,
    pub media_duration_samples: u32,
    pub rtp_sequence: u16,
    pub rtp_timestamp: u32,
    pub protection_nonce: Option<u32>,
    pub source_frame_epoch: Option<u64>,
    pub source_media_position_ms: Option<u64>,
    pub source_media_position_samples: Option<u64>,
    pub source_media_byte_position: Option<u64>,
    pub committed_heard_media: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PreparedTrackQueueDepthSampleSnapshot {
    pub sample_index: u64,
    pub phase: PreparedTrackQueueSamplePhase,
    pub depth: PlaybackBufferDepthSnapshot,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum PreparedPlayoutQueueEventKind {
    #[default]
    Unknown,
    Enqueued,
    DequeuedToDeadlineSender,
    DroppedBeforeSend,
    Rebuilt,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum PreparedPlayoutQueueEventReason {
    #[default]
    Unspecified,
    SteadyPlayback,
    Pause,
    Stop,
    DaveTransitionRecovery,
    Reconnect,
    SourceUnderrun,
    NaturalEnd,
    Interruption,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreparedPlayoutQueueEventSnapshot {
    pub event_index: u64,
    pub event_kind: PreparedPlayoutQueueEventKind,
    pub reason: PreparedPlayoutQueueEventReason,
    pub command_kind: PlaybackSendCommandKind,
    pub media_duration_ms: u64,
    pub media_duration_samples: u32,
    pub rtp_sequence: u32,
    pub rtp_timestamp: u32,
    pub protection_nonce: Option<u32>,
    pub source_frame_epoch: Option<u64>,
    pub source_media_position_ms: Option<u64>,
    pub source_media_position_samples: Option<u64>,
    pub source_media_byte_position: Option<u64>,
    pub queue_depth_after: PlaybackBufferDepthSnapshot,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PreparedPlayoutTrackDropIdentity {
    reason: PreparedPlayoutQueueEventReason,
    media_duration_samples: u32,
    source_frame_epoch: u64,
    source_media_position_samples: u64,
    source_media_byte_position: Option<u64>,
}

fn prepared_track_drop_counts_as_unrecovered(event: &PreparedPlayoutQueueEventSnapshot) -> bool {
    !matches!(
        event.reason,
        PreparedPlayoutQueueEventReason::Stop | PreparedPlayoutQueueEventReason::Reconnect
    )
}

fn prepared_playout_track_drop_identity(
    event: &PreparedPlayoutQueueEventSnapshot,
) -> Option<PreparedPlayoutTrackDropIdentity> {
    if event.command_kind != PlaybackSendCommandKind::Track {
        return None;
    }

    Some(PreparedPlayoutTrackDropIdentity {
        reason: event.reason,
        media_duration_samples: event.media_duration_samples,
        source_frame_epoch: event.source_frame_epoch?,
        source_media_position_samples: event.source_media_position_samples?,
        source_media_byte_position: event.source_media_byte_position,
    })
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PlaybackStabilitySnapshot {
    pub playback_epoch: u64,
    pub video_id: Option<String>,
    pub selected_itag: Option<u32>,
    pub track_packet_count: usize,
    pub continuity_silence_packet_count: usize,
    pub inserted_silence_duration_ms: u64,
    pub track_interval: DurationStatsSnapshot,
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
    pub skipped_source_duration_samples: u64,
    pub tempo_rebase_count: u64,
    pub expected_track_frame_count: u64,
    pub sent_track_frame_count: u64,
    pub silence_frame_count: u64,
    pub frame_deficit_count: u64,
    pub dropped_frame_count: u64,
    pub late_frame_count: u64,
    pub all_packet_interval: DurationStatsSnapshot,
    pub sender_lateness: DurationStatsSnapshot,
    pub max_consecutive_late_packets: usize,
    pub current_consecutive_late_packets: usize,
    pub current_buffer_depth: PlaybackBufferDepthSnapshot,
    pub min_buffer_depth: PlaybackBufferDepthSnapshot,
    pub max_buffer_depth: PlaybackBufferDepthSnapshot,
    pub current_source_buffer_depth: PlaybackBufferDepthSnapshot,
    pub min_source_buffer_depth: PlaybackBufferDepthSnapshot,
    pub max_source_buffer_depth: PlaybackBufferDepthSnapshot,
    pub source_buffer_depth: PlaybackQueueDepthStatsSnapshot,
    pub current_playout_buffer_depth: PlaybackBufferDepthSnapshot,
    pub min_playout_buffer_depth: PlaybackBufferDepthSnapshot,
    pub max_playout_buffer_depth: PlaybackBufferDepthSnapshot,
    pub egress_buffer_target_ms: u64,
    pub current_egress_buffer_depth: PlaybackBufferDepthSnapshot,
    pub min_egress_buffer_depth: PlaybackBufferDepthSnapshot,
    pub max_egress_buffer_depth: PlaybackBufferDepthSnapshot,
    pub prepared_rtp_queue_depth_ms: u64,
    pub prepared_track_queue_target_ms: u64,
    pub prepared_track_queue_low_watermark_ms: u64,
    pub prepared_track_queue_high_watermark_ms: u64,
    pub active_pre_pause_prepared_track_queue_depth: PlaybackQueueDepthStatsSnapshot,
    pub active_post_resume_prepared_track_queue_depth: PlaybackQueueDepthStatsSnapshot,
    pub prepared_track_queue_depth_sample_count: usize,
    pub prepared_track_queue_empty_count: u64,
    pub raw_send_events: Vec<PlaybackSendEventSnapshot>,
    pub raw_prepared_track_queue_samples: Vec<PreparedTrackQueueDepthSampleSnapshot>,
    pub raw_prepared_playout_queue_events: Vec<PreparedPlayoutQueueEventSnapshot>,
    pub current_scheduled_silence_queue_depth: PlaybackBufferDepthSnapshot,
    pub max_scheduled_silence_queue_depth: PlaybackBufferDepthSnapshot,
    pub current_boundary_queue_depth: PlaybackBufferDepthSnapshot,
    pub max_boundary_queue_depth: PlaybackBufferDepthSnapshot,
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
    pub discarded_source_duration_samples: u64,
    pub stop_discarded_source_frame_count: u64,
    pub stop_discarded_source_duration_ms: u64,
    pub stop_discarded_source_duration_samples: u64,
    pub interruption_discarded_source_frame_count: u64,
    pub interruption_discarded_source_duration_ms: u64,
    pub interruption_discarded_source_duration_samples: u64,
    pub restored_source_frame_count: u64,
    pub restored_source_duration_ms: u64,
    pub restored_source_duration_samples: u64,
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
    pub egress_dropped_music_duration_samples: u64,
    pub source_underrun_count: u64,
    pub rebuffer_count: u64,
    pub refill_duration: DurationStatsSnapshot,
    pub source_producer_fill_duration: DurationStatsSnapshot,
    pub producer_stall_duration: DurationStatsSnapshot,
    pub max_producer_lag_ms: u64,
    pub http_retry_count: u64,
    pub response_open_count: u64,
    pub range_reopen_count: u64,
    pub read_error_reopen_count: u64,
    pub url_reresolve_count: u64,
    pub pause_resume_first_intervals_ms: Vec<u64>,
    pub post_stall_first_intervals_ms: Vec<u64>,
    pub post_rebuffer_first_intervals_ms: Vec<u64>,
    pub playout_sender_lateness: DurationStatsSnapshot,
    pub playout_builder_prepare_duration: DurationStatsSnapshot,
    pub sender_send_duration: DurationStatsSnapshot,
    pub sender_loop_non_send_work_duration: DurationStatsSnapshot,
    pub max_consecutive_playout_late_packets: usize,
    pub max_consecutive_late_egress_ticks: usize,
    pub speaking_prepare_duration: DurationStatsSnapshot,
    pub sender_forbidden_work_count: u64,
    pub gateway_event_drain_duration: DurationStatsSnapshot,
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum MediaClockResetReason {
    PauseResume,
    SchedulerLate,
    SourceUnderrun,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct ProducerMetricsSample {
    pub fill_duration: Duration,
    pub produced_frames: usize,
    pub source_buffer_depth: OpusBufferDepth,
    pub stream_metrics: HttpOpusStreamMetrics,
    pub recovery_metrics: PlaybackRecoveryMetrics,
}

#[derive(Debug, Clone, Copy)]
struct TrackPacketTiming {
    send_started_at: Instant,
    duration: Duration,
    media_start: Duration,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct PlaybackSendEventRecord {
    pub(crate) packet_index: u64,
    pub(crate) command_kind: PlaybackSendCommandKind,
    pub(crate) expected_deadline: Instant,
    pub(crate) send_started_at: Instant,
    pub(crate) sent_at: Instant,
    pub(crate) media_duration_ms: u64,
    pub(crate) media_duration_samples: u32,
    pub(crate) rtp_sequence: u16,
    pub(crate) rtp_timestamp: u32,
    pub(crate) protection_nonce: Option<u32>,
    pub(crate) source_frame_epoch: Option<u64>,
    pub(crate) source_media_position_ms: Option<u64>,
    pub(crate) source_media_position_samples: Option<u64>,
    pub(crate) source_media_byte_position: Option<u64>,
    pub(crate) committed_heard_media: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PreparedTrackQueuePhase {
    PrePause,
    PostResume,
}

#[derive(Debug, Default)]
struct PreparedTrackQueueDepthCollector {
    warmed: bool,
    samples: Vec<PlaybackBufferDepthSnapshot>,
    empty_count: u64,
}

impl PreparedTrackQueueDepthCollector {
    fn record(&mut self, depth: PlaybackBufferDepthSnapshot, target_ms: u64) -> bool {
        if !self.warmed {
            if depth.duration_samples < samples_from_duration_ms_u64(target_ms) {
                return false;
            }
            self.warmed = true;
        }

        if depth.duration_samples == 0 {
            self.empty_count = self.empty_count.saturating_add(1);
        }
        self.samples.push(depth);
        true
    }

    fn snapshot(&self) -> PlaybackQueueDepthStatsSnapshot {
        PlaybackQueueDepthStatsSnapshot::from_samples(&self.samples, self.empty_count)
    }
}

#[derive(Debug)]
pub(crate) struct PlaybackStabilityCollector {
    playback_epoch: u64,
    video_id: String,
    selected_itag: u32,
    recovery_baseline: PlaybackRecoveryMetrics,
    track_packet_count: usize,
    continuity_silence_packet_count: usize,
    inserted_silence_duration_ms: u64,
    track_intervals: Vec<Duration>,
    all_packet_intervals: Vec<Duration>,
    sender_lateness: Vec<Duration>,
    refill_durations: Vec<Duration>,
    producer_stall_durations: Vec<Duration>,
    last_track_send_started_at: Option<Instant>,
    last_track_duration: Option<Duration>,
    active_track_wall_clock_elapsed: Duration,
    track_packet_timings: Vec<TrackPacketTiming>,
    last_any_send_at: Option<Instant>,
    last_producer_sample_at: Option<Instant>,
    max_producer_lag: Duration,
    current_buffer_depth: PlaybackBufferDepthSnapshot,
    min_buffer_depth: PlaybackBufferDepthSnapshot,
    max_buffer_depth: PlaybackBufferDepthSnapshot,
    current_source_buffer_depth: PlaybackBufferDepthSnapshot,
    min_source_buffer_depth: PlaybackBufferDepthSnapshot,
    max_source_buffer_depth: PlaybackBufferDepthSnapshot,
    source_buffer_depth_samples: Vec<PlaybackBufferDepthSnapshot>,
    source_buffer_empty_count: u64,
    current_playout_buffer_depth: PlaybackBufferDepthSnapshot,
    min_playout_buffer_depth: PlaybackBufferDepthSnapshot,
    max_playout_buffer_depth: PlaybackBufferDepthSnapshot,
    prepared_track_queue_target_ms: u64,
    prepared_track_queue_low_watermark_ms: u64,
    prepared_track_queue_high_watermark_ms: u64,
    prepared_track_queue_phase: PreparedTrackQueuePhase,
    active_pre_pause_prepared_track_queue_depth: PreparedTrackQueueDepthCollector,
    active_post_resume_prepared_track_queue_depth: PreparedTrackQueueDepthCollector,
    raw_send_events: Vec<PlaybackSendEventRecord>,
    raw_prepared_track_queue_samples: Vec<PreparedTrackQueueDepthSampleSnapshot>,
    raw_prepared_playout_queue_events: Vec<PreparedPlayoutQueueEventSnapshot>,
    unmatched_prepared_track_drops: Vec<PreparedPlayoutTrackDropIdentity>,
    current_scheduled_silence_queue_depth: PlaybackBufferDepthSnapshot,
    max_scheduled_silence_queue_depth: PlaybackBufferDepthSnapshot,
    current_boundary_queue_depth: PlaybackBufferDepthSnapshot,
    max_boundary_queue_depth: PlaybackBufferDepthSnapshot,
    prepared_track_packet_drop_count: u64,
    prepared_silence_packet_drop_count: u64,
    prepared_packet_rebuild_count: u64,
    scheduled_silence_packet_count: u64,
    pause_media_boundary_count: u64,
    stop_media_boundary_count: u64,
    recovery_media_boundary_count: u64,
    natural_end_media_boundary_count: u64,
    dave_transition_recovery_reached_builder_count: u64,
    dave_transition_recovery_reached_deadline_sender_count: u64,
    source_underrun_reached_builder_count: u64,
    source_underrun_reached_deadline_sender_count: u64,
    discarded_source_frame_count: u64,
    discarded_source_duration_samples: u64,
    stop_discarded_source_frame_count: u64,
    stop_discarded_source_duration_samples: u64,
    interruption_discarded_source_frame_count: u64,
    interruption_discarded_source_duration_samples: u64,
    restored_source_frame_count: u64,
    restored_source_duration_samples: u64,
    source_buffer_target_ms: u64,
    adaptive_buffer_target_ms: u64,
    max_adaptive_buffer_target_ms: u64,
    buffer_low_watermark_count: u64,
    source_buffer_low_watermark_count: u64,
    playout_buffer_low_watermark_count: u64,
    buffer_underrun_count: u64,
    playout_underrun_count: u64,
    egress_dropped_music_frame_count: u64,
    egress_dropped_music_duration_samples: u64,
    source_underrun_count: u64,
    rebuffer_count: u64,
    pause_resume_first_intervals_ms: Vec<u64>,
    post_stall_first_intervals_ms: Vec<u64>,
    post_rebuffer_first_intervals_ms: Vec<u64>,
    pause_resume_intervals_remaining: usize,
    post_stall_intervals_remaining: usize,
    post_rebuffer_intervals_remaining: usize,
    http_retry_count: u64,
    response_open_count: u64,
    range_reopen_count: u64,
    read_error_reopen_count: u64,
    url_reresolve_count: u64,
    max_consecutive_late_packets: usize,
    current_consecutive_late_packets: usize,
    late_frame_count: u64,
    playout_sender_lateness: Vec<Duration>,
    max_consecutive_playout_late_packets: usize,
    current_consecutive_playout_late_packets: usize,
    speaking_prepare_durations: Vec<Duration>,
    playout_builder_prepare_durations: Vec<Duration>,
    sender_send_durations: Vec<Duration>,
    sender_loop_non_send_work_durations: Vec<Duration>,
    sender_forbidden_work_count: u64,
    gateway_event_drain_durations: Vec<Duration>,
    gateway_event_drain_count: u64,
    dave_transition_count: u64,
    dave_transition_count_during_playback: u64,
    stale_dave_send_prevented_count: u64,
    controlled_media_interruption_count: u64,
    track_media_duration_sent: Duration,
    track_fast_interval_count: u64,
    track_fast_interval_min: Option<Duration>,
    track_tempo_window_count: u64,
    track_tempo_window_post_source_buffer_count: u64,
    track_tempo_window_min_ratio_ppm: u64,
    track_tempo_window_max_ratio_ppm: u64,
    track_tempo_window_fast_count: u64,
    track_tempo_window_fastest_ratio_ppm: u64,
    track_tempo_window_fastest_media_ms: u64,
    track_tempo_window_fastest_wall_clock: Option<Duration>,
    track_tempo_window_slow_count: u64,
    track_tempo_window_slowest_ratio_ppm: u64,
    track_tempo_window_slowest_media_ms: u64,
    track_tempo_window_slowest_wall_clock: Option<Duration>,
    skipped_source_frame_count: u64,
    skipped_source_duration_samples: u64,
    tempo_rebase_count: u64,
    media_clock_reset_count: u64,
    scheduler_late_reset_count: u64,
    source_underrun_reset_count: u64,
    pause_resume_reset_count: u64,
    dave_transition_recovery_reset_count: u64,
}

impl PlaybackStabilityCollector {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        playback_epoch: u64,
        video_id: String,
        selected_itag: u32,
        initial_depth: OpusBufferDepth,
        initial_source_depth: OpusBufferDepth,
        source_buffer_target_ms: u64,
        prepared_track_queue_target_ms: u64,
        prepared_track_queue_low_watermark_ms: u64,
        prepared_track_queue_high_watermark_ms: u64,
        recovery_baseline: PlaybackRecoveryMetrics,
    ) -> Self {
        let initial_depth = PlaybackBufferDepthSnapshot::from(initial_depth);
        let initial_source_depth = PlaybackBufferDepthSnapshot::from(initial_source_depth);
        Self {
            playback_epoch,
            video_id,
            selected_itag,
            recovery_baseline,
            track_packet_count: 0,
            continuity_silence_packet_count: 0,
            inserted_silence_duration_ms: 0,
            track_intervals: Vec::new(),
            all_packet_intervals: Vec::new(),
            sender_lateness: Vec::new(),
            refill_durations: Vec::new(),
            producer_stall_durations: Vec::new(),
            last_track_send_started_at: None,
            last_track_duration: None,
            active_track_wall_clock_elapsed: Duration::ZERO,
            track_packet_timings: Vec::new(),
            last_any_send_at: None,
            last_producer_sample_at: None,
            max_producer_lag: Duration::ZERO,
            current_buffer_depth: initial_depth,
            min_buffer_depth: initial_depth,
            max_buffer_depth: initial_depth,
            current_source_buffer_depth: initial_source_depth,
            min_source_buffer_depth: initial_source_depth,
            max_source_buffer_depth: initial_source_depth,
            source_buffer_depth_samples: vec![initial_source_depth],
            source_buffer_empty_count: u64::from(initial_source_depth.duration_samples == 0),
            current_playout_buffer_depth: PlaybackBufferDepthSnapshot::default(),
            min_playout_buffer_depth: PlaybackBufferDepthSnapshot::default(),
            max_playout_buffer_depth: PlaybackBufferDepthSnapshot::default(),
            prepared_track_queue_target_ms,
            prepared_track_queue_low_watermark_ms,
            prepared_track_queue_high_watermark_ms,
            prepared_track_queue_phase: PreparedTrackQueuePhase::PrePause,
            active_pre_pause_prepared_track_queue_depth: PreparedTrackQueueDepthCollector::default(
            ),
            active_post_resume_prepared_track_queue_depth:
                PreparedTrackQueueDepthCollector::default(),
            raw_send_events: Vec::new(),
            raw_prepared_track_queue_samples: Vec::new(),
            raw_prepared_playout_queue_events: Vec::new(),
            unmatched_prepared_track_drops: Vec::new(),
            current_scheduled_silence_queue_depth: PlaybackBufferDepthSnapshot::default(),
            max_scheduled_silence_queue_depth: PlaybackBufferDepthSnapshot::default(),
            current_boundary_queue_depth: PlaybackBufferDepthSnapshot::default(),
            max_boundary_queue_depth: PlaybackBufferDepthSnapshot::default(),
            prepared_track_packet_drop_count: 0,
            prepared_silence_packet_drop_count: 0,
            prepared_packet_rebuild_count: 0,
            scheduled_silence_packet_count: 0,
            pause_media_boundary_count: 0,
            stop_media_boundary_count: 0,
            recovery_media_boundary_count: 0,
            natural_end_media_boundary_count: 0,
            dave_transition_recovery_reached_builder_count: 0,
            dave_transition_recovery_reached_deadline_sender_count: 0,
            source_underrun_reached_builder_count: 0,
            source_underrun_reached_deadline_sender_count: 0,
            discarded_source_frame_count: 0,
            discarded_source_duration_samples: 0,
            stop_discarded_source_frame_count: 0,
            stop_discarded_source_duration_samples: 0,
            interruption_discarded_source_frame_count: 0,
            interruption_discarded_source_duration_samples: 0,
            restored_source_frame_count: 0,
            restored_source_duration_samples: 0,
            source_buffer_target_ms,
            adaptive_buffer_target_ms: 0,
            max_adaptive_buffer_target_ms: 0,
            buffer_low_watermark_count: 0,
            source_buffer_low_watermark_count: 0,
            playout_buffer_low_watermark_count: 0,
            buffer_underrun_count: 0,
            playout_underrun_count: 0,
            egress_dropped_music_frame_count: 0,
            egress_dropped_music_duration_samples: 0,
            source_underrun_count: 0,
            rebuffer_count: 0,
            pause_resume_first_intervals_ms: Vec::new(),
            post_stall_first_intervals_ms: Vec::new(),
            post_rebuffer_first_intervals_ms: Vec::new(),
            pause_resume_intervals_remaining: 0,
            post_stall_intervals_remaining: 0,
            post_rebuffer_intervals_remaining: 0,
            http_retry_count: 0,
            response_open_count: 0,
            range_reopen_count: 0,
            read_error_reopen_count: 0,
            url_reresolve_count: 0,
            max_consecutive_late_packets: 0,
            current_consecutive_late_packets: 0,
            late_frame_count: 0,
            playout_sender_lateness: Vec::new(),
            max_consecutive_playout_late_packets: 0,
            current_consecutive_playout_late_packets: 0,
            speaking_prepare_durations: Vec::new(),
            playout_builder_prepare_durations: Vec::new(),
            sender_send_durations: Vec::new(),
            sender_loop_non_send_work_durations: Vec::new(),
            sender_forbidden_work_count: 0,
            gateway_event_drain_durations: Vec::new(),
            gateway_event_drain_count: 0,
            dave_transition_count: 0,
            dave_transition_count_during_playback: 0,
            stale_dave_send_prevented_count: 0,
            controlled_media_interruption_count: 0,
            track_media_duration_sent: Duration::ZERO,
            track_fast_interval_count: 0,
            track_fast_interval_min: None,
            track_tempo_window_count: 0,
            track_tempo_window_post_source_buffer_count: 0,
            track_tempo_window_min_ratio_ppm: 0,
            track_tempo_window_max_ratio_ppm: 0,
            track_tempo_window_fast_count: 0,
            track_tempo_window_fastest_ratio_ppm: 0,
            track_tempo_window_fastest_media_ms: 0,
            track_tempo_window_fastest_wall_clock: None,
            track_tempo_window_slow_count: 0,
            track_tempo_window_slowest_ratio_ppm: 0,
            track_tempo_window_slowest_media_ms: 0,
            track_tempo_window_slowest_wall_clock: None,
            skipped_source_frame_count: 0,
            skipped_source_duration_samples: 0,
            tempo_rebase_count: 0,
            media_clock_reset_count: 0,
            scheduler_late_reset_count: 0,
            source_underrun_reset_count: 0,
            pause_resume_reset_count: 0,
            dave_transition_recovery_reset_count: 0,
        }
    }

    pub(crate) fn record_producer_sample(&mut self, sample: ProducerMetricsSample) {
        self.refill_durations.push(sample.fill_duration);
        self.producer_stall_durations.push(sample.fill_duration);

        let now = Instant::now();
        if let Some(previous) = self.last_producer_sample_at.replace(now) {
            let lag = now.saturating_duration_since(previous);
            self.max_producer_lag = self.max_producer_lag.max(lag);
        }

        self.response_open_count = sample.stream_metrics.response_open_count;
        self.range_reopen_count = sample.stream_metrics.range_reopen_count;
        self.read_error_reopen_count = sample.stream_metrics.read_error_reopen_count;
        self.http_retry_count = sample
            .recovery_metrics
            .http_retry_count
            .saturating_sub(self.recovery_baseline.http_retry_count);
        self.url_reresolve_count = sample
            .recovery_metrics
            .url_reresolve_count
            .saturating_sub(self.recovery_baseline.url_reresolve_count);

        if sample.produced_frames > 0 || sample.source_buffer_depth.packets == 0 {
            self.record_source_buffer_depth(sample.source_buffer_depth, u64::MAX);
        }
    }

    pub(crate) fn record_source_buffer_depth(
        &mut self,
        depth: OpusBufferDepth,
        low_watermark_ms: u64,
    ) {
        let depth = PlaybackBufferDepthSnapshot::from(depth);
        self.current_source_buffer_depth = depth;
        self.min_source_buffer_depth = min_depth(self.min_source_buffer_depth, depth);
        self.max_source_buffer_depth = max_depth(self.max_source_buffer_depth, depth);
        self.source_buffer_depth_samples.push(depth);
        if depth.duration_samples == 0 {
            self.source_buffer_empty_count = self.source_buffer_empty_count.saturating_add(1);
        }
        if low_watermark_ms != u64::MAX
            && depth.duration_samples <= samples_from_duration_ms_u64(low_watermark_ms)
        {
            self.source_buffer_low_watermark_count =
                self.source_buffer_low_watermark_count.saturating_add(1);
        }
    }

    pub(crate) fn record_playout_buffer_depth(
        &mut self,
        depth: OpusBufferDepth,
        low_watermark_ms: u64,
    ) {
        let depth = PlaybackBufferDepthSnapshot::from(depth);
        self.current_playout_buffer_depth = depth;
        self.min_playout_buffer_depth = min_depth(self.min_playout_buffer_depth, depth);
        self.max_playout_buffer_depth = max_depth(self.max_playout_buffer_depth, depth);
        if low_watermark_ms != u64::MAX
            && depth.duration_samples <= samples_from_duration_ms_u64(low_watermark_ms)
        {
            self.playout_buffer_low_watermark_count =
                self.playout_buffer_low_watermark_count.saturating_add(1);
        }
    }

    pub(crate) fn record_prepared_track_queue_depth(&mut self, depth: OpusBufferDepth) {
        let depth = PlaybackBufferDepthSnapshot::from(depth);
        let (phase, recorded) = match self.prepared_track_queue_phase {
            PreparedTrackQueuePhase::PrePause => (
                PreparedTrackQueueSamplePhase::ActivePrePause,
                self.active_pre_pause_prepared_track_queue_depth
                    .record(depth, self.prepared_track_queue_target_ms),
            ),
            PreparedTrackQueuePhase::PostResume => (
                PreparedTrackQueueSamplePhase::ActivePostResume,
                self.active_post_resume_prepared_track_queue_depth
                    .record(depth, self.prepared_track_queue_target_ms),
            ),
        };
        if recorded {
            self.raw_prepared_track_queue_samples
                .push(PreparedTrackQueueDepthSampleSnapshot {
                    sample_index: self.raw_prepared_track_queue_samples.len() as u64,
                    phase,
                    depth,
                });
        }
    }

    pub(crate) fn record_prepared_playout_queue_event(
        &mut self,
        mut event: PreparedPlayoutQueueEventSnapshot,
    ) {
        event.event_index = self.raw_prepared_playout_queue_events.len() as u64;
        if event.event_kind == PreparedPlayoutQueueEventKind::DroppedBeforeSend {
            match event.command_kind {
                PlaybackSendCommandKind::Track => {
                    if prepared_track_drop_counts_as_unrecovered(&event) {
                        self.prepared_track_packet_drop_count =
                            self.prepared_track_packet_drop_count.saturating_add(1);
                        if let Some(identity) = prepared_playout_track_drop_identity(&event) {
                            self.unmatched_prepared_track_drops.push(identity);
                        }
                    }
                }
                PlaybackSendCommandKind::ScheduledSilence
                | PlaybackSendCommandKind::BoundarySilence
                | PlaybackSendCommandKind::OtherBoundary => {
                    self.prepared_silence_packet_drop_count =
                        self.prepared_silence_packet_drop_count.saturating_add(1);
                }
            }
        }
        if event.event_kind == PreparedPlayoutQueueEventKind::Rebuilt {
            self.prepared_packet_rebuild_count =
                self.prepared_packet_rebuild_count.saturating_add(1);
            if let Some(identity) = prepared_playout_track_drop_identity(&event)
                && let Some(index) = self
                    .unmatched_prepared_track_drops
                    .iter()
                    .position(|drop| *drop == identity)
            {
                self.unmatched_prepared_track_drops.remove(index);
                self.prepared_track_packet_drop_count =
                    self.prepared_track_packet_drop_count.saturating_sub(1);
            }
        }
        self.raw_prepared_playout_queue_events.push(event);
    }

    pub(crate) fn record_non_track_playout_queue_depth(
        &mut self,
        command_kind: PlaybackSendCommandKind,
        depth: OpusBufferDepth,
    ) {
        let depth = PlaybackBufferDepthSnapshot::from(depth);
        match command_kind {
            PlaybackSendCommandKind::ScheduledSilence => {
                self.current_scheduled_silence_queue_depth = depth;
                self.max_scheduled_silence_queue_depth =
                    max_depth(self.max_scheduled_silence_queue_depth, depth);
            }
            PlaybackSendCommandKind::BoundarySilence | PlaybackSendCommandKind::OtherBoundary => {
                self.current_boundary_queue_depth = depth;
                self.max_boundary_queue_depth = max_depth(self.max_boundary_queue_depth, depth);
            }
            PlaybackSendCommandKind::Track => {}
        }
    }

    pub(crate) fn record_explicit_media_boundary(
        &mut self,
        reason: PreparedPlayoutQueueEventReason,
    ) {
        match reason {
            PreparedPlayoutQueueEventReason::Pause => {
                self.pause_media_boundary_count = self.pause_media_boundary_count.saturating_add(1);
            }
            PreparedPlayoutQueueEventReason::Stop
            | PreparedPlayoutQueueEventReason::Interruption => {
                self.stop_media_boundary_count = self.stop_media_boundary_count.saturating_add(1);
            }
            PreparedPlayoutQueueEventReason::DaveTransitionRecovery
            | PreparedPlayoutQueueEventReason::Reconnect
            | PreparedPlayoutQueueEventReason::SourceUnderrun => {
                self.recovery_media_boundary_count =
                    self.recovery_media_boundary_count.saturating_add(1);
            }
            PreparedPlayoutQueueEventReason::NaturalEnd => {
                self.natural_end_media_boundary_count =
                    self.natural_end_media_boundary_count.saturating_add(1);
            }
            PreparedPlayoutQueueEventReason::Unspecified
            | PreparedPlayoutQueueEventReason::SteadyPlayback => {}
        }
    }

    pub(crate) fn record_source_underrun_reached_builder(&mut self) {
        self.source_underrun_reached_builder_count =
            self.source_underrun_reached_builder_count.saturating_add(1);
    }

    pub(crate) fn record_source_underrun_reached_deadline_sender(&mut self) {
        self.source_underrun_reached_deadline_sender_count = self
            .source_underrun_reached_deadline_sender_count
            .saturating_add(1);
    }

    pub(crate) fn record_restored_source_frames(
        &mut self,
        frame_count: u64,
        duration_samples: u64,
    ) {
        self.restored_source_frame_count =
            self.restored_source_frame_count.saturating_add(frame_count);
        self.restored_source_duration_samples = self
            .restored_source_duration_samples
            .saturating_add(duration_samples);
    }

    pub(crate) fn record_discarded_source_frames(
        &mut self,
        reason: PreparedPlayoutQueueEventReason,
        frame_count: u64,
        duration_samples: u64,
    ) {
        self.discarded_source_frame_count = self
            .discarded_source_frame_count
            .saturating_add(frame_count);
        self.discarded_source_duration_samples = self
            .discarded_source_duration_samples
            .saturating_add(duration_samples);
        match reason {
            PreparedPlayoutQueueEventReason::Stop | PreparedPlayoutQueueEventReason::NaturalEnd => {
                self.stop_discarded_source_frame_count = self
                    .stop_discarded_source_frame_count
                    .saturating_add(frame_count);
                self.stop_discarded_source_duration_samples = self
                    .stop_discarded_source_duration_samples
                    .saturating_add(duration_samples);
            }
            PreparedPlayoutQueueEventReason::Interruption
            | PreparedPlayoutQueueEventReason::Reconnect
            | PreparedPlayoutQueueEventReason::DaveTransitionRecovery
            | PreparedPlayoutQueueEventReason::SourceUnderrun => {
                self.interruption_discarded_source_frame_count = self
                    .interruption_discarded_source_frame_count
                    .saturating_add(frame_count);
                self.interruption_discarded_source_duration_samples = self
                    .interruption_discarded_source_duration_samples
                    .saturating_add(duration_samples);
            }
            PreparedPlayoutQueueEventReason::Unspecified
            | PreparedPlayoutQueueEventReason::SteadyPlayback
            | PreparedPlayoutQueueEventReason::Pause => {}
        }
    }

    pub(crate) fn record_adaptive_buffer_target(&mut self, target_ms: u64, max_target_ms: u64) {
        self.adaptive_buffer_target_ms = target_ms;
        self.max_adaptive_buffer_target_ms = self.max_adaptive_buffer_target_ms.max(max_target_ms);
    }

    pub(crate) fn record_source_underrun(&mut self, depth: OpusBufferDepth) {
        self.source_underrun_count = self.source_underrun_count.saturating_add(1);
        self.post_stall_intervals_remaining = FIRST_INTERVAL_SAMPLE_LIMIT;
        self.post_stall_first_intervals_ms.clear();
        self.record_source_buffer_depth(depth, u64::MAX);
    }

    pub(crate) fn record_rebuffer(&mut self, depth: OpusBufferDepth, low_watermark_ms: u64) {
        self.rebuffer_count = self.rebuffer_count.saturating_add(1);
        self.post_rebuffer_intervals_remaining = FIRST_INTERVAL_SAMPLE_LIMIT;
        self.post_rebuffer_first_intervals_ms.clear();
        self.record_source_buffer_depth(depth, low_watermark_ms);
    }

    pub(crate) fn record_playout_underrun(&mut self, depth: OpusBufferDepth) {
        self.playout_underrun_count = self.playout_underrun_count.saturating_add(1);
        self.record_playout_buffer_depth(depth, u64::MAX);
    }

    pub(crate) fn record_egress_dropped_music_frames(
        &mut self,
        frame_count: u64,
        duration_samples: u64,
    ) {
        self.egress_dropped_music_frame_count = self
            .egress_dropped_music_frame_count
            .saturating_add(frame_count);
        self.egress_dropped_music_duration_samples = self
            .egress_dropped_music_duration_samples
            .saturating_add(duration_samples);
    }

    pub(crate) fn record_speaking_prepare_duration(&mut self, duration: Duration) {
        self.speaking_prepare_durations.push(duration);
    }

    pub(crate) fn record_playout_builder_prepare_duration(&mut self, duration: Duration) {
        self.playout_builder_prepare_durations.push(duration);
    }

    pub(crate) fn record_sender_send_duration(&mut self, duration: Duration) {
        self.sender_send_durations.push(duration);
    }

    pub(crate) fn record_sender_loop_non_send_work_duration(&mut self, duration: Duration) {
        self.sender_loop_non_send_work_durations.push(duration);
    }

    pub(crate) fn record_gateway_drain(&mut self, report: VoiceGatewayDrainReport) {
        self.gateway_event_drain_durations.push(report.duration);
        self.gateway_event_drain_count = self
            .gateway_event_drain_count
            .saturating_add(report.event_count);
        self.dave_transition_count = self
            .dave_transition_count
            .saturating_add(report.dave_transition_count);
        self.dave_transition_count_during_playback = self
            .dave_transition_count_during_playback
            .saturating_add(report.dave_transition_count);
    }

    pub(crate) fn record_media_clock_reset(&mut self, reason: MediaClockResetReason) {
        self.media_clock_reset_count = self.media_clock_reset_count.saturating_add(1);
        match reason {
            MediaClockResetReason::PauseResume => {
                self.pause_resume_reset_count = self.pause_resume_reset_count.saturating_add(1);
            }
            MediaClockResetReason::SchedulerLate => {
                self.scheduler_late_reset_count = self.scheduler_late_reset_count.saturating_add(1);
                self.controlled_media_interruption_count =
                    self.controlled_media_interruption_count.saturating_add(1);
            }
            MediaClockResetReason::SourceUnderrun => {
                self.source_underrun_reset_count =
                    self.source_underrun_reset_count.saturating_add(1);
                self.controlled_media_interruption_count =
                    self.controlled_media_interruption_count.saturating_add(1);
                self.reset_active_track_tempo_window();
            }
        }
    }

    fn reset_active_track_tempo_window(&mut self) {
        self.last_track_send_started_at = None;
        self.last_track_duration = None;
        self.track_packet_timings.clear();
        self.last_any_send_at = None;
    }

    pub(crate) fn record_resumed_from_pause(&mut self) {
        self.reset_active_track_tempo_window();
        self.pause_resume_intervals_remaining = FIRST_INTERVAL_SAMPLE_LIMIT;
        self.pause_resume_first_intervals_ms.clear();
        self.prepared_track_queue_phase = PreparedTrackQueuePhase::PostResume;
    }

    pub(crate) fn record_send_event(&mut self, event: PlaybackSendEventRecord) {
        if event.command_kind == PlaybackSendCommandKind::ScheduledSilence {
            self.scheduled_silence_packet_count =
                self.scheduled_silence_packet_count.saturating_add(1);
        }
        self.raw_send_events.push(event);
    }

    pub(crate) fn record_track_packet(
        &mut self,
        expected_deadline: Instant,
        send_started_at: Instant,
        _sent_at: Instant,
        _duration_ms: u64,
        duration_samples: u32,
        tempo_rebased: bool,
    ) {
        let frame_duration = duration_from_samples(u64::from(duration_samples));
        let media_start = self.track_media_duration_sent;
        self.track_media_duration_sent = self
            .track_media_duration_sent
            .saturating_add(frame_duration);
        if tempo_rebased {
            self.tempo_rebase_count = self.tempo_rebase_count.saturating_add(1);
        }

        let lateness = send_started_at
            .checked_duration_since(expected_deadline)
            .unwrap_or(Duration::ZERO);
        self.sender_lateness.push(lateness);
        self.playout_sender_lateness.push(lateness);
        if lateness > LATE_PACKET_THRESHOLD {
            self.late_frame_count = self.late_frame_count.saturating_add(1);
            self.current_consecutive_late_packets =
                self.current_consecutive_late_packets.saturating_add(1);
            self.max_consecutive_late_packets = self
                .max_consecutive_late_packets
                .max(self.current_consecutive_late_packets);
            self.current_consecutive_playout_late_packets = self
                .current_consecutive_playout_late_packets
                .saturating_add(1);
            self.max_consecutive_playout_late_packets = self
                .max_consecutive_playout_late_packets
                .max(self.current_consecutive_playout_late_packets);
        } else {
            self.current_consecutive_late_packets = 0;
            self.current_consecutive_playout_late_packets = 0;
        }

        if let Some(previous) = self.last_track_send_started_at.replace(send_started_at) {
            let interval = send_started_at.saturating_duration_since(previous);
            self.track_intervals.push(interval);
            if let Some(previous_duration) = self.last_track_duration
                && interval < previous_duration
            {
                self.track_fast_interval_count = self.track_fast_interval_count.saturating_add(1);
                self.track_fast_interval_min = Some(
                    self.track_fast_interval_min
                        .map_or(interval, |current| current.min(interval)),
                );
            }

            if self.pause_resume_intervals_remaining > 0 {
                self.pause_resume_first_intervals_ms
                    .push(duration_millis(interval));
                self.pause_resume_intervals_remaining -= 1;
            }
            if self.post_stall_intervals_remaining > 0 {
                self.post_stall_first_intervals_ms
                    .push(duration_millis(interval));
                self.post_stall_intervals_remaining -= 1;
            }
            if self.post_rebuffer_intervals_remaining > 0 {
                self.post_rebuffer_first_intervals_ms
                    .push(duration_millis(interval));
                self.post_rebuffer_intervals_remaining -= 1;
            }
            if let Some(previous_duration) = self.last_track_duration {
                self.active_track_wall_clock_elapsed = self
                    .active_track_wall_clock_elapsed
                    .saturating_sub(previous_duration)
                    + interval
                    + frame_duration;
            }
        } else {
            self.active_track_wall_clock_elapsed += frame_duration;
        }
        self.last_track_duration = Some(frame_duration);
        self.track_packet_timings.push(TrackPacketTiming {
            send_started_at,
            duration: frame_duration,
            media_start,
        });
        self.record_latest_track_tempo_window();

        self.record_any_packet(send_started_at);
        self.track_packet_count = self.track_packet_count.saturating_add(1);
    }

    fn record_latest_track_tempo_window(&mut self) {
        if self.track_packet_timings.len() < TRACK_TEMPO_WINDOW_PACKETS {
            return;
        }

        let start = self.track_packet_timings.len() - TRACK_TEMPO_WINDOW_PACKETS;
        let window = &self.track_packet_timings[start..];
        let Some(first) = window.first() else {
            return;
        };
        let Some(last) = window.last() else {
            return;
        };

        let media_duration = window
            .iter()
            .fold(Duration::ZERO, |total, packet| total + packet.duration);
        let wall_clock_duration = last
            .send_started_at
            .saturating_duration_since(first.send_started_at)
            + last.duration;

        self.track_tempo_window_count = self.track_tempo_window_count.saturating_add(1);
        if first.media_start >= Duration::from_millis(TRACK_POST_SOURCE_BUFFER_MEDIA_MS) {
            self.track_tempo_window_post_source_buffer_count = self
                .track_tempo_window_post_source_buffer_count
                .saturating_add(1);
        }

        let ratio = media_to_wall_clock_ratio_ppm_duration(media_duration, wall_clock_duration);
        self.track_tempo_window_min_ratio_ppm = if self.track_tempo_window_min_ratio_ppm == 0 {
            ratio
        } else {
            self.track_tempo_window_min_ratio_ppm.min(ratio)
        };
        self.track_tempo_window_max_ratio_ppm = self.track_tempo_window_max_ratio_ppm.max(ratio);

        if ratio > MAX_MEDIA_TO_WALL_CLOCK_RATIO_PPM {
            self.track_tempo_window_fast_count =
                self.track_tempo_window_fast_count.saturating_add(1);
            if ratio > self.track_tempo_window_fastest_ratio_ppm {
                self.track_tempo_window_fastest_ratio_ppm = ratio;
                self.track_tempo_window_fastest_media_ms = duration_millis(media_duration);
                self.track_tempo_window_fastest_wall_clock = Some(wall_clock_duration);
            }
        }

        if ratio < MIN_MEDIA_TO_WALL_CLOCK_RATIO_PPM {
            self.track_tempo_window_slow_count =
                self.track_tempo_window_slow_count.saturating_add(1);
            if self.track_tempo_window_slowest_ratio_ppm == 0
                || ratio < self.track_tempo_window_slowest_ratio_ppm
            {
                self.track_tempo_window_slowest_ratio_ppm = ratio;
                self.track_tempo_window_slowest_media_ms = duration_millis(media_duration);
                self.track_tempo_window_slowest_wall_clock = Some(wall_clock_duration);
            }
        }
    }

    pub(crate) fn record_continuity_silence_packet(&mut self, sent_at: Instant, duration_ms: u64) {
        self.continuity_silence_packet_count =
            self.continuity_silence_packet_count.saturating_add(1);
        self.inserted_silence_duration_ms = self
            .inserted_silence_duration_ms
            .saturating_add(duration_ms);
        self.record_any_packet(sent_at);
    }

    #[allow(dead_code)]
    pub(crate) fn record_skipped_source_frames(&mut self, frame_count: u64, duration_samples: u64) {
        self.skipped_source_frame_count =
            self.skipped_source_frame_count.saturating_add(frame_count);
        self.skipped_source_duration_samples = self
            .skipped_source_duration_samples
            .saturating_add(duration_samples);
    }

    fn record_any_packet(&mut self, sent_at: Instant) {
        if let Some(previous) = self.last_any_send_at.replace(sent_at) {
            self.all_packet_intervals
                .push(sent_at.saturating_duration_since(previous));
        }
    }

    fn track_wall_clock_elapsed_ms(&self) -> u64 {
        duration_millis(self.active_track_wall_clock_elapsed)
    }

    pub(crate) fn snapshot(
        &self,
        gateway_interruptions: u64,
        dave_interruptions: u64,
        reconnect_interruptions: u64,
        ended: bool,
    ) -> PlaybackStabilitySnapshot {
        let track_wall_clock_elapsed_ms = self.track_wall_clock_elapsed_ms();
        let sent_track_frame_count = self.track_packet_count as u64;
        let silence_frame_count = u64::try_from(self.continuity_silence_packet_count)
            .unwrap_or(u64::MAX)
            .max(self.scheduled_silence_packet_count);
        let dropped_frame_count = self
            .prepared_track_packet_drop_count
            .saturating_add(self.prepared_silence_packet_drop_count)
            .saturating_add(self.egress_dropped_music_frame_count);
        let dropped_track_frame_count = self
            .prepared_track_packet_drop_count
            .saturating_add(self.egress_dropped_music_frame_count);
        let expected_track_frame_count = sent_track_frame_count
            .saturating_add(silence_frame_count)
            .saturating_add(dropped_track_frame_count);
        let frame_deficit_count =
            expected_track_frame_count.saturating_sub(sent_track_frame_count + silence_frame_count);
        let track_media_duration_sent_ms = duration_millis(self.track_media_duration_sent);
        let skipped_source_duration_ms =
            duration_ms_from_samples(self.skipped_source_duration_samples);
        let discarded_source_duration_ms =
            duration_ms_from_samples(self.discarded_source_duration_samples);
        let stop_discarded_source_duration_ms =
            duration_ms_from_samples(self.stop_discarded_source_duration_samples);
        let interruption_discarded_source_duration_ms =
            duration_ms_from_samples(self.interruption_discarded_source_duration_samples);
        let restored_source_duration_ms =
            duration_ms_from_samples(self.restored_source_duration_samples);
        let egress_dropped_music_duration_ms =
            duration_ms_from_samples(self.egress_dropped_music_duration_samples);
        let track_media_to_wall_clock_ratio_ppm = media_to_wall_clock_ratio_ppm_duration(
            self.track_media_duration_sent,
            self.active_track_wall_clock_elapsed,
        );
        let active_pre_pause_prepared_track_queue_depth =
            self.active_pre_pause_prepared_track_queue_depth.snapshot();
        let active_post_resume_prepared_track_queue_depth = self
            .active_post_resume_prepared_track_queue_depth
            .snapshot();
        let prepared_track_queue_depth_sample_count = active_pre_pause_prepared_track_queue_depth
            .sample_count
            .saturating_add(active_post_resume_prepared_track_queue_depth.sample_count);
        let prepared_track_queue_empty_count = active_pre_pause_prepared_track_queue_depth
            .empty_count
            .saturating_add(active_post_resume_prepared_track_queue_depth.empty_count);
        PlaybackStabilitySnapshot {
            playback_epoch: self.playback_epoch,
            video_id: Some(self.video_id.clone()),
            selected_itag: Some(self.selected_itag),
            track_packet_count: self.track_packet_count,
            continuity_silence_packet_count: self.continuity_silence_packet_count,
            inserted_silence_duration_ms: self.inserted_silence_duration_ms,
            track_interval: DurationStatsSnapshot::from_samples(&self.track_intervals),
            track_media_duration_sent_ms,
            track_wall_clock_elapsed_ms,
            track_media_to_wall_clock_ratio_ppm,
            track_fast_interval_count: self.track_fast_interval_count,
            track_fast_interval_min_ms: self.track_fast_interval_min.map_or(0, duration_millis),
            track_fast_interval_min_us: self.track_fast_interval_min.map_or(0, duration_micros),
            track_tempo_window_count: self.track_tempo_window_count,
            track_tempo_window_post_source_buffer_count: self
                .track_tempo_window_post_source_buffer_count,
            track_tempo_window_min_ratio_ppm: self.track_tempo_window_min_ratio_ppm,
            track_tempo_window_max_ratio_ppm: self.track_tempo_window_max_ratio_ppm,
            track_tempo_window_fast_count: self.track_tempo_window_fast_count,
            track_tempo_window_fastest_ratio_ppm: self.track_tempo_window_fastest_ratio_ppm,
            track_tempo_window_fastest_media_ms: self.track_tempo_window_fastest_media_ms,
            track_tempo_window_fastest_wall_clock_us: self
                .track_tempo_window_fastest_wall_clock
                .map_or(0, duration_micros),
            track_tempo_window_slow_count: self.track_tempo_window_slow_count,
            track_tempo_window_slowest_ratio_ppm: self.track_tempo_window_slowest_ratio_ppm,
            track_tempo_window_slowest_media_ms: self.track_tempo_window_slowest_media_ms,
            track_tempo_window_slowest_wall_clock_us: self
                .track_tempo_window_slowest_wall_clock
                .map_or(0, duration_micros),
            skipped_source_frame_count: self.skipped_source_frame_count,
            skipped_source_duration_ms,
            skipped_source_duration_samples: self.skipped_source_duration_samples,
            tempo_rebase_count: self.tempo_rebase_count,
            expected_track_frame_count,
            sent_track_frame_count,
            silence_frame_count,
            frame_deficit_count,
            dropped_frame_count,
            late_frame_count: self.late_frame_count,
            all_packet_interval: DurationStatsSnapshot::from_samples(&self.all_packet_intervals),
            sender_lateness: DurationStatsSnapshot::from_samples(&self.sender_lateness),
            max_consecutive_late_packets: self.max_consecutive_late_packets,
            current_consecutive_late_packets: self.current_consecutive_late_packets,
            current_buffer_depth: self.current_buffer_depth,
            min_buffer_depth: self.min_buffer_depth,
            max_buffer_depth: self.max_buffer_depth,
            current_source_buffer_depth: self.current_source_buffer_depth,
            min_source_buffer_depth: self.min_source_buffer_depth,
            max_source_buffer_depth: self.max_source_buffer_depth,
            source_buffer_depth: PlaybackQueueDepthStatsSnapshot::from_samples(
                &self.source_buffer_depth_samples,
                self.source_buffer_empty_count,
            ),
            current_playout_buffer_depth: self.current_playout_buffer_depth,
            min_playout_buffer_depth: self.min_playout_buffer_depth,
            max_playout_buffer_depth: self.max_playout_buffer_depth,
            egress_buffer_target_ms: 400,
            current_egress_buffer_depth: self.current_playout_buffer_depth,
            min_egress_buffer_depth: self.min_playout_buffer_depth,
            max_egress_buffer_depth: self.max_playout_buffer_depth,
            prepared_rtp_queue_depth_ms: self.current_playout_buffer_depth.duration_ms,
            prepared_track_queue_target_ms: self.prepared_track_queue_target_ms,
            prepared_track_queue_low_watermark_ms: self.prepared_track_queue_low_watermark_ms,
            prepared_track_queue_high_watermark_ms: self.prepared_track_queue_high_watermark_ms,
            active_pre_pause_prepared_track_queue_depth,
            active_post_resume_prepared_track_queue_depth,
            prepared_track_queue_depth_sample_count,
            prepared_track_queue_empty_count,
            raw_send_events: playback_send_event_snapshots(&self.raw_send_events),
            raw_prepared_track_queue_samples: self.raw_prepared_track_queue_samples.clone(),
            raw_prepared_playout_queue_events: self.raw_prepared_playout_queue_events.clone(),
            current_scheduled_silence_queue_depth: self.current_scheduled_silence_queue_depth,
            max_scheduled_silence_queue_depth: self.max_scheduled_silence_queue_depth,
            current_boundary_queue_depth: self.current_boundary_queue_depth,
            max_boundary_queue_depth: self.max_boundary_queue_depth,
            prepared_track_packet_drop_count: self.prepared_track_packet_drop_count,
            prepared_silence_packet_drop_count: self.prepared_silence_packet_drop_count,
            prepared_packet_rebuild_count: self.prepared_packet_rebuild_count,
            scheduled_silence_packet_count: self.scheduled_silence_packet_count,
            pause_media_boundary_count: self.pause_media_boundary_count,
            stop_media_boundary_count: self.stop_media_boundary_count,
            recovery_media_boundary_count: self.recovery_media_boundary_count,
            natural_end_media_boundary_count: self.natural_end_media_boundary_count,
            dave_transition_recovery_reached_builder_count: self
                .dave_transition_recovery_reached_builder_count,
            dave_transition_recovery_reached_deadline_sender_count: self
                .dave_transition_recovery_reached_deadline_sender_count,
            source_underrun_reached_builder_count: self.source_underrun_reached_builder_count,
            source_underrun_reached_deadline_sender_count: self
                .source_underrun_reached_deadline_sender_count,
            discarded_source_frame_count: self.discarded_source_frame_count,
            discarded_source_duration_ms,
            discarded_source_duration_samples: self.discarded_source_duration_samples,
            stop_discarded_source_frame_count: self.stop_discarded_source_frame_count,
            stop_discarded_source_duration_ms,
            stop_discarded_source_duration_samples: self.stop_discarded_source_duration_samples,
            interruption_discarded_source_frame_count: self
                .interruption_discarded_source_frame_count,
            interruption_discarded_source_duration_ms,
            interruption_discarded_source_duration_samples: self
                .interruption_discarded_source_duration_samples,
            restored_source_frame_count: self.restored_source_frame_count,
            restored_source_duration_ms,
            restored_source_duration_samples: self.restored_source_duration_samples,
            source_buffer_target_ms: self.source_buffer_target_ms,
            adaptive_buffer_target_ms: self.adaptive_buffer_target_ms,
            max_adaptive_buffer_target_ms: self.max_adaptive_buffer_target_ms,
            buffer_low_watermark_count: self.buffer_low_watermark_count,
            source_buffer_low_watermark_count: self.source_buffer_low_watermark_count,
            playout_buffer_low_watermark_count: self.playout_buffer_low_watermark_count,
            buffer_underrun_count: self.buffer_underrun_count,
            playout_underrun_count: self.playout_underrun_count,
            egress_underrun_count: self.playout_underrun_count,
            egress_inserted_silence_duration_ms: self.inserted_silence_duration_ms,
            egress_dropped_music_frame_count: self.egress_dropped_music_frame_count,
            egress_dropped_music_duration_ms,
            egress_dropped_music_duration_samples: self.egress_dropped_music_duration_samples,
            source_underrun_count: self.source_underrun_count,
            rebuffer_count: self.rebuffer_count,
            refill_duration: DurationStatsSnapshot::from_samples(&self.refill_durations),
            source_producer_fill_duration: DurationStatsSnapshot::from_samples(
                &self.refill_durations,
            ),
            producer_stall_duration: DurationStatsSnapshot::from_samples(
                &self.producer_stall_durations,
            ),
            max_producer_lag_ms: duration_millis(self.max_producer_lag),
            http_retry_count: self.http_retry_count,
            response_open_count: self.response_open_count,
            range_reopen_count: self.range_reopen_count,
            read_error_reopen_count: self.read_error_reopen_count,
            url_reresolve_count: self.url_reresolve_count,
            pause_resume_first_intervals_ms: self.pause_resume_first_intervals_ms.clone(),
            post_stall_first_intervals_ms: self.post_stall_first_intervals_ms.clone(),
            post_rebuffer_first_intervals_ms: self.post_rebuffer_first_intervals_ms.clone(),
            playout_sender_lateness: DurationStatsSnapshot::from_samples(
                &self.playout_sender_lateness,
            ),
            playout_builder_prepare_duration: DurationStatsSnapshot::from_samples(
                &self.playout_builder_prepare_durations,
            ),
            sender_send_duration: DurationStatsSnapshot::from_samples(&self.sender_send_durations),
            sender_loop_non_send_work_duration: DurationStatsSnapshot::from_samples(
                &self.sender_loop_non_send_work_durations,
            ),
            max_consecutive_playout_late_packets: self.max_consecutive_playout_late_packets,
            max_consecutive_late_egress_ticks: self.max_consecutive_playout_late_packets,
            speaking_prepare_duration: DurationStatsSnapshot::from_samples(
                &self.speaking_prepare_durations,
            ),
            sender_forbidden_work_count: self.sender_forbidden_work_count,
            gateway_event_drain_duration: DurationStatsSnapshot::from_samples(
                &self.gateway_event_drain_durations,
            ),
            gateway_event_drain_count: self.gateway_event_drain_count,
            dave_transition_count: self.dave_transition_count,
            dave_transition_count_during_playback: self.dave_transition_count_during_playback,
            stale_dave_send_prevented_count: self.stale_dave_send_prevented_count,
            controlled_media_interruption_count: self.controlled_media_interruption_count,
            media_clock_reset_count: self.media_clock_reset_count,
            egress_clock_reset_count: self.media_clock_reset_count,
            scheduler_late_reset_count: self.scheduler_late_reset_count,
            source_underrun_reset_count: self.source_underrun_reset_count,
            pause_resume_reset_count: self.pause_resume_reset_count,
            dave_transition_recovery_reset_count: self.dave_transition_recovery_reset_count,
            gateway_interruptions,
            dave_interruptions,
            reconnect_interruptions,
            ended,
        }
    }
}

fn playback_send_event_snapshots(
    events: &[PlaybackSendEventRecord],
) -> Vec<PlaybackSendEventSnapshot> {
    let Some(base_deadline) = events.first().map(|event| event.expected_deadline) else {
        return Vec::new();
    };

    events
        .iter()
        .map(|event| PlaybackSendEventSnapshot {
            packet_index: event.packet_index,
            command_kind: event.command_kind,
            expected_deadline_offset_us: instant_offset_micros(
                base_deadline,
                event.expected_deadline,
            ),
            send_started_offset_us: instant_offset_micros(base_deadline, event.send_started_at),
            sent_offset_us: instant_offset_micros(base_deadline, event.sent_at),
            media_duration_ms: event.media_duration_ms,
            media_duration_samples: event.media_duration_samples,
            rtp_sequence: event.rtp_sequence,
            rtp_timestamp: event.rtp_timestamp,
            protection_nonce: event.protection_nonce,
            source_frame_epoch: event.source_frame_epoch,
            source_media_position_ms: event.source_media_position_ms,
            source_media_position_samples: event.source_media_position_samples,
            source_media_byte_position: event.source_media_byte_position,
            committed_heard_media: event.committed_heard_media,
        })
        .collect()
}

fn instant_offset_micros(base: Instant, instant: Instant) -> u64 {
    duration_micros(instant.saturating_duration_since(base))
}

fn min_depth(
    current: PlaybackBufferDepthSnapshot,
    observed: PlaybackBufferDepthSnapshot,
) -> PlaybackBufferDepthSnapshot {
    if observed.duration_samples < current.duration_samples {
        observed
    } else {
        current
    }
}

fn max_depth(
    current: PlaybackBufferDepthSnapshot,
    observed: PlaybackBufferDepthSnapshot,
) -> PlaybackBufferDepthSnapshot {
    if observed.duration_samples > current.duration_samples {
        observed
    } else {
        current
    }
}

fn percentile_duration(sorted: &[Duration], percentile: usize) -> Duration {
    let index = ((sorted.len() - 1) * percentile).div_ceil(100);
    sorted[index]
}

fn percentile_depth(
    sorted: &[PlaybackBufferDepthSnapshot],
    percentile: usize,
) -> PlaybackBufferDepthSnapshot {
    let index = ((sorted.len() - 1) * percentile).div_ceil(100);
    sorted[index]
}

fn duration_millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

fn duration_micros(duration: Duration) -> u64 {
    u64::try_from(duration.as_micros()).unwrap_or(u64::MAX)
}

fn media_to_wall_clock_ratio_ppm_duration(
    media_duration: Duration,
    wall_clock_elapsed: Duration,
) -> u64 {
    let media_duration_us = media_duration.as_micros();
    let wall_clock_elapsed_us = wall_clock_elapsed.as_micros();
    if media_duration_us == 0 || wall_clock_elapsed_us == 0 {
        return 0;
    }

    ((media_duration_us * 1_000_000) / wall_clock_elapsed_us)
        .try_into()
        .unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn collector() -> PlaybackStabilityCollector {
        PlaybackStabilityCollector::new(
            1,
            "video".to_owned(),
            250,
            OpusBufferDepth::default(),
            OpusBufferDepth::default(),
            TRACK_POST_SOURCE_BUFFER_MEDIA_MS,
            400,
            300,
            500,
            PlaybackRecoveryMetrics::default(),
        )
    }

    fn snapshot_for_offsets(offsets: &[Duration]) -> PlaybackStabilitySnapshot {
        let mut collector = collector();
        let start = Instant::now();
        for offset in offsets {
            let sent_at = start + *offset;
            collector.record_track_packet(sent_at, sent_at, sent_at, 20, 960, false);
        }
        collector.snapshot(0, 0, 0, false)
    }

    fn offsets_for_window_wall_clock(wall_clock_duration: Duration) -> Vec<Duration> {
        let packet_duration = Duration::from_millis(20);
        let span_us = wall_clock_duration
            .checked_sub(packet_duration)
            .expect("window wall clock includes final packet duration")
            .as_micros();
        (0..TRACK_TEMPO_WINDOW_PACKETS)
            .map(|index| {
                Duration::from_micros(
                    ((span_us * index as u128) / (TRACK_TEMPO_WINDOW_PACKETS - 1) as u128)
                        .try_into()
                        .unwrap(),
                )
            })
            .collect()
    }

    fn prepared_track_event(
        event_kind: PreparedPlayoutQueueEventKind,
        reason: PreparedPlayoutQueueEventReason,
        source_media_position_samples: u64,
    ) -> PreparedPlayoutQueueEventSnapshot {
        PreparedPlayoutQueueEventSnapshot {
            event_index: 0,
            event_kind,
            reason,
            command_kind: PlaybackSendCommandKind::Track,
            media_duration_ms: 2,
            media_duration_samples: 120,
            rtp_sequence: 42,
            rtp_timestamp: source_media_position_samples as u32,
            protection_nonce: Some(42),
            source_frame_epoch: Some(7),
            source_media_position_ms: Some(duration_ms_from_samples(source_media_position_samples)),
            source_media_position_samples: Some(source_media_position_samples),
            source_media_byte_position: Some(source_media_position_samples / 120 * 32),
            queue_depth_after: PlaybackBufferDepthSnapshot::default(),
        }
    }

    #[test]
    fn track_tempo_window_counts_fifty_packets_over_900ms_as_fast() {
        let snapshot =
            snapshot_for_offsets(&offsets_for_window_wall_clock(Duration::from_millis(900)));

        assert_eq!(snapshot.track_tempo_window_count, 1);
        assert_eq!(snapshot.expected_track_frame_count, 50);
        assert_eq!(snapshot.sent_track_frame_count, 50);
        assert_eq!(snapshot.frame_deficit_count, 0);
        assert_eq!(snapshot.track_tempo_window_fast_count, 1);
        assert_eq!(snapshot.track_tempo_window_slow_count, 0);
        assert!(snapshot.track_tempo_window_fastest_ratio_ppm > MAX_MEDIA_TO_WALL_CLOCK_RATIO_PPM);
        assert_eq!(snapshot.track_tempo_window_fastest_media_ms, 1_000);
        assert_eq!(snapshot.track_tempo_window_fastest_wall_clock_us, 900_000);
    }

    #[test]
    fn track_tempo_window_counts_fifty_packets_over_1100ms_as_slow() {
        let snapshot =
            snapshot_for_offsets(&offsets_for_window_wall_clock(Duration::from_millis(1_100)));

        assert_eq!(snapshot.track_tempo_window_count, 1);
        assert_eq!(snapshot.expected_track_frame_count, 50);
        assert_eq!(snapshot.sent_track_frame_count, 50);
        assert_eq!(snapshot.frame_deficit_count, 0);
        assert_eq!(snapshot.track_tempo_window_fast_count, 0);
        assert_eq!(snapshot.track_tempo_window_slow_count, 1);
        assert!(snapshot.track_tempo_window_slowest_ratio_ppm < MIN_MEDIA_TO_WALL_CLOCK_RATIO_PPM);
        assert_eq!(snapshot.track_tempo_window_slowest_media_ms, 1_000);
        assert_eq!(snapshot.track_tempo_window_slowest_wall_clock_us, 1_100_000);
    }

    #[test]
    fn track_fast_interval_counts_nineteen_ms_correction_interval() {
        let mut offsets = Vec::with_capacity(TRACK_TEMPO_WINDOW_PACKETS);
        let mut offset = Duration::ZERO;
        offsets.push(offset);
        for interval in std::iter::once(Duration::from_millis(19))
            .chain(std::iter::once(Duration::from_millis(21)))
            .chain(std::iter::repeat_n(
                Duration::from_millis(20),
                TRACK_TEMPO_WINDOW_PACKETS - 3,
            ))
        {
            offset += interval;
            offsets.push(offset);
        }

        let snapshot = snapshot_for_offsets(&offsets);

        assert_eq!(snapshot.track_fast_interval_count, 1);
        assert_eq!(snapshot.track_fast_interval_min_ms, 19);
        assert_eq!(snapshot.track_fast_interval_min_us, 19_000);
        assert_eq!(snapshot.track_tempo_window_count, 1);
        assert_eq!(snapshot.track_tempo_window_fast_count, 0);
        assert_eq!(snapshot.track_tempo_window_slow_count, 0);
        assert_eq!(snapshot.track_tempo_window_min_ratio_ppm, 1_000_000);
        assert_eq!(snapshot.track_tempo_window_max_ratio_ppm, 1_000_000);
        assert_eq!(snapshot.expected_track_frame_count, 50);
        assert_eq!(snapshot.sent_track_frame_count, 50);
        assert_eq!(snapshot.frame_deficit_count, 0);
    }

    #[test]
    fn track_intervals_use_send_start_cadence_not_completion_jitter() {
        let mut collector = collector();
        let start = Instant::now();

        for index in 0..TRACK_TEMPO_WINDOW_PACKETS {
            let send_started_at = start + Duration::from_millis(index as u64 * 20);
            let sent_at = send_started_at
                + if index % 2 == 0 {
                    Duration::from_micros(200)
                } else {
                    Duration::from_micros(100)
                };
            collector.record_track_packet(
                send_started_at,
                send_started_at,
                sent_at,
                20,
                960,
                false,
            );
        }

        let snapshot = collector.snapshot(0, 0, 0, false);
        assert_eq!(snapshot.track_fast_interval_count, 0);
        assert_eq!(snapshot.track_fast_interval_min_us, 0);
        assert_eq!(snapshot.track_media_to_wall_clock_ratio_ppm, 1_000_000);
        assert_eq!(snapshot.track_tempo_window_fast_count, 0);
        assert_eq!(snapshot.track_tempo_window_slow_count, 0);
    }

    #[test]
    fn track_tempo_window_uses_sample_duration_for_fractional_packets() {
        let mut collector = collector();
        let start = Instant::now();
        let mut sent_at = start;

        for index in 0..TRACK_TEMPO_WINDOW_PACKETS {
            if index > 0 {
                sent_at += Duration::from_micros(2_500);
            }
            collector.record_track_packet(sent_at, sent_at, sent_at, 2, 120, false);
        }

        let snapshot = collector.snapshot(0, 0, 0, false);

        assert_eq!(snapshot.track_media_duration_sent_ms, 125);
        assert_eq!(snapshot.track_wall_clock_elapsed_ms, 125);
        assert_eq!(snapshot.track_media_to_wall_clock_ratio_ppm, 1_000_000);
        assert_eq!(snapshot.track_tempo_window_count, 1);
        assert_eq!(snapshot.track_tempo_window_fast_count, 0);
        assert_eq!(snapshot.track_tempo_window_slow_count, 0);
        assert_eq!(snapshot.track_tempo_window_min_ratio_ppm, 1_000_000);
        assert_eq!(snapshot.track_tempo_window_max_ratio_ppm, 1_000_000);
        assert_eq!(
            snapshot.expected_track_frame_count,
            TRACK_TEMPO_WINDOW_PACKETS as u64
        );
        assert_eq!(
            snapshot.sent_track_frame_count,
            TRACK_TEMPO_WINDOW_PACKETS as u64
        );
        assert_eq!(snapshot.frame_deficit_count, 0);
    }

    #[test]
    fn prepared_track_drop_count_excludes_rebuilt_source_frames() {
        let mut collector = collector();

        collector.record_prepared_playout_queue_event(prepared_track_event(
            PreparedPlayoutQueueEventKind::DroppedBeforeSend,
            PreparedPlayoutQueueEventReason::Pause,
            120,
        ));
        collector.record_prepared_playout_queue_event(prepared_track_event(
            PreparedPlayoutQueueEventKind::Rebuilt,
            PreparedPlayoutQueueEventReason::Pause,
            120,
        ));

        let snapshot = collector.snapshot(0, 0, 0, false);
        assert_eq!(snapshot.prepared_track_packet_drop_count, 0);
        assert_eq!(snapshot.prepared_packet_rebuild_count, 1);
        assert_eq!(snapshot.dropped_frame_count, 0);
        assert_eq!(snapshot.frame_deficit_count, 0);
    }

    #[test]
    fn prepared_track_drop_count_excludes_controlled_stop_drain() {
        let mut collector = collector();

        collector.record_prepared_playout_queue_event(prepared_track_event(
            PreparedPlayoutQueueEventKind::DroppedBeforeSend,
            PreparedPlayoutQueueEventReason::Stop,
            120,
        ));
        collector.record_restored_source_frames(1, 120);

        let snapshot = collector.snapshot(0, 0, 1, false);
        assert_eq!(snapshot.prepared_track_packet_drop_count, 0);
        assert_eq!(snapshot.restored_source_frame_count, 1);
        assert_eq!(snapshot.dropped_frame_count, 0);
        assert_eq!(snapshot.frame_deficit_count, 0);
        assert_eq!(snapshot.reconnect_interruptions, 1);
    }

    #[test]
    fn prepared_track_drop_count_keeps_unmatched_source_frames() {
        let mut collector = collector();

        collector.record_prepared_playout_queue_event(prepared_track_event(
            PreparedPlayoutQueueEventKind::DroppedBeforeSend,
            PreparedPlayoutQueueEventReason::Pause,
            120,
        ));

        let snapshot = collector.snapshot(0, 0, 0, false);
        assert_eq!(snapshot.prepared_track_packet_drop_count, 1);
        assert_eq!(snapshot.dropped_frame_count, 1);
        assert_eq!(snapshot.frame_deficit_count, 1);
    }

    #[test]
    fn frame_accounting_counts_silence_drops_and_late_frames() {
        let mut collector = collector();
        let start = Instant::now();
        let mut sent_at = start;

        for index in 0..10 {
            if index > 0 {
                sent_at += Duration::from_millis(20);
            }
            let expected_deadline = if index == 5 {
                sent_at - Duration::from_millis(6)
            } else {
                sent_at
            };
            collector.record_track_packet(expected_deadline, sent_at, sent_at, 20, 960, false);
        }

        sent_at += Duration::from_millis(20);
        collector.record_send_event(PlaybackSendEventRecord {
            packet_index: 10,
            command_kind: PlaybackSendCommandKind::ScheduledSilence,
            expected_deadline: sent_at,
            send_started_at: sent_at,
            sent_at,
            media_duration_ms: 20,
            media_duration_samples: 960,
            rtp_sequence: 10,
            rtp_timestamp: 9_600,
            protection_nonce: Some(10),
            source_frame_epoch: None,
            source_media_position_ms: None,
            source_media_position_samples: None,
            source_media_byte_position: None,
            committed_heard_media: false,
        });
        collector.record_continuity_silence_packet(sent_at, 20);
        collector.prepared_track_packet_drop_count = 1;
        collector.prepared_silence_packet_drop_count = 2;
        collector.record_egress_dropped_music_frames(2, 1_920);

        let snapshot = collector.snapshot(0, 0, 0, false);
        assert_eq!(snapshot.expected_track_frame_count, 14);
        assert_eq!(snapshot.sent_track_frame_count, 10);
        assert_eq!(snapshot.silence_frame_count, 1);
        assert_eq!(snapshot.frame_deficit_count, 3);
        assert_eq!(snapshot.dropped_frame_count, 5);
        assert_eq!(snapshot.prepared_silence_packet_drop_count, 2);
        assert_eq!(snapshot.egress_dropped_music_frame_count, 2);
        assert_eq!(snapshot.egress_dropped_music_duration_ms, 40);
        assert_eq!(snapshot.egress_dropped_music_duration_samples, 1_920);
        assert_eq!(snapshot.late_frame_count, 1);
    }

    #[test]
    fn source_frame_accounting_preserves_fractional_duration_samples() {
        let mut collector = collector();

        collector.record_skipped_source_frames(2, 240);
        collector.record_restored_source_frames(3, 720);
        collector.record_discarded_source_frames(
            PreparedPlayoutQueueEventReason::Interruption,
            2,
            360,
        );
        collector.record_discarded_source_frames(PreparedPlayoutQueueEventReason::Stop, 1, 120);
        collector.record_egress_dropped_music_frames(2, 240);

        let snapshot = collector.snapshot(0, 0, 0, false);
        assert_eq!(snapshot.skipped_source_frame_count, 2);
        assert_eq!(snapshot.skipped_source_duration_samples, 240);
        assert_eq!(snapshot.skipped_source_duration_ms, 5);
        assert_eq!(snapshot.restored_source_frame_count, 3);
        assert_eq!(snapshot.restored_source_duration_samples, 720);
        assert_eq!(snapshot.restored_source_duration_ms, 15);
        assert_eq!(snapshot.discarded_source_frame_count, 3);
        assert_eq!(snapshot.discarded_source_duration_samples, 480);
        assert_eq!(snapshot.discarded_source_duration_ms, 10);
        assert_eq!(snapshot.stop_discarded_source_frame_count, 1);
        assert_eq!(snapshot.stop_discarded_source_duration_samples, 120);
        assert_eq!(snapshot.stop_discarded_source_duration_ms, 2);
        assert_eq!(snapshot.interruption_discarded_source_frame_count, 2);
        assert_eq!(snapshot.interruption_discarded_source_duration_samples, 360);
        assert_eq!(snapshot.interruption_discarded_source_duration_ms, 7);
        assert_eq!(snapshot.egress_dropped_music_frame_count, 2);
        assert_eq!(snapshot.egress_dropped_music_duration_samples, 240);
        assert_eq!(snapshot.egress_dropped_music_duration_ms, 5);
    }

    #[test]
    fn source_underrun_rebuffer_and_reset_metrics_are_published() {
        let mut collector = collector();
        let empty_depth = OpusBufferDepth::default();
        let resumed_depth = OpusBufferDepth {
            packets: 5,
            bytes: 2_560,
            duration_ms: 100,
            duration_samples: 4_800,
        };

        collector.record_source_underrun(empty_depth);
        collector.record_media_clock_reset(MediaClockResetReason::SourceUnderrun);
        collector.record_rebuffer(resumed_depth, 300);

        let snapshot = collector.snapshot(0, 0, 0, false);
        assert_eq!(snapshot.source_underrun_count, 1);
        assert_eq!(snapshot.source_underrun_reset_count, 1);
        assert_eq!(snapshot.rebuffer_count, 1);
        assert_eq!(snapshot.current_source_buffer_depth.duration_ms, 100);
        assert_eq!(snapshot.source_buffer_low_watermark_count, 1);
        assert_eq!(snapshot.controlled_media_interruption_count, 1);
    }

    #[test]
    fn track_tempo_window_excludes_controlled_pause_from_active_tempo() {
        let mut collector = collector();
        let start = Instant::now();
        let mut sent_at = start;

        for index in 0..60 {
            if index > 0 {
                sent_at += Duration::from_millis(20);
            }
            collector.record_track_packet(sent_at, sent_at, sent_at, 20, 960, false);
        }

        collector.record_resumed_from_pause();
        sent_at += Duration::from_secs(3);

        for index in 0..60 {
            if index > 0 {
                sent_at += Duration::from_millis(20);
            }
            collector.record_track_packet(sent_at, sent_at, sent_at, 20, 960, false);
        }

        let snapshot = collector.snapshot(0, 0, 0, false);
        assert_eq!(snapshot.track_media_duration_sent_ms, 2_400);
        assert_eq!(snapshot.track_wall_clock_elapsed_ms, 2_400);
        assert_eq!(snapshot.track_media_to_wall_clock_ratio_ppm, 1_000_000);
        assert_eq!(snapshot.track_tempo_window_fast_count, 0);
        assert_eq!(snapshot.track_tempo_window_slow_count, 0);
        assert_eq!(snapshot.track_tempo_window_min_ratio_ppm, 1_000_000);
        assert_eq!(snapshot.track_tempo_window_max_ratio_ppm, 1_000_000);
    }

    #[test]
    fn track_tempo_window_excludes_source_underrun_silence_from_active_tempo() {
        let mut collector = collector();
        let start = Instant::now();
        let mut sent_at = start;

        for index in 0..60 {
            if index > 0 {
                sent_at += Duration::from_millis(20);
            }
            collector.record_track_packet(sent_at, sent_at, sent_at, 20, 960, false);
        }

        collector.record_media_clock_reset(MediaClockResetReason::SourceUnderrun);
        for _ in 0..8 {
            sent_at += Duration::from_millis(20);
            collector.record_continuity_silence_packet(sent_at, 20);
        }

        for _ in 0..60 {
            sent_at += Duration::from_millis(20);
            collector.record_track_packet(sent_at, sent_at, sent_at, 20, 960, false);
        }

        let snapshot = collector.snapshot(0, 0, 0, false);
        assert_eq!(snapshot.track_media_duration_sent_ms, 2_400);
        assert_eq!(snapshot.track_wall_clock_elapsed_ms, 2_400);
        assert_eq!(snapshot.track_media_to_wall_clock_ratio_ppm, 1_000_000);
        assert_eq!(snapshot.track_tempo_window_fast_count, 0);
        assert_eq!(snapshot.track_tempo_window_slow_count, 0);
        assert_eq!(snapshot.track_tempo_window_min_ratio_ppm, 1_000_000);
        assert_eq!(snapshot.track_tempo_window_max_ratio_ppm, 1_000_000);
        assert_eq!(snapshot.continuity_silence_packet_count, 8);
        assert_eq!(snapshot.source_underrun_reset_count, 1);
        assert_eq!(snapshot.controlled_media_interruption_count, 1);
    }

    #[test]
    fn track_tempo_window_counts_sustained_18ms_intervals_as_fast() {
        let offsets = (0..TRACK_TEMPO_WINDOW_PACKETS)
            .map(|index| Duration::from_millis((index as u64) * 18))
            .collect::<Vec<_>>();

        let snapshot = snapshot_for_offsets(&offsets);

        assert_eq!(snapshot.track_tempo_window_fast_count, 1);
        assert_eq!(snapshot.track_tempo_window_slow_count, 0);
        assert!(snapshot.track_tempo_window_max_ratio_ppm > MAX_MEDIA_TO_WALL_CLOCK_RATIO_PPM);
    }

    #[test]
    fn track_tempo_window_counts_sustained_22ms_intervals_as_slow() {
        let offsets = (0..TRACK_TEMPO_WINDOW_PACKETS)
            .map(|index| Duration::from_millis((index as u64) * 22))
            .collect::<Vec<_>>();

        let snapshot = snapshot_for_offsets(&offsets);

        assert_eq!(snapshot.track_tempo_window_fast_count, 0);
        assert_eq!(snapshot.track_tempo_window_slow_count, 1);
        assert!(snapshot.track_tempo_window_min_ratio_ppm < MIN_MEDIA_TO_WALL_CLOCK_RATIO_PPM);
    }
}
