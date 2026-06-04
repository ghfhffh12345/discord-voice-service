use std::time::Duration;

use discord_voice_service_playback::media::http_stream::HttpOpusStreamMetrics;
use discord_voice_service_playback::media::opus_queue::OpusBufferDepth;
use discord_voice_service_playback::recovery::PlaybackRecoveryMetrics;
use discord_voice_service_voice::VoiceGatewayDrainReport;
use tokio::time::Instant;

const FIRST_INTERVAL_SAMPLE_LIMIT: usize = 10;
const LATE_PACKET_THRESHOLD: Duration = Duration::from_millis(5);

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

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PlaybackStabilitySnapshot {
    pub playback_epoch: u64,
    pub video_id: Option<String>,
    pub selected_itag: Option<u32>,
    pub track_packet_count: usize,
    pub continuity_silence_packet_count: usize,
    pub inserted_silence_duration_ms: u64,
    pub track_interval: DurationStatsSnapshot,
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
    pub current_playout_buffer_depth: PlaybackBufferDepthSnapshot,
    pub min_playout_buffer_depth: PlaybackBufferDepthSnapshot,
    pub max_playout_buffer_depth: PlaybackBufferDepthSnapshot,
    pub prepared_rtp_queue_depth_ms: u64,
    pub source_buffer_target_ms: u64,
    pub adaptive_buffer_target_ms: u64,
    pub max_adaptive_buffer_target_ms: u64,
    pub buffer_low_watermark_count: u64,
    pub source_buffer_low_watermark_count: u64,
    pub playout_buffer_low_watermark_count: u64,
    pub buffer_underrun_count: u64,
    pub playout_underrun_count: u64,
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
    pub speaking_prepare_duration: DurationStatsSnapshot,
    pub sender_forbidden_work_count: u64,
    pub gateway_event_drain_duration: DurationStatsSnapshot,
    pub gateway_event_drain_count: u64,
    pub dave_transition_count: u64,
    pub dave_transition_count_during_playback: u64,
    pub stale_dave_send_prevented_count: u64,
    pub controlled_media_interruption_count: u64,
    pub media_clock_reset_count: u64,
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
    SourceUnderrun,
    PauseResume,
    SchedulerLate,
    DaveTransitionRecovery,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct ProducerMetricsSample {
    pub fill_duration: Duration,
    pub produced_frames: usize,
    pub source_buffer_depth: OpusBufferDepth,
    pub stream_metrics: HttpOpusStreamMetrics,
    pub recovery_metrics: PlaybackRecoveryMetrics,
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
    last_track_send_at: Option<Instant>,
    last_any_send_at: Option<Instant>,
    last_producer_sample_at: Option<Instant>,
    max_producer_lag: Duration,
    current_buffer_depth: PlaybackBufferDepthSnapshot,
    min_buffer_depth: PlaybackBufferDepthSnapshot,
    max_buffer_depth: PlaybackBufferDepthSnapshot,
    current_source_buffer_depth: PlaybackBufferDepthSnapshot,
    min_source_buffer_depth: PlaybackBufferDepthSnapshot,
    max_source_buffer_depth: PlaybackBufferDepthSnapshot,
    current_playout_buffer_depth: PlaybackBufferDepthSnapshot,
    min_playout_buffer_depth: PlaybackBufferDepthSnapshot,
    max_playout_buffer_depth: PlaybackBufferDepthSnapshot,
    source_buffer_target_ms: u64,
    adaptive_buffer_target_ms: u64,
    max_adaptive_buffer_target_ms: u64,
    buffer_low_watermark_count: u64,
    source_buffer_low_watermark_count: u64,
    playout_buffer_low_watermark_count: u64,
    buffer_underrun_count: u64,
    playout_underrun_count: u64,
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
    media_clock_reset_count: u64,
    scheduler_late_reset_count: u64,
    source_underrun_reset_count: u64,
    pause_resume_reset_count: u64,
    dave_transition_recovery_reset_count: u64,
}

impl PlaybackStabilityCollector {
    pub(crate) fn new(
        playback_epoch: u64,
        video_id: String,
        selected_itag: u32,
        initial_depth: OpusBufferDepth,
        initial_source_depth: OpusBufferDepth,
        source_buffer_target_ms: u64,
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
            last_track_send_at: None,
            last_any_send_at: None,
            last_producer_sample_at: None,
            max_producer_lag: Duration::ZERO,
            current_buffer_depth: initial_depth,
            min_buffer_depth: initial_depth,
            max_buffer_depth: initial_depth,
            current_source_buffer_depth: initial_source_depth,
            min_source_buffer_depth: initial_source_depth,
            max_source_buffer_depth: initial_source_depth,
            current_playout_buffer_depth: PlaybackBufferDepthSnapshot::default(),
            min_playout_buffer_depth: PlaybackBufferDepthSnapshot::default(),
            max_playout_buffer_depth: PlaybackBufferDepthSnapshot::default(),
            source_buffer_target_ms,
            adaptive_buffer_target_ms: 0,
            max_adaptive_buffer_target_ms: 0,
            buffer_low_watermark_count: 0,
            source_buffer_low_watermark_count: 0,
            playout_buffer_low_watermark_count: 0,
            buffer_underrun_count: 0,
            playout_underrun_count: 0,
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
        if low_watermark_ms != u64::MAX && depth.duration_ms <= low_watermark_ms {
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
        if low_watermark_ms != u64::MAX && depth.duration_ms <= low_watermark_ms {
            self.playout_buffer_low_watermark_count =
                self.playout_buffer_low_watermark_count.saturating_add(1);
        }
    }

    pub(crate) fn record_adaptive_buffer_target(&mut self, target_ms: u64, max_target_ms: u64) {
        self.adaptive_buffer_target_ms = target_ms;
        self.max_adaptive_buffer_target_ms = self.max_adaptive_buffer_target_ms.max(max_target_ms);
    }

    pub(crate) fn record_source_underrun(&mut self, depth: OpusBufferDepth) {
        self.source_underrun_count = self.source_underrun_count.saturating_add(1);
        self.record_source_buffer_depth(depth, u64::MAX);
    }

    pub(crate) fn record_speaking_prepare_duration(&mut self, duration: Duration) {
        self.speaking_prepare_durations.push(duration);
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

    pub(crate) fn record_stale_dave_send_prevented(&mut self) {
        self.stale_dave_send_prevented_count =
            self.stale_dave_send_prevented_count.saturating_add(1);
        self.controlled_media_interruption_count =
            self.controlled_media_interruption_count.saturating_add(1);
    }

    pub(crate) fn record_media_clock_reset(&mut self, reason: MediaClockResetReason) {
        self.media_clock_reset_count = self.media_clock_reset_count.saturating_add(1);
        match reason {
            MediaClockResetReason::SourceUnderrun => {
                self.source_underrun_reset_count =
                    self.source_underrun_reset_count.saturating_add(1);
                self.controlled_media_interruption_count =
                    self.controlled_media_interruption_count.saturating_add(1);
            }
            MediaClockResetReason::PauseResume => {
                self.pause_resume_reset_count = self.pause_resume_reset_count.saturating_add(1);
            }
            MediaClockResetReason::SchedulerLate => {
                self.scheduler_late_reset_count = self.scheduler_late_reset_count.saturating_add(1);
                self.controlled_media_interruption_count =
                    self.controlled_media_interruption_count.saturating_add(1);
            }
            MediaClockResetReason::DaveTransitionRecovery => {
                self.dave_transition_recovery_reset_count =
                    self.dave_transition_recovery_reset_count.saturating_add(1);
                self.controlled_media_interruption_count =
                    self.controlled_media_interruption_count.saturating_add(1);
            }
        }
    }

    pub(crate) fn record_resumed_from_pause(&mut self) {
        self.last_track_send_at = None;
        self.last_any_send_at = None;
        self.pause_resume_intervals_remaining = FIRST_INTERVAL_SAMPLE_LIMIT;
        self.pause_resume_first_intervals_ms.clear();
    }

    pub(crate) fn record_track_packet(
        &mut self,
        expected_deadline: Instant,
        send_started_at: Instant,
        sent_at: Instant,
    ) {
        let lateness = send_started_at
            .checked_duration_since(expected_deadline)
            .unwrap_or(Duration::ZERO);
        self.sender_lateness.push(lateness);
        self.playout_sender_lateness.push(lateness);
        if lateness > LATE_PACKET_THRESHOLD {
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

        if let Some(previous) = self.last_track_send_at.replace(sent_at) {
            let interval = sent_at.saturating_duration_since(previous);
            self.track_intervals.push(interval);

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
        }

        self.record_any_packet(sent_at);
        self.track_packet_count = self.track_packet_count.saturating_add(1);
    }

    pub(crate) fn record_continuity_silence_packet(&mut self, sent_at: Instant, duration_ms: u64) {
        self.continuity_silence_packet_count =
            self.continuity_silence_packet_count.saturating_add(1);
        self.inserted_silence_duration_ms = self
            .inserted_silence_duration_ms
            .saturating_add(duration_ms);
        self.record_any_packet(sent_at);
    }

    fn record_any_packet(&mut self, sent_at: Instant) {
        if let Some(previous) = self.last_any_send_at.replace(sent_at) {
            self.all_packet_intervals
                .push(sent_at.saturating_duration_since(previous));
        }
    }

    pub(crate) fn snapshot(
        &self,
        gateway_interruptions: u64,
        dave_interruptions: u64,
        reconnect_interruptions: u64,
        ended: bool,
    ) -> PlaybackStabilitySnapshot {
        PlaybackStabilitySnapshot {
            playback_epoch: self.playback_epoch,
            video_id: Some(self.video_id.clone()),
            selected_itag: Some(self.selected_itag),
            track_packet_count: self.track_packet_count,
            continuity_silence_packet_count: self.continuity_silence_packet_count,
            inserted_silence_duration_ms: self.inserted_silence_duration_ms,
            track_interval: DurationStatsSnapshot::from_samples(&self.track_intervals),
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
            current_playout_buffer_depth: self.current_playout_buffer_depth,
            min_playout_buffer_depth: self.min_playout_buffer_depth,
            max_playout_buffer_depth: self.max_playout_buffer_depth,
            prepared_rtp_queue_depth_ms: self.current_playout_buffer_depth.duration_ms,
            source_buffer_target_ms: self.source_buffer_target_ms,
            adaptive_buffer_target_ms: self.adaptive_buffer_target_ms,
            max_adaptive_buffer_target_ms: self.max_adaptive_buffer_target_ms,
            buffer_low_watermark_count: self.buffer_low_watermark_count,
            source_buffer_low_watermark_count: self.source_buffer_low_watermark_count,
            playout_buffer_low_watermark_count: self.playout_buffer_low_watermark_count,
            buffer_underrun_count: self.buffer_underrun_count,
            playout_underrun_count: self.playout_underrun_count,
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

fn min_depth(
    current: PlaybackBufferDepthSnapshot,
    observed: PlaybackBufferDepthSnapshot,
) -> PlaybackBufferDepthSnapshot {
    if observed.duration_ms < current.duration_ms {
        observed
    } else {
        current
    }
}

fn max_depth(
    current: PlaybackBufferDepthSnapshot,
    observed: PlaybackBufferDepthSnapshot,
) -> PlaybackBufferDepthSnapshot {
    if observed.duration_ms > current.duration_ms {
        observed
    } else {
        current
    }
}

fn percentile_duration(sorted: &[Duration], percentile: usize) -> Duration {
    let index = ((sorted.len() - 1) * percentile).div_ceil(100);
    sorted[index]
}

fn duration_millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}
