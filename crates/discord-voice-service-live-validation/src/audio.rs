use anyhow::{Context, Result, bail};
use discord_voice_service_playback::media::opus_queue::opus_packet_duration;
use opus_rs::OpusDecoder;
use serde::Serialize;
use std::{
    panic::{AssertUnwindSafe, catch_unwind},
    time::{Duration, Instant},
};

const SAMPLE_RATE_HZ: i32 = 48_000;
const NON_SILENCE_PEAK_THRESHOLD: f32 = 0.001;
const OBSERVER_GAP_THRESHOLD: Duration = Duration::from_millis(100);
// Receiver-side timestamps include Discord/network delivery jitter. Use a
// wider window than the local sender metrics while still requiring consecutive
// packets and catching sustained 18ms/22ms tempo drift.
const OBSERVER_TEMPO_WINDOW_PACKETS: usize = 250;
const OBSERVER_POST_SOURCE_BUFFER_AUDIO_MS: u64 = 5_000;
const MIN_MEDIA_TO_WALL_CLOCK_RATIO_PPM: u64 = 980_000;
const MAX_MEDIA_TO_WALL_CLOCK_RATIO_PPM: u64 = 1_020_000;
// Local staging receive timestamps can move by a few scheduler ticks; keep this
// per-packet observer allowance below 1ms and never feed it back into service
// scheduling.
const LOCAL_OBSERVER_JITTER_ALLOWANCE: Duration = Duration::from_micros(750);

#[derive(Debug, Clone, Copy)]
pub struct ObservedOpusPacket<'a> {
    pub sequence: u16,
    pub timestamp: u32,
    pub payload: &'a [u8],
}

#[derive(Debug, Clone, Default, PartialEq, Serialize)]
pub struct AudioValidationStats {
    pub observed_packet_count: u64,
    pub decoded_audio_ms: u64,
    pub wall_clock_elapsed_ms: u64,
    pub decoded_audio_to_wall_clock_ratio_ppm: u64,
    pub non_silent_audio_ms: u64,
    pub rtp_inter_arrival: AudioIntervalStats,
    pub rtp_gap_count_gte_100ms: u64,
    pub rtp_fast_interval_count: u64,
    pub rtp_fast_interval_min_ms: u64,
    pub rtp_fast_interval_min_us: u64,
    pub decoded_audio_tempo_window_count: u64,
    pub decoded_audio_tempo_window_post_source_buffer_count: u64,
    pub decoded_audio_tempo_window_min_ratio_ppm: u64,
    pub decoded_audio_tempo_window_max_ratio_ppm: u64,
    pub decoded_audio_tempo_window_fast_count: u64,
    pub decoded_audio_tempo_window_fastest_ratio_ppm: u64,
    pub decoded_audio_tempo_window_fastest_media_ms: u64,
    pub decoded_audio_tempo_window_fastest_wall_clock_us: u64,
    pub decoded_audio_tempo_window_slow_count: u64,
    pub decoded_audio_tempo_window_slowest_ratio_ppm: u64,
    pub decoded_audio_tempo_window_slowest_media_ms: u64,
    pub decoded_audio_tempo_window_slowest_wall_clock_us: u64,
    pub max_peak_amplitude: f32,
    pub rms_amplitude: f32,
    pub first_sequence: Option<u16>,
    pub last_sequence: Option<u16>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct AudioIntervalStats {
    pub samples: u64,
    pub p50_ms: u64,
    pub p95_ms: u64,
    pub p99_ms: u64,
    pub min_ms: u64,
    pub max_ms: u64,
}

pub struct AudioValidationAccumulator {
    mono_decoder: Option<OpusDecoder>,
    stereo_decoder: Option<OpusDecoder>,
    stats: AudioValidationStats,
    total_squared_amplitude: f64,
    total_samples: usize,
    active_wall_clock_elapsed: Duration,
    last_observed_at: Option<Instant>,
    last_packet_duration: Option<Duration>,
    last_packet_duration_samples: Option<u32>,
    last_packet_timestamp: Option<u32>,
    inter_arrivals: Vec<Duration>,
    observed_packet_timings: Vec<ObservedPacketTiming>,
}

#[derive(Debug, Clone, Copy)]
struct ObservedPacketTiming {
    observed_at: Instant,
    duration: Duration,
    decoded_audio_start_ms: u64,
}

impl AudioValidationAccumulator {
    pub fn new() -> Self {
        Self {
            mono_decoder: None,
            stereo_decoder: None,
            stats: AudioValidationStats::default(),
            total_squared_amplitude: 0.0,
            total_samples: 0,
            active_wall_clock_elapsed: Duration::ZERO,
            last_observed_at: None,
            last_packet_duration: None,
            last_packet_duration_samples: None,
            last_packet_timestamp: None,
            inter_arrivals: Vec::new(),
            observed_packet_timings: Vec::new(),
        }
    }

    pub fn observe_packet(
        &mut self,
        packet: ObservedOpusPacket<'_>,
    ) -> Result<AudioValidationStats> {
        self.observe_packet_at(packet, Instant::now())
    }

    pub fn observe_packet_at(
        &mut self,
        packet: ObservedOpusPacket<'_>,
        observed_at: Instant,
    ) -> Result<AudioValidationStats> {
        if packet.payload.is_empty() {
            bail!("observed opus payload must not be empty");
        }

        let declared_duration = opus_packet_duration(packet.payload).with_context(|| {
            format!(
                "unsupported opus packet header at sequence {}",
                packet.sequence
            )
        })?;
        let frame_samples = usize::try_from(declared_duration.samples)
            .context("declared opus packet duration samples exceed platform usize")?;
        let channels = opus_packet_channels(packet.payload).with_context(|| {
            format!(
                "unsupported opus packet channel layout at sequence {}",
                packet.sequence
            )
        })?;
        let duration_ms = declared_duration.ms;
        let packet_duration = Duration::from_nanos(
            u64::from(declared_duration.samples).saturating_mul(1_000_000_000) / 48_000,
        );
        let decoder = match channels {
            1 => self.mono_decoder.get_or_insert(
                OpusDecoder::new(SAMPLE_RATE_HZ, 1)
                    .map_err(anyhow::Error::msg)
                    .context("create mono opus decoder")?,
            ),
            2 => self.stereo_decoder.get_or_insert(
                OpusDecoder::new(SAMPLE_RATE_HZ, 2)
                    .map_err(anyhow::Error::msg)
                    .context("create stereo opus decoder")?,
            ),
            _ => bail!("unsupported opus channel count: {channels}"),
        };
        let mut pcm = vec![0.0f32; frame_samples * channels];
        let decode_result = catch_unwind(AssertUnwindSafe(|| {
            decoder.decode(packet.payload, frame_samples, &mut pcm)
        }))
        .map_err(|_| {
            anyhow::anyhow!(
                "decode opus packet panicked at sequence {}",
                packet.sequence
            )
        })?;
        let decoded_samples = decode_result
            .map_err(anyhow::Error::msg)
            .with_context(|| format!("decode opus packet at sequence {}", packet.sequence))?;
        if decoded_samples != frame_samples {
            bail!(
                "decoded opus packet at sequence {} returned {} samples; declared duration was {} samples",
                packet.sequence,
                decoded_samples,
                frame_samples
            );
        }
        self.validate_rtp_timestamp_delta(packet.timestamp)?;

        let packet_peak = pcm
            .iter()
            .fold(0.0f32, |peak, sample| peak.max(sample.abs()));
        let packet_square_sum: f64 = pcm
            .iter()
            .map(|sample| {
                let value = f64::from(*sample);
                value * value
            })
            .sum();

        let decoded_audio_start_ms = self.stats.decoded_audio_ms;
        self.stats.observed_packet_count += 1;
        self.stats.decoded_audio_ms += duration_ms;
        if packet_peak > NON_SILENCE_PEAK_THRESHOLD {
            self.stats.non_silent_audio_ms += duration_ms;
        }
        self.stats.max_peak_amplitude = self.stats.max_peak_amplitude.max(packet_peak);
        self.stats.first_sequence.get_or_insert(packet.sequence);
        self.stats.last_sequence = Some(packet.sequence);
        self.record_inter_arrival(observed_at, packet_duration);
        self.last_packet_duration = Some(packet_duration);
        self.last_packet_duration_samples = Some(declared_duration.samples);
        self.last_packet_timestamp = Some(packet.timestamp);
        self.observed_packet_timings.push(ObservedPacketTiming {
            observed_at,
            duration: packet_duration,
            decoded_audio_start_ms,
        });
        self.record_latest_decoded_audio_tempo_window();

        self.total_squared_amplitude += packet_square_sum;
        self.total_samples += pcm.len();

        Ok(self.stats())
    }

    fn record_inter_arrival(&mut self, observed_at: Instant, current_packet_duration: Duration) {
        if let Some(previous) = self.last_observed_at.replace(observed_at) {
            let interval = observed_at.saturating_duration_since(previous);
            if interval >= OBSERVER_GAP_THRESHOLD {
                self.stats.rtp_gap_count_gte_100ms += 1;
            }
            if let Some(previous_duration) = self.last_packet_duration
                && interval + LOCAL_OBSERVER_JITTER_ALLOWANCE < previous_duration
            {
                self.stats.rtp_fast_interval_count =
                    self.stats.rtp_fast_interval_count.saturating_add(1);
                let interval_ms = duration_ms(interval);
                let interval_us = duration_us(interval);
                self.stats.rtp_fast_interval_min_ms = if self.stats.rtp_fast_interval_min_ms == 0 {
                    interval_ms
                } else {
                    self.stats.rtp_fast_interval_min_ms.min(interval_ms)
                };
                self.stats.rtp_fast_interval_min_us = if self.stats.rtp_fast_interval_min_us == 0 {
                    interval_us
                } else {
                    self.stats.rtp_fast_interval_min_us.min(interval_us)
                };
            }
            if let Some(previous_duration) = self.last_packet_duration {
                self.active_wall_clock_elapsed = self
                    .active_wall_clock_elapsed
                    .saturating_sub(previous_duration)
                    + interval
                    + current_packet_duration;
            }
            self.inter_arrivals.push(interval);
            self.stats.rtp_inter_arrival = interval_stats(&self.inter_arrivals);
        } else {
            self.active_wall_clock_elapsed += current_packet_duration;
        }
    }

    fn validate_rtp_timestamp_delta(&self, timestamp: u32) -> Result<()> {
        if let (Some(previous_timestamp), Some(previous_samples)) = (
            self.last_packet_timestamp,
            self.last_packet_duration_samples,
        ) {
            let delta = timestamp.wrapping_sub(previous_timestamp);
            if delta != previous_samples {
                bail!(
                    "rtp timestamp delta was {delta} samples; expected previous opus duration {previous_samples} samples"
                );
            }
        }

        Ok(())
    }

    fn record_latest_decoded_audio_tempo_window(&mut self) {
        if self.observed_packet_timings.len() < OBSERVER_TEMPO_WINDOW_PACKETS {
            return;
        }

        let start = self.observed_packet_timings.len() - OBSERVER_TEMPO_WINDOW_PACKETS;
        let window = &self.observed_packet_timings[start..];
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
            .observed_at
            .saturating_duration_since(first.observed_at)
            + last.duration;

        self.stats.decoded_audio_tempo_window_count = self
            .stats
            .decoded_audio_tempo_window_count
            .saturating_add(1);
        if first.decoded_audio_start_ms >= OBSERVER_POST_SOURCE_BUFFER_AUDIO_MS {
            self.stats
                .decoded_audio_tempo_window_post_source_buffer_count = self
                .stats
                .decoded_audio_tempo_window_post_source_buffer_count
                .saturating_add(1);
        }

        let ratio = media_to_wall_clock_ratio_ppm_duration(media_duration, wall_clock_duration);
        self.stats.decoded_audio_tempo_window_min_ratio_ppm =
            if self.stats.decoded_audio_tempo_window_min_ratio_ppm == 0 {
                ratio
            } else {
                self.stats
                    .decoded_audio_tempo_window_min_ratio_ppm
                    .min(ratio)
            };
        self.stats.decoded_audio_tempo_window_max_ratio_ppm = self
            .stats
            .decoded_audio_tempo_window_max_ratio_ppm
            .max(ratio);

        if ratio > MAX_MEDIA_TO_WALL_CLOCK_RATIO_PPM {
            self.stats.decoded_audio_tempo_window_fast_count = self
                .stats
                .decoded_audio_tempo_window_fast_count
                .saturating_add(1);
            if ratio > self.stats.decoded_audio_tempo_window_fastest_ratio_ppm {
                self.stats.decoded_audio_tempo_window_fastest_ratio_ppm = ratio;
                self.stats.decoded_audio_tempo_window_fastest_media_ms =
                    duration_ms(media_duration);
                self.stats.decoded_audio_tempo_window_fastest_wall_clock_us =
                    duration_us(wall_clock_duration);
            }
        }

        if ratio < MIN_MEDIA_TO_WALL_CLOCK_RATIO_PPM {
            self.stats.decoded_audio_tempo_window_slow_count = self
                .stats
                .decoded_audio_tempo_window_slow_count
                .saturating_add(1);
            if self.stats.decoded_audio_tempo_window_slowest_ratio_ppm == 0
                || ratio < self.stats.decoded_audio_tempo_window_slowest_ratio_ppm
            {
                self.stats.decoded_audio_tempo_window_slowest_ratio_ppm = ratio;
                self.stats.decoded_audio_tempo_window_slowest_media_ms =
                    duration_ms(media_duration);
                self.stats.decoded_audio_tempo_window_slowest_wall_clock_us =
                    duration_us(wall_clock_duration);
            }
        }
    }

    pub fn stats(&self) -> AudioValidationStats {
        let mut stats = self.stats.clone();
        if self.stats.observed_packet_count > 0 {
            stats.wall_clock_elapsed_ms = duration_ms(self.active_wall_clock_elapsed);
            stats.decoded_audio_to_wall_clock_ratio_ppm =
                media_to_wall_clock_ratio_ppm(stats.decoded_audio_ms, stats.wall_clock_elapsed_ms);
        }
        stats.rms_amplitude = if self.total_samples == 0 {
            0.0
        } else {
            ((self.total_squared_amplitude / self.total_samples as f64).sqrt()) as f32
        };
        stats
    }

    pub fn reset_inter_arrival_baseline(&mut self) {
        self.last_observed_at = None;
        self.last_packet_duration = None;
        self.last_packet_duration_samples = None;
        self.last_packet_timestamp = None;
        self.observed_packet_timings.clear();
    }

    pub fn into_stats(self) -> Result<AudioValidationStats> {
        let stats = self.stats();
        if stats.observed_packet_count == 0 {
            bail!("expected at least one observed opus packet");
        }

        Ok(stats)
    }
}

impl Default for AudioValidationAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

pub fn analyze_opus_packets<'a, I>(packets: I) -> Result<AudioValidationStats>
where
    I: IntoIterator<Item = ObservedOpusPacket<'a>>,
{
    let mut accumulator = AudioValidationAccumulator::new();

    for packet in packets {
        accumulator.observe_packet(packet)?;
    }

    accumulator.into_stats()
}

fn interval_stats(intervals: &[Duration]) -> AudioIntervalStats {
    let mut sorted = intervals.to_vec();
    sorted.sort_unstable();
    AudioIntervalStats {
        samples: u64::try_from(sorted.len()).unwrap_or(u64::MAX),
        p50_ms: duration_ms(percentile_duration(&sorted, 50)),
        p95_ms: duration_ms(percentile_duration(&sorted, 95)),
        p99_ms: duration_ms(percentile_duration(&sorted, 99)),
        min_ms: duration_ms(sorted[0]),
        max_ms: duration_ms(sorted[sorted.len() - 1]),
    }
}

fn percentile_duration(sorted: &[Duration], percentile: usize) -> Duration {
    sorted[((sorted.len() - 1) * percentile).div_ceil(100)]
}

fn duration_ms(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

fn duration_us(duration: Duration) -> u64 {
    u64::try_from(duration.as_micros()).unwrap_or(u64::MAX)
}

fn media_to_wall_clock_ratio_ppm(media_duration_ms: u64, wall_clock_elapsed_ms: u64) -> u64 {
    if media_duration_ms == 0 || wall_clock_elapsed_ms == 0 {
        return 0;
    }

    ((u128::from(media_duration_ms) * 1_000_000) / u128::from(wall_clock_elapsed_ms))
        .try_into()
        .unwrap_or(u64::MAX)
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

fn opus_packet_channels(packet: &[u8]) -> Option<usize> {
    let toc = *packet.first()?;
    Some(if toc & 0x04 != 0 { 2 } else { 1 })
}
