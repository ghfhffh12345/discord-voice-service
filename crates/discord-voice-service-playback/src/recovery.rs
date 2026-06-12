use std::collections::VecDeque;
use std::time::Duration;

use super::ytmusic_client::YtMusicClient;
use crate::error::PlaybackError;
use crate::media::http_stream::HttpOpusStream;
use crate::media::opus_queue::{duration_ms_from_samples, samples_from_duration_ms_u64};
use crate::media::position::{PlaybackPosition, shared_playback_position};
use crate::media::webm_demux::{DemuxedPacket, WebmOpusDemux};
use crate::source::{PlaybackSource, ResolvedPlaybackSource};
use bytes::Bytes;
use reqwest::StatusCode;
use tokio::time::timeout;
use tracing::warn;

const INITIAL_OPEN_CHUNK_TIMEOUT: Duration = Duration::from_secs(15);
const STEADY_STATE_CHUNK_TIMEOUT: Duration = Duration::from_secs(2);
const OPEN_CHUNK_ATTEMPTS: usize = 2;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct PlaybackRecoveryMetrics {
    pub http_retry_count: u64,
    pub url_reresolve_count: u64,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct PlaybackResumePosition {
    samples: u64,
}

impl PlaybackResumePosition {
    pub const fn from_samples(samples: u64) -> Self {
        Self { samples }
    }

    pub fn from_millis(duration_ms: u64) -> Self {
        Self {
            samples: samples_from_duration_ms_u64(duration_ms),
        }
    }

    pub const fn samples(self) -> u64 {
        self.samples
    }

    pub fn millis(self) -> u64 {
        duration_ms_from_samples(self.samples)
    }

    pub const fn is_start(self) -> bool {
        self.samples == 0
    }
}

#[derive(Debug)]
pub struct PlaybackRecovery {
    client: YtMusicClient,
    last_video_id: Option<String>,
    last_resolved: Option<ResolvedPlaybackSource>,
    metrics: PlaybackRecoveryMetrics,
}

impl PlaybackRecovery {
    pub fn new(client: YtMusicClient) -> Self {
        Self {
            client,
            last_video_id: None,
            last_resolved: None,
            metrics: PlaybackRecoveryMetrics::default(),
        }
    }

    pub async fn recover(
        &mut self,
        video_id: &str,
        position: PlaybackResumePosition,
    ) -> Result<PlaybackSource, PlaybackError> {
        if self.last_video_id.as_deref() == Some(video_id)
            && let Ok(source) = self.try_reopen_existing(position).await
        {
            return Ok(source);
        }

        self.resolve_and_open(video_id, position).await
    }

    pub fn reset(&mut self) {
        self.last_video_id = None;
        self.last_resolved = None;
    }

    pub fn metrics(&self) -> PlaybackRecoveryMetrics {
        self.metrics
    }

    pub async fn read_stream_chunk(
        &mut self,
        video_id: Option<&str>,
        source: &mut PlaybackSource,
    ) -> Result<Option<Bytes>, PlaybackError> {
        match read_source_chunk_with_timeout(source).await {
            Ok(chunk) => Ok(chunk),
            Err(err) if is_steady_state_read_timeout(&err) => {
                self.metrics.http_retry_count = self.metrics.http_retry_count.saturating_add(1);
                source.reset_stream_to_current_byte_offset();
                match read_source_chunk_with_timeout(source).await {
                    Ok(chunk) => Ok(chunk),
                    Err(err) if should_reresolve_after_steady_read_failure(&err) => {
                        self.reresolve_steady_source(video_id, source, err).await?;
                        read_source_chunk_with_timeout(source).await
                    }
                    Err(err) => Err(err),
                }
            }
            Err(err) if should_reresolve_after_steady_read_failure(&err) => {
                self.reresolve_steady_source(video_id, source, err).await?;
                read_source_chunk_with_timeout(source).await
            }
            Err(err) => Err(err),
        }
    }

    fn remember_source(&mut self, video_id: &str, source: &PlaybackSource) {
        self.last_video_id = Some(video_id.to_owned());
        self.last_resolved = Some(source.resolved().clone());
    }

    async fn reresolve_steady_source(
        &mut self,
        video_id: Option<&str>,
        source: &mut PlaybackSource,
        original_err: PlaybackError,
    ) -> Result<(), PlaybackError> {
        let Some(video_id) = video_id else {
            return Err(original_err);
        };
        self.metrics.url_reresolve_count = self.metrics.url_reresolve_count.saturating_add(1);
        let resolved = self.client.resolve_playback_source(video_id).await?;
        source.replace_resolved_stream_at_current_byte_offset(resolved);
        self.remember_source(video_id, source);
        Ok(())
    }

    async fn try_reopen_existing(
        &mut self,
        position: PlaybackResumePosition,
    ) -> Result<PlaybackSource, PlaybackError> {
        let resolved = self
            .last_resolved
            .clone()
            .ok_or(PlaybackError::InvalidState(
                "no playback source available to reopen",
            ))?;
        self.open_from_position(resolved, position).await
    }

    async fn resolve_and_open(
        &mut self,
        video_id: &str,
        position: PlaybackResumePosition,
    ) -> Result<PlaybackSource, PlaybackError> {
        let resolved = self.client.resolve_playback_source(video_id).await?;
        match self.open_from_position(resolved, position).await {
            Ok(source) => {
                self.remember_source(video_id, &source);
                Ok(source)
            }
            Err(err) if should_reresolve_after_open_failure(&err) => {
                self.metrics.url_reresolve_count =
                    self.metrics.url_reresolve_count.saturating_add(1);
                let resolved = self.client.resolve_playback_source(video_id).await?;
                let source = self.open_from_position(resolved, position).await?;
                self.remember_source(video_id, &source);
                Ok(source)
            }
            Err(err) => Err(err),
        }
    }

    async fn open_from_position(
        &mut self,
        resolved: ResolvedPlaybackSource,
        resume_position: PlaybackResumePosition,
    ) -> Result<PlaybackSource, PlaybackError> {
        let mut stream = HttpOpusStream::new(resolved.playable_url.clone());
        let mut demux = WebmOpusDemux::default();
        let mut pending_packets = VecDeque::new();
        let mut playback_position = PlaybackPosition::default();

        let opening = read_opening_chunk(&mut stream, &resolved.playable_url).await?;
        self.metrics.http_retry_count = self
            .metrics
            .http_retry_count
            .saturating_add(opening.retry_count);
        let Some(chunk) = opening.chunk else {
            return Err(PlaybackError::MediaParse("unexpected end of stream"));
        };
        demux.push_bytes(chunk);
        for packet in demux.drain_packets()? {
            playback_position.record_buffered(&packet);
            if packet_overlaps_resume(&packet, resume_position) {
                pending_packets.push_back(packet);
            }
        }

        while pending_packets.is_empty()
            || playback_position.timestamp_samples() < resume_position.samples()
        {
            let Some(chunk) = read_chunk_with_timeout(&mut stream, &resolved.playable_url).await?
            else {
                break;
            };

            demux.push_bytes(chunk);
            for packet in demux.drain_packets()? {
                playback_position.record_buffered(&packet);
                if packet_overlaps_resume(&packet, resume_position) {
                    pending_packets.push_back(packet);
                }
            }
        }

        if playback_position.timestamp_samples() < resume_position.samples() {
            return Err(PlaybackError::MediaParseDetail(format!(
                "playback source ended before requested resume position {}ms/{} samples; reached {}ms/{} samples",
                resume_position.millis(),
                resume_position.samples(),
                playback_position.timestamp_ms(),
                playback_position.timestamp_samples()
            )));
        }

        if !resume_position.is_start() {
            playback_position.set_sent_duration_samples(resume_position.samples());
        }

        Ok(PlaybackSource::new(
            resolved,
            stream,
            demux,
            pending_packets,
            shared_playback_position(playback_position),
        ))
    }
}

fn packet_end_samples(packet: &DemuxedPacket) -> u64 {
    packet
        .timestamp_samples
        .saturating_add(u64::from(packet.duration_samples))
}

fn packet_overlaps_resume(packet: &DemuxedPacket, position: PlaybackResumePosition) -> bool {
    packet_end_samples(packet) > position.samples()
}

async fn read_opening_chunk(
    stream: &mut HttpOpusStream,
    playable_url: &str,
) -> Result<OpeningChunk, PlaybackError> {
    let mut retry_count = 0;
    for attempt in 1..=OPEN_CHUNK_ATTEMPTS {
        match timeout(INITIAL_OPEN_CHUNK_TIMEOUT, stream.read_chunk()).await {
            Ok(result) => {
                return result.map(|chunk| OpeningChunk { chunk, retry_count });
            }
            Err(_) if attempt < OPEN_CHUNK_ATTEMPTS => {
                retry_count += 1;
                warn!(
                    attempt,
                    timeout_ms = INITIAL_OPEN_CHUNK_TIMEOUT.as_millis(),
                    url = playable_url,
                    "playback source open attempt timed out; retrying"
                );
            }
            Err(_) => {
                return Err(PlaybackError::MediaParseDetail(format!(
                    "timed out opening playback source for {playable_url}"
                )));
            }
        }
    }

    unreachable!("open chunk attempts loop must return")
}

struct OpeningChunk {
    chunk: Option<bytes::Bytes>,
    retry_count: u64,
}

async fn read_chunk_with_timeout(
    stream: &mut HttpOpusStream,
    playable_url: &str,
) -> Result<Option<bytes::Bytes>, PlaybackError> {
    timeout(STEADY_STATE_CHUNK_TIMEOUT, stream.read_chunk())
        .await
        .map_err(|_| {
            PlaybackError::MediaParseDetail(format!(
                "timed out reading playback source for {playable_url}"
            ))
        })?
}

async fn read_source_chunk_with_timeout(
    source: &mut PlaybackSource,
) -> Result<Option<Bytes>, PlaybackError> {
    let playable_url = source.playable_url().to_owned();
    read_chunk_with_timeout(source.stream_mut(), &playable_url).await
}

fn should_reresolve_after_open_failure(err: &PlaybackError) -> bool {
    matches!(err, PlaybackError::Http(err) if err.status().is_some_and(is_stale_source_status))
        || matches!(err, PlaybackError::MediaParseDetail(message) if message.contains("timed out opening playback source"))
}

fn should_reresolve_after_steady_read_failure(err: &PlaybackError) -> bool {
    matches!(err, PlaybackError::Http(err) if err.status().is_some_and(is_stale_source_status))
        || is_steady_state_read_timeout(err)
}

fn is_steady_state_read_timeout(err: &PlaybackError) -> bool {
    matches!(err, PlaybackError::MediaParseDetail(message) if message.contains("timed out reading playback source"))
}

fn is_stale_source_status(status: StatusCode) -> bool {
    matches!(
        status,
        StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN | StatusCode::NOT_FOUND | StatusCode::GONE
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    use bytes::Bytes;

    #[test]
    fn resume_position_preserves_fractional_samples_while_exposing_floor_ms() {
        let resume_position = PlaybackResumePosition::from_samples(120);
        let mut playback_position = PlaybackPosition::default();
        playback_position.set_sent_duration_samples(resume_position.samples());

        assert_eq!(resume_position.samples(), 120);
        assert_eq!(resume_position.millis(), 2);
        assert_eq!(playback_position.sent_duration_samples(), 120);
        assert_eq!(playback_position.sent_duration_ms(), 2);
        assert_eq!(
            samples_from_duration_ms_u64(resume_position.millis()),
            96,
            "a millisecond-only resume position would lose the 24 fractional samples"
        );
    }

    #[test]
    fn resume_filter_uses_sample_boundaries_for_fractional_packets() {
        let first = packet(0, 120);
        let second = packet(120, 120);
        let third = packet(240, 120);

        let after_first = PlaybackResumePosition::from_samples(120);
        assert!(!packet_overlaps_resume(&first, after_first));
        assert!(packet_overlaps_resume(&second, after_first));

        let after_second = PlaybackResumePosition::from_samples(240);
        assert!(!packet_overlaps_resume(&second, after_second));
        assert!(packet_overlaps_resume(&third, after_second));
    }

    fn packet(timestamp_samples: u64, duration_samples: u32) -> DemuxedPacket {
        DemuxedPacket {
            data: Bytes::from_static(b"packet"),
            timestamp_ms: duration_ms_from_samples(timestamp_samples),
            timestamp_samples,
            duration_ms: duration_ms_from_samples(u64::from(duration_samples)),
            duration_samples,
        }
    }
}
