use std::collections::VecDeque;
use std::time::Duration;

use super::ytmusic_client::YtMusicClient;
use crate::error::PlaybackError;
use crate::media::http_stream::HttpOpusStream;
use crate::media::position::{PlaybackPosition, shared_playback_position};
use crate::media::webm_demux::{DemuxedPacket, WebmOpusDemux};
use crate::source::{PlaybackSource, ResolvedPlaybackSource};
use reqwest::StatusCode;
use tokio::time::timeout;
use tracing::warn;

const INITIAL_OPEN_CHUNK_TIMEOUT: Duration = Duration::from_secs(15);
const STEADY_STATE_CHUNK_TIMEOUT: Duration = Duration::from_secs(2);
const OPEN_CHUNK_ATTEMPTS: usize = 2;

#[derive(Debug)]
pub struct PlaybackRecovery {
    client: YtMusicClient,
    last_video_id: Option<String>,
    last_resolved: Option<ResolvedPlaybackSource>,
}

impl PlaybackRecovery {
    pub fn new(client: YtMusicClient) -> Self {
        Self {
            client,
            last_video_id: None,
            last_resolved: None,
        }
    }

    pub async fn recover(
        &mut self,
        video_id: &str,
        position_ms: u64,
    ) -> Result<PlaybackSource, PlaybackError> {
        if self.last_video_id.as_deref() == Some(video_id)
            && let Ok(source) = self.try_reopen_existing(position_ms).await
        {
            return Ok(source);
        }

        self.resolve_and_open(video_id, position_ms).await
    }

    pub fn reset(&mut self) {
        self.last_video_id = None;
        self.last_resolved = None;
    }

    fn remember_source(&mut self, video_id: &str, source: &PlaybackSource) {
        self.last_video_id = Some(video_id.to_owned());
        self.last_resolved = Some(source.resolved().clone());
    }

    async fn try_reopen_existing(
        &mut self,
        position_ms: u64,
    ) -> Result<PlaybackSource, PlaybackError> {
        let resolved = self
            .last_resolved
            .clone()
            .ok_or(PlaybackError::InvalidState(
                "no playback source available to reopen",
            ))?;
        self.open_from_position(resolved, position_ms).await
    }

    async fn resolve_and_open(
        &mut self,
        video_id: &str,
        position_ms: u64,
    ) -> Result<PlaybackSource, PlaybackError> {
        let resolved = self.client.resolve_playback_source(video_id).await?;
        match self.open_from_position(resolved, position_ms).await {
            Ok(source) => {
                self.remember_source(video_id, &source);
                Ok(source)
            }
            Err(err) if should_reresolve_after_open_failure(&err) => {
                let resolved = self.client.resolve_playback_source(video_id).await?;
                let source = self.open_from_position(resolved, position_ms).await?;
                self.remember_source(video_id, &source);
                Ok(source)
            }
            Err(err) => Err(err),
        }
    }

    async fn open_from_position(
        &mut self,
        resolved: ResolvedPlaybackSource,
        position_ms: u64,
    ) -> Result<PlaybackSource, PlaybackError> {
        let mut stream = HttpOpusStream::new(resolved.playable_url.clone());
        let mut demux = WebmOpusDemux::default();
        let mut pending_packets = VecDeque::new();
        let mut position = PlaybackPosition::default();

        let Some(chunk) = read_opening_chunk(&mut stream, &resolved.playable_url).await? else {
            return Err(PlaybackError::MediaParse("unexpected end of stream"));
        };
        demux.push_bytes(chunk);
        for packet in demux.drain_packets()? {
            position.record_buffered(&packet);
            if packet_end_ms(&packet) > position_ms {
                pending_packets.push_back(packet);
            }
        }

        while pending_packets.is_empty() || position.timestamp_ms() < position_ms {
            let Some(chunk) = read_chunk_with_timeout(&mut stream, &resolved.playable_url).await?
            else {
                break;
            };

            demux.push_bytes(chunk);
            for packet in demux.drain_packets()? {
                position.record_buffered(&packet);
                if packet_end_ms(&packet) > position_ms {
                    pending_packets.push_back(packet);
                }
            }
        }

        if position.timestamp_ms() < position_ms {
            return Err(PlaybackError::MediaParseDetail(format!(
                "playback source ended before requested resume position {position_ms}ms; reached {}ms",
                position.timestamp_ms()
            )));
        }

        if position_ms > 0 {
            position.record_sent_packet(position_ms);
        }

        Ok(PlaybackSource::new(
            resolved,
            stream,
            demux,
            pending_packets,
            shared_playback_position(position),
        ))
    }
}

fn packet_end_ms(packet: &DemuxedPacket) -> u64 {
    packet.timestamp_ms.saturating_add(packet.duration_ms)
}

async fn read_opening_chunk(
    stream: &mut HttpOpusStream,
    playable_url: &str,
) -> Result<Option<bytes::Bytes>, PlaybackError> {
    for attempt in 1..=OPEN_CHUNK_ATTEMPTS {
        match timeout(INITIAL_OPEN_CHUNK_TIMEOUT, stream.read_chunk()).await {
            Ok(result) => return result,
            Err(_) if attempt < OPEN_CHUNK_ATTEMPTS => {
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

fn should_reresolve_after_open_failure(err: &PlaybackError) -> bool {
    matches!(err, PlaybackError::Http(err) if err.status().is_some_and(is_stale_source_status))
        || matches!(err, PlaybackError::MediaParseDetail(message) if message.contains("timed out opening playback source"))
}

fn is_stale_source_status(status: StatusCode) -> bool {
    matches!(
        status,
        StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN | StatusCode::NOT_FOUND | StatusCode::GONE
    )
}
