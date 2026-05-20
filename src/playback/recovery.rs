use std::collections::VecDeque;
use std::time::Duration;

use crate::error::AppError;
use crate::media::http_stream::HttpOpusStream;
use crate::media::position::{PlaybackPosition, shared_playback_position};
use crate::media::webm_demux::{DemuxedPacket, WebmOpusDemux};
use crate::playback::source::PlaybackSource;
use crate::ytmusic::client::{ResolvedPlaybackSource, YtMusicClient};
use reqwest::StatusCode;
use tokio::time::timeout;

const OPEN_CHUNK_TIMEOUT: Duration = Duration::from_millis(500);

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
    ) -> Result<PlaybackSource, AppError> {
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

    async fn try_reopen_existing(&mut self, position_ms: u64) -> Result<PlaybackSource, AppError> {
        let resolved = self.last_resolved.clone().ok_or(AppError::InvalidState(
            "no playback source available to reopen",
        ))?;
        self.open_from_position(resolved, position_ms).await
    }

    async fn resolve_and_open(
        &mut self,
        video_id: &str,
        position_ms: u64,
    ) -> Result<PlaybackSource, AppError> {
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
    ) -> Result<PlaybackSource, AppError> {
        let mut stream = HttpOpusStream::new(resolved.playable_url.clone());
        let mut demux = WebmOpusDemux::default();
        let mut pending_packets = VecDeque::new();
        let mut position = PlaybackPosition::default();
        let mut saw_chunk = false;

        while pending_packets.is_empty() || position.timestamp_ms() < position_ms {
            let Some(chunk) = timeout(OPEN_CHUNK_TIMEOUT, stream.read_chunk())
                .await
                .map_err(|_| {
                    AppError::MediaParseDetail(format!(
                        "timed out opening playback source for {}",
                        resolved.playable_url
                    ))
                })??
            else {
                break;
            };
            saw_chunk = true;

            demux.push_bytes(chunk);
            for packet in demux.drain_packets()? {
                position.record_buffered(&packet);
                if packet_end_ms(&packet) > position_ms {
                    pending_packets.push_back(packet);
                }
            }
        }

        if !saw_chunk {
            return Err(AppError::MediaParse("unexpected end of stream"));
        }

        if position.timestamp_ms() < position_ms {
            return Err(AppError::MediaParseDetail(format!(
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

fn should_reresolve_after_open_failure(err: &AppError) -> bool {
    matches!(err, AppError::Http(err) if err.status().is_some_and(is_stale_source_status))
        || matches!(err, AppError::MediaParseDetail(message) if message.contains("timed out opening playback source"))
}

fn is_stale_source_status(status: StatusCode) -> bool {
    matches!(
        status,
        StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN | StatusCode::NOT_FOUND | StatusCode::GONE
    )
}
