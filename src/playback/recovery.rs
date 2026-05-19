use std::collections::VecDeque;

use crate::error::AppError;
use crate::media::http_stream::HttpOpusStream;
use crate::media::position::PlaybackPosition;
use crate::media::webm_demux::{DemuxedPacket, WebmOpusDemux};
use crate::playback::source::PlaybackSource;
use crate::ytmusic::client::{ResolvedPlaybackSource, YtMusicClient};

#[derive(Debug)]
pub struct PlaybackRecovery {
    client: YtMusicClient,
    last_resolved: Option<ResolvedPlaybackSource>,
}

impl PlaybackRecovery {
    pub fn new(client: YtMusicClient) -> Self {
        Self {
            client,
            last_resolved: None,
        }
    }

    pub async fn recover(
        &mut self,
        video_id: &str,
        position_ms: u64,
    ) -> Result<PlaybackSource, AppError> {
        if let Ok(source) = self.try_reopen_existing(position_ms).await {
            return Ok(source);
        }

        let resolved = self.client.resolve_playback_source(video_id).await?;
        match self.open_from_position(resolved, position_ms).await {
            Ok(source) => Ok(source),
            Err(_) => {
                let resolved = self.client.resolve_playback_source(video_id).await?;
                self.open_from_position(resolved, position_ms).await
            }
        }
    }

    pub fn remember_source(&mut self, source: &PlaybackSource) {
        self.last_resolved = Some(source.resolved().clone());
    }

    async fn try_reopen_existing(&mut self, position_ms: u64) -> Result<PlaybackSource, AppError> {
        let resolved = self.last_resolved.clone().ok_or(AppError::InvalidState(
            "no playback source available to reopen",
        ))?;
        self.open_from_position(resolved, position_ms).await
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
            let Some(chunk) = stream.read_chunk().await? else {
                break;
            };
            saw_chunk = true;

            demux.push_bytes(chunk);
            for packet in demux.drain_packets()? {
                position.record_buffered(&packet);
                if packet_end_ms(&packet) >= position_ms {
                    pending_packets.push_back(packet);
                }
            }
        }

        if !saw_chunk {
            return Err(AppError::MediaParse("unexpected end of stream"));
        }

        if position_ms > 0 {
            position.record_sent_packet(position_ms);
        }

        self.last_resolved = Some(resolved.clone());
        Ok(PlaybackSource::new(
            resolved,
            stream,
            demux,
            pending_packets,
            position,
        ))
    }
}

fn packet_end_ms(packet: &DemuxedPacket) -> u64 {
    packet.timestamp_ms.saturating_add(packet.duration_ms)
}
