use std::collections::VecDeque;

use crate::error::AppError;
use crate::media::http_stream::HttpOpusStream;
use crate::media::opus_queue::OpusFrameQueue;
use crate::media::position::PlaybackPosition;
use crate::media::webm_demux::WebmOpusDemux;
use crate::playback::source::PlaybackSource;
use crate::ytmusic::client::YtMusicClient;
use crate::ytmusic::selector::select_song_stream_format;
use crate::ytmusic::v1::SongStreamFormat;

const DEFAULT_PREBUFFER_TARGET: usize = 4;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlaybackPlan {
    pub video_id: String,
    pub selected_itag: u32,
}

impl PlaybackPlan {
    pub fn from_formats(video_id: &str, formats: &[SongStreamFormat]) -> Option<Self> {
        let selected = select_song_stream_format(formats).ok()?;
        Some(Self {
            video_id: video_id.to_owned(),
            selected_itag: selected.itag,
        })
    }
}

pub struct PlaybackWorker {
    client: YtMusicClient,
    position: PlaybackPosition,
    prebuffer_target: usize,
}

impl PlaybackWorker {
    pub fn new(client: YtMusicClient) -> Self {
        Self {
            client,
            position: PlaybackPosition::default(),
            prebuffer_target: DEFAULT_PREBUFFER_TARGET,
        }
    }

    pub async fn prepare(
        &mut self,
        video_id: &str,
        queue: &mut OpusFrameQueue,
    ) -> Result<PlaybackSource, AppError> {
        self.position = PlaybackPosition::default();

        let resolved = self.client.resolve_playback_source(video_id).await?;
        let mut stream = HttpOpusStream::new(resolved.playable_url.clone());
        let mut demux = WebmOpusDemux::default();
        let mut pending_packets = VecDeque::new();

        while queue.len() < self.prebuffer_target {
            let Some(chunk) = stream.read_chunk().await? else {
                return Err(AppError::MediaParse("unexpected end of stream"));
            };

            demux.push_bytes(chunk);
            for packet in demux.drain_packets()? {
                self.position.record_buffered(&packet);
                if queue.len() < self.prebuffer_target {
                    queue
                        .push(packet.data.clone())
                        .map_err(|_| AppError::BufferFull)?;
                } else {
                    pending_packets.push_back(packet);
                }
            }
        }

        Ok(PlaybackSource::new(
            resolved,
            stream,
            demux,
            pending_packets,
            self.position.snapshot(),
        ))
    }
}
