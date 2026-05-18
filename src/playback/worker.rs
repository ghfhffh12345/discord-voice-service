use bytes::Bytes;

use crate::error::AppError;
use crate::media::opus_queue::OpusFrameQueue;
use crate::ytmusic::client::{ResolvedPlaybackSource, YtMusicClient};
use crate::ytmusic::selector::select_song_stream_format;
use crate::ytmusic::v1::SongStreamFormat;

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
}

impl PlaybackWorker {
    pub fn new(client: YtMusicClient) -> Self {
        Self { client }
    }

    pub async fn prepare(
        &mut self,
        video_id: &str,
        queue: &mut OpusFrameQueue,
    ) -> Result<ResolvedPlaybackSource, AppError> {
        let source = self.client.resolve_playback_source(video_id).await?;
        queue
            .push(Bytes::from_static(b"prefetched-opus-frame"))
            .map_err(|_| AppError::BufferFull)?;
        Ok(source)
    }
}
