use bytes::Bytes;

use crate::error::AppError;
use crate::media::opus_queue::OpusFrameQueue;
use crate::ytmusic::client::{ResolvedPlaybackSource, YtMusicClient};
use crate::ytmusic::selector::{StreamFormat, select_format};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlaybackPlan {
    pub video_id: String,
    pub selected_itag: u32,
}

impl PlaybackPlan {
    pub fn from_formats(video_id: &str, formats: &[StreamFormat]) -> Option<Self> {
        let selected = select_format(formats)?;
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
        &self,
        video_id: &str,
        formats: &[StreamFormat],
        queue: &mut OpusFrameQueue,
    ) -> Result<ResolvedPlaybackSource, AppError> {
        let source = self
            .client
            .resolve_playback_source(video_id, formats)
            .await?;
        queue
            .push(Bytes::from_static(b"prefetched-opus-frame"))
            .map_err(|_| AppError::BufferFull)?;
        Ok(source)
    }
}
