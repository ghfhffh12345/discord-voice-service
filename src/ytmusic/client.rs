use crate::error::AppError;
use crate::ytmusic::selector::{StreamFormat, select_format};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResolvedPlaybackSource {
    pub selected_itag: u32,
    pub playable_url: String,
}

#[derive(Clone, Debug)]
pub struct YtMusicClient {
    pub endpoint: String,
}

impl YtMusicClient {
    pub fn new(endpoint: String) -> Self {
        Self { endpoint }
    }

    pub async fn healthcheck(&self) -> Result<(), AppError> {
        let _ = &self.endpoint;
        Ok(())
    }

    pub async fn resolve_playback_source(
        &self,
        video_id: &str,
        adaptive_formats: &[StreamFormat],
    ) -> Result<ResolvedPlaybackSource, AppError> {
        let selected = select_format(adaptive_formats).ok_or(AppError::UnsupportedFormat)?;
        Ok(ResolvedPlaybackSource {
            selected_itag: selected.itag,
            playable_url: format!("{}/deciphered/{}", self.endpoint, video_id),
        })
    }
}
