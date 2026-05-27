use crate::error::PlaybackError;
use crate::selector::select_song_stream_format;
use crate::source::ResolvedPlaybackSource;
use ytmusic_service_client::v2::{DecipherRequest, GetSongRequest};
use ytmusic_service_client::{ClientError, YtMusicServiceClient};

#[derive(Debug)]
pub struct YtMusicClient {
    inner: YtMusicServiceClient,
}

impl YtMusicClient {
    pub async fn connect(endpoint: String) -> Result<Self, PlaybackError> {
        let inner = YtMusicServiceClient::connect(endpoint)
            .await
            .map_err(|error| match error {
                ClientError::Transport(error) => PlaybackError::Transport(error),
            })?;
        Ok(Self { inner })
    }

    pub async fn resolve_playback_source(
        &mut self,
        video_id: &str,
    ) -> Result<ResolvedPlaybackSource, PlaybackError> {
        let song = self
            .inner
            .music()
            .inner_mut()
            .get_song(GetSongRequest {
                video_id: video_id.into(),
            })
            .await?
            .into_inner();
        let streaming_data = song
            .streaming_data
            .ok_or(PlaybackError::UnsupportedFormat)?;
        let selected = select_song_stream_format(&streaming_data.adaptive_formats)?;
        let deciphered = self
            .inner
            .cipher()
            .inner_mut()
            .decipher(DecipherRequest {
                signature_cipher: selected.signature_cipher.clone(),
            })
            .await?
            .into_inner();

        Ok(ResolvedPlaybackSource::from_parts(
            selected.itag,
            deciphered.playable_url,
        ))
    }
}
