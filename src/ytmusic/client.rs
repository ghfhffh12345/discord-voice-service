use crate::error::AppError;
use crate::ytmusic::selector::select_song_stream_format;
use crate::ytmusic::v1::yt_music_public_client::YtMusicPublicClient;
use crate::ytmusic::v1::{DecipherRequest, GetSongRequest, SongStreamFormat};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResolvedPlaybackSource {
    pub selected_itag: u32,
    pub playable_url: String,
}

impl ResolvedPlaybackSource {
    fn from_song(selected: SongStreamFormat, playable_url: String) -> Self {
        Self {
            selected_itag: selected.itag,
            playable_url,
        }
    }
}

#[derive(Debug)]
pub struct YtMusicClient {
    inner: YtMusicPublicClient<tonic::transport::Channel>,
}

impl YtMusicClient {
    pub async fn connect(endpoint: String) -> Result<Self, AppError> {
        let channel = tonic::transport::Endpoint::from_shared(endpoint)?
            .connect()
            .await?;
        Ok(Self {
            inner: YtMusicPublicClient::new(channel),
        })
    }

    pub async fn healthcheck(&self) -> Result<(), AppError> {
        let _ = &self.inner;
        Ok(())
    }

    pub async fn resolve_playback_source(
        &mut self,
        video_id: &str,
    ) -> Result<ResolvedPlaybackSource, AppError> {
        let song = self
            .inner
            .get_song(GetSongRequest {
                video_id: video_id.into(),
            })
            .await?
            .into_inner();
        let streaming_data = song.streaming_data.ok_or(AppError::UnsupportedFormat)?;
        let selected = select_song_stream_format(&streaming_data.adaptive_formats)?;
        let deciphered = self
            .inner
            .decipher(DecipherRequest {
                signature_cipher: selected.signature_cipher.clone(),
            })
            .await?
            .into_inner();

        Ok(ResolvedPlaybackSource::from_song(
            selected,
            deciphered.playable_url,
        ))
    }
}
