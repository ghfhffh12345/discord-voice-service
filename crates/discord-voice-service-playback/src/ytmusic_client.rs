use crate::error::PlaybackError;
use crate::selector::select_song_stream_format;
use crate::source::ResolvedPlaybackSource;
use ytmusic_service_proto::ytmusic::v1::yt_music_public_client::YtMusicPublicClient;
use ytmusic_service_proto::ytmusic::v1::{DecipherRequest, GetSongRequest};

#[derive(Debug)]
pub struct YtMusicClient {
    inner: YtMusicPublicClient<tonic::transport::Channel>,
}

impl YtMusicClient {
    pub async fn connect(endpoint: String) -> Result<Self, PlaybackError> {
        let channel = tonic::transport::Endpoint::from_shared(endpoint)?
            .connect()
            .await?;
        Ok(Self {
            inner: YtMusicPublicClient::new(channel),
        })
    }

    pub async fn resolve_playback_source(
        &mut self,
        video_id: &str,
    ) -> Result<ResolvedPlaybackSource, PlaybackError> {
        let song = self
            .inner
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
