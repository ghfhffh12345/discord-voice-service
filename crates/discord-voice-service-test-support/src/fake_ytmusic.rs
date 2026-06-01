#![allow(dead_code)]

use std::sync::{Arc, Mutex};

use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tokio::time::{Duration, sleep};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;
use tonic::{Request, Response, Status};
use ytmusic_service_client::v2::yt_cipher_server::{YtCipher, YtCipherServer};
use ytmusic_service_client::v2::yt_music_server::{YtMusic, YtMusicServer};
use ytmusic_service_client::v2::{
    AccountInfoResponse, ContinuationRequest, DecipherRequest, DecipherResponse, Empty,
    GetSignatureTimestampResponse, GetSongRequest, GetSongResponse,
    GetWatchPlaylistContinuationRequest, GetWatchPlaylistRequest, LibraryAlbumsResponse,
    LibraryArtistsResponse, LibraryChannelsResponse, LibraryPlaylistsResponse,
    LibraryPodcastsResponse, LibrarySongsResponse, LibrarySubscriptionsResponse,
    LikedSongsResponse, RefreshCipherResponse, SavedEpisodesResponse, SearchContinuationRequest,
    SearchRequest, SearchResponse, SongStreamFormat, SongStreamingData, WatchPlaylistResponse,
};

pub struct FakeYtMusic {
    endpoint: String,
    calls: Arc<Mutex<Vec<String>>>,
    playable_url: Arc<Mutex<String>>,
    stale_playable_url_once: Arc<Mutex<Option<String>>>,
    decipher_delay: Arc<Mutex<Option<Duration>>>,
    _server: JoinHandle<()>,
}

impl FakeYtMusic {
    pub async fn spawn() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let calls = Arc::new(Mutex::new(Vec::new()));
        let playable_url = Arc::new(Mutex::new("https://cdn.example/audio.webm".to_owned()));
        let stale_playable_url_once = Arc::new(Mutex::new(None));
        let decipher_delay = Arc::new(Mutex::new(None));
        let service = FakeYtMusicService {
            calls: Arc::clone(&calls),
            playable_url: Arc::clone(&playable_url),
            stale_playable_url_once: Arc::clone(&stale_playable_url_once),
            decipher_delay: Arc::clone(&decipher_delay),
        };

        let server = tokio::spawn(async move {
            Server::builder()
                .add_service(YtMusicServer::new(service.clone()))
                .add_service(YtCipherServer::new(service))
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await
                .unwrap();
        });

        Self {
            endpoint,
            calls,
            playable_url,
            stale_playable_url_once,
            decipher_delay,
            _server: server,
        }
    }

    pub fn endpoint(&self) -> String {
        self.endpoint.clone()
    }

    pub fn calls(&self) -> Vec<String> {
        self.calls.lock().unwrap().clone()
    }

    pub async fn set_playable_url(&self, playable_url: impl Into<String>) {
        *self.playable_url.lock().unwrap() = playable_url.into();
    }

    pub async fn set_first_playable_url_once(&self, playable_url: impl Into<String>) {
        *self.stale_playable_url_once.lock().unwrap() = Some(playable_url.into());
    }

    pub async fn set_decipher_delay(&self, delay: Duration) {
        *self.decipher_delay.lock().unwrap() = Some(delay);
    }
}

#[derive(Clone)]
struct FakeYtMusicService {
    calls: Arc<Mutex<Vec<String>>>,
    playable_url: Arc<Mutex<String>>,
    stale_playable_url_once: Arc<Mutex<Option<String>>>,
    decipher_delay: Arc<Mutex<Option<Duration>>>,
}

impl FakeYtMusicService {
    fn record(&self, call: &str) {
        self.calls.lock().unwrap().push(call.to_owned());
    }
}

fn unimplemented(name: &str) -> Status {
    Status::unimplemented(name)
}

#[tonic::async_trait]
impl YtMusic for FakeYtMusicService {
    async fn search(
        &self,
        _request: Request<SearchRequest>,
    ) -> Result<Response<SearchResponse>, Status> {
        Err(unimplemented("search"))
    }

    async fn search_continuation(
        &self,
        _request: Request<SearchContinuationRequest>,
    ) -> Result<Response<SearchResponse>, Status> {
        Err(unimplemented("search_continuation"))
    }

    async fn get_watch_playlist(
        &self,
        _request: Request<GetWatchPlaylistRequest>,
    ) -> Result<Response<WatchPlaylistResponse>, Status> {
        Err(unimplemented("get_watch_playlist"))
    }

    async fn get_watch_playlist_continuation(
        &self,
        _request: Request<GetWatchPlaylistContinuationRequest>,
    ) -> Result<Response<WatchPlaylistResponse>, Status> {
        Err(unimplemented("get_watch_playlist_continuation"))
    }

    async fn get_song(
        &self,
        request: Request<GetSongRequest>,
    ) -> Result<Response<GetSongResponse>, Status> {
        self.record("GetSong");
        let video_id = request.into_inner().video_id;

        let streaming_data = if video_id == "missing-streaming-data" {
            None
        } else {
            let adaptive_formats = if video_id == "missing-lower" {
                vec![SongStreamFormat {
                    itag: 251,
                    mime_type: "audio/webm; codecs=\"opus\"".to_owned(),
                    bitrate: 160_000,
                    approx_duration_ms: Some(181_000),
                    audio_sample_rate: Some(48_000),
                    audio_channels: Some(2),
                    signature_cipher: format!("cipher-{video_id}-251"),
                    ..Default::default()
                }]
            } else {
                vec![
                    SongStreamFormat {
                        itag: 251,
                        mime_type: "audio/webm; codecs=\"opus\"".to_owned(),
                        bitrate: 160_000,
                        approx_duration_ms: Some(181_000),
                        audio_sample_rate: Some(48_000),
                        audio_channels: Some(2),
                        signature_cipher: format!("cipher-{video_id}-251"),
                        ..Default::default()
                    },
                    SongStreamFormat {
                        itag: 250,
                        mime_type: "audio/webm; codecs=\"opus\"".to_owned(),
                        bitrate: 70_000,
                        approx_duration_ms: Some(181_000),
                        audio_sample_rate: Some(48_000),
                        audio_channels: Some(2),
                        signature_cipher: format!("cipher-{video_id}-250"),
                        ..Default::default()
                    },
                ]
            };

            Some(SongStreamingData {
                adaptive_formats,
                ..Default::default()
            })
        };

        let response = GetSongResponse {
            video_details: Some(Default::default()),
            playability_status: Some(Default::default()),
            streaming_data,
            microformat: Some(Default::default()),
        };

        Ok(Response::new(response))
    }

    async fn get_library_playlists(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<LibraryPlaylistsResponse>, Status> {
        Err(unimplemented("get_library_playlists"))
    }

    async fn get_library_playlists_continuation(
        &self,
        _request: Request<ContinuationRequest>,
    ) -> Result<Response<LibraryPlaylistsResponse>, Status> {
        Err(unimplemented("get_library_playlists_continuation"))
    }

    async fn get_account_info(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<AccountInfoResponse>, Status> {
        Err(unimplemented("get_account_info"))
    }

    async fn get_library_artists(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<LibraryArtistsResponse>, Status> {
        Err(unimplemented("get_library_artists"))
    }

    async fn get_library_artists_continuation(
        &self,
        _request: Request<ContinuationRequest>,
    ) -> Result<Response<LibraryArtistsResponse>, Status> {
        Err(unimplemented("get_library_artists_continuation"))
    }

    async fn get_library_albums(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<LibraryAlbumsResponse>, Status> {
        Err(unimplemented("get_library_albums"))
    }

    async fn get_library_albums_continuation(
        &self,
        _request: Request<ContinuationRequest>,
    ) -> Result<Response<LibraryAlbumsResponse>, Status> {
        Err(unimplemented("get_library_albums_continuation"))
    }

    async fn get_library_subscriptions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<LibrarySubscriptionsResponse>, Status> {
        Err(unimplemented("get_library_subscriptions"))
    }

    async fn get_library_subscriptions_continuation(
        &self,
        _request: Request<ContinuationRequest>,
    ) -> Result<Response<LibrarySubscriptionsResponse>, Status> {
        Err(unimplemented("get_library_subscriptions_continuation"))
    }

    async fn get_library_channels(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<LibraryChannelsResponse>, Status> {
        Err(unimplemented("get_library_channels"))
    }

    async fn get_library_channels_continuation(
        &self,
        _request: Request<ContinuationRequest>,
    ) -> Result<Response<LibraryChannelsResponse>, Status> {
        Err(unimplemented("get_library_channels_continuation"))
    }

    async fn get_library_podcasts(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<LibraryPodcastsResponse>, Status> {
        Err(unimplemented("get_library_podcasts"))
    }

    async fn get_library_podcasts_continuation(
        &self,
        _request: Request<ContinuationRequest>,
    ) -> Result<Response<LibraryPodcastsResponse>, Status> {
        Err(unimplemented("get_library_podcasts_continuation"))
    }

    async fn get_library_songs(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<LibrarySongsResponse>, Status> {
        Err(unimplemented("get_library_songs"))
    }

    async fn get_library_songs_continuation(
        &self,
        _request: Request<ContinuationRequest>,
    ) -> Result<Response<LibrarySongsResponse>, Status> {
        Err(unimplemented("get_library_songs_continuation"))
    }

    async fn get_liked_songs(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<LikedSongsResponse>, Status> {
        Err(unimplemented("get_liked_songs"))
    }

    async fn get_liked_songs_continuation(
        &self,
        _request: Request<ContinuationRequest>,
    ) -> Result<Response<LikedSongsResponse>, Status> {
        Err(unimplemented("get_liked_songs_continuation"))
    }

    async fn get_saved_episodes(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<SavedEpisodesResponse>, Status> {
        Err(unimplemented("get_saved_episodes"))
    }

    async fn get_saved_episodes_continuation(
        &self,
        _request: Request<ContinuationRequest>,
    ) -> Result<Response<SavedEpisodesResponse>, Status> {
        Err(unimplemented("get_saved_episodes_continuation"))
    }
}

#[tonic::async_trait]
impl YtCipher for FakeYtMusicService {
    async fn get_signature_timestamp(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<GetSignatureTimestampResponse>, Status> {
        Err(unimplemented("get_signature_timestamp"))
    }

    async fn refresh(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<RefreshCipherResponse>, Status> {
        Err(unimplemented("refresh"))
    }

    async fn decipher(
        &self,
        request: Request<DecipherRequest>,
    ) -> Result<Response<DecipherResponse>, Status> {
        self.record("Decipher");
        let decipher_delay = *self.decipher_delay.lock().unwrap();
        if let Some(delay) = decipher_delay {
            sleep(delay).await;
        }
        let signature_cipher = request.into_inner().signature_cipher;
        if !signature_cipher.starts_with("cipher-") || !signature_cipher.ends_with("-250") {
            return Err(Status::invalid_argument("unexpected cipher"));
        }
        Ok(Response::new(DecipherResponse {
            playable_url: self
                .stale_playable_url_once
                .lock()
                .unwrap()
                .take()
                .unwrap_or_else(|| self.playable_url.lock().unwrap().clone()),
        }))
    }
}
