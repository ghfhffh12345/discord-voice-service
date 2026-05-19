#[path = "support/fake_ytmusic.rs"]
mod fake_ytmusic;
#[path = "support/fixtures.rs"]
mod fixtures;

use std::time::{Duration, Instant};

use discord_voice_service::playback::recovery::PlaybackRecovery;
use discord_voice_service::ytmusic::client::YtMusicClient;

use self::fake_ytmusic::FakeYtMusic;
use self::fixtures::{spawn_status_server, spawn_stream_server};

#[tokio::test]
async fn recovery_reruns_get_song_and_decipher_when_playable_url_is_stale() {
    let fake = FakeYtMusic::spawn().await;
    let http = spawn_stream_server("tests/fixtures/audio-itag250.webm").await;
    let expired = spawn_status_server("HTTP/1.1 403 Forbidden").await;
    fake.set_playable_url(http.url()).await;
    fake.set_first_playable_url_once(expired.url()).await;

    let mut recovery =
        PlaybackRecovery::new(YtMusicClient::connect(fake.endpoint()).await.unwrap());
    let start = Instant::now();
    let result = recovery.recover("video-1", 180).await.unwrap();

    assert!(start.elapsed() < Duration::from_secs(2));
    assert!(result.playable_url().starts_with("http://"));
    assert!(
        fake.calls()
            .iter()
            .filter(|call| *call == "GetSong")
            .count()
            >= 2
    );
    assert!(
        fake.calls()
            .iter()
            .filter(|call| *call == "Decipher")
            .count()
            >= 2
    );
}

#[tokio::test]
async fn recovery_does_not_rerun_resolution_when_open_fails_with_non_stale_http_status() {
    let fake = FakeYtMusic::spawn().await;
    let failed = spawn_status_server("HTTP/1.1 500 Internal Server Error").await;
    fake.set_playable_url(failed.url()).await;

    let mut recovery =
        PlaybackRecovery::new(YtMusicClient::connect(fake.endpoint()).await.unwrap());
    let result = recovery.recover("video-1", 180).await;

    assert!(result.is_err());
    assert_eq!(
        fake.calls()
            .iter()
            .filter(|call| *call == "GetSong")
            .count(),
        1
    );
    assert_eq!(
        fake.calls()
            .iter()
            .filter(|call| *call == "Decipher")
            .count(),
        1
    );
}

#[tokio::test]
async fn recovery_errors_when_requested_position_cannot_be_reached() {
    let fake = FakeYtMusic::spawn().await;
    let http = spawn_stream_server("tests/fixtures/audio-itag250.webm").await;
    fake.set_playable_url(http.url()).await;

    let mut recovery =
        PlaybackRecovery::new(YtMusicClient::connect(fake.endpoint()).await.unwrap());
    let result = recovery.recover("video-1", 999_999).await;

    let err = match result {
        Ok(_) => panic!("recovery should fail when resume point is unreachable"),
        Err(err) => err,
    };
    assert!(err.to_string().contains("requested resume position"));
    assert_eq!(
        fake.calls()
            .iter()
            .filter(|call| *call == "GetSong")
            .count(),
        1
    );
    assert_eq!(
        fake.calls()
            .iter()
            .filter(|call| *call == "Decipher")
            .count(),
        1
    );
}
