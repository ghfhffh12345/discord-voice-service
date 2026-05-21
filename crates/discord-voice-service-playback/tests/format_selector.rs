use discord_voice_service_test_support::fake_ytmusic::FakeYtMusic;

use discord_voice_service_playback::YtMusicClient;

#[tokio::test]
async fn resolve_playback_source_calls_get_song_and_decipher() {
    let fake = FakeYtMusic::spawn().await;
    let mut client = YtMusicClient::connect(fake.endpoint()).await.unwrap();

    let source = client.resolve_playback_source("video-1").await.unwrap();

    assert_eq!(source.selected_itag, 250);
    assert_eq!(source.playable_url, "https://cdn.example/audio.webm");
    assert_eq!(fake.calls(), vec!["GetSong", "Decipher"]);
}

#[tokio::test]
async fn resolve_playback_source_rejects_unsupported_formats() {
    let fake = FakeYtMusic::spawn().await;
    let mut client = YtMusicClient::connect(fake.endpoint()).await.unwrap();

    let error = client
        .resolve_playback_source("missing-lower")
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        discord_voice_service_playback::PlaybackError::UnsupportedFormat
    ));
    assert_eq!(fake.calls(), vec!["GetSong"]);
}
