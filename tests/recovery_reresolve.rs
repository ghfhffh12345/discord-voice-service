#[path = "support/fake_ytmusic.rs"]
mod fake_ytmusic;
#[path = "support/fixtures.rs"]
mod fixtures;

use discord_voice_service::playback::recovery::PlaybackRecovery;
use discord_voice_service::ytmusic::client::YtMusicClient;

use self::fake_ytmusic::FakeYtMusic;
use self::fixtures::spawn_stream_server;

#[tokio::test]
async fn recovery_reruns_get_song_and_decipher_when_playable_url_is_stale() {
    let fake = FakeYtMusic::spawn().await;
    let http = spawn_stream_server("tests/fixtures/audio-itag250.webm").await;
    fake.set_playable_url(http.url()).await;
    fake.fail_first_url_once().await;

    let mut recovery =
        PlaybackRecovery::new(YtMusicClient::connect(fake.endpoint()).await.unwrap());
    let result = recovery.recover("video-1", 18_000).await.unwrap();

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
