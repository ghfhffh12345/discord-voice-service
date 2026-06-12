use discord_voice_service_test_support::fake_ytmusic::FakeYtMusic;
use discord_voice_service_test_support::fixtures::{
    spawn_hanging_server, spawn_status_server, spawn_stream_server,
    spawn_stream_server_with_initial_delay,
};

use std::time::{Duration, Instant};

use discord_voice_service_playback::YtMusicClient;
use discord_voice_service_playback::media::http_stream::HttpOpusStream;
use discord_voice_service_playback::media::position::{PlaybackPosition, shared_playback_position};
use discord_voice_service_playback::media::webm_demux::WebmOpusDemux;
use discord_voice_service_playback::recovery::{PlaybackRecovery, PlaybackResumePosition};
use discord_voice_service_playback::source::{PlaybackSource, ResolvedPlaybackSource};
use std::collections::VecDeque;

#[tokio::test]
async fn recovery_reruns_get_song_and_decipher_when_playable_url_is_stale() {
    let fake = FakeYtMusic::spawn().await;
    let http = spawn_stream_server("audio-itag250.webm").await;
    let expired = spawn_status_server("HTTP/1.1 403 Forbidden").await;
    fake.set_playable_url(http.url()).await;
    fake.set_first_playable_url_once(expired.url()).await;

    let mut recovery =
        PlaybackRecovery::new(YtMusicClient::connect(fake.endpoint()).await.unwrap());
    let start = Instant::now();
    let result = recovery.recover("video-1", resume_ms(180)).await.unwrap();

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
async fn recovery_reopens_same_video_without_rerunning_resolution() {
    let fake = FakeYtMusic::spawn().await;
    let http = spawn_stream_server("audio-itag250.webm").await;
    fake.set_playable_url(http.url()).await;

    let mut recovery =
        PlaybackRecovery::new(YtMusicClient::connect(fake.endpoint()).await.unwrap());
    recovery.recover("video-1", resume_ms(0)).await.unwrap();
    recovery.recover("video-1", resume_ms(180)).await.unwrap();

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
async fn recovery_does_not_rerun_resolution_when_open_fails_with_non_stale_http_status() {
    let fake = FakeYtMusic::spawn().await;
    let failed = spawn_status_server("HTTP/1.1 500 Internal Server Error").await;
    fake.set_playable_url(failed.url()).await;

    let mut recovery =
        PlaybackRecovery::new(YtMusicClient::connect(fake.endpoint()).await.unwrap());
    let result = recovery.recover("video-1", resume_ms(180)).await;

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
    let http = spawn_stream_server("audio-itag250.webm").await;
    fake.set_playable_url(http.url()).await;

    let mut recovery =
        PlaybackRecovery::new(YtMusicClient::connect(fake.endpoint()).await.unwrap());
    let result = recovery.recover("video-1", resume_ms(999_999)).await;

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

#[tokio::test]
async fn recovery_tolerates_a_slow_but_valid_first_media_chunk() {
    let fake = FakeYtMusic::spawn().await;
    let http =
        spawn_stream_server_with_initial_delay("audio-itag250.webm", Duration::from_millis(750))
            .await;
    fake.set_playable_url(http.url()).await;

    let mut recovery =
        PlaybackRecovery::new(YtMusicClient::connect(fake.endpoint()).await.unwrap());
    let result = recovery.recover("video-1", resume_ms(0)).await.unwrap();

    assert!(result.playable_url().starts_with("http://"));
    assert_eq!(
        fake.calls()
            .iter()
            .filter(|call| *call == "GetSong")
            .count(),
        1
    );
}

#[tokio::test]
async fn recovery_tolerates_a_ci_slow_first_media_chunk() {
    let fake = FakeYtMusic::spawn().await;
    let http =
        spawn_stream_server_with_initial_delay("audio-itag250.webm", Duration::from_millis(2500))
            .await;
    fake.set_playable_url(http.url()).await;

    let mut recovery =
        PlaybackRecovery::new(YtMusicClient::connect(fake.endpoint()).await.unwrap());
    let result = recovery.recover("video-1", resume_ms(0)).await.unwrap();

    assert!(result.playable_url().starts_with("http://"));
    assert_eq!(
        fake.calls()
            .iter()
            .filter(|call| *call == "GetSong")
            .count(),
        1
    );
}

#[tokio::test(start_paused = true)]
async fn recovery_fails_when_the_first_media_chunk_never_arrives_within_policy() {
    let fake = FakeYtMusic::spawn().await;
    let http = spawn_hanging_server().await;
    fake.set_playable_url(http.url()).await;

    let mut recovery =
        PlaybackRecovery::new(YtMusicClient::connect(fake.endpoint()).await.unwrap());
    let err = recovery
        .recover("video-1", resume_ms(0))
        .await
        .map(|_| ())
        .expect_err("open should time out");
    assert!(
        err.to_string()
            .contains("timed out opening playback source")
    );
    assert_eq!(
        fake.calls()
            .iter()
            .filter(|call| *call == "GetSong")
            .count(),
        2
    );
    assert_eq!(
        fake.calls()
            .iter()
            .filter(|call| *call == "Decipher")
            .count(),
        2
    );
}

#[tokio::test]
async fn steady_stream_read_reresolves_stale_url_without_advancing_sent_position() {
    let fake = FakeYtMusic::spawn().await;
    let http = spawn_stream_server("audio-itag250.webm").await;
    let expired = spawn_status_server("HTTP/1.1 403 Forbidden").await;
    fake.set_playable_url(http.url()).await;

    let mut position = PlaybackPosition::default();
    position.set_sent_duration_samples(960);
    let mut source = PlaybackSource::new(
        ResolvedPlaybackSource {
            selected_itag: 250,
            playable_url: expired.url(),
            approx_duration_ms: None,
        },
        HttpOpusStream::new(expired.url()),
        WebmOpusDemux::default(),
        VecDeque::new(),
        shared_playback_position(position),
    );
    let mut recovery =
        PlaybackRecovery::new(YtMusicClient::connect(fake.endpoint()).await.unwrap());

    let chunk = recovery
        .read_stream_chunk(Some("video-1"), &mut source)
        .await
        .expect("steady read should reresolve stale URL")
        .expect("replacement stream should provide media");

    assert!(!chunk.is_empty());
    assert_eq!(source.playable_url(), http.url());
    assert_eq!(source.position().sent_duration_samples(), 960);
    assert_eq!(recovery.metrics().url_reresolve_count, 1);
    assert_eq!(
        fake.calls()
            .iter()
            .filter(|call| *call == "GetSong")
            .count(),
        1
    );
}

#[tokio::test(start_paused = true)]
async fn steady_stream_read_times_out_instead_of_waiting_forever() {
    let fake = FakeYtMusic::spawn().await;
    let http = spawn_hanging_server().await;
    fake.set_playable_url(http.url()).await;

    let mut source = PlaybackSource::new(
        ResolvedPlaybackSource {
            selected_itag: 250,
            playable_url: http.url(),
            approx_duration_ms: None,
        },
        HttpOpusStream::new(http.url()),
        WebmOpusDemux::default(),
        VecDeque::new(),
        shared_playback_position(PlaybackPosition::default()),
    );
    let mut recovery =
        PlaybackRecovery::new(YtMusicClient::connect(fake.endpoint()).await.unwrap());

    let err = recovery
        .read_stream_chunk(None, &mut source)
        .await
        .map(|_| ())
        .expect_err("steady read should time out without a video id to reresolve");

    assert!(
        err.to_string()
            .contains("timed out reading playback source")
    );
    assert_eq!(recovery.metrics().http_retry_count, 1);
}

fn resume_ms(duration_ms: u64) -> PlaybackResumePosition {
    PlaybackResumePosition::from_millis(duration_ms)
}
