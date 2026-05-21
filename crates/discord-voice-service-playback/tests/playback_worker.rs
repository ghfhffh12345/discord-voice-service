#[path = "support/fake_ytmusic.rs"]
mod fake_ytmusic;
#[path = "support/fixtures.rs"]
mod fixtures;

use discord_voice_service_playback::media::opus_queue::OpusFrameQueue;
use discord_voice_service_playback::{PlaybackError, PlaybackWorker, YtMusicClient};

use self::fake_ytmusic::FakeYtMusic;
use self::fixtures::{spawn_status_server, spawn_stream_server};

#[tokio::test]
async fn prepare_buffers_real_packets_and_returns_selected_itag() {
    let fake_yt = FakeYtMusic::spawn().await;
    let http = spawn_stream_server("tests/fixtures/audio-itag250.webm").await;
    fake_yt.set_playable_url(http.url()).await;

    let mut worker = PlaybackWorker::new(
        YtMusicClient::connect(fake_yt.endpoint())
            .await
            .expect("client"),
    );
    let mut queue = OpusFrameQueue::new(32);

    let mut source = worker
        .prepare("video-1", &mut queue)
        .await
        .expect("source should be prepared");

    assert_eq!(source.selected_itag(), 250);
    assert!(source.position().byte_offset() > 0);
    assert!(source.position().timestamp_ms() > 0);
    assert_eq!(source.position().sent_duration_ms(), 0);
    assert!(!queue.is_empty());

    let first_packet = queue.pop().expect("queue should contain a packet");
    source.record_sent_packet(first_packet.duration_ms);
    assert_eq!(
        source.position().sent_duration_ms(),
        first_packet.duration_ms
    );
}

#[tokio::test]
async fn prepare_reruns_resolution_when_initial_playable_url_is_stale() {
    let fake = FakeYtMusic::spawn().await;
    let http = spawn_stream_server("tests/fixtures/audio-itag250.webm").await;
    let expired = spawn_status_server("HTTP/1.1 403 Forbidden").await;
    fake.set_playable_url(http.url()).await;
    fake.set_first_playable_url_once(expired.url()).await;

    let mut worker = PlaybackWorker::new(
        YtMusicClient::connect(fake.endpoint())
            .await
            .expect("client"),
    );
    let mut queue = OpusFrameQueue::new(32);

    let source = worker.prepare("video-1", &mut queue).await.unwrap();

    assert_eq!(source.selected_itag(), 250);
    assert!(!queue.is_empty());
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
async fn prepare_rejects_unsupported_formats() {
    let fake = FakeYtMusic::spawn().await;
    let mut worker = PlaybackWorker::new(
        YtMusicClient::connect(fake.endpoint())
            .await
            .expect("client"),
    );
    let mut queue = OpusFrameQueue::new(1);

    let error = match worker.prepare("missing-lower", &mut queue).await {
        Ok(_) => panic!("unsupported format should fail"),
        Err(error) => error,
    };

    assert!(matches!(error, PlaybackError::UnsupportedFormat));
    assert_eq!(queue.len(), 0);
}
