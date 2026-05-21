use discord_voice_service_test_support::fake_ytmusic::FakeYtMusic;
use discord_voice_service_test_support::fixtures::spawn_stream_server;

use bytes::Bytes;
use discord_voice_service_playback::media::opus_queue::OpusFrame;
use discord_voice_service_playback::media::opus_queue::OpusFrameQueue;
use discord_voice_service_playback::{PlaybackError, PlaybackWorker, YtMusicClient};

use discord_voice_service_test_support::fixtures::spawn_status_server;

#[tokio::test]
async fn prepare_buffers_real_packets_and_returns_selected_itag() {
    let fake_yt = FakeYtMusic::spawn().await;
    let http = spawn_stream_server("audio-itag250.webm").await;
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
    let http = spawn_stream_server("audio-itag250.webm").await;
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

#[tokio::test]
async fn prepare_preserves_current_track_position_when_recovering_same_video() {
    let fake = FakeYtMusic::spawn().await;
    let http = spawn_stream_server("audio-itag250.webm").await;
    fake.set_playable_url(http.url()).await;

    let mut worker = PlaybackWorker::new(
        YtMusicClient::connect(fake.endpoint())
            .await
            .expect("client"),
    );
    let mut first_queue = OpusFrameQueue::new(32);

    let mut first_source = worker.prepare("video-1", &mut first_queue).await.unwrap();
    let first_packet = first_queue.pop().expect("queue should contain a packet");
    first_source.record_sent_packet(first_packet.duration_ms);
    assert_eq!(
        first_source.position().sent_duration_ms(),
        first_packet.duration_ms
    );

    let mut recovery_queue = OpusFrameQueue::new(32);
    let recovered = worker
        .prepare("video-1", &mut recovery_queue)
        .await
        .unwrap();

    assert_eq!(
        recovered.position().sent_duration_ms(),
        first_packet.duration_ms
    );
    assert!(!recovery_queue.is_empty());
    assert!(recovered.position().timestamp_ms() >= first_packet.duration_ms);

    let expected_next_packet = first_queue
        .pop()
        .expect("first queue should still contain the next packet");
    let recovered_first_packet = recovery_queue
        .pop()
        .expect("recovery queue should contain the next packet");

    assert_eq!(recovered_first_packet, expected_next_packet);
    assert_ne!(recovered_first_packet, first_packet);
}

#[tokio::test]
async fn reset_discards_same_video_resume_state() {
    let fake = FakeYtMusic::spawn().await;
    let http = spawn_stream_server("audio-itag250.webm").await;
    fake.set_playable_url(http.url()).await;

    let mut worker = PlaybackWorker::new(
        YtMusicClient::connect(fake.endpoint())
            .await
            .expect("client"),
    );
    let mut first_queue = OpusFrameQueue::new(32);

    let mut first_source = worker.prepare("video-1", &mut first_queue).await.unwrap();
    let first_packet = first_queue.pop().expect("queue should contain a packet");
    first_source.record_sent_packet(first_packet.duration_ms);

    worker.reset();

    let mut replay_queue = OpusFrameQueue::new(32);
    let replay_source = worker.prepare("video-1", &mut replay_queue).await.unwrap();
    let replay_first_packet = replay_queue
        .pop()
        .expect("replay queue should contain a packet");

    assert_eq!(replay_source.position().sent_duration_ms(), 0);
    assert_eq!(replay_first_packet, first_packet);
}

#[tokio::test]
async fn prepare_rejects_full_queue() {
    let fake = FakeYtMusic::spawn().await;
    let http = spawn_stream_server("audio-itag250.webm").await;
    fake.set_playable_url(http.url()).await;

    let mut worker = PlaybackWorker::new(
        YtMusicClient::connect(fake.endpoint())
            .await
            .expect("client"),
    );
    let mut queue = OpusFrameQueue::new(1);
    queue
        .push(OpusFrame::new(Bytes::from_static(b"existing-frame"), 20))
        .expect("queue should accept the initial frame");

    let error = match worker.prepare("video-1", &mut queue).await {
        Ok(_) => panic!("full queue should fail"),
        Err(error) => error,
    };

    assert!(matches!(error, PlaybackError::BufferFull));
    assert_eq!(queue.len(), 1);
    assert_eq!(
        queue.pop(),
        Some(OpusFrame::new(Bytes::from_static(b"existing-frame"), 20))
    );
}
