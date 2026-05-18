use bytes::Bytes;
use discord_voice_service::error::AppError;
use discord_voice_service::media::opus_queue::OpusFrameQueue;
use discord_voice_service::playback::worker::PlaybackPlan;
use discord_voice_service::playback::worker::PlaybackWorker;
use discord_voice_service::ytmusic::client::{ResolvedPlaybackSource, YtMusicClient};
use discord_voice_service::ytmusic::selector::StreamFormat;

#[test]
fn playback_plan_uses_selected_itag_and_decipher_path() {
    let formats = vec![StreamFormat::new(
        250,
        "audio/webm; codecs=\"opus\"",
        70_000,
        Some(48_000),
        Some(2),
        false,
    )];

    let plan = PlaybackPlan::from_formats("video123", &formats).expect("plan should be built");
    assert_eq!(plan.video_id, "video123");
    assert_eq!(plan.selected_itag, 250);
}

#[tokio::test]
async fn prepare_resolves_source_and_prefetches_one_frame() {
    let worker = PlaybackWorker::new(YtMusicClient::new("https://ytmusic.example".to_owned()));
    let formats = vec![
        StreamFormat::new(
            251,
            "audio/webm; codecs=\"opus\"",
            160_000,
            Some(48_000),
            Some(2),
            false,
        ),
        StreamFormat::new(
            250,
            "audio/webm; codecs=\"opus\"",
            70_000,
            Some(48_000),
            Some(2),
            false,
        ),
    ];
    let mut queue = OpusFrameQueue::new(2);

    let source = worker
        .prepare("video123", &formats, &mut queue)
        .await
        .expect("source should be prepared");

    assert_eq!(
        source,
        ResolvedPlaybackSource {
            selected_itag: 250,
            playable_url: "https://ytmusic.example/deciphered/video123".to_owned(),
        }
    );
    assert_eq!(queue.len(), 1);
    assert_eq!(
        queue.pop(),
        Some(Bytes::from_static(b"prefetched-opus-frame"))
    );
    assert_eq!(queue.pop(), None);
}

#[tokio::test]
async fn prepare_rejects_unsupported_formats() {
    let worker = PlaybackWorker::new(YtMusicClient::new("https://ytmusic.example".to_owned()));
    let formats = vec![StreamFormat::new(
        140,
        "audio/mp4; codecs=\"mp4a.40.2\"",
        128_000,
        Some(44_100),
        Some(2),
        false,
    )];
    let mut queue = OpusFrameQueue::new(1);

    let result = worker.prepare("video123", &formats, &mut queue).await;

    assert!(matches!(result, Err(AppError::UnsupportedFormat)));
    assert_eq!(queue.len(), 0);
}

#[tokio::test]
async fn prepare_rejects_full_queue() {
    let worker = PlaybackWorker::new(YtMusicClient::new("https://ytmusic.example".to_owned()));
    let formats = vec![StreamFormat::new(
        250,
        "audio/webm; codecs=\"opus\"",
        70_000,
        Some(48_000),
        Some(2),
        false,
    )];
    let mut queue = OpusFrameQueue::new(1);
    queue
        .push(Bytes::from_static(b"existing-frame"))
        .expect("queue should accept the initial frame");

    let result = worker.prepare("video123", &formats, &mut queue).await;

    assert!(matches!(result, Err(AppError::BufferFull)));
    assert_eq!(queue.len(), 1);
    assert_eq!(queue.pop(), Some(Bytes::from_static(b"existing-frame")));
}
