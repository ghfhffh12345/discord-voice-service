#[path = "support/fake_ytmusic.rs"]
mod fake_ytmusic;

use bytes::Bytes;
use discord_voice_service::error::AppError;
use discord_voice_service::media::opus_queue::OpusFrameQueue;
use discord_voice_service::playback::worker::PlaybackPlan;
use discord_voice_service::playback::worker::PlaybackWorker;
use discord_voice_service::ytmusic::client::{ResolvedPlaybackSource, YtMusicClient};
use discord_voice_service::ytmusic::v1::SongStreamFormat;

use self::fake_ytmusic::FakeYtMusic;

fn stream_format(
    itag: u32,
    mime_type: &str,
    bitrate: u64,
    audio_sample_rate: Option<u32>,
    audio_channels: Option<u32>,
) -> SongStreamFormat {
    SongStreamFormat {
        itag,
        mime_type: mime_type.to_owned(),
        bitrate,
        audio_sample_rate,
        audio_channels,
        signature_cipher: format!("cipher-{itag}"),
        ..Default::default()
    }
}

#[test]
fn playback_plan_uses_selected_itag_and_decipher_path() {
    let formats = vec![stream_format(
        250,
        "audio/webm; codecs=\"opus\"",
        70_000,
        Some(48_000),
        Some(2),
    )];

    let plan = PlaybackPlan::from_formats("video123", &formats).expect("plan should be built");
    assert_eq!(plan.video_id, "video123");
    assert_eq!(plan.selected_itag, 250);
}

#[tokio::test]
async fn prepare_resolves_source_and_prefetches_one_frame() {
    let fake = FakeYtMusic::spawn().await;
    let mut worker =
        PlaybackWorker::new(YtMusicClient::connect(fake.endpoint()).await.expect("client"));
    let mut queue = OpusFrameQueue::new(2);

    let source = worker
        .prepare("video-1", &mut queue)
        .await
        .expect("source should be prepared");

    assert_eq!(
        source,
        ResolvedPlaybackSource {
            selected_itag: 250,
            playable_url: "https://cdn.example/audio.webm".to_owned(),
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
    let fake = FakeYtMusic::spawn().await;
    let mut worker =
        PlaybackWorker::new(YtMusicClient::connect(fake.endpoint()).await.expect("client"));
    let mut queue = OpusFrameQueue::new(1);

    let result = worker.prepare("missing-lower", &mut queue).await;

    assert!(matches!(result, Err(AppError::UnsupportedFormat)));
    assert_eq!(queue.len(), 0);
}

#[tokio::test]
async fn prepare_rejects_full_queue() {
    let fake = FakeYtMusic::spawn().await;
    let mut worker =
        PlaybackWorker::new(YtMusicClient::connect(fake.endpoint()).await.expect("client"));
    let mut queue = OpusFrameQueue::new(1);
    queue
        .push(Bytes::from_static(b"existing-frame"))
        .expect("queue should accept the initial frame");

    let result = worker.prepare("video-1", &mut queue).await;

    assert!(matches!(result, Err(AppError::BufferFull)));
    assert_eq!(queue.len(), 1);
    assert_eq!(queue.pop(), Some(Bytes::from_static(b"existing-frame")));
}
