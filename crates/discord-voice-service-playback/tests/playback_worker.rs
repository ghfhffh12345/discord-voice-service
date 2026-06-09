use std::collections::VecDeque;

use discord_voice_service_test_support::fake_ytmusic::FakeYtMusic;
use discord_voice_service_test_support::fixtures::{spawn_hanging_server, spawn_stream_server};

use bytes::Bytes;
use discord_voice_service_playback::media::http_stream::HttpOpusStream;
use discord_voice_service_playback::media::opus_queue::OpusFrame;
use discord_voice_service_playback::media::opus_queue::OpusFrameQueue;
use discord_voice_service_playback::media::position::{PlaybackPosition, shared_playback_position};
use discord_voice_service_playback::media::webm_demux::{DemuxedPacket, WebmOpusDemux};
use discord_voice_service_playback::{
    PlaybackError, PlaybackWorker, ResolvedPlaybackSource, YtMusicClient,
};

use discord_voice_service_playback::source::PlaybackSource;
use discord_voice_service_test_support::fixtures::spawn_status_server;
use tokio::time::{Duration, timeout};

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
    source.record_sent_packet_samples(first_packet.duration_samples);
    assert_eq!(
        source.position().sent_duration_ms(),
        first_packet.duration_ms
    );
}

#[tokio::test]
async fn fill_queue_to_duration_ms_releases_smaller_producer_batches() {
    let fake_yt = FakeYtMusic::spawn().await;
    let http = spawn_hanging_server().await;
    let mut worker = PlaybackWorker::new(
        YtMusicClient::connect(fake_yt.endpoint())
            .await
            .expect("client"),
    );
    let pending_packets = (0..100)
        .map(|index| DemuxedPacket {
            data: Bytes::from(format!("frame-{index}")),
            timestamp_ms: index * 20,
            timestamp_samples: index * 960,
            duration_ms: 20,
            duration_samples: 960,
        })
        .collect::<VecDeque<_>>();
    let mut source = PlaybackSource::new(
        ResolvedPlaybackSource {
            selected_itag: 250,
            playable_url: http.url(),
            approx_duration_ms: None,
        },
        HttpOpusStream::new(http.url()),
        WebmOpusDemux::default(),
        pending_packets,
        shared_playback_position(PlaybackPosition::default()),
    );
    let mut queue = OpusFrameQueue::new(100);

    worker
        .fill_queue_to_duration_ms(&mut source, &mut queue, 1_000)
        .await
        .expect("pending packets should fill without HTTP");

    assert_eq!(queue.buffered_duration_ms(), 1_000);
    assert_eq!(queue.len(), 50);
    assert_eq!(source.pending_packets_mut().len(), 50);

    let first_frame = queue.pop().expect("queue should preserve first frame");
    let second_frame = queue.pop().expect("queue should preserve second frame");
    assert_eq!(first_frame.source_position_ms, 0);
    assert_eq!(second_frame.source_position_ms, 20);
}

#[tokio::test]
async fn fill_queue_to_duration_ms_preserves_fractional_source_sample_positions() {
    let fake_yt = FakeYtMusic::spawn().await;
    let http = spawn_hanging_server().await;
    let mut worker = PlaybackWorker::new(
        YtMusicClient::connect(fake_yt.endpoint())
            .await
            .expect("client"),
    );
    let pending_packets = VecDeque::from([
        DemuxedPacket {
            data: Bytes::from_static(b"frame-a"),
            timestamp_ms: 0,
            timestamp_samples: 0,
            duration_ms: 2,
            duration_samples: 120,
        },
        DemuxedPacket {
            data: Bytes::from_static(b"frame-b"),
            timestamp_ms: 2,
            timestamp_samples: 120,
            duration_ms: 2,
            duration_samples: 120,
        },
    ]);
    let mut source = PlaybackSource::new(
        ResolvedPlaybackSource {
            selected_itag: 250,
            playable_url: http.url(),
            approx_duration_ms: None,
        },
        HttpOpusStream::new(http.url()),
        WebmOpusDemux::default(),
        pending_packets,
        shared_playback_position(PlaybackPosition::default()),
    );
    let mut queue = OpusFrameQueue::new(10);

    worker
        .fill_queue_to_duration_ms(&mut source, &mut queue, 5)
        .await
        .expect("fractional pending packets should fill without HTTP");

    let depth = queue.depth();
    assert_eq!(depth.packets, 2);
    assert_eq!(depth.duration_samples, 240);
    assert_eq!(depth.duration_ms, 5);
    assert_eq!(source.pending_packets_mut().len(), 0);

    let first_frame = queue.pop().expect("queue should preserve first frame");
    let second_frame = queue.pop().expect("queue should preserve second frame");
    assert_eq!(first_frame.source_position_samples, 0);
    assert_eq!(second_frame.source_position_samples, 120);
    assert_eq!(first_frame.duration_samples, 120);
    assert_eq!(second_frame.duration_samples, 120);
}

#[tokio::test]
async fn fill_queue_to_duration_ms_stops_at_fractional_sample_cap_without_buffer_full() {
    const TARGET_BUFFER_MS: u64 = 5_000;
    const FRAME_SAMPLES: u32 = 360;
    const ACCEPTED_FRAMES: usize = 666;
    const TOTAL_FRAMES: usize = ACCEPTED_FRAMES + 1;

    let fake_yt = FakeYtMusic::spawn().await;
    let http = spawn_hanging_server().await;
    let mut worker = PlaybackWorker::new(
        YtMusicClient::connect(fake_yt.endpoint())
            .await
            .expect("client"),
    );
    let pending_packets = (0..TOTAL_FRAMES)
        .map(|index| DemuxedPacket {
            data: Bytes::from(format!("frame-{index}")),
            timestamp_ms: (index as u64 * 7_500) / 1_000,
            timestamp_samples: index as u64 * u64::from(FRAME_SAMPLES),
            duration_ms: 7,
            duration_samples: FRAME_SAMPLES,
        })
        .collect::<VecDeque<_>>();
    let mut source = PlaybackSource::new(
        ResolvedPlaybackSource {
            selected_itag: 250,
            playable_url: http.url(),
            approx_duration_ms: None,
        },
        HttpOpusStream::new(http.url()),
        WebmOpusDemux::default(),
        pending_packets,
        shared_playback_position(PlaybackPosition::default()),
    );
    let mut queue =
        OpusFrameQueue::with_resource_limits(TOTAL_FRAMES, 4 * 1024 * 1024, TARGET_BUFFER_MS);

    worker
        .fill_queue_to_duration_ms(&mut source, &mut queue, TARGET_BUFFER_MS)
        .await
        .expect("fractional source fill should stop before overflowing the sample cap");

    let depth = queue.depth();
    assert_eq!(depth.packets, ACCEPTED_FRAMES);
    assert_eq!(
        depth.duration_samples,
        ACCEPTED_FRAMES as u64 * u64::from(FRAME_SAMPLES)
    );
    assert_eq!(depth.duration_ms, 4_995);
    assert_eq!(source.pending_packets_mut().len(), 1);
    assert_eq!(
        source
            .pending_packets_mut()
            .front()
            .unwrap()
            .timestamp_samples,
        ACCEPTED_FRAMES as u64 * u64::from(FRAME_SAMPLES)
    );
}

#[tokio::test]
async fn fill_queue_to_duration_ms_can_fill_five_second_source_buffer() {
    const TARGET_BUFFER_MS: u64 = 5_000;
    const FRAME_DURATION_MS: u64 = 20;
    const FRAME_SAMPLES: u32 = 960;
    const TOTAL_PENDING_FRAMES: usize = 300;
    const TARGET_FRAME_COUNT: usize = (TARGET_BUFFER_MS / FRAME_DURATION_MS) as usize;

    let fake_yt = FakeYtMusic::spawn().await;
    let http = spawn_hanging_server().await;
    let mut worker = PlaybackWorker::new(
        YtMusicClient::connect(fake_yt.endpoint())
            .await
            .expect("client"),
    );
    let pending_packets = (0..TOTAL_PENDING_FRAMES)
        .map(|index| DemuxedPacket {
            data: Bytes::from(format!("frame-{index}")),
            timestamp_ms: index as u64 * FRAME_DURATION_MS,
            timestamp_samples: index as u64 * u64::from(FRAME_SAMPLES),
            duration_ms: FRAME_DURATION_MS,
            duration_samples: FRAME_SAMPLES,
        })
        .collect::<VecDeque<_>>();
    let mut source = PlaybackSource::new(
        ResolvedPlaybackSource {
            selected_itag: 250,
            playable_url: http.url(),
            approx_duration_ms: None,
        },
        HttpOpusStream::new(http.url()),
        WebmOpusDemux::default(),
        pending_packets,
        shared_playback_position(PlaybackPosition::default()),
    );
    let mut queue =
        OpusFrameQueue::with_resource_limits(TARGET_FRAME_COUNT, 4 * 1024 * 1024, TARGET_BUFFER_MS);

    worker
        .fill_queue_to_duration_ms(&mut source, &mut queue, TARGET_BUFFER_MS)
        .await
        .expect("pending packets should fill a five-second source buffer without HTTP");

    let depth = queue.depth();
    assert_eq!(depth.duration_ms, TARGET_BUFFER_MS);
    assert_eq!(
        depth.duration_samples,
        u64::from(FRAME_SAMPLES) * TARGET_FRAME_COUNT as u64
    );
    assert_eq!(depth.packets, TARGET_FRAME_COUNT);
    assert_eq!(
        source.pending_packets_mut().len(),
        TOTAL_PENDING_FRAMES - TARGET_FRAME_COUNT
    );

    let first_frame = queue
        .pop()
        .expect("source buffer should preserve the first producer frame");
    let second_frame = queue
        .pop()
        .expect("source buffer should preserve the second producer frame");
    assert_eq!(first_frame.source_position_ms, 0);
    assert_eq!(second_frame.source_position_ms, FRAME_DURATION_MS);
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
    first_source.record_sent_packet_samples(first_packet.duration_samples);
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
    first_source.record_sent_packet_samples(first_packet.duration_samples);

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

#[tokio::test]
async fn fill_queue_waits_for_configured_prebuffer_target() {
    let fake = FakeYtMusic::spawn().await;
    let http = spawn_hanging_server().await;
    let mut worker = PlaybackWorker::new(
        YtMusicClient::connect(fake.endpoint())
            .await
            .expect("client"),
    );
    let mut queue = OpusFrameQueue::new(4);
    for index in 0..3 {
        queue
            .push(OpusFrame::new(
                Bytes::from(format!("buffered-frame-{index}")),
                20,
            ))
            .unwrap();
    }
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

    let refill = timeout(
        Duration::from_millis(50),
        worker.fill_queue(&mut source, &mut queue),
    )
    .await;

    assert!(
        refill.is_err(),
        "fill_queue should still be waiting on HTTP while below the worker prebuffer target"
    );
    assert_eq!(
        queue.len(),
        3,
        "producer-side prebuffering should leave existing queued frames in place while it waits"
    );
}
